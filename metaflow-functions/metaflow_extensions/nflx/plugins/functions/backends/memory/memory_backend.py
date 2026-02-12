import os
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import signal
import traceback
import asyncio

from metaflow_extensions.nflx.plugins.functions.debug import debug
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
    MetaflowFunctionRuntimeException,
    MetaflowFunctionUserException,
    MetaflowFunctionMemorySignalException,
)

from metaflow_extensions.nflx.plugins.functions.memory.memory import (
    UUID_BYTES,
    ReadBuffer,
    WriteBuffer,
)
from metaflow_extensions.nflx.plugins.functions.common.runtime_utils import (
    create_function_parameters,
    run_with_exception_handling,
)
from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
    get_global_registry,
    aggregate_results,
)
from ..abstract_backend import AbstractBackend
from ..backend_type import BackendType
from metaflow_extensions.nflx.plugins.functions.core.function_payload import (
    FunctionPayload,
    parse_function_payload,
)

from ...memory.concurrency import Semaphore

IO_WAIT = 10**-6
BUFFER_PER_PROCESS = 2
MFF_ERROR_KEY = "__mf_ERROR__"
KEYWORDS = {"process", "params"}

# Sentinel object to distinguish "no result ready" from "result is falsy"
# (e.g., None, 0, False, [], {}, "", empty dataframes)
_NO_RESULT = object()


@dataclass
class ApplyState:
    kwargs: Optional[Dict[str, Any]] = None
    error: str = ""
    can_yield: bool = False
    done: bool = False
    uuids: deque = field(default_factory=deque)


class MemoryBackend(AbstractBackend):
    @property
    def backend_type(self) -> BackendType:
        """Return the type of this backend"""
        return BackendType.MEMORY

    @staticmethod
    def _update_kwargs_from_subprocess(
        kwargs: Optional[Dict[str, Any]], result_kwargs: Dict[str, Any]
    ) -> None:
        """
        Helper function to update original kwargs with modified values from subprocess.

        Parameters
        ----------
        kwargs : Optional[Dict[str, Any]]
            Original kwargs dict to update (modified in-place)
        result_kwargs : Dict[str, Any]
            Modified kwargs returned from subprocess
        """
        if kwargs is not None:
            for key, new_value in result_kwargs.items():
                if key in kwargs and type(kwargs[key]) == type(new_value):
                    # Update objects of the same type in-place to preserve references
                    original = kwargs[key]
                    try:
                        # Clear and update dict-like objects
                        if hasattr(original, "clear") and hasattr(original, "update"):
                            original.clear()
                            original.update(new_value)

                        # Copy all private attributes (starting with _) from new_value to original
                        for attr_name in dir(new_value):
                            if attr_name.startswith("_") and not attr_name.startswith(
                                "__"
                            ):
                                if hasattr(original, attr_name):
                                    setattr(
                                        original,
                                        attr_name,
                                        getattr(new_value, attr_name),
                                    )
                    except:
                        # If in-place update fails, fall back to replacement
                        kwargs[key] = new_value
                else:
                    kwargs[key] = new_value

    @classmethod
    def _setup_apply(cls, func_instance, data, kwargs):
        """
        Setup to run apply
        """
        from metaflow_extensions.nflx.plugins.functions.backends.memory.supervisor.supervisor import (
            function_supervisor,
        )

        # Get the process count if it is set
        process = kwargs.get("process", 1)

        # Get an instance of a function runtime. This may cause a
        # rebuild of the function locally.
        lease = None
        lease = function_supervisor.lease(func_instance, process=process)

        # Raise an exception if there was a process error
        lease.runtime.raise_on_process_exit(clean_dir=False)

        # Get type classes for serialization/deserialization
        output_types = func_instance.output_types
        if isinstance(data, (memoryview, bytearray, bytes)):
            output_type = type(data)
        else:
            output_type = cls._map_type_info_to_python_type(
                output_types, func_instance.spec
            )

        # Filter out backend specific keywords from kwargs since the
        # runtime will provide its own FunctionParameters and these
        # don't need serialization
        filtered_kwargs = {k: v for k, v in kwargs.items() if k not in KEYWORDS}

        return lease, output_type, filtered_kwargs

    @classmethod
    def _handle_error(cls, lease, queue, state):
        """
        Handle errors by clearing the queue and draining buffers
        """
        # Check if an exception was thrown in the function
        if state.kwargs is not None and MFF_ERROR_KEY in state.kwargs:
            # Cleanup and mark as done
            error = state.kwargs[MFF_ERROR_KEY]

            # If we have an error just drain the data queue.
            # We need to free though all the maps.
            queue.clear()
            debug.functions_exec(f"Draining buffers on error. {len(state.uuids)}")
            while state.uuids:
                uuid = state.uuids.pop()
                with lease.runtime.io["output_map"].acquire(uuid=uuid) as out_mem:
                    if out_mem.valid:
                        lease.runtime.io["output_map"].free(out_mem)
                    else:
                        state.uuids.append(uuid)
            return error
        return None

    @classmethod
    def _process_exceptions(cls, lease, e):
        """
        Handle different types of exceptions
        """
        # Just re-raise user errors
        if isinstance(e, MetaflowFunctionUserException):
            raise e

        # Re-raise runtime errors, close down runtime but leave
        # the directory intact for a potential fast restart.
        if isinstance(e, MetaflowFunctionRuntimeException):
            lease.runtime.close(clean_dir=False)
            raise e

        # For other errors re-raise them as RuntimeErrors and close
        # down runtime but leave the directory intact for a potential
        # fast restart.
        if isinstance(e, Exception):
            lease.runtime.close(clean_dir=False)
            raise MetaflowFunctionRuntimeException(
                f"Runtime exception:\n{traceback.format_exc()}"
            )

    @classmethod
    def _next(cls, lease, registry, queue, kwargs, output_type, state):
        """
        The runloop performs the following operations:
        1) write data into memory if buffer and data available
        2) reads from memory when buffer available
        3) processes IO only if available
        4) check if the function process is alive
        """
        result = _NO_RESULT
        state.can_yield = True

        # Put a single item from the data queue
        if len(queue) > 0:
            uuid = os.urandom(UUID_BYTES)
            with lease.runtime.io["input_map"].acquire(uuid) as inp_mem:
                if inp_mem.valid:
                    head = queue.popleft()
                    head = FunctionPayload(head, kwargs)
                    data_len, unwritten = registry.serialize(head, inp_mem.buf)
                    debug.functions_exec(f"Wrote {data_len} to input buffer")
                    inp_mem.truncate(data_len)
                    lease.runtime.io["input_map"].commit(inp_mem)
                    queue.extendleft(unwritten)
                    state.uuids.append(uuid)
                    state.can_yield = False
                    lease.runtime.io["data_watcher"].post()

        # Get a result from processing
        if len(state.uuids):
            uuid = state.uuids[0]
            with lease.runtime.io["output_map"].acquire(uuid) as out_mem:
                if out_mem.valid:
                    # Deserialize FunctionPayload to get both
                    # result and modified
                    # kwargs. Deserializers are responsible
                    # for tracking their own memory (e.g. in
                    # the returned object) or making copies as
                    # needed.
                    debug.functions_exec(f"Got output buffer of length {out_mem.size}")
                    result_payload = parse_function_payload(
                        out_mem.buf, output_type, return_raw_data=False
                    )

                    result = result_payload.data  # Reconstructed object
                    state.kwargs = result_payload.kwargs  # Keep track of latest kwargs

                    # Free up this slot of memory
                    lease.runtime.io["output_map"].free(out_mem)
                    state.uuids.popleft()
                    state.can_yield = False

        # Check if error:
        state.error = cls._handle_error(lease, queue, state)

        # We are all done if the data and result queue are zero'd
        state.done = len(queue) == 0 and len(state.uuids) == 0
        if state.done:
            state.can_yield = False

        # Raise an exception if there was a process error
        lease.runtime.raise_on_process_exit(clean_dir=False)

        # Process any IO
        lease.runtime.process_io(IO_WAIT)

        return result, state

    @classmethod
    async def apply_async(
        cls,
        func_instance,
        data: Any,
        **kwargs,
    ) -> Any:
        from metaflow_extensions.nflx.plugins.functions.backends.memory.supervisor.supervisor import (
            function_supervisor,
        )

        lease, output_type, filtered_kwargs = cls._setup_apply(
            func_instance, data, kwargs
        )

        # Get the global registry
        registry = get_global_registry()

        # Writers may produce unwritten parts that are tracked in a queue
        queue: deque[Any] = deque()
        queue.append(data)
        results: List[Any] = []

        # Ensure serializers are available (registry will handle this automatically)
        cls._ensure_serializer_for_data_type(type(queue[0]))

        # Keep track of loop state
        state = ApplyState()

        try:
            while not state.done:
                # Execute the run-loop once
                result, state = cls._next(
                    lease, registry, queue, filtered_kwargs, output_type, state
                )
                if state.can_yield:
                    await asyncio.sleep(0)

                if result is not _NO_RESULT:
                    results.append(result)

            # Process user error
            if state.error:
                raise MetaflowFunctionUserException(
                    f"Exception in function '{lease.runtime.uuid}': {state.error}"
                )

            # Update original kwargs with modified values from subprocess
            if state.kwargs:
                cls._update_kwargs_from_subprocess(filtered_kwargs, state.kwargs)

            # Aggregate results using OUTPUT type
            return aggregate_results(results, output_type)

        except Exception as e:
            cls._process_exceptions(lease, e)

        finally:
            function_supervisor.free(lease)

    @classmethod
    def apply(
        cls,
        func_instance,
        data: Any,
        **kwargs,
    ) -> Any:
        from metaflow_extensions.nflx.plugins.functions.backends.memory.supervisor.supervisor import (
            function_supervisor,
        )

        lease, output_type, filtered_kwargs = cls._setup_apply(
            func_instance, data, kwargs
        )

        # Get serializer registry
        registry = get_global_registry()

        # Track writing with a queue in case are writer is chunkable
        queue: deque[Any] = deque()
        queue.append(data)
        results: List[Any] = []

        # Ensure serializers are available (registry will handle this automatically)
        cls._ensure_serializer_for_data_type(type(queue[0]))

        # Keep track of loop state
        state = ApplyState()
        state.uuids = deque()

        try:
            while not state.done:
                # Execute the run-loop once
                result, state = cls._next(
                    lease, registry, queue, filtered_kwargs, output_type, state
                )

                if result is not _NO_RESULT:
                    results.append(result)

            # Process user error
            if state.error:
                raise MetaflowFunctionUserException(
                    f"Exception in function '{lease.runtime.uuid}': {state.error}"
                )

            # Update original kwargs with modified values from subprocess
            if state.kwargs:
                cls._update_kwargs_from_subprocess(filtered_kwargs, state.kwargs)

            # Aggregate results using OUTPUT type
            return aggregate_results(results, output_type)

        except Exception as e:
            cls._process_exceptions(lease, e)

        finally:
            function_supervisor.free(lease)

    @classmethod
    def _ensure_serializer_for_data_type(cls, data_type: type) -> None:
        """Ensure a serializer is available for the given data type."""
        from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
            is_type_registered,
        )

        # Note: Core serializers are now registered automatically in create_function_type()

        # Check if type is already registered
        if is_type_registered(data_type):
            return

        # Raise exception if no serializer is registered for this type
        from metaflow_extensions.nflx.plugins.functions.serializers.config import (
            get_canonical_type_string,
        )
        from metaflow_extensions.nflx.plugins.functions.exceptions import (
            MetaflowFunctionException,
        )

        canonical_type = get_canonical_type_string(data_type)
        raise MetaflowFunctionException(
            f"No serializer registered for type: {canonical_type}"
        )

    @classmethod
    def start(
        cls,
        func_instance,
        **kwargs,
    ):
        from metaflow_extensions.nflx.plugins.functions.backends.memory.supervisor.supervisor import (
            function_supervisor,
        )

        # Get an instance of a function runtime. This may cause a
        # rebuild of the function locally.
        lease = None
        lease = function_supervisor.lease(func_instance, **kwargs)

        # If there is an error try to restart it.
        if lease.runtime.failed:
            # Throw if we can't restart
            if not lease.runtime.start():
                lease.runtime.raise_on_process_exit(clean_dir=True)

        function_supervisor.free(lease)

    @classmethod
    def close(
        cls,
        func_instance,
        clean_dir: bool = True,
        **kwargs,
    ):
        from metaflow_extensions.nflx.plugins.functions.backends.memory.supervisor.supervisor import (
            function_supervisor,
        )

        if function_supervisor.is_loaded(func_instance):
            function_supervisor.clear(func_instance, clean_dir)

    @classmethod
    def get_runtime_command(
        cls, connection_params: Dict[str, Any], reference: str, interpreter: str
    ) -> List[str]:
        input_map_name = connection_params.get("input_map_name")
        output_map_name = connection_params.get("output_map_name")
        data_watcher_name = connection_params.get("data_watcher_name")
        prefetch_artifacts = connection_params.get("prefetch_artifacts", False)

        if not input_map_name or not output_map_name or not data_watcher_name:
            raise ValueError(
                f"Memory backend requires 'input_map_name' and 'output_map_name' "
                f"and 'data_watcher_name' in connection_params. Got: {connection_params}"
            )

        # Download S3 reference to local temp file
        from metaflow_extensions.nflx.plugins.functions.core.function_spec import (
            FunctionSpec,
        )

        local_reference = FunctionSpec.download_to_temp(reference)

        cmd = [
            interpreter,
            "-m",
            "metaflow_extensions.nflx.plugins.functions.backends.memory.memory_cli",
            "--input-map",
            input_map_name,
            "--output-map",
            output_map_name,
            "--data-watcher",
            data_watcher_name,
            "--reference",
            local_reference,
        ]

        # Add prefetch-artifacts flag if enabled
        if prefetch_artifacts:
            cmd.append("--prefetch-artifacts")

        return cmd

    @classmethod
    def open_connection(cls, connection_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate memory-specific IO

        Parameters
        ----------
        connection_params: Dict[str, Any]
            Connection parameters generated from 'generate_connection_params'
        Returns
        -------
        Dict[str, Any]
            Memory-specific connections
        """
        num_rings = connection_params["num_rings"]

        # Try to open a writer, it may throw an exception.
        wb = WriteBuffer(
            connection_params["input_map_name"], owns=True, num_rings=num_rings
        )

        # Try to open a reader, if it fails, make sure to clean up the writer.
        try:
            rb = ReadBuffer(
                connection_params["output_map_name"], owns=True, num_rings=num_rings
            )

        except Exception as e:
            wb.close()
            raise e

        # Try to open data watcher semaphore
        try:
            sem = Semaphore(
                connection_params["data_watcher_name"],
                initial_value=0,  # no data is ready so we initialize to zero
            )

        except Exception as e:
            rb.close()
            wb.close()
            raise e

        return dict(input_map=wb, output_map=rb, data_watcher=sem)

    @classmethod
    def close_connection(cls, memory_io: Dict[str, Any]):
        """
        Close memory specific IO

        Parameters
        ----------
        memory_io: Dict[str, Any]
            Memory back-end specific IO object
        """
        for item in memory_io.values():
            item.close()

    @classmethod
    def generate_connection_params(cls, uuid: str, **kwargs: Any) -> Dict[str, Any]:
        """
        Generate memory-specific connection parameters.

        Parameters
        ----------
        uuid : str
            Function UUID
        **kwargs : Any
            Additional parameters (e.g., prefetch_artifacts, process)

        Returns
        -------
        Dict[str, Any]
            Memory-specific connection parameters
        """
        params = {
            "input_map_name": f"{uuid}-inp-map",
            "output_map_name": f"{uuid}-out-map",
            "data_watcher_name": f"{uuid}-watcher",
        }

        # Pass through any additional parameters
        if "prefetch_artifacts" in kwargs:
            params["prefetch_artifacts"] = kwargs["prefetch_artifacts"]

        if "process" in kwargs:
            params["num_rings"] = kwargs["process"] * BUFFER_PER_PROCESS
            params["process"] = kwargs["process"]

        return params

    @classmethod
    def runtime(cls, connection_params: Dict[str, Any], reference: str):
        """
        Parameters
        ----------
        connection_params : Dict[str, Any]
            Connection parameters containing input_map_name and output_map_name
        reference : str
            Reference to a function
        """
        # Lazy import to avoid any circular dependencies
        from metaflow_extensions.nflx.plugins.functions.core.function import (
            function_from_json,
        )

        input_map_name = connection_params.get("input_map_name")
        output_map_name = connection_params.get("output_map_name")
        data_watcher_name = connection_params.get("data_watcher_name")
        prefetch_artifacts = connection_params.get("prefetch_artifacts", False)

        if not input_map_name or not output_map_name:
            raise MetaflowFunctionRuntimeException(
                f"Memory backend requires 'input_map_name' and 'output_map_name' "
                f"in connection_params. Got: {connection_params}"
            )

        # Load the function from the JSON specification
        debug.functions_exec(f"Loading function: {reference}")
        debug.functions_exec(
            "Subprocess: About to call function_from_json with use_proxy=False"
        )
        # Don't start runtime - function is already running in this subprocess
        func_instance = function_from_json(
            reference, use_proxy=False, start_runtime=False
        )
        debug.functions_exec(
            f"Subprocess: Loaded function instance: {type(func_instance)}"
        )

        debug.functions_exec("Creating runtime IO buffers")
        try:
            inp = ReadBuffer(input_map_name, owns=False)
        except Exception as e:
            raise MetaflowFunctionRuntimeException(
                f"Failed to create read shared memory buffers for runtime: {e}"
            )

        try:
            out = WriteBuffer(output_map_name, owns=False)
        except Exception as e:
            inp.close()
            raise MetaflowFunctionRuntimeException(
                f"Failed to create write shared memory buffers for runtime: {e}"
            )

        try:
            sem = Semaphore(connection_params["data_watcher_name"])

        except Exception as e:
            inp.close()
            out.close()
            raise MetaflowFunctionRuntimeException(
                f"Failed to create data watcher shared memory for runtime: {e}"
            )

        debug.functions_exec("Runtime IO buffers ready")

        try:
            cls._runtime_with_buffers(inp, out, sem, func_instance, prefetch_artifacts)
        finally:
            # Clean up I/O buffers
            inp.close()
            out.close()
            sem.close()

    @classmethod
    def _runtime_with_buffers(
        cls,
        inp: ReadBuffer,
        out: WriteBuffer,
        sem: Semaphore,
        func_instance,
        prefetch_artifacts: bool = False,
    ):
        debug.functions_exec("Setting up serializers using registry")
        registry = get_global_registry()

        # Note: Core serializers are now registered automatically in create_function_type()

        func_spec = func_instance.spec
        input_types = func_spec.input_spec
        output_types = func_spec.output_spec
        input_type = cls._map_type_info_to_python_type(input_types, func_spec)

        serializer = lambda data, buf: registry.serialize(data, buf)
        deserializer = lambda buf: parse_function_payload(
            buf, input_type, return_raw_data=False
        )

        # Get the output type in case of an exception, we need to create an empty one.
        from metaflow_extensions.nflx.plugins.functions.utils import (
            load_type_from_string,
        )

        output_cls = load_type_from_string(output_types["type"])
        if output_cls is None:
            raise MetaflowFunctionException(
                f"Could not load output class {output_types['type']}"
            )

        # Setup the signal handler
        global terminate_process
        terminate_process = False  # type: ignore

        def sigterm_handler(signum, frame):
            global terminate_process
            terminate_process = True  # type: ignore

        # Register the signal handler
        signal.signal(signal.SIGTERM, sigterm_handler)

        # Setup static parameters with explicit prefetch_artifacts flag passed via CLI
        # Pre-fetching downloads all artifacts from S3 during initialization,
        # improving first execute() call latency at the cost of slower startup
        parameters = create_function_parameters(
            func_spec, prefetch_artifacts=prefetch_artifacts
        )

        # A asynchronous run loop
        results: deque[Any] = deque()
        inp_mem = None
        out_mem = None
        while not terminate_process:  # type: ignore
            # Wait on the semaphore
            try:
                sem.wait()

            except MetaflowFunctionMemorySignalException as e:
                debug.functions_exec("Semaphore caught signal handler interrupt.")

            try:
                # Check if we have an input buffer available and write to
                # it. Otherwise continue with our loop.
                inp_mem = inp.acquire()
                if inp_mem.valid:
                    debug.functions_exec(f"{inp_mem.uuid}")
                    debug.functions_exec("Deserialzing input")
                    function_payload = deserializer(inp_mem.buf)

                    # FunctionPayload contains the deserialized data
                    input_data = function_payload.data
                    kwargs = {}

                    # Keep reference to kwargs before function execution
                    kwargs_copy = function_payload.kwargs.copy()

                    try:
                        debug.functions_exec("Call the function _execute method")
                        result = func_instance.execute(
                            input_data, parameters, **kwargs_copy
                        )
                    except Exception as user_error:
                        debug.functions_exec("User exception")
                        result = output_cls()
                        kwargs = {MFF_ERROR_KEY: traceback.format_exc()}

                    # Wrap result with potentially modified kwargs (modifications happen in-place)
                    kwargs.update(kwargs_copy)
                    result_payload = FunctionPayload(result, kwargs)

                    # We need to hold on to the input memory since the
                    # result may reference it
                    results.append((inp_mem, result_payload))

                # Check for an output buffer and write to it, drain results if possible.
                while results:
                    result = results[0]
                    debug.functions_exec(f"{result[0].uuid}")
                    with out.acquire(result[0].uuid) as out_mem:
                        if out_mem.valid:
                            debug.functions_exec("Serializing to output buffer")
                            data_len, unwritten = serializer(result[1], out_mem.buf)
                            results.extendleft(unwritten)
                            debug.functions_exec(f"Freeing output buffer {data_len}")
                            inp.free(result[0])
                            result[0].close()
                            out_mem.truncate(data_len)
                            out.commit(out_mem)
                            out_mem.close()
                            results.popleft()

            except Exception as e:
                inp_mem = None
                out_mem = None
                results = deque()
                raise e

        # Reset value now that the run loop has finished.
        terminate_process = False  # type: ignore

    @classmethod
    def runtime_main(
        cls,
        input_map_name: str,
        output_map_name: str,
        data_watcher_name: str,
        reference: str,
        prefetch_artifacts: bool = False,
    ) -> None:
        """
        This function is called from a subprocess and handles memory buffer-based
        inter-process communication for function execution.

        Parameters
        ----------
        input_map_name : str
            Name of the input shared memory map
        output_map_name : str
            Name of the output shared memory map
        data_watcher_name : str
            Name of the data watcher shared memory
        reference : str
            Path to the function specification file
        prefetch_artifacts : bool, default False
            Whether to pre-fetch all artifacts during initialization
        """

        def execute_runtime():
            # Create memory-specific execution context and delegate to memory backend
            io_context = {
                "input_map_name": input_map_name,
                "output_map_name": output_map_name,
                "data_watcher_name": data_watcher_name,
                "prefetch_artifacts": prefetch_artifacts,
            }

            debug.functions_exec(
                f"Starting memory backend runtime with io context: {io_context}"
            )
            cls.runtime(io_context, reference)

        # Run with standardized exception handling
        run_with_exception_handling(execute_runtime)

    @classmethod
    def _map_type_info_to_python_type(cls, type_info, func_spec):
        """
        Map type information from function specs to Python types for deserialization.
        """
        type_name = type_info.get("type")

        from metaflow_extensions.nflx.plugins.functions.utils import (
            load_type_from_string,
        )

        loaded_class = load_type_from_string(type_name)
        if loaded_class:
            return loaded_class

        raise MetaflowFunctionRuntimeException(
            f"Could not load class {type_name} for deserialization"
        )
