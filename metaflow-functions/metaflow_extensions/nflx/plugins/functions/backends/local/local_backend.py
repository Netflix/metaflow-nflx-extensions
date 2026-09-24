from typing import Any, Optional
from ..abstract_backend import AbstractBackend
from ..backend_type import BackendType
from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
    get_global_registry,
)
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionRuntimeException,
    MetaflowFunctionException,
    MetaflowFunctionUserException,
)
from metaflow_extensions.nflx.plugins.functions.debug import debug

from ..keywords import KEYWORDS
from .runtime import close_runtime, runtime_for
import threading
import traceback

# Runtime components cannot serve two invocations at once: routing
# (``Cls.active_instance``) is class-level state and a component's per-call
# buffer lives on the instance, so overlapping invocations interleave both. The
# other backends can't hit this -- the memory backend runs a single-threaded
# subprocess runloop and a Ray actor is single-threaded -- but local mode
# executes in the caller's thread, so a threaded caller can.
#
# Still needed alongside runtime thread-affinity, which only covers one handle:
# ``active_instance`` is per component *class*, so two different functions
# sharing a component class collide across threads even though each is owned by
# its own thread.
#
# Reentrant on purpose: acquire(blocking=False) then succeeds for the *same*
# thread, so an invocation nested inside another one (or anything else
# re-entering on one thread) is allowed, while a genuinely concurrent
# invocation from a second thread is refused.
_COMPONENT_INVOCATION_LOCK = threading.RLock()


class _guard_component_invocation:
    """Refuse a concurrent invocation of a function that has runtime components.

    Raises rather than serialising. Serialising would silently remove the
    parallelism a threaded caller was asking for; raising says what the
    constraint is. Functions with no components are unaffected -- there is
    nothing to interleave, so concurrent local invocation stays allowed.

    A class rather than a ``@contextmanager``: the generator machinery costs
    ~0.6us of a ~3.5us apply(), and functions with no components pay it too.
    """

    __slots__ = ("_func_instance", "_held")

    def __init__(self, func_instance):
        self._func_instance = func_instance
        self._held = False

    def __enter__(self):
        if not self._func_instance.runtime_components:
            return self

        if not _COMPONENT_INVOCATION_LOCK.acquire(blocking=False):
            raise MetaflowFunctionRuntimeException(
                f"Function '{self._func_instance.name}' has runtime components "
                "and is already being invoked on another thread. Runtime "
                "components do not support concurrent invocation: routing and "
                "per-call buffers are shared, so overlapping calls would mix "
                "rows between invocations. Invoke it from one thread at a time, "
                "or load a separate copy per thread and serialise calls within "
                "each."
            )
        self._held = True
        return self

    def __exit__(self, *exc_info):
        if self._held:
            _COMPONENT_INVOCATION_LOCK.release()
        return False


class LocalBackend(AbstractBackend):
    """
    Backend for direct in-process execution.

    Executes functions directly in the same Python process with no isolation.
    This is the simplest backend with minimal overhead, suitable for:
    - Development and debugging
    - Quick prototyping
    - Functions that don't need isolation or parallelization
    """

    @property
    def backend_type(self) -> BackendType:
        return BackendType.LOCAL

    @staticmethod
    def _needs_hydration(func_instance) -> bool:
        """Whether this handle still has to load its code before it can run.

        Not the same as `_func is None`: a pipeline never has a single
        decorated function, so that test calls every pipeline a proxy.
        Re-hydrating a concrete one re-downloads code it already has;
        re-hydrating a locally built one replaces the caller's object with a
        copy from the datastore. A pipeline is ready when its constituents are.
        """
        constituents = getattr(func_instance, "functions", None)
        if constituents is not None:
            return any(LocalBackend._needs_hydration(c) for c in constituents)
        return func_instance._func is None

    @classmethod
    def _route_component_output(cls, func_instance, collected) -> None:
        """Stamp component output onto the caller's own component instances.

        Mirrors ``MemoryBackend._route_component_output``. A hydrated handle
        runs the *concrete* function's component instances, not the proxy's,
        so output has to be matched back by ``component_id``. For a handle
        that was already concrete these are the same objects and this is a
        no-op.
        """
        if not collected:
            return
        for component in func_instance.runtime_components:
            component_id = type(component).component_id
            if component_id in collected:
                component.output = collected[component_id]

    @classmethod
    def start(cls, func_instance, **kwargs):
        """Warm the function's runtime so the first call is not the slow one.

        Like ``MemoryBackend.start``, an optimization and never a
        precondition: ``apply()`` warms the same runtime if nothing has yet.
        """
        runtime_for(func_instance, process=kwargs.get("process", 1))

    @classmethod
    async def apply_async(cls, func_instance, data: Any, **kwargs) -> Any:
        return cls.apply(func_instance, data, **kwargs)

    @classmethod
    def apply(cls, func_instance, data: Any, **kwargs) -> Any:
        """
        Execute function directly in the same process.

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance to execute
        data : Any
            Input data for the function
        **kwargs : Any
            Additional keyword arguments

        Returns
        -------
        Any
            Result from function execution
        """
        runtime = runtime_for(func_instance, process=kwargs.get("process", 1))
        return cls._apply_warm(runtime, func_instance, data, **kwargs)

    @classmethod
    def _apply_warm(cls, runtime, caller_instance, data: Any, **kwargs) -> Any:
        # The hydrated function the runtime holds, which is the caller's own
        # handle when it was already concrete.
        func_instance = runtime.function

        # An explicit params= still wins over the runtime's cached ones.
        parameters = kwargs.get("params")
        if parameters is None:
            parameters = runtime.params

        kwargs = {k: v for k, v in kwargs.items() if k not in KEYWORDS}

        from metaflow_extensions.nflx.plugins.functions.components.runtime import (
            start_components,
            before_call_components,
            after_call_components,
        )

        # One invocation at a time for a function with components -- see
        # _guard_component_invocation above.
        with _guard_component_invocation(func_instance):
            if not func_instance._component_instances:
                func_instance._component_instances = start_components(
                    func_instance.runtime_components,
                    function=func_instance,
                )

            try:
                before_call_components(func_instance._component_instances)
            except Exception as e:
                raise MetaflowFunctionRuntimeException(
                    f"Runtime component exception in function '{func_instance.name}': {str(e)}\n{traceback.format_exc()}"
                )

            user_exception: Optional[MetaflowFunctionUserException]
            try:
                result = func_instance.execute(data, parameters, **kwargs)
            except Exception as e:
                user_exception = MetaflowFunctionUserException(
                    f"Exception in function '{func_instance.name}': {str(e)}\n{traceback.format_exc()}"
                )
                result = None
            else:
                user_exception = None

            # after_call must run whether or not the function call itself failed,
            # so components (e.g. metrics/logging) see every invocation.
            # TODO(local-backend exception parity): thread the raw exception
            # through here (`after_call_components(func_instance._component_instances,
            # exception=raw_exception)`) so after_call()/collect_output() can see
            # the failure, matching memory_backend.py. Requires keeping a
            # reference to the raw exception from the `except Exception as e:`
            # block above (currently only its wrapped `MetaflowFunctionUserException`
            # message is kept, not the exception object itself).
            try:
                collected = after_call_components(func_instance._component_instances)
            except Exception as e:
                if user_exception is None:
                    raise MetaflowFunctionRuntimeException(
                        f"Runtime component exception in function '{func_instance.name}': {str(e)}\n{traceback.format_exc()}"
                    )
                # A user exception is already in flight; don't let a component
                # failure on the error path mask it.
                debug.functions_exec(
                    f"Runtime component exception in after_call for '{func_instance.name}' "
                    f"while handling a prior user exception: {e!r}"
                )
            else:
                if caller_instance is not func_instance:
                    cls._route_component_output(caller_instance, collected)

            if user_exception is not None:
                raise user_exception

        return result

    @classmethod
    def close(cls, func_instance, clean_dir: bool = True, **kwargs):
        close_runtime(func_instance, clean_dir)

    @classmethod
    def apply_binary(cls, func_instance, data: bytes, **kwargs) -> bytes:
        """
        Execute function with binary serialized data.

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance to execute
        data : bytes
            Serialized input data
        **kwargs : Any
            Additional keyword arguments

        Returns
        -------
        bytes
            Serialized result
        """
        registry = get_global_registry()
        input_types = func_instance.input_types
        expected_input_type = cls._map_type_info_to_python_type(
            input_types, type(func_instance), func_instance.spec
        )
        deserialized_data = registry.deserialize(data, expected_input_type)

        # apply() takes the user's own input type and returns the user's own
        # output type. Wrapping the input in a FunctionPayload here handed the
        # user's function the wrapper instead of its declared input; unwrapping
        # `.data` from the result did the mirror of that on the way out.
        result = cls.apply(func_instance, deserialized_data, **kwargs)

        serializer = registry.get_serializer_for_type(type(result))
        if serializer is None:
            raise MetaflowFunctionException(
                f"No serializer registered for type {type(result)}"
            )

        serialized_data, _ = serializer(result)
        return serialized_data

    @classmethod
    def _map_type_info_to_python_type(cls, type_info, function_cls, func_spec):
        """Helper to map type info to Python type."""
        type_name = type_info.get("type")
        from metaflow_extensions.nflx.plugins.functions.utils import (
            load_type_from_string,
        )

        loaded_class = load_type_from_string(type_name)
        if not loaded_class:
            raise MetaflowFunctionRuntimeException(f"Could not load class {type_name}")
        return loaded_class
