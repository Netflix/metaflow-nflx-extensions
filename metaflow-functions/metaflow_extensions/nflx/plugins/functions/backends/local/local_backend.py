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
from metaflow_extensions.nflx.plugins.functions.common.runtime_utils import (
    create_function_parameters,
)
from metaflow_extensions.nflx.plugins.functions.debug import debug
import sys
import threading
import traceback
from contextlib import contextmanager


# Runtime components cannot serve two invocations at once: routing
# (``Cls.active_instance``) is class-level state and a component's per-call
# buffer lives on the instance, so overlapping invocations interleave both. The
# other backends can't hit this -- the memory backend runs a single-threaded
# subprocess runloop and a Ray actor is single-threaded -- but local mode
# executes in the caller's thread, so a threaded caller can.
#
# Reentrant on purpose: acquire(blocking=False) then succeeds for the *same*
# thread, so an invocation nested inside another one (or anything else
# re-entering on one thread) is allowed, while a genuinely concurrent
# invocation from a second thread is refused.
_COMPONENT_INVOCATION_LOCK = threading.RLock()


@contextmanager
def _guard_component_invocation(func_instance):
    """Refuse a concurrent invocation of a function that has runtime components.

    Raises rather than serialising. Serialising would silently remove the
    parallelism a threaded caller was asking for; raising says what the
    constraint is. Functions with no components are unaffected -- there is
    nothing to interleave, so concurrent local invocation stays allowed.
    """
    if not getattr(func_instance, "_runtime_components", None):
        yield
        return

    if not _COMPONENT_INVOCATION_LOCK.acquire(blocking=False):
        raise MetaflowFunctionRuntimeException(
            f"Function '{func_instance.name}' has runtime components and is "
            "already being invoked on another thread. Runtime components do not "
            "support concurrent invocation: routing and per-call buffers are "
            "shared, so overlapping calls would mix rows between invocations. "
            "Invoke it from one thread at a time, or load a separate copy per "
            "thread and serialise calls within each."
        )
    try:
        yield
    finally:
        _COMPONENT_INVOCATION_LOCK.release()


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
    def _is_proxy(func_instance) -> bool:
        """True when this handle still needs its code package hydrated."""
        return hasattr(func_instance, "_func") and func_instance._func is None

    @classmethod
    def _hydrate(cls, func_instance):
        """Materialize a proxy handle into a concrete, executable function.

        Downloads and extracts the code package, then loads the decorated
        function out of it. Expensive -- start() calls this once so apply()
        does not have to.
        """
        from metaflow_extensions.nflx.plugins.functions.core.function import (
            function_from_json,
        )
        from metaflow_extensions.nflx.plugins.functions.core.function_spec import (
            FunctionSpec,
        )

        func_spec = func_instance.spec

        if not func_spec.reference:
            raise MetaflowFunctionRuntimeException(
                "Function spec missing reference path"
            )

        # Download S3 reference to local temp file if needed
        local_reference = FunctionSpec.download_to_temp(func_spec.reference)

        # Carry the proxy's runtime_components over to the concrete function -
        # function_from_json() below has no way to see the proxy's, and would
        # otherwise silently default to none.
        runtime_components = getattr(func_instance, "_runtime_components", [])

        # Load concrete function from reference. This handles both regular functions
        # and pipelines by delegating to the appropriate from_spec() implementation.
        # Don't start runtime - function executes directly in this process
        return function_from_json(
            local_reference,
            use_proxy=False,
            backend="local",
            start_runtime=False,
            runtime_components=runtime_components,
        )

    @classmethod
    def start(cls, func_instance, **kwargs):
        """
        Warm up in-process execution so the first call is not the slow one.

        The local backend has no runtime to launch, but it does have per-call
        work that only needs doing once. Without this, ``apply()`` re-hydrates
        the code package on every invocation of a proxy handle and rebuilds the
        function parameters each time -- fine for a script, wrong for a server
        that loads a function once and then calls it under latency SLOs.

        Three things are set up here:

        * the hydrated concrete function, cached on the handle as
          ``_local_concrete``;
        * its ``FunctionParameters``, cached as ``_local_params`` and honouring
          the ``_prefetch_artifacts`` flag that ``function_from_json`` sets when
          called with ``start_runtime=True``, so artifacts are resolved now
          rather than on the first call;
        * the function's root directory on ``sys.path``, *persistently*.
          ``from_spec()`` only adds it for the duration of the load (see
          ``run_in_path``, which restores ``sys.path`` in a ``finally``), so any
          import the user's code defers to call time -- generated protobuf
          stubs are the common case -- fails after the load window closes.

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance to prepare
        """
        concrete = (
            cls._hydrate(func_instance)
            if cls._is_proxy(func_instance)
            else func_instance
        )

        try:
            root_dir = concrete.function_root_dir
        except Exception:
            root_dir = None
        if root_dir and root_dir not in sys.path:
            sys.path.insert(0, root_dir)

        prefetch = getattr(func_instance, "_prefetch_artifacts", False)
        params = create_function_parameters(concrete.spec, prefetch_artifacts=prefetch)

        # Cache on both handles: the caller keeps hold of the proxy, while
        # apply() may be handed either one.
        for handle in (func_instance, concrete):
            handle._local_concrete = concrete
            handle._local_params = params

        debug.functions_exec(
            "LocalBackend.start: warmed '%s' (prefetch_artifacts=%s, sys.path+=%s)"
            % (concrete.name, prefetch, root_dir)
        )

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
        # If func_instance is a proxy (i.e., _func is None), convert to concrete
        # function. start() does this once up front; without it, every call pays
        # for it.
        if cls._is_proxy(func_instance):
            cached = getattr(func_instance, "_local_concrete", None)
            func_instance = (
                cached if cached is not None else cls._hydrate(func_instance)
            )

        # Use params from kwargs if provided, then whatever start() prepared,
        # otherwise create new ones.
        parameters = kwargs.pop("params", None)
        if parameters is None:
            parameters = getattr(func_instance, "_local_params", None)
        if parameters is None:
            parameters = create_function_parameters(func_instance.spec)

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
                    getattr(func_instance, "_runtime_components", []),
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
                after_call_components(func_instance._component_instances)
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

            if user_exception is not None:
                raise user_exception

        return result

    @classmethod
    def close(cls, func_instance, clean_dir: bool = True, **kwargs):
        # Components were started on whatever handle apply() ran, which is the
        # concrete function start() cached -- not necessarily the proxy the
        # caller is closing.
        target = getattr(func_instance, "_local_concrete", None) or func_instance

        instances = target._component_instances
        if instances:
            from metaflow_extensions.nflx.plugins.functions.components.runtime import (
                stop_components,
            )

            stop_components(instances)
            target._component_instances = []

        # Drop what start() warmed up, so a re-start() re-hydrates rather than
        # handing back a function whose components have been stopped.
        for handle in (func_instance, target):
            handle._local_concrete = None
            handle._local_params = None

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
        # input_types is a property on MetaflowFunction/FunctionPipeline, not a
        # classmethod taking a spec.
        input_types = func_instance.input_types
        expected_input_type = cls._map_type_info_to_python_type(
            input_types, type(func_instance), func_instance.spec
        )
        deserialized_data = registry.deserialize(data, expected_input_type)

        # apply() takes the user's own input type and returns the user's own
        # output type -- same contract as the memory backend. Wrapping the input
        # in a FunctionPayload here handed the user's function the wrapper
        # instead of its declared input; unwrapping `.data` from the result did
        # the mirror of that on the way out.
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
