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

# components are shared so if a thread spawns more thread we need this lock
_COMPONENT_INVOCATION_LOCK = threading.RLock()


class _guard_component_invocation:
    """
    Refuse a concurrent invocation of a function that has runtime components.
    You must load the function within a thread.
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
        """
        Check whether this handle still has to load its code before it can run.
        """
        constituents = getattr(func_instance, "functions", None)
        if constituents is not None:
            return any(LocalBackend._needs_hydration(c) for c in constituents)
        return func_instance._func is None

    @classmethod
    def start(cls, func_instance, **kwargs):
        """
        Warm the function's runtime so the first call is not the slow one.
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
        return cls._apply_warm(runtime, data, **kwargs)

    @classmethod
    def _apply_warm(cls, runtime, data: Any, **kwargs) -> Any:
        # The hydrated function the runtime holds, which is the caller's own
        # handle when it was already concrete. Component instances are carried
        # across by _hydrate, so output lands on the caller's own objects --
        # memory and ray have to route theirs back across a process boundary.
        func_instance = runtime.function

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
