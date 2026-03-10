from typing import Any
from ..abstract_backend import AbstractBackend
from ..backend_type import BackendType
from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
    get_global_registry,
)
from metaflow_extensions.nflx.plugins.functions.core.function_payload import (
    FunctionPayload,
)
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionRuntimeException,
    MetaflowFunctionException,
    MetaflowFunctionUserException,
)
from metaflow_extensions.nflx.plugins.functions.common.runtime_utils import (
    create_function_parameters,
)
import traceback


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
        # If func_instance is a proxy (i.e., _func is None), convert to concrete function
        if hasattr(func_instance, "_func") and func_instance._func is None:
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

            # Load concrete function from reference. This handles both regular functions
            # and pipelines by delegating to the appropriate from_spec() implementation.
            # Don't start runtime - function executes directly in this process
            func_instance = function_from_json(
                local_reference, use_proxy=False, backend="local", start_runtime=False
            )

        # Use params from kwargs if provided, otherwise create new ones
        parameters = kwargs.pop("params", None)
        if parameters is None:
            parameters = create_function_parameters(func_instance.spec)

        try:
            return func_instance.execute(data, parameters, **kwargs)
        except Exception as e:
            # Wrap user exceptions for consistency with other backends
            raise MetaflowFunctionUserException(
                f"Exception in function '{func_instance.name}': {str(e)}\n{traceback.format_exc()}"
            )

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
        input_types = func_instance.__class__.get_input_types(func_instance.spec)
        expected_input_type = cls._map_type_info_to_python_type(
            input_types, type(func_instance), func_instance.spec
        )
        deserialized_data = registry.deserialize(data, expected_input_type)
        payload_data = FunctionPayload(deserialized_data, kwargs)

        result_payload = cls.apply(func_instance, payload_data, **kwargs)

        serializer = registry.get_serializer_for_type(type(result_payload.data))
        if serializer is None:
            raise MetaflowFunctionException(
                f"No serializer registered for type {type(result_payload.data)}"
            )

        serialized_data, _ = serializer(result_payload.data)
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
