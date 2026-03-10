from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, cast
from dataclasses import dataclass
import inspect
import os
import sys

from metaflow_extensions.nflx.plugins.functions.core.function import MetaflowFunction
from metaflow_extensions.nflx.plugins.functions.core.function_decorator import (
    MetaflowFunctionDecorator,
)
from metaflow_extensions.nflx.plugins.functions.core.function_decorator_spec import (
    FunctionDecoratorSpec,
)
from metaflow_extensions.nflx.plugins.functions.core.function_spec import FunctionSpec
from metaflow_extensions.nflx.plugins.functions.utils import (
    validate_function_signature,
    get_caller_module,
)
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
)
from metaflow_extensions.nflx.plugins.functions.serializers.import_interceptor import (
    register_serializer_for_type,
)


F = TypeVar("F", bound=Callable[..., Any])


@dataclass
class FunctionTypeConfig:
    """Configuration for a function type."""

    name: str
    param_validators: List[Callable[[Type], bool]]
    return_validator: Callable[[Type], bool]
    param_count: Optional[int] = 2  # Most functions take (data, params)
    custom_validator: Optional[Callable[[Callable, List, Dict, str], None]] = None
    type_serializers: Optional[Dict[str, Dict]] = (
        None  # Dict of {canonical_type: serializer_config}
    )
    type_serializer_resolvers: Optional[List[Callable[[Type], Optional[Dict]]]] = (
        None  # List of functions that resolve serializer configs for types
    )


def create_function_type(
    config: FunctionTypeConfig,
) -> tuple[Type[MetaflowFunction], Callable[[F], F]]:
    """
    Create a complete function type from a config.

    Parameters
    ----------
    config : FunctionTypeConfig
        Configuration specifying validators and parameter count

    Returns
    -------
    tuple[Type[MetaflowFunction], Callable[[F], F]]
        (FunctionClass, decorator_function)

    Example
    -------
    >>> JsonFunction, json_function = create_function_type(FunctionTypeConfig(
    ...     name="json_function",
    ...     param_validators=[is_json_type, is_optional_function_parameters_type],
    ...     return_validator=is_json_type
    ... ))
    """

    # Register core serializers directly (needed by all function types)
    # Register FunctionPayload serializer (which depends on FunctionParameters)
    from metaflow_extensions.nflx.plugins.functions.core.function_payload import (
        register_function_payload_serializer,
    )

    register_function_payload_serializer()

    # Register FunctionParameters serializer
    # This must come after FunctionPayload because it has circular dependencies
    from metaflow_extensions.nflx.plugins.functions.serializers.function_parameters_serializer import (
        register_function_parameters_serializer,
    )

    register_function_parameters_serializer()

    # Register no-op identity serializers for binary types directly
    from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
        get_global_registry,
        get_canonical_type_string,
    )
    from metaflow_extensions.nflx.plugins.functions.serializers.base import (
        BaseSerializer,
    )

    class IdentitySerializer(BaseSerializer):
        """No-op identity serializer for types that don't need real serialization."""

        def __init__(self, supported_type, convert_fn=None):
            self._supported_type = supported_type
            self._convert_fn = convert_fn or (lambda x: x)

        @property
        def supported_type(self):
            return self._supported_type

        def serialize(self, obj):
            return self._convert_fn(obj), []

        def deserialize(self, data: bytes):
            if self._supported_type == bytes:
                return data
            elif self._supported_type == bytearray:
                return bytearray(data)
            elif self._supported_type == memoryview:
                return memoryview(data)

    # Register identity serializers directly in the registry for binary types
    registry = get_global_registry()

    for binary_type, convert_fn in [
        (bytes, None),
        (bytearray, lambda x: bytes(x)),
        (memoryview, lambda x: x.tobytes()),
    ]:
        canonical_type = get_canonical_type_string(binary_type)
        serializer = IdentitySerializer(binary_type, convert_fn)
        registry._loaded_serializers[canonical_type] = serializer

    # Register primitive type serializers
    from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
        _create_serializer_configs_for_types,
        register_serializer_config,
        get_global_registry,
    )

    primitive_types = [
        "builtins.str",
        "builtins.dict",
        "builtins.list",
        "builtins.int",
        "builtins.float",
        "builtins.bool",
    ]

    primitive_configs = _create_serializer_configs_for_types(
        primitive_types,
        "metaflow_extensions.nflx.plugins.avro_function.serializers",
        "AvroSerializer",
    )

    registry = get_global_registry()
    for primitive_config in primitive_configs:
        if primitive_config.canonical_type not in registry._serializer_configs:
            register_serializer_config(primitive_config)

    # Register serializers for all types declared by this function type
    if config.type_serializers:
        from metaflow_extensions.nflx.plugins.functions.serializers.config import (
            SerializerConfig,
        )
        from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
            register_serializer_config,
        )

        for canonical_type, serializer_config_dict in config.type_serializers.items():
            # Add canonical_type field to complete the serializer config
            serializer_config_dict = {
                **serializer_config_dict,
                "canonical_type": canonical_type,
            }
            serializer_config = SerializerConfig(**serializer_config_dict)

            # Built-in types are always already imported, so register immediately
            if canonical_type.startswith("builtins."):
                register_serializer_config(serializer_config)
            else:
                # For other types, use the import interceptor to register when imported
                register_serializer_for_type(canonical_type, serializer_config)

    # Create proper class names
    base_name = config.name.replace("_function", "")
    if base_name == "dataframe":
        base_name = "DataFrame"
    else:
        base_name = base_name.replace("_", " ").title().replace(" ", "")

    # 1. Create the decorator spec class
    @dataclass(kw_only=True)
    class GeneratedDecoratorSpec(FunctionDecoratorSpec):
        type: str = config.name
        input_schema: Optional[Dict[str, Any]] = None
        parameter_schema: Optional[Dict[str, Any]] = None
        return_schema: Optional[Dict[str, Any]] = None

    # 2. Create the function spec class
    @dataclass(kw_only=True)
    class GeneratedFunctionSpec(FunctionSpec):
        @classmethod
        def _build_deco_spec(cls, desc: Dict[str, Any]) -> GeneratedDecoratorSpec:
            try:
                return GeneratedDecoratorSpec(**desc)
            except Exception as e:
                raise MetaflowFunctionException(
                    f"Cannot create {config.name} spec: {str(e)}"
                )

        @classmethod
        def _from_json_impl_from_data(
            cls, desc: Dict[str, Any]
        ) -> "GeneratedFunctionSpec":
            function_spec = None
            if "function" in desc and desc["function"]:
                function_spec = cls._build_deco_spec(desc["function"])
            filtered_desc = {k: v for k, v in desc.items() if k != "function"}
            spec = cls(**filtered_desc)
            spec.function = function_spec
            return spec

    # 3. Create the decorator class
    class GeneratedDecorator(MetaflowFunctionDecorator):
        TYPE = config.name

        def __init__(self, func):
            super().__init__(func)
            # Register serializers when decorator is created
            self._register_serializers()

        def _register_serializers(self):
            """Register serializers from config with the global registry."""
            # Note: Core serializers (FunctionPayload + primitives) are now registered
            # in create_function_type() when the function type is created

            # Register serializers for types discovered in function signature
            self._register_function_signature_types()

        def _register_function_signature_types(self):
            """Register serializers for types found in function signature."""
            from typing import get_type_hints

            # Get type hints - let errors bubble up so users can fix their code
            type_hints = get_type_hints(self.func)

            for param_name, param_type in type_hints.items():
                self._register_serializer_for_type(param_type)

        def _register_serializer_for_type(self, param_type):
            """Register appropriate serializer for a specific type."""
            from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
                register_serializer_config,
            )

            # Get canonical type string
            canonical_type = f"{param_type.__module__}.{param_type.__name__}"

            # Skip if already registered
            from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
                get_global_registry,
            )

            registry = get_global_registry()
            if canonical_type in registry._serializer_configs:
                return

            # Determine appropriate serializer based on type
            serializer_config = self._get_serializer_config_for_type(
                param_type, canonical_type
            )
            if serializer_config:
                register_serializer_config(serializer_config)

        def _get_serializer_config_for_type(self, param_type, canonical_type):
            """Get appropriate serializer config for a type using registered resolvers."""
            if not config.type_serializer_resolvers:
                return None

            for resolver in config.type_serializer_resolvers:
                serializer_config_dict = resolver(param_type)
                if serializer_config_dict:
                    # Add canonical_type to complete the config
                    serializer_config_dict = {
                        **serializer_config_dict,
                        "canonical_type": canonical_type,
                    }
                    from metaflow_extensions.nflx.plugins.functions.serializers.config import (
                        SerializerConfig,
                    )

                    return SerializerConfig(**serializer_config_dict)

            return None

        def _build_deco_spec(self) -> GeneratedDecoratorSpec:
            input_type, output_type, input_type_str, output_type_str = (
                validate_function_signature(
                    self.func,
                    expected_param_count=config.param_count or 2,
                    param_validators=config.param_validators,
                    return_validator=config.return_validator,
                    decorator_name=config.name,
                    custom_validator=config.custom_validator,
                )
            )

            file_name = os.path.basename(inspect.getfile(self.func))
            file_name = os.path.splitext(file_name)[0]

            # Extract detailed schema information if available
            input_schema = self._extract_schema_info(
                input_type, input_type_str, is_input=True
            )
            return_schema = self._extract_schema_info(
                output_type, output_type_str, is_input=False
            )

            # Build parameter schema - just check if 'params' parameter exists with correct type
            from metaflow_extensions.nflx.plugins.functions.utils import (
                is_optional_function_parameters_type,
                get_type_string,
            )
            from typing import get_type_hints, get_origin, get_args, Union

            type_hints = get_type_hints(self.func)
            parameter_schema = None

            params_type = type_hints.get("params")
            if params_type and is_optional_function_parameters_type(params_type):
                # Extract the actual concrete type, unwrapping Optional if needed
                concrete_type = params_type
                if get_origin(params_type) is Union:
                    # Handle Optional[T] which is Union[T, None]
                    args = get_args(params_type)
                    for arg in args:
                        if arg is not type(None):
                            concrete_type = arg
                            break

                parameter_schema = {"type": get_type_string(concrete_type)}

            return GeneratedDecoratorSpec(
                name=self.func.__name__,
                file_name=file_name,
                module=self.__module__,
                doc=self.__doc__.strip() if self.__doc__ else None,
                type=self.TYPE,
                input_schema=input_schema,
                parameter_schema=parameter_schema,
                return_schema=return_schema,
            )

        def _extract_schema_info(self, type_hint, type_str, is_input=True):
            """Extract detailed schema information from type hints."""
            from typing import get_origin, get_args

            # Start with basic type info
            schema = {"type": type_str}

            # Check if this is a generic type with schema parameters
            origin = get_origin(type_hint)
            if origin is not None:
                args = get_args(type_hint)
                if args and len(args) > 0:
                    schema_type = args[0]
                    # Extract schema information from dataclass fields
                    if hasattr(schema_type, "__dataclass_fields__"):
                        fields = {}
                        for (
                            field_name,
                            field_info,
                        ) in schema_type.__dataclass_fields__.items():
                            # Convert field types to string representations
                            field_type = str(field_info.type)
                            if hasattr(field_info.type, "__name__"):
                                field_type = field_info.type.__name__
                            fields[field_name] = {"type": field_type}

                        schema_key = "input_schema" if is_input else "output_schema"
                        schema[schema_key] = [
                            {"name": schema_type.__name__, "fields": fields}
                        ]

            return schema

    # 4. Create the function class
    class GeneratedFunction(MetaflowFunction):
        function_spec_cls: Type[GeneratedFunctionSpec] = GeneratedFunctionSpec

        def __init__(self, *args, **kwargs):
            """Initialize with default backend if none provided."""
            super().__init__(*args, **kwargs)
            # Set default backend if none is set
            if not hasattr(self, "_backend") or self._backend is None:
                from metaflow_extensions.nflx.plugins.functions.backends.factory import (
                    get_backend,
                )

                self._backend = get_backend()

        def _build_function_spec(self, **kwargs):
            """Build function spec and populate serializer configs."""
            func_spec = super()._build_function_spec(**kwargs)

            # Add serializer configs for this function's types
            from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
                get_global_registry,
            )

            registry = get_global_registry()
            configs = {}

            # Check input/output types that are already in the spec
            for type_str in [
                func_spec.input_spec.get("type"),
                func_spec.output_spec.get("type"),
            ]:
                if type_str and type_str in registry._serializer_configs:
                    config = registry._serializer_configs[type_str]
                    configs[type_str] = {
                        "serializer": config.serializer,
                        "supports_chunking": config.supports_chunking,
                    }
                    if config.extra_kwargs:
                        configs[type_str]["extra_kwargs"] = config.extra_kwargs

            func_spec.serializer_configs = configs
            return func_spec

        @property
        def input_types(self) -> Dict[str, Any]:
            func_spec = self.spec
            if not func_spec:
                raise MetaflowFunctionException(
                    f"No function spec provided to {config.name}"
                )

            if func_spec.input_spec is None:
                raise MetaflowFunctionException(
                    f"No input spec provided to {config.name}"
                )
            return func_spec.input_spec

        @property
        def output_types(self) -> Dict[str, Any]:
            func_spec = self.spec
            if not func_spec:
                raise MetaflowFunctionException(
                    f"No function spec provided to {config.name}"
                )

            if func_spec.output_spec is None:
                raise MetaflowFunctionException(
                    f"No output spec provided to {config.name}"
                )
            return func_spec.output_spec

        def is_compatible_with(self, other: "MetaflowFunction") -> bool:
            """
            Check if this function's output is compatible with another function's input.

            Parameters
            ----------
            other : MetaflowFunction
                The other function to check compatibility with

            Returns
            -------
            bool
                True if compatible, False otherwise
            """
            try:
                # Get output spec of this function
                my_output_spec = self.spec.output_spec
                if my_output_spec is None:
                    return False
                my_output_type = my_output_spec.get("type")

                # Get input spec of other function
                other_input_spec = other.spec.input_spec
                if other_input_spec is None:
                    return False
                other_input_type = other_input_spec.get("type")

                # First check basic type compatibility
                if my_output_type != other_input_type:
                    return False

                # If both have detailed schema information, check schema compatibility
                my_output_schema = my_output_spec.get("output_schema")
                other_input_schema = other_input_spec.get("input_schema")

                if my_output_schema and other_input_schema:
                    # Check if the required fields match
                    my_fields = (
                        my_output_schema[0].get("fields", {})
                        if my_output_schema
                        else {}
                    )
                    other_fields = (
                        other_input_schema[0].get("fields", {})
                        if other_input_schema
                        else {}
                    )

                    # Other function's required fields must be available in my output
                    for field_name in other_fields.keys():
                        if field_name not in my_fields:
                            return False

                return True

            except (AttributeError, KeyError, MetaflowFunctionException):
                # If we can't determine types, assume incompatible
                return False

    # 5. Create the decorator function
    def decorator_function(func: F) -> F:
        return cast(F, GeneratedDecorator(func))

    # Set proper names for all generated classes
    GeneratedDecoratorSpec.__name__ = f"{base_name}DecoratorSpec"
    GeneratedDecoratorSpec.__qualname__ = GeneratedDecoratorSpec.__name__

    GeneratedFunctionSpec.__name__ = f"{base_name}FunctionSpec"
    GeneratedFunctionSpec.__qualname__ = GeneratedFunctionSpec.__name__

    GeneratedDecorator.__name__ = f"{base_name}Decorator"
    GeneratedDecorator.__qualname__ = GeneratedDecorator.__name__

    GeneratedFunction.__name__ = f"{base_name}Function"
    GeneratedFunction.__qualname__ = GeneratedFunction.__name__

    # CRITICAL: Register generated classes in caller's module for from_json to work
    # Get the calling module (where create_function_type was called)
    caller_module_name = get_caller_module(frames_back=2)

    if caller_module_name and caller_module_name in sys.modules:
        caller_module = sys.modules[caller_module_name]

        # Add generated classes to caller module's namespace
        setattr(caller_module, GeneratedFunction.__name__, GeneratedFunction)
        setattr(caller_module, GeneratedFunctionSpec.__name__, GeneratedFunctionSpec)
        setattr(caller_module, GeneratedDecorator.__name__, GeneratedDecorator)
        setattr(caller_module, GeneratedDecoratorSpec.__name__, GeneratedDecoratorSpec)

        # Set proper module names for the classes
        GeneratedFunction.__module__ = caller_module_name
        GeneratedFunctionSpec.__module__ = caller_module_name
        GeneratedDecorator.__module__ = caller_module_name
        GeneratedDecoratorSpec.__module__ = caller_module_name

    decorator_function.__name__ = config.name

    return GeneratedFunction, decorator_function


def is_json_serializable_type(type_hint: Type) -> bool:
    """Check if type can be JSON serialized (str, dict, list, int, float, bool)."""
    json_types = (str, dict, list, int, float, bool)
    return type_hint in json_types or (
        hasattr(type_hint, "__origin__") and type_hint.__origin__ in json_types
    )
