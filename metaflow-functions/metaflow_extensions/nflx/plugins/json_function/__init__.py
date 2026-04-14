from metaflow_extensions.nflx.plugins.functions.factory import (
    create_function_type,
    FunctionTypeConfig,
    is_json_serializable_type,
)
from metaflow_extensions.nflx.plugins.functions.utils import (
    is_optional_function_parameters_type,
)


def _create_serializer_configs(builtin_types, serializer_module, serializer_class):
    """Create serializer configs for a list of builtin types."""
    from metaflow_extensions.nflx.plugins.functions.serializers.config import (
        SerializerConfig,
    )

    serializer_full_name = f"{serializer_module}.{serializer_class}"
    return [
        SerializerConfig(
            canonical_type=builtin_type,
            serializer=serializer_full_name,
        )
        for builtin_type in builtin_types
    ]


def register_serializers_from_signature(func_spec):
    """Register JSON serializers based on function spec."""
    from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
        register_serializer_config,
    )

    builtin_types = [
        "builtins.str",
        "builtins.dict",
        "builtins.list",
        "builtins.int",
        "builtins.float",
        "builtins.bool",
    ]

    json_configs = _create_serializer_configs(
        builtin_types,
        "metaflow_extensions.nflx.plugins.json_function.serializers",
        "JsonSerializer",
    )

    for config in json_configs:
        register_serializer_config(config)


def _register_json_serializer(type_hint):
    """Register JSON serializer for a type if it's JSON-compatible. Returns True if handled."""
    from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
        register_serializer,
        is_type_registered,
    )
    from .serializers import JsonSerializer

    json_types = (str, dict, list, int, float, bool)
    if type_hint in json_types and not is_type_registered(type_hint):
        register_serializer(JsonSerializer(type_hint))
        return True
    return False


JsonFunction, json_function = create_function_type(
    FunctionTypeConfig(
        name="json_function",
        param_validators=[
            is_json_serializable_type,
            is_optional_function_parameters_type,
        ],
        return_validator=is_json_serializable_type,
    )
)

# Usage example:
# @json_function
# def parse_data(data: str, params: FunctionParameters) -> dict:
#     import json
#     return json.loads(data)

__all__ = ["JsonFunction", "json_function"]
