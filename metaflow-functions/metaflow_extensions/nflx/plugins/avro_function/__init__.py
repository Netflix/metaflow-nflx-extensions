from metaflow_extensions.nflx.plugins.functions.factory import (
    create_function_type,
    FunctionTypeConfig,
)
from metaflow_extensions.nflx.plugins.functions.utils import (
    is_optional_function_parameters_type,
)
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionArgumentsException,
)
from typing import Type

# Constants
AVRO_SERIALIZER = (
    "metaflow_extensions.nflx.plugins.avro_function.serializers.AvroSerializer"
)


def avro_serializer_config():
    """Create avro serializer config dict."""
    return {
        "serializer": AVRO_SERIALIZER,
    }


def _create_type_serializers_dict(builtin_types, serializer_config):
    """Create type_serializers dict from builtin types list and serializer config."""
    return {builtin_type: serializer_config for builtin_type in builtin_types}


def is_avro_serializable_type(type_hint: Type) -> bool:
    """Check if type can be serialized (str, dict, list, int, float, bool, bytes)."""
    avro_types = (str, dict, list, int, float, bool, bytes)
    return type_hint in avro_types or (
        hasattr(type_hint, "__origin__") and type_hint.__origin__ in avro_types
    )


def validate_avro_function_params(func, params, type_hints, decorator_name):
    """Custom validator for avro functions that enforces params parameter name."""
    if not is_avro_serializable_type(type_hints.get("data")):
        raise MetaflowFunctionArgumentsException(
            "First parameter must be avro-serializable type (str, dict, list, int, float, bool, bytes)"
        )

    # Use the standard parameter validation to enforce optional FunctionParameters
    from metaflow_extensions.nflx.plugins.functions.utils import (
        validate_standard_function_parameters,
    )

    validate_standard_function_parameters(
        params, type_hints, decorator_name, start_index=1
    )


AvroFunction, avro_function = create_function_type(
    FunctionTypeConfig(
        name="avro_function",
        param_validators=[],  # Use custom validator instead
        return_validator=is_avro_serializable_type,
        custom_validator=validate_avro_function_params,
        type_serializers=_create_type_serializers_dict(
            [
                "builtins.str",
                "builtins.dict",
                "builtins.list",
                "builtins.int",
                "builtins.float",
                "builtins.bool",
                "builtins.bytes",
            ],
            avro_serializer_config(),
        ),
    )
)

# Usage example:
# @avro_function
# def process_data(data: dict, params: FunctionParameters = FunctionParameters()) -> list:
#     return [data["key1"], data["key2"]]

__all__ = ["AvroFunction", "avro_function"]
