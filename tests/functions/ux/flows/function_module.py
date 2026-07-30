from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.avro_function import avro_function
from metaflow_extensions.nflx.plugins.json_function import json_function


@avro_function
def avro_simple_string(
    data: str, params: FunctionParameters = FunctionParameters()
) -> str:
    """Simple avro function with no external dependencies."""
    suffix = params.suffix if hasattr(params, "suffix") else "default"
    return data.upper().replace(" ", "") + "_" + str(suffix)


@avro_function
def avro_pydash_string(
    data: str, params: FunctionParameters = FunctionParameters()
) -> str:
    """Avro function that explicitly depends on pydash, to exercise conda-env
    dependency resolution for backends that isolate execution (memory, ray).
    Not exercised for the local backend, which runs in-process using whatever
    the calling Python process already has installed."""
    import pydash as _

    suffix = params.suffix if hasattr(params, "suffix") else "default"
    return _.upper_case(data).replace(" ", "") + "_" + str(suffix)


@avro_function
def avro_add_field(
    data: dict, params: FunctionParameters = FunctionParameters()
) -> dict:
    """First stage of the avro pipeline: adds an incremented field."""
    increment = params.increment if hasattr(params, "increment") else 1
    return {**data, "incremented": data.get("value", 0) + increment}


@avro_function
def avro_double_values(
    data: dict, params: FunctionParameters = FunctionParameters()
) -> dict:
    """Second stage of the avro pipeline: doubles numeric values."""
    multiplier = params.multiplier if hasattr(params, "multiplier") else 2
    return {
        k: v * multiplier if isinstance(v, (int, float)) else v for k, v in data.items()
    }


@avro_function
def avro_raise_user_error(
    data: str, params: FunctionParameters = FunctionParameters()
) -> str:
    """Always raises, to exercise user-exception propagation (wrapped as
    MetaflowFunctionUserException) across backends."""
    raise RuntimeError(f"intentional user error for input: {data}")


@json_function
def json_simple_object(
    data: dict, params: FunctionParameters = FunctionParameters()
) -> dict:
    """Simple json function with no external dependencies."""
    increment = params.increment if hasattr(params, "increment") else 1
    return {**data, "processed": True, "increment": increment}
