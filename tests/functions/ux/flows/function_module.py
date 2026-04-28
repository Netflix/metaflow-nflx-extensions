from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.avro_function import avro_function
from metaflow_extensions.nflx.plugins.json_function import json_function


@avro_function
def avro_transform_string(
    data: str, params: FunctionParameters = FunctionParameters()
) -> str:
    """Simple avro function that transforms a string using pydash"""
    import pydash as _

    suffix = params.suffix if hasattr(params, "suffix") else "default"
    return _.upper_case(data).replace(" ", "") + "_" + str(suffix)


@avro_function
def avro_process_dict(
    data: dict, params: FunctionParameters = FunctionParameters()
) -> dict:
    """Avro function that processes a dict"""
    multiplier = params.multiplier if hasattr(params, "multiplier") else 2
    return {k: v * multiplier for k, v in data.items() if isinstance(v, (int, float))}


@json_function
def json_transform_list(
    data: list, params: FunctionParameters = FunctionParameters()
) -> list:
    """JSON function that filters a list"""
    threshold = params.threshold if hasattr(params, "threshold") else 0
    return [x for x in data if x > threshold]


@json_function
def json_process_object(
    data: dict, params: FunctionParameters = FunctionParameters()
) -> dict:
    """JSON function that adds a field using pydash"""
    import pydash as _

    increment = params.increment if hasattr(params, "increment") else 1
    result = _.merge({}, data, {"processed": True, "increment": increment})
    return result
