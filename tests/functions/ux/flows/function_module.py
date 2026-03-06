from typing import Optional
from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.avro_function import avro_function
from metaflow_extensions.nflx.plugins.json_function import json_function


@avro_function
def avro_transform_string(
    data: str, params: Optional[FunctionParameters] = None
) -> str:
    """Simple avro function that transforms a string"""
    suffix = params.suffix if params and hasattr(params, "suffix") else "default"
    return data.upper() + "_" + str(suffix)


@avro_function
def avro_process_dict(data: dict, params: Optional[FunctionParameters] = None) -> dict:
    """Avro function that processes a dict"""
    multiplier = params.multiplier if params and hasattr(params, "multiplier") else 2
    return {k: v * multiplier for k, v in data.items() if isinstance(v, (int, float))}


@json_function
def json_transform_list(
    data: list, params: Optional[FunctionParameters] = None
) -> list:
    """JSON function that filters a list"""
    threshold = params.threshold if params and hasattr(params, "threshold") else 0
    return [x for x in data if x > threshold]


@json_function
def json_process_object(
    data: dict, params: Optional[FunctionParameters] = None
) -> dict:
    """JSON function that adds fields to a dict"""
    increment = params.increment if params and hasattr(params, "increment") else 1
    return {**data, "processed": True, "increment": increment}
