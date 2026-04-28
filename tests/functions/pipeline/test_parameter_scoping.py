import pytest
from unittest.mock import MagicMock

pytestmark = pytest.mark.no_backend_parametrization

from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.functions.core.function_pipeline import (
    FunctionPipeline,
)
from metaflow_extensions.nflx.plugins.functions.utils import get_type_string


class TypedParams(FunctionParameters):
    increment: int
    multiplier: int


def make_pipeline() -> FunctionPipeline:
    """Create a bare FunctionPipeline instance without triggering __init__."""
    return FunctionPipeline.__new__(FunctionPipeline)


def make_func_with_schema(parameter_schema) -> MagicMock:
    """Create a mock function whose spec has the given parameter_schema."""
    func = MagicMock()
    func.spec.function.parameter_schema = parameter_schema
    return func


def test_optional_params_passes_pipeline_params_through():
    """Optional[FunctionParameters] (parameter_schema=None) passes pipeline params through."""
    pipeline = make_pipeline()
    pipeline_params = FunctionParameters(increment=10, multiplier=3)
    func = make_func_with_schema(None)

    result = pipeline._make_func_params(pipeline_params, func)
    assert result is pipeline_params


def test_base_params_passes_pipeline_params_through():
    """Base FunctionParameters (empty schema) passes pipeline params through."""
    pipeline = make_pipeline()
    pipeline_params = FunctionParameters(increment=10, multiplier=3)
    func = make_func_with_schema({})

    result = pipeline._make_func_params(pipeline_params, func)
    assert result is pipeline_params


def test_typed_params_isinstance():
    """Typed FunctionParameters subclass returns an instance of that type."""
    pipeline = make_pipeline()
    pipeline_params = FunctionParameters(increment=10, multiplier=3)
    func = make_func_with_schema({"type": get_type_string(TypedParams)})

    result = pipeline._make_func_params(pipeline_params, func)
    assert isinstance(result, TypedParams)
