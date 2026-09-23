import json
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.no_backend_parametrization

from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.avro_function import AvroFunction, avro_function
from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
    LocalBackend,
)
from metaflow_extensions.nflx.plugins.functions.core import function as function_module
from metaflow_extensions.nflx.plugins.functions.core.function import MetaflowFunction
from metaflow_extensions.nflx.plugins.functions.core.function_pipeline import (
    FunctionPipeline,
)
from metaflow_extensions.nflx.plugins.functions.core.function_pipeline_spec import (
    FunctionPipelineSpec,
)


class _Task:
    pathspec = "Flow/1/step/task"
    code = SimpleNamespace(path="/tmp/code.tar")
    metadata_dict = {"conda_env_id": json.dumps(["test", "1", "linux-64"])}
    artifacts = []
    successful = True


@pytest.fixture(autouse=True)
def _no_export(monkeypatch):
    monkeypatch.setattr(
        MetaflowFunction,
        "_export",
        classmethod(lambda cls, func_spec, package_suffixes=None: func_spec),
    )


def _concrete_function():
    @avro_function
    def passthrough(
        data: str, params: FunctionParameters = FunctionParameters()
    ) -> str:
        return data

    return AvroFunction(passthrough, task=_Task())


def _proxy_function():
    return AvroFunction._create_proxy_from_spec(_concrete_function().spec)


def _pipeline_of(functions):
    pipeline = FunctionPipeline.__new__(FunctionPipeline)
    pipeline.functions = functions
    return pipeline


def test_a_concrete_function_is_ready():
    assert LocalBackend._needs_hydration(_concrete_function()) is False


def test_a_proxy_function_needs_hydration():
    assert LocalBackend._needs_hydration(_proxy_function()) is True


def test_a_locally_built_pipeline_is_ready():
    pipeline = FunctionPipeline([_concrete_function(), _concrete_function()], "p")

    assert LocalBackend._needs_hydration(pipeline) is False


def test_a_pipeline_reconstructed_from_a_spec_is_ready():
    pipeline = FunctionPipeline._create_from_spec(
        FunctionPipelineSpec(name="p", uuid="0" * 32), [_concrete_function()]
    )

    assert LocalBackend._needs_hydration(pipeline) is False


def test_a_pipeline_of_proxies_needs_hydration():
    assert LocalBackend._needs_hydration(_pipeline_of([_proxy_function()])) is True


def test_one_unloaded_constituent_is_enough():
    pipeline = _pipeline_of([_concrete_function(), _proxy_function()])

    assert LocalBackend._needs_hydration(pipeline) is True


def test_apply_does_not_rehydrate_a_ready_pipeline(monkeypatch):
    """The observable cost of the old `_func is None` test: a pipeline that
    already holds its code was reloaded from the datastore on every call."""
    pipeline = FunctionPipeline([_concrete_function()], "p")

    def fail(*args, **kwargs):
        raise AssertionError("apply() re-loaded a pipeline that was already ready")

    monkeypatch.setattr(function_module, "function_from_json", fail)
    monkeypatch.setattr(
        FunctionPipeline, "execute", lambda self, data, params, **kwargs: data
    )

    assert LocalBackend.apply(pipeline, "hello") == "hello"
