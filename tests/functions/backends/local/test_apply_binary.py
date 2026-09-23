import json
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.no_backend_parametrization

from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.avro_function import AvroFunction, avro_function
from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
    LocalBackend,
)
from metaflow_extensions.nflx.plugins.functions.core.function import MetaflowFunction
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
)
from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
    get_global_registry,
)


class _Task:
    pathspec = "Flow/1/step/task"
    code = SimpleNamespace(path="/tmp/code.tar")
    metadata_dict = {"conda_env_id": json.dumps(["test", "1", "linux-64"])}
    artifacts = []
    successful = True


@pytest.fixture
def function(monkeypatch):
    """A concrete AvroFunction recording the type its handler was called with."""
    monkeypatch.setattr(
        MetaflowFunction,
        "_export",
        classmethod(lambda cls, func_spec, package_suffixes=None: func_spec),
    )

    seen = []

    @avro_function
    def shout(data: str, params: FunctionParameters = FunctionParameters()) -> str:
        seen.append(type(data))
        return data.upper()

    instance = AvroFunction(shout, task=_Task())
    instance.seen = seen
    return instance


def _serialize(value):
    serialized, _ = get_global_registry().get_serializer_for_type(type(value))(value)
    return serialized


def test_apply_binary_round_trips(function):
    result = LocalBackend.apply_binary(function, _serialize("hello"))

    assert get_global_registry().deserialize(result, str) == "HELLO"


def test_apply_binary_passes_the_declared_input_type_to_the_function(function):
    LocalBackend.apply_binary(function, _serialize("hello"))

    # Not a FunctionPayload: apply() forwards data straight to execute(), so
    # wrapping it here would hand the handler the wrapper instead of its input.
    assert function.seen == [str]


def test_apply_binary_raises_when_the_result_type_has_no_serializer(
    function, monkeypatch
):
    payload = _serialize(
        "hello"
    )  # before the patch -- _serialize needs the real lookup

    registry = get_global_registry()
    monkeypatch.setattr(
        registry, "get_serializer_for_type", lambda type_class: None, raising=False
    )

    with pytest.raises(MetaflowFunctionException):
        LocalBackend.apply_binary(function, payload)
