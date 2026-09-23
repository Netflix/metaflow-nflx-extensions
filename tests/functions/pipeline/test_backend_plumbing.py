import pytest

pytestmark = pytest.mark.no_backend_parametrization

from metaflow_extensions.nflx.plugins.functions import backends as backends_pkg
from metaflow_extensions.nflx.plugins.functions.backends import (
    factory as backend_factory,
)
from metaflow_extensions.nflx.plugins.functions.core import function as function_module
from metaflow_extensions.nflx.plugins.functions.core.function_pipeline import (
    FunctionPipeline,
)
from metaflow_extensions.nflx.plugins.functions.core.function_pipeline_spec import (
    FunctionPipelineSpec,
)
from metaflow_extensions.nflx.plugins.functions.core.function_spec import FunctionSpec


@pytest.fixture
def requested_backends(monkeypatch):
    """Record the backend name every get_backend() call in the module receives."""
    seen = []

    def fake_get_backend(backend_name=None):
        seen.append(backend_name)
        return object()

    # _create_from_spec imports from the package, _create_proxy_from_spec from
    # the factory module; both have to be patched.
    monkeypatch.setattr(backends_pkg, "get_backend", fake_get_backend)
    monkeypatch.setattr(backend_factory, "get_backend", fake_get_backend)
    return seen


@pytest.fixture
def constituent_backends(monkeypatch):
    """Record the backend name each constituent's function_from_json receives."""
    seen = []

    def fake_function_from_json(reference, **kwargs):
        seen.append(kwargs.get("backend"))
        return object()

    monkeypatch.setattr(function_module, "function_from_json", fake_function_from_json)
    return seen


def _spec():
    return FunctionPipelineSpec(name="a_pipeline", uuid="0" * 32)


def test_create_from_spec_uses_the_requested_backend(requested_backends):
    FunctionPipeline._create_from_spec(_spec(), [], backend="local")

    assert requested_backends == ["local"]


def test_create_from_spec_falls_back_to_the_configured_backend(requested_backends):
    FunctionPipeline._create_from_spec(_spec(), [])

    assert requested_backends == [None]


def test_create_proxy_from_spec_uses_the_requested_backend(
    monkeypatch, requested_backends, constituent_backends
):
    # One constituent, resolved without touching a real spec or the datastore.
    monkeypatch.setattr(
        FunctionPipeline,
        "_get_function_references",
        classmethod(lambda cls, func_spec: [{"reference": "s3://ref.json"}]),
    )
    monkeypatch.setattr(
        FunctionSpec,
        "_detect_subclass_from_data",
        classmethod(lambda cls, data: FunctionPipeline),
    )
    monkeypatch.setattr(
        FunctionPipelineSpec,
        "_from_json_impl_from_data",
        classmethod(lambda cls, data: FunctionPipelineSpec(**data)),
    )
    monkeypatch.setattr(
        FunctionSpec, "download_to_temp", staticmethod(lambda reference: reference)
    )

    FunctionPipeline._create_proxy_from_spec(_spec(), backend="local")

    assert requested_backends == ["local"]
    # The constituents are the half that regressed: a pipeline on the right
    # backend whose functions are on the default one is still wrong.
    assert constituent_backends == ["local"]


def test_from_spec_forwards_the_backend(monkeypatch):
    seen = {}

    monkeypatch.setattr(
        FunctionPipeline,
        "_reconstruct_from_spec",
        classmethod(
            lambda cls, spec, base_path=None, backend=None: seen.update(backend=backend)
        ),
    )

    FunctionPipeline.from_spec(_spec(), backend="local")

    assert seen == {"backend": "local"}
