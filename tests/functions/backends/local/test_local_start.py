"""Unit tests for LocalBackend.start() warm-up.

These exercise start()/apply()/close() against a stub function handle rather
than a real code package: what is under test is the caching contract between
the three, not code-package extraction.
"""

import sys

import pytest

from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
    LocalBackend,
)

pytestmark = pytest.mark.local_only


class _StubSpec:
    reference = "s3://bucket/fn.json"
    # create_function_parameters() reads .function.parameter_schema to decide
    # whether a typed FunctionParameters subclass is called for; None means the
    # plain base class.
    function = None
    artifacts = {}
    uuid = "fn-uuid"
    name = "stub_fn"
    input_spec = {}
    output_spec = {}


class _StubFunction:
    """Minimal MetaflowFunction stand-in.

    ``_func is None`` makes it look like a proxy handle, which is what
    LocalBackend._is_proxy() keys off.
    """

    def __init__(self, root_dir=None, concrete=False):
        self._func = object() if concrete else None
        self._function_spec = _StubSpec()
        self._function_root_dir = root_dir
        self._component_instances = []
        self._runtime_components = []
        self.calls = []

    @property
    def spec(self):
        return self._function_spec

    @property
    def name(self):
        return self._function_spec.name

    @property
    def function_root_dir(self):
        if self._function_root_dir is None:
            raise RuntimeError("Function root dir is not set.")
        return self._function_root_dir

    def execute(self, data, parameters, **kwargs):
        self.calls.append((data, parameters))
        return data


@pytest.fixture
def concrete(tmp_path):
    return _StubFunction(root_dir=str(tmp_path), concrete=True)


@pytest.fixture
def proxy(monkeypatch, concrete):
    """A proxy handle whose hydration is stubbed out to return `concrete`."""
    hydrations = []

    def fake_hydrate(func_instance):
        hydrations.append(func_instance)
        return concrete

    monkeypatch.setattr(LocalBackend, "_hydrate", staticmethod(fake_hydrate))
    handle = _StubFunction(root_dir=str(concrete.function_root_dir))
    handle.hydrations = hydrations
    return handle


@pytest.fixture(autouse=True)
def restore_sys_path():
    original = sys.path.copy()
    yield
    sys.path[:] = original


def test_start_caches_concrete_and_params(proxy, concrete):
    LocalBackend.start(proxy)

    assert proxy._local_concrete is concrete
    assert proxy._local_params is not None
    # Cached on both handles - apply() may be handed either one.
    assert concrete._local_concrete is concrete
    assert concrete._local_params is proxy._local_params


def test_start_puts_function_root_dir_on_sys_path(proxy, concrete):
    root = concrete.function_root_dir
    assert root not in sys.path

    LocalBackend.start(proxy)

    assert sys.path[0] == root


def test_start_is_idempotent_on_sys_path(proxy, concrete):
    LocalBackend.start(proxy)
    LocalBackend.start(proxy)

    assert sys.path.count(concrete.function_root_dir) == 1


def test_start_tolerates_missing_root_dir(monkeypatch, proxy, concrete):
    concrete._function_root_dir = None
    before = sys.path.copy()

    LocalBackend.start(proxy)

    assert sys.path == before
    assert proxy._local_concrete is concrete


def test_start_honours_prefetch_artifacts(monkeypatch, proxy):
    seen = {}

    def fake_create(func_spec, prefetch_artifacts=False):
        seen["prefetch"] = prefetch_artifacts
        return "params"

    monkeypatch.setattr(
        "metaflow_extensions.nflx.plugins.functions.backends.local.local_backend.create_function_parameters",
        fake_create,
    )

    proxy._prefetch_artifacts = True
    LocalBackend.start(proxy)

    assert seen["prefetch"] is True
    assert proxy._local_params == "params"


def test_apply_after_start_does_not_rehydrate(proxy, concrete):
    LocalBackend.start(proxy)
    assert len(proxy.hydrations) == 1

    LocalBackend.apply(proxy, "payload")
    LocalBackend.apply(proxy, "payload")

    # Still only the one hydration from start().
    assert len(proxy.hydrations) == 1
    assert len(concrete.calls) == 2


def test_apply_reuses_started_params(proxy, concrete):
    LocalBackend.start(proxy)
    LocalBackend.apply(proxy, "payload")

    _, parameters = concrete.calls[0]
    assert parameters is proxy._local_params


def test_explicit_params_kwarg_still_wins(proxy, concrete):
    LocalBackend.start(proxy)
    LocalBackend.apply(proxy, "payload", params="explicit")

    _, parameters = concrete.calls[0]
    assert parameters == "explicit"


def test_apply_without_start_still_works(proxy, concrete):
    LocalBackend.apply(proxy, "payload")

    assert len(proxy.hydrations) == 1
    assert len(concrete.calls) == 1


def test_close_clears_the_warm_cache(proxy, concrete):
    LocalBackend.start(proxy)
    LocalBackend.close(proxy)

    assert proxy._local_concrete is None
    assert proxy._local_params is None
    assert concrete._local_concrete is None


def test_close_stops_components_on_the_concrete_handle(proxy, concrete):
    stopped = []

    class _Component:
        def stop(self, *args, **kwargs):
            stopped.append(self)

    LocalBackend.start(proxy)
    # apply() starts components on the concrete function, not the proxy.
    concrete._component_instances = [_Component()]

    LocalBackend.close(proxy)

    assert len(stopped) == 1
    assert concrete._component_instances == []
