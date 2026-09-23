"""Unit tests for LocalBackend.start() and the warm pool behind it.

These exercise start()/apply()/close() against a stub function handle rather
than a real code package: what is under test is the pooling contract between
the three, not code-package extraction.
"""

import sys

import pytest

from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
    _SYS_PATH_REFCOUNTS,
    _WARM_POOL,
    LocalBackend,
)
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
    MetaflowFunctionRuntimeException,
)

pytestmark = pytest.mark.local_only


class _StubSpec:
    reference = "s3://bucket/fn.json"
    # create_function_parameters() reads .function.parameter_schema to decide
    # whether a typed FunctionParameters subclass is called for; None means the
    # plain base class.
    function = None
    artifacts = {}
    name = "stub_fn"
    input_spec = {}
    output_spec = {}


class _StubFunction:
    """Minimal MetaflowFunction stand-in."""

    def __init__(self, root_dir=None, concrete=False, uuid="fn-uuid", components=()):
        self._func = object() if concrete else None
        # None, not [], so _needs_hydration() reads this as a plain function
        # rather than a pipeline with no constituents.
        self.functions = None
        self.uuid = uuid
        self._function_spec = _StubSpec()
        self._function_root_dir = root_dir
        self._component_instances = []
        self._runtime_components = list(components)
        self._runtime_id = None
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
            raise MetaflowFunctionException("Function root dir is not set.")
        return self._function_root_dir

    def execute(self, data, parameters, **kwargs):
        self.calls.append((data, parameters, kwargs))
        return data


@pytest.fixture(autouse=True)
def clean_pool():
    original_path = sys.path.copy()
    yield
    assert not _WARM_POOL, "test leaked warm-pool entries"
    sys.path[:] = original_path
    _WARM_POOL.clear()
    _SYS_PATH_REFCOUNTS.clear()


@pytest.fixture
def concrete(tmp_path):
    return _StubFunction(root_dir=str(tmp_path), concrete=True)


@pytest.fixture
def hydrate_to(monkeypatch):
    """Point LocalBackend._hydrate() at a fixed concrete function."""

    def _install(target):
        hydrations = []

        def fake_hydrate(func_instance):
            hydrations.append(func_instance)
            return target

        monkeypatch.setattr(LocalBackend, "_hydrate", staticmethod(fake_hydrate))
        return hydrations

    return _install


@pytest.fixture
def proxy(hydrate_to, concrete):
    handle = _StubFunction()
    handle.hydrations = hydrate_to(concrete)
    return handle


def test_start_hydrates_once_and_apply_reuses_it(proxy, concrete):
    LocalBackend.start(proxy)
    LocalBackend.apply(proxy, "a")
    LocalBackend.apply(proxy, "b")

    assert len(proxy.hydrations) == 1
    assert [c[0] for c in concrete.calls] == ["a", "b"]

    LocalBackend.close(proxy)


def test_start_puts_function_root_dir_on_sys_path(proxy, concrete):
    root = concrete.function_root_dir
    assert root not in sys.path

    LocalBackend.start(proxy)

    assert sys.path[0] == root

    LocalBackend.close(proxy)
    assert root not in sys.path


def test_start_is_idempotent(proxy, concrete):
    LocalBackend.start(proxy)
    LocalBackend.start(proxy)

    assert len(proxy.hydrations) == 1
    assert sys.path.count(concrete.function_root_dir) == 1
    assert _WARM_POOL[proxy._runtime_id].attached == 1

    LocalBackend.close(proxy)


def test_two_handles_share_one_entry(hydrate_to, concrete):
    first, second = _StubFunction(), _StubFunction()
    hydrations = hydrate_to(concrete)

    LocalBackend.start(first)
    LocalBackend.start(second)

    assert len(hydrations) == 1
    assert first._runtime_id == second._runtime_id
    assert _WARM_POOL[first._runtime_id].attached == 2

    LocalBackend.close(first)
    LocalBackend.close(second)


def test_close_tears_down_only_at_the_last_handle(hydrate_to, concrete):
    first, second = _StubFunction(), _StubFunction()
    hydrate_to(concrete)
    root = concrete.function_root_dir

    LocalBackend.start(first)
    LocalBackend.start(second)
    LocalBackend.close(first)

    # The entry, and the sys.path entry it holds, survive for `second`.
    assert root in sys.path
    LocalBackend.apply(second, "still works")
    assert concrete.calls[-1][0] == "still works"

    LocalBackend.close(second)
    assert not _WARM_POOL
    assert root not in sys.path


def test_different_components_do_not_share(hydrate_to, concrete, monkeypatch):
    monkeypatch.setattr(
        "metaflow_extensions.nflx.plugins.functions.components.runtime"
        ".serialize_components",
        lambda components: [str(c) for c in components],
    )
    plain = _StubFunction()
    with_component = _StubFunction(components=("metrics",))
    hydrations = hydrate_to(concrete)

    LocalBackend.start(plain)
    LocalBackend.start(with_component)

    assert plain._runtime_id != with_component._runtime_id
    assert len(hydrations) == 2

    LocalBackend.close(plain)
    LocalBackend.close(with_component)


def test_apply_without_start_still_works(proxy, concrete):
    result = LocalBackend.apply(proxy, "payload")

    assert result == "payload"
    assert len(proxy.hydrations) == 1

    LocalBackend.close(proxy)


def test_explicit_params_beat_the_cached_ones(proxy, concrete):
    LocalBackend.start(proxy)
    sentinel = object()

    LocalBackend.apply(proxy, "x", params=sentinel)

    assert concrete.calls[-1][1] is sentinel

    LocalBackend.close(proxy)


def test_close_without_start_is_a_no_op(proxy):
    LocalBackend.close(proxy)

    assert proxy._runtime_id is None
    assert not _WARM_POOL


def test_start_refuses_multiple_processes(proxy):
    with pytest.raises(MetaflowFunctionRuntimeException, match="process=4"):
        LocalBackend.start(proxy, process=4)

    assert not _WARM_POOL


def test_apply_refuses_multiple_processes(proxy):
    with pytest.raises(MetaflowFunctionRuntimeException, match="process=2"):
        LocalBackend.apply(proxy, "x", process=2)

    assert not _WARM_POOL


def test_process_one_is_not_forwarded_to_the_user_function(proxy, concrete):
    LocalBackend.apply(proxy, "x", process=1)

    assert "process" not in concrete.calls[-1][2]

    LocalBackend.close(proxy)


def test_a_function_with_no_uuid_runs_unpooled(tmp_path):
    # A function built in this process, never published: no uuid, so no
    # identity to pool on. It still runs.
    local = _StubFunction(root_dir=str(tmp_path), concrete=True, uuid=None)

    assert LocalBackend.apply(local, "x") == "x"
    assert local._runtime_id is None
    assert not _WARM_POOL

    LocalBackend.start(local)
    assert not _WARM_POOL
