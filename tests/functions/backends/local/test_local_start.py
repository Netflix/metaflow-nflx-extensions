"""Unit tests for LocalBackend.start() warm-up.

These exercise start()/apply()/close() against a stub function handle rather
than a real code package: what is under test is the caching contract between
the three, not code-package extraction.
"""

import sys
import threading

import pytest

from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
    _SYS_PATH_REFCOUNTS,
    LocalBackend,
)
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
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

    ``_is_proxy_handle`` is the marker LocalBackend._is_proxy() keys off --
    ``_func`` alone cannot say, since a concrete pipeline also has none.
    """

    def __init__(self, root_dir=None, concrete=False, functions=()):
        self._func = object() if concrete else None
        self._is_proxy_handle = not concrete
        self.functions = list(functions)
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
            raise MetaflowFunctionException("Function root dir is not set.")
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
    _SYS_PATH_REFCOUNTS.clear()


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


def test_close_takes_the_root_dir_back_off_sys_path(proxy, concrete):
    root = concrete.function_root_dir

    LocalBackend.start(proxy)
    assert root in sys.path

    LocalBackend.close(proxy)

    # A server that loads and unloads functions must not leave dead entries at
    # the front of every import search.
    assert root not in sys.path
    assert proxy._local_sys_path is None


def test_a_shared_root_dir_survives_closing_one_of_its_functions(
    monkeypatch, tmp_path
):
    """Two functions out of one code package share a directory.

    Closing the first must not break the second's deferred imports, which is
    why the path entries are refcounted rather than simply removed.
    """
    root = str(tmp_path)
    first_concrete = _StubFunction(root_dir=root, concrete=True)
    second_concrete = _StubFunction(root_dir=root, concrete=True)

    LocalBackend.start(first_concrete)
    LocalBackend.start(second_concrete)
    assert sys.path.count(root) == 1

    LocalBackend.close(first_concrete)
    assert root in sys.path

    LocalBackend.close(second_concrete)
    assert root not in sys.path


def test_second_start_does_not_replace_the_warm_function(proxy, concrete):
    """start() is idempotent in hydration, not just in sys.path.

    Components start lazily on whichever handle apply() ran and close() stops
    only the cached one, so replacing the cache would strand the first
    concrete's components with nothing left holding them.
    """
    LocalBackend.start(proxy)
    first = proxy._local_concrete
    concrete._component_instances = ["live-component"]

    LocalBackend.start(proxy)

    assert len(proxy.hydrations) == 1
    assert proxy._local_concrete is first
    assert concrete._component_instances == ["live-component"]


def test_start_does_not_hydrate_a_concrete_pipeline(monkeypatch, tmp_path):
    """A concrete pipeline has _func None but owns its code already.

    Hydrating it would download and extract every constituent's package a
    second time and leave a second live pipeline behind.
    """
    hydrations = []
    monkeypatch.setattr(
        LocalBackend,
        "_hydrate",
        staticmethod(lambda handle: hydrations.append(handle)),
    )

    pipeline = _StubFunction(root_dir=str(tmp_path), concrete=True)
    pipeline._func = None  # exactly what FunctionPipeline._create_from_spec leaves

    LocalBackend.start(pipeline)

    assert hydrations == []
    assert pipeline._local_concrete is pipeline


def test_start_puts_every_constituent_dir_on_sys_path(monkeypatch, tmp_path):
    """A pipeline reports only its first constituent's root dir.

    A deferred import in constituent #2 -- generated protobuf stubs, the case
    start() exists for -- has to resolve too.
    """
    first = _StubFunction(root_dir=str(tmp_path / "fn1"), concrete=True)
    second = _StubFunction(root_dir=str(tmp_path / "fn2"), concrete=True)
    pipeline = _StubFunction(
        root_dir=str(first.function_root_dir), concrete=True, functions=[first, second]
    )

    LocalBackend.start(pipeline)

    assert str(tmp_path / "fn1") in sys.path
    assert str(tmp_path / "fn2") in sys.path

    LocalBackend.close(pipeline)

    assert str(tmp_path / "fn1") not in sys.path
    assert str(tmp_path / "fn2") not in sys.path


def test_close_clears_state_even_when_a_component_fails_to_stop(proxy, concrete):
    """stop_components() raises by design when a component's stop() fails.

    A component that failed to stop is not one to keep calling, and the dead
    function must not stay cached behind it.
    """

    class _Component:
        def stop(self, *args, **kwargs):
            raise RuntimeError("flush timed out")

    LocalBackend.start(proxy)
    root = concrete.function_root_dir
    concrete._component_instances = [_Component()]

    with pytest.raises(Exception):
        LocalBackend.close(proxy)

    assert concrete._component_instances == []
    assert proxy._local_concrete is None
    assert concrete._local_concrete is None
    assert root not in sys.path


def test_concurrent_start_on_one_handle_hydrates_once(monkeypatch, tmp_path):
    """start() is check-then-act, so it holds the handle's lock.

    Two threads passing the "already warm?" test would both hydrate and both
    take a sys.path reference, leaving the directory stuck on the path forever
    and one hydrated concrete orphaned with any components it started.
    """
    root = str(tmp_path)
    concrete = _StubFunction(root_dir=root, concrete=True)
    hydrations = []
    ready = threading.Barrier(2)

    def slow_hydrate(func_instance):
        hydrations.append(func_instance)
        return concrete

    monkeypatch.setattr(LocalBackend, "_hydrate", staticmethod(slow_hydrate))
    handle = _StubFunction(root_dir=root)

    def warm():
        ready.wait()
        LocalBackend.start(handle)

    threads = [threading.Thread(target=warm) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(hydrations) == 1
    assert _SYS_PATH_REFCOUNTS[root] == 1

    LocalBackend.close(handle)

    assert root not in sys.path


def test_run_in_path_leaves_a_concurrently_added_entry_alone(tmp_path):
    """run_in_path removes what it added, not a snapshot of the whole list.

    A snapshot restore deletes every sys.path change made while the load was
    open -- including a persistent entry start() added on another thread, which
    would then vanish while its refcount still claimed it was there.
    """
    from metaflow_extensions.nflx.plugins.functions.environment import run_in_path

    load_dir = str(tmp_path / "loading")
    other_dir = str(tmp_path / "warmed-elsewhere")
    (tmp_path / "loading").mkdir()

    def loader():
        # Stands in for another thread's start() landing mid-load.
        sys.path.insert(0, other_dir)
        return "loaded"

    assert run_in_path(loader, load_dir) == "loaded"

    assert other_dir in sys.path
    assert load_dir not in sys.path
