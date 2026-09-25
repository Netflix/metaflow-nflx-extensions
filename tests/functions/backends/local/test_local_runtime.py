"""Warm in-process runtimes: start(), sys.path lifetime, refcounted teardown.

Uses real AvroFunction handles (as tests/functions/backends/local/
test_needs_hydration.py does); only the code-package download is stubbed out.
"""

import json
import sys
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.no_backend_parametrization

from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.avro_function import AvroFunction, avro_function
from metaflow_extensions.nflx.plugins.functions.backends.local import runtime as rt
from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
    LocalBackend,
)
from metaflow_extensions.nflx.plugins.functions.core.function import MetaflowFunction
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
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


@pytest.fixture(autouse=True)
def _fresh_refcounts(monkeypatch):
    """The refcount table is process-global; other modules leave entries in it."""
    monkeypatch.setattr(rt, "_SYS_PATH_REFCOUNTS", {})


@pytest.fixture(autouse=True)
def _restore_sys_path():
    original = sys.path.copy()
    yield
    sys.path[:] = original


def _concrete_function(root_dir=None, uuid="0" * 32):
    @avro_function
    def passthrough(
        data: str, params: FunctionParameters = FunctionParameters()
    ) -> str:
        return data

    func = AvroFunction(passthrough, task=_Task())
    func._function_root_dir = root_dir
    func.spec.uuid = uuid
    return func


def _proxy_function(spec=None):
    return AvroFunction._create_proxy_from_spec(
        spec if spec is not None else _concrete_function().spec
    )


@pytest.fixture
def hydration(monkeypatch, tmp_path):
    """Stub out the code-package download, counting hydrations."""
    concrete = _concrete_function(root_dir=str(tmp_path))
    calls = []

    def fake_hydrate(func_instance):
        calls.append(func_instance)
        # The real _hydrate carries the proxy's components onto the concrete.
        concrete._runtime_components = getattr(func_instance, "_runtime_components", [])
        return concrete

    monkeypatch.setattr(rt, "_hydrate", fake_hydrate)
    return SimpleNamespace(concrete=concrete, calls=calls, root=str(tmp_path))


# --- start() ---------------------------------------------------------------


def test_start_hydrates_and_apply_does_not_repeat_it(hydration):
    proxy = _proxy_function()

    LocalBackend.start(proxy)
    assert len(hydration.calls) == 1

    assert LocalBackend.apply(proxy, "payload") == "payload"
    assert LocalBackend.apply(proxy, "payload") == "payload"
    assert len(hydration.calls) == 1


def test_apply_without_start_still_works_and_hydrates_once(hydration):
    proxy = _proxy_function()

    assert LocalBackend.apply(proxy, "payload") == "payload"
    assert LocalBackend.apply(proxy, "payload") == "payload"
    assert len(hydration.calls) == 1


def test_start_is_idempotent(hydration):
    proxy = _proxy_function()

    LocalBackend.start(proxy)
    LocalBackend.start(proxy)

    assert len(hydration.calls) == 1
    assert sys.path.count(hydration.root) == 1


def test_start_puts_the_function_root_on_sys_path_persistently(hydration):
    proxy = _proxy_function()
    assert hydration.root not in sys.path

    LocalBackend.start(proxy)

    assert sys.path[0] == hydration.root


def test_close_takes_the_sys_path_entry_back(hydration):
    proxy = _proxy_function()
    LocalBackend.start(proxy)

    LocalBackend.close(proxy)

    assert hydration.root not in sys.path


def test_close_then_apply_warms_again(hydration):
    proxy = _proxy_function()
    LocalBackend.start(proxy)
    LocalBackend.close(proxy)

    assert LocalBackend.apply(proxy, "payload") == "payload"

    assert len(hydration.calls) == 2
    assert hydration.root in sys.path


# --- one runtime per handle ------------------------------------------------


def test_each_handle_gets_its_own_runtime(hydration):
    spec = _concrete_function().spec
    first, second = _proxy_function(spec), _proxy_function(spec)

    LocalBackend.start(first)
    LocalBackend.start(second)

    assert first._local_runtime is not second._local_runtime
    assert len(hydration.calls) == 2


def test_closing_one_handle_leaves_the_others_sys_path_entry(hydration):
    """Why the refcount exists: two handles out of one code package."""
    spec = _concrete_function().spec
    first, second = _proxy_function(spec), _proxy_function(spec)
    LocalBackend.start(first)
    LocalBackend.start(second)

    LocalBackend.close(first)

    assert hydration.root in sys.path
    assert LocalBackend.apply(second, "payload") == "payload"

    LocalBackend.close(second)
    assert hydration.root not in sys.path


def test_close_on_a_handle_that_was_never_started_is_a_no_op():
    LocalBackend.close(_proxy_function())


def test_a_handle_that_is_not_a_metaflow_function_is_refused():
    with pytest.raises(MetaflowFunctionException, match="runs MetaflowFunction"):
        LocalBackend.apply(object(), "payload")


# --- thread affinity ------------------------------------------------------


def _call_on_new_thread(fn):
    """Run fn() on another thread, returning its result or its exception."""
    import threading

    box = {}

    def target():
        try:
            box["value"] = fn()
        except BaseException as e:  # noqa: BLE001
            box["error"] = e

    t = threading.Thread(target=target)
    t.start()
    t.join()
    return box


def test_a_second_thread_is_refused_even_without_overlapping(hydration):
    """Affinity, not a lock: the calls here are strictly sequential."""
    proxy = _proxy_function()
    LocalBackend.apply(proxy, "payload")

    box = _call_on_new_thread(lambda: LocalBackend.apply(proxy, "payload"))

    assert isinstance(box.get("error"), MetaflowFunctionException)
    assert "owned by thread" in str(box["error"])


def test_the_owning_thread_keeps_working(hydration):
    proxy = _proxy_function()

    assert LocalBackend.apply(proxy, "a") == "a"
    assert LocalBackend.apply(proxy, "b") == "b"


def test_close_releases_the_claim(hydration):
    proxy = _proxy_function()
    LocalBackend.apply(proxy, "payload")
    LocalBackend.close(proxy)

    box = _call_on_new_thread(lambda: LocalBackend.apply(proxy, "payload"))

    assert "error" not in box, box.get("error")
    assert box["value"] == "payload"


def test_one_handle_per_thread_is_the_supported_pattern(hydration):
    """What the refusal message tells callers to do has to actually work."""
    mine = _proxy_function()
    theirs = _proxy_function()

    assert LocalBackend.apply(mine, "payload") == "payload"
    box = _call_on_new_thread(lambda: LocalBackend.apply(theirs, "payload"))

    assert "error" not in box, box.get("error")
    assert len(hydration.calls) == 2


def test_two_simultaneous_first_calls_hydrate_once(hydration):
    """The claim is taken under a lock, so only one thread reaches hydration."""
    import threading
    from concurrent.futures import ThreadPoolExecutor

    proxy = _proxy_function()
    start = threading.Barrier(2)

    def call(_):
        start.wait()
        try:
            return LocalBackend.apply(proxy, "payload")
        except MetaflowFunctionException:
            return None

    with ThreadPoolExecutor(max_workers=2) as ex:
        results = list(ex.map(call, range(2)))

    assert len(hydration.calls) == 1
    assert sorted(results, key=lambda r: r is None) == ["payload", None]
    assert sys.path.count(hydration.root) == 1


# --- kwargs ----------------------------------------------------------------


def test_a_caller_cannot_substitute_its_own_params(monkeypatch, hydration):
    """Parameters are bound to the loaded function, as they are on memory."""
    seen = []
    monkeypatch.setattr(
        AvroFunction,
        "execute",
        lambda self, data, params, **kwargs: seen.append(params) or data,
    )
    proxy = _proxy_function()
    mine = FunctionParameters()

    LocalBackend.apply(proxy, "payload", params=mine)

    assert seen[0] is not mine


def test_the_cached_params_are_built_once(monkeypatch, hydration):
    seen = []
    monkeypatch.setattr(
        AvroFunction,
        "execute",
        lambda self, data, params, **kwargs: seen.append(params) or data,
    )
    proxy = _proxy_function()

    LocalBackend.apply(proxy, "payload")
    LocalBackend.apply(proxy, "payload")

    assert seen[0] is seen[1]


def test_backend_keywords_do_not_reach_the_function(monkeypatch, hydration):
    seen = []
    monkeypatch.setattr(
        AvroFunction,
        "execute",
        lambda self, data, params, **kwargs: seen.append(kwargs) or data,
    )
    proxy = _proxy_function()

    LocalBackend.apply(proxy, "payload", process=1, params=FunctionParameters())

    assert seen == [{}]


def test_process_greater_than_one_is_refused(hydration):
    proxy = _proxy_function()

    with pytest.raises(MetaflowFunctionException, match="cannot provide 2 workers"):
        LocalBackend.apply(proxy, "payload", process=2)

    assert hydration.calls == []


# --- component output ------------------------------------------------------


def test_component_output_reaches_the_callers_own_handle(hydration):
    """A hydrated handle runs the concrete function's component instances, so
    the proxy the caller holds only sees output if the backend routes it back
    by id."""
    from metaflow_extensions.nflx.plugins.functions.components.abstract_component import (
        AbstractRuntimeComponent,
    )

    class _Collector(AbstractRuntimeComponent):
        component_id = "test.collector"

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

        def collect_output(self, *args, **kwargs):
            return "collected"

    proxy = _proxy_function()
    proxy._runtime_components = [_Collector()]

    LocalBackend.apply(proxy, "payload")

    assert proxy._runtime_components[0].output == "collected"
