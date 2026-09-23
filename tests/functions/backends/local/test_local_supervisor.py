"""Warm in-process runtimes: start(), sharing, refcounted teardown.

Uses real AvroFunction handles (as tests/functions/backends/local/
test_needs_hydration.py does) so the runtime key is the real content-hash
uuid; only the code-package download is stubbed out.
"""

import json
import sys
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.no_backend_parametrization

from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.avro_function import AvroFunction, avro_function
from metaflow_extensions.nflx.plugins.functions.backends.local import local_backend
from metaflow_extensions.nflx.plugins.functions.backends.local import supervisor as sup
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
def supervisor(monkeypatch):
    """A fresh supervisor per test.

    The real one is a process-global singleton that other test modules attach
    handles to and never detach, so a shared one makes these tests depend on
    what ran before them.
    """
    fresh = sup.LocalSupervisor()
    monkeypatch.setattr(local_backend, "local_supervisor", fresh)
    monkeypatch.setattr(sup, "_SYS_PATH_REFCOUNTS", {})
    return fresh


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
    # _export is stubbed out above, so nothing has assigned the content hash
    # the runtime key is built from.
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

    monkeypatch.setattr(sup, "_hydrate", fake_hydrate)
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


# --- sharing and refcounted teardown ---------------------------------------


def test_two_handles_on_the_same_reference_share_one_runtime(hydration):
    spec = _concrete_function().spec
    first, second = _proxy_function(spec), _proxy_function(spec)

    LocalBackend.start(first)
    LocalBackend.start(second)

    assert len(hydration.calls) == 1
    assert first._runtime_id == second._runtime_id


def test_closing_one_handle_leaves_the_other_working(hydration):
    spec = _concrete_function().spec
    first, second = _proxy_function(spec), _proxy_function(spec)
    LocalBackend.start(first)
    LocalBackend.start(second)

    LocalBackend.close(first)

    assert hydration.root in sys.path
    assert LocalBackend.apply(second, "payload") == "payload"
    assert len(hydration.calls) == 1

    LocalBackend.close(second)
    assert hydration.root not in sys.path


def test_different_components_do_not_share_a_runtime(hydration, monkeypatch):
    from metaflow_extensions.nflx.plugins.functions.components.runtime_metrics import (
        RuntimeMetrics,
    )

    spec = _concrete_function().spec
    plain = _proxy_function(spec)
    with_component = _proxy_function(spec)
    with_component._runtime_components = [RuntimeMetrics()]

    LocalBackend.start(plain)
    LocalBackend.start(with_component)

    assert plain._runtime_id != with_component._runtime_id
    assert len(hydration.calls) == 2


def test_close_on_a_handle_that_was_never_started_is_a_no_op():
    LocalBackend.close(_proxy_function())


# --- lease accounting ------------------------------------------------------


def test_in_flight_count_returns_to_zero(supervisor, hydration):
    proxy = _proxy_function()
    LocalBackend.apply(proxy, "payload")

    entry = supervisor._process_map[proxy._runtime_id]
    assert entry.leased == 0
    assert entry.attached == 1


def test_in_flight_count_returns_to_zero_when_the_function_raises(
    monkeypatch, supervisor, hydration
):
    def boom(self, data, params, **kwargs):
        raise ValueError("nope")

    monkeypatch.setattr(AvroFunction, "execute", boom)
    proxy = _proxy_function()

    with pytest.raises(Exception):
        LocalBackend.apply(proxy, "payload")

    entry = supervisor._process_map[proxy._runtime_id]
    assert entry.leased == 0


# --- kwargs ----------------------------------------------------------------


def test_explicit_params_win_over_the_cached_ones(monkeypatch, hydration):
    seen = []
    monkeypatch.setattr(
        AvroFunction,
        "execute",
        lambda self, data, params, **kwargs: seen.append(params) or data,
    )
    proxy = _proxy_function()
    mine = FunctionParameters()

    LocalBackend.apply(proxy, "payload", params=mine)
    LocalBackend.apply(proxy, "payload")

    assert seen[0] is mine
    assert seen[1] is not mine


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


def test_component_output_reaches_a_handle_that_did_not_create_the_runtime(
    hydration,
):
    """The runtime runs its creator's component instances, so a second handle's
    own instances only see output if the backend routes it back by id."""
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

    spec = _concrete_function().spec
    first, second = _proxy_function(spec), _proxy_function(spec)
    first._runtime_components = [_Collector()]
    second._runtime_components = [_Collector()]

    LocalBackend.apply(first, "payload")
    assert len(hydration.calls) == 1

    LocalBackend.apply(second, "payload")

    assert first._runtime_components[0].output == "collected"
    assert second._runtime_components[0].output == "collected"
