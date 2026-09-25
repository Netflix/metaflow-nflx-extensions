"""apply_async runs off the event loop instead of blocking it."""

import asyncio
import json
import threading
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.no_backend_parametrization

from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.avro_function import AvroFunction, avro_function
from metaflow_extensions.nflx.plugins.functions.backends.local import runtime as rt
from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
    LocalBackend,
)
from metaflow_extensions.nflx.plugins.functions.components.abstract_component import (
    AbstractRuntimeComponent,
)
from metaflow_extensions.nflx.plugins.functions.core.function import MetaflowFunction
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionUserException,
)


class _Task:
    pathspec = "Flow/1/step/task"
    code = SimpleNamespace(path="/tmp/code.tar")
    metadata_dict = {"conda_env_id": json.dumps(["test", "1", "linux-64"])}
    artifacts = []
    successful = True


class _Noop(AbstractRuntimeComponent):
    component_id = "test.noop"

    def start(self, *args, **kwargs):
        pass

    def stop(self, *args, **kwargs):
        pass

    def before_call(self, *args, **kwargs):
        pass

    def after_call(self, *args, **kwargs):
        pass


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


def _function(uuid="0" * 32):
    @avro_function
    def passthrough(
        data: str, params: FunctionParameters = FunctionParameters()
    ) -> str:
        return data

    func = AvroFunction(passthrough, task=_Task())
    func.spec.uuid = uuid
    return func


def test_it_returns_what_apply_returns():
    func = _function()

    assert asyncio.run(LocalBackend.apply_async(func, "payload")) == "payload"


def test_it_does_not_run_on_the_event_loop_thread(monkeypatch):
    ran_on = {}
    monkeypatch.setattr(
        AvroFunction,
        "execute",
        lambda self, data, params, **kwargs: ran_on.setdefault(
            "thread", threading.get_ident()
        )
        or data,
    )
    func = _function()

    async def go():
        await LocalBackend.apply_async(func, "payload")
        return threading.get_ident()

    loop_thread = asyncio.run(go())

    assert ran_on["thread"] != loop_thread


def test_the_loop_keeps_turning_during_the_call(monkeypatch):
    """The regression this PR exists for: a blocking apply_async starves
    everything else on a serving host's loop."""
    started = threading.Event()
    release = threading.Event()

    def slow(self, data, params, **kwargs):
        started.set()
        if not release.wait(2):
            # The loop never got to tick, so it never released us.
            return "loop-was-blocked"
        return data

    monkeypatch.setattr(AvroFunction, "execute", slow)
    func = _function()

    async def go():
        ticks = 0
        call = asyncio.ensure_future(LocalBackend.apply_async(func, "payload"))
        while not started.is_set():
            await asyncio.sleep(0.001)
        for _ in range(5):
            ticks += 1
            await asyncio.sleep(0.001)
        release.set()
        return ticks, await call

    ticks, result = asyncio.run(go())

    assert ticks == 5
    assert result == "payload"


def test_user_exceptions_propagate():
    def boom(self, data, params, **kwargs):
        raise ValueError("nope")

    func = _function()
    func.execute = boom.__get__(func, AvroFunction)

    with pytest.raises(MetaflowFunctionUserException, match="nope"):
        asyncio.run(LocalBackend.apply_async(func, "payload"))


def test_concurrent_calls_on_a_component_bearing_function_queue(monkeypatch):
    """The invocation guard refuses overlapping component-bearing calls. Async
    callers must queue for the slot, not get an exception -- serialising them
    is what they already got when apply_async ran on the loop."""
    overlap = []
    active = []
    lock = threading.Lock()

    def tracked(self, data, params, **kwargs):
        with lock:
            active.append(1)
            overlap.append(len(active))
        threading.Event().wait(0.02)
        with lock:
            active.pop()
        return data

    monkeypatch.setattr(AvroFunction, "execute", tracked)
    func = _function()
    func._runtime_components = [_Noop()]

    async def go():
        return await asyncio.gather(
            LocalBackend.apply_async(func, "a"),
            LocalBackend.apply_async(func, "b"),
        )

    assert asyncio.run(go()) == ["a", "b"]
    assert max(overlap) == 1


def test_successive_calls_on_one_handle_keep_the_same_worker():
    """A runtime is owned by the thread that claimed it, so every offloaded
    call for a handle has to land on that same thread. The default executor
    would spread them and the second call would be refused."""
    func = _function()
    threads = []

    async def go():
        for _ in range(4):
            await LocalBackend.apply_async(func, "payload")

    asyncio.run(go())
    asyncio.run(go())  # a second loop, same handle

    assert func._local_runtime.owner_thread is not None


def test_closing_releases_the_worker():
    from metaflow_extensions.nflx.plugins.functions.backends.local.runtime import (
        close_runtime,
    )

    func = _function()
    asyncio.run(LocalBackend.apply_async(func, "payload"))
    assert func._local_async_executor is not None

    close_runtime(func)

    assert func._local_async_executor is None


def test_component_free_calls_can_overlap(monkeypatch):
    barrier = threading.Barrier(2, timeout=5)

    def wait_for_the_other(self, data, params, **kwargs):
        barrier.wait()
        return data

    monkeypatch.setattr(AvroFunction, "execute", wait_for_the_other)
    first, second = _function("a" * 32), _function("b" * 32)

    async def go():
        return await asyncio.gather(
            LocalBackend.apply_async(first, "a"),
            LocalBackend.apply_async(second, "b"),
        )

    # Deadlocks on the barrier if the two calls are not genuinely concurrent.
    assert asyncio.run(go()) == ["a", "b"]
