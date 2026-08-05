"""
Tests for the runtime components system.

Covers:
  1. Lifecycle mechanics — start/stop/before_call/after_call fire in the right order
  2. active_instance — set on start, cleared on stop
  3. Serialisation — serialize_components / load_component_instances round-trip
  4. Local backend integration — lifecycle fires end-to-end through LocalBackend.apply()
  5. Memory backend serialisation — component specs round-trip through connection_params
     and CLI args without spawning a subprocess
  6. configure() — class-level config accumulation, per-subclass isolation, and
     visibility from start()

The test component (RecordingComponent) writes one line per lifecycle event to a
temporary file so tests that run in a subprocess (e.g. memory backend) can verify
events without shared in-process state.  In-process tests can read the same file.
"""

import os
import tempfile

import pytest

pytestmark = pytest.mark.no_backend_parametrization

from metaflow_extensions.nflx.plugins.functions.components.abstract_component import (
    AbstractRuntimeComponent,
    ComponentMeta,
)
from metaflow_extensions.nflx.plugins.functions.components.runtime import (
    serialize_components,
    load_component_instances,
    start_components,
    stop_components,
    before_call_components,
    after_call_components,
)
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
    MetaflowFunctionRuntimeException,
)


# ---------------------------------------------------------------------------
# Concrete test component
# ---------------------------------------------------------------------------


class RecordingComponent(AbstractRuntimeComponent):
    """
    Component that appends one line per lifecycle event to a file.

    Set ``RecordingComponent._log_path`` to a writable file path before
    activating the component.  Each event line has the form::

        start
        before_call
        call:<message>
        after_call
        stop
    """

    _log_path: str = ""  # set by each test before use
    component_id = "recording"

    def _write(self, event: str) -> None:
        with open(type(self)._log_path, "a") as fh:
            fh.write(event + "\n")

    def start(self, *args, **kwargs) -> None:
        self._write("start")

    def stop(self, *args, **kwargs) -> None:
        self._write("stop")

    def before_call(self, *args, **kwargs) -> None:
        self._write("before_call")

    def after_call(self, *args, **kwargs) -> None:
        self._write("after_call")

    def __call__(self, message: str = "", **kwargs) -> None:
        self._write(f"call:{message}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_events(path: str):
    with open(path) as fh:
        return [line.strip() for line in fh if line.strip()]


def _tmp_log():
    """Return a path to a fresh empty temp file."""
    fd, path = tempfile.mkstemp(prefix="mff_component_test_", suffix=".log")
    os.close(fd)
    return path


# ---------------------------------------------------------------------------
# 1. Lifecycle unit tests
# ---------------------------------------------------------------------------


def test_component_lifecycle_order():
    """start → before_call → after_call → stop fire in the right order."""
    log = _tmp_log()
    RecordingComponent._log_path = log
    try:
        instances = start_components([RecordingComponent()])
        before_call_components(instances)
        after_call_components(instances)
        stop_components(instances)

        assert _read_events(log) == ["start", "before_call", "after_call", "stop"]
    finally:
        RecordingComponent._log_path = ""
        os.unlink(log)


def test_active_instance_is_scoped_to_an_invocation_not_to_start():
    """active_instance names the instance whose call is in flight.

    Deliberately *not* set by start_components: setting it at start time made
    routing wrong in two opposite ways -- one function copy invoked from several
    threads only logged from the thread that started it, and one copy loaded per
    thread had each copy overwrite the others. Scoping it to before/after_call
    means whichever instance is actually serving the call is the one that
    receives user-facing classmethod calls.
    """
    log = _tmp_log()
    RecordingComponent._log_path = log
    try:
        instances = start_components([RecordingComponent()])
        assert RecordingComponent.active_instance is None  # not started-scoped

        before_call_components(instances)
        assert RecordingComponent.active_instance is instances[0]

        after_call_components(instances)
        assert RecordingComponent.active_instance is None  # cleared per call

        stop_components(instances)
        assert RecordingComponent.active_instance is None
    finally:
        RecordingComponent._log_path = ""
        os.unlink(log)


def test_stop_clears_instance_even_on_error():
    """stop() deactivates the class even if stop() itself raises."""

    class BrokenStop(AbstractRuntimeComponent):
        component_id = "broken_stop"

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            raise RuntimeError("boom")

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

    instances = start_components([BrokenStop()])
    # active_instance is invocation-scoped now, so simulate being mid-call.
    before_call_components(instances)
    assert BrokenStop.active_instance is not None

    with pytest.raises(MetaflowFunctionException, match="boom"):
        stop_components(instances)

    # active_instance must be cleared regardless
    assert BrokenStop.active_instance is None


def test_stop_components_is_best_effort_across_failures():
    """A failing stop() must not prevent later components from stopping.

    All instances should be deactivated and every stop() should run, even
    when an earlier one raises; the errors are aggregated and re-raised once
    all instances have been drained.
    """

    class BrokenStopFirst(AbstractRuntimeComponent):
        component_id = "broken_stop_first"

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            raise RuntimeError("first boom")

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

    class HealthyStop(AbstractRuntimeComponent):
        component_id = "healthy_stop"
        stopped = False

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            type(self).stopped = True

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

    instances = start_components([BrokenStopFirst(), HealthyStop()])
    # active_instance is invocation-scoped now, so simulate being mid-call.
    before_call_components(instances)
    assert BrokenStopFirst.active_instance is not None
    assert HealthyStop.active_instance is not None

    with pytest.raises(MetaflowFunctionException, match="1 runtime component"):
        stop_components(instances)

    # The later component's stop() must still have run...
    assert HealthyStop.stopped is True
    # ...and every instance must be deactivated, regardless of failure.
    assert BrokenStopFirst.active_instance is None
    assert HealthyStop.active_instance is None


def test_start_failure_rolls_back_already_started_components():
    """A failing start() must stop the components started before it and
    raise a MetaflowFunctionRuntimeException (a system error), never leaving
    a partially-started, unreferenced set of live components behind.
    """

    class HealthyStart(AbstractRuntimeComponent):
        component_id = "healthy_start"
        stopped = False

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            type(self).stopped = True

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

    class BrokenStart(AbstractRuntimeComponent):
        component_id = "broken_start"

        def start(self, *args, **kwargs):
            raise RuntimeError("start boom")

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

    with pytest.raises(MetaflowFunctionRuntimeException, match="start boom"):
        start_components([HealthyStart(), BrokenStart()])

    # The already-started component must have been rolled back (stopped and
    # deactivated), and the one that never finished starting must also be
    # deactivated.
    assert HealthyStart.stopped is True
    assert HealthyStart.active_instance is None
    assert BrokenStart.active_instance is None


def test_start_failure_rollback_is_best_effort():
    """If rollback's own stop() also fails, the original start error still
    surfaces (rollback failures must not mask the triggering failure).
    """

    class BrokenStopOnRollback(AbstractRuntimeComponent):
        component_id = "broken_stop_on_rollback"

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            raise RuntimeError("rollback stop boom")

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

    class BrokenStart(AbstractRuntimeComponent):
        component_id = "broken_start_2"

        def start(self, *args, **kwargs):
            raise RuntimeError("start boom")

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

    with pytest.raises(MetaflowFunctionRuntimeException, match="start boom"):
        start_components([BrokenStopOnRollback(), BrokenStart()])

    assert BrokenStopOnRollback.active_instance is None


# ---------------------------------------------------------------------------
# 2. active_instance / serialisation
# ---------------------------------------------------------------------------


def test_active_instance_none_before_start():
    """active_instance is None before start_components is called."""
    assert RecordingComponent.active_instance is None


def test_serialize_instance_no_kwargs():
    """serialize_components produces 'module.ClassName:{}' for a no-kwargs instance."""
    specs = serialize_components([RecordingComponent()])
    assert len(specs) == 1
    fqn = f"{RecordingComponent.__module__}.{RecordingComponent.__qualname__}"
    assert specs[0] == f"{fqn}:{{}}"


def test_serialize_instance_with_kwargs():
    """serialize_components embeds init kwargs as JSON for an instance."""
    import json

    class KwargsComponent(AbstractRuntimeComponent):
        component_id = "kwargs_component"

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

    inst = KwargsComponent(stream="my_stream", version=3)
    specs = serialize_components([inst])
    assert len(specs) == 1
    class_part, kwargs_part = specs[0].split(":", 1)
    assert class_part.endswith("KwargsComponent")
    assert json.loads(kwargs_part) == {"stream": "my_stream", "version": 3}


def test_load_component_instances_roundtrip():
    """Fully-qualified class name deserialises to an instance of that class."""
    fqn = f"{RecordingComponent.__module__}.{RecordingComponent.__qualname__}"
    instances = load_component_instances([fqn])
    assert len(instances) == 1
    assert isinstance(instances[0], RecordingComponent)


def test_load_component_instances_with_kwargs():
    """'ClassName:json' deserialises to an instance constructed with those kwargs."""
    import json

    fqn = f"{RecordingComponent.__module__}.{RecordingComponent.__qualname__}"
    spec = f"{fqn}:{json.dumps({'key': 'val'})}"
    instances = load_component_instances([spec])
    assert isinstance(instances[0], RecordingComponent)
    assert instances[0]._init_kwargs == {"key": "val"}


def test_load_component_instances_unknown_raises():
    """load_component_instances raises MetaflowFunctionException for an unknown class."""
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionException,
    )

    with pytest.raises(MetaflowFunctionException):
        load_component_instances(["does.not.Exist"])


# ---------------------------------------------------------------------------
# 4. Local backend integration
# ---------------------------------------------------------------------------


class _MockFunction:
    """Minimal stand-in for a MetaflowFunction usable by LocalBackend.apply()."""

    name = "test_mock_function"

    def __init__(self, component_classes):
        self._runtime_components = component_classes
        self._component_instances = []

    def execute(self, data, params, **kwargs):
        return f"echo:{data}"


def test_local_backend_fires_lifecycle():
    """start fires once on first apply(); stop fires on close(); before/after wrap each call."""
    from metaflow import FunctionParameters
    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )

    log = _tmp_log()
    RecordingComponent._log_path = log
    try:
        func = _MockFunction([RecordingComponent()])

        result = LocalBackend.apply(func, "hello", params=FunctionParameters())
        assert result == "echo:hello"
        assert _read_events(log) == ["start", "before_call", "after_call"]

        # second call: start must NOT fire again
        LocalBackend.apply(func, "world", params=FunctionParameters())
        assert _read_events(log) == [
            "start",
            "before_call",
            "after_call",
            "before_call",
            "after_call",
        ]

        # stop fires only on close
        LocalBackend.close(func)
        assert _read_events(log) == [
            "start",
            "before_call",
            "after_call",
            "before_call",
            "after_call",
            "stop",
        ]
    finally:
        RecordingComponent._log_path = ""
        os.unlink(log)


def test_local_backend_stop_not_called_on_exception():
    """stop() is NOT called when execute() raises — consistent with memory/ray backends."""
    from metaflow import FunctionParameters
    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionUserException,
    )

    log = _tmp_log()
    RecordingComponent._log_path = log

    class FailingFunction(_MockFunction):
        def execute(self, data, params, **kwargs):
            raise ValueError("user error")

    try:
        func = FailingFunction([RecordingComponent()])
        with pytest.raises(MetaflowFunctionUserException):
            LocalBackend.apply(func, "x", params=FunctionParameters())

        events = _read_events(log)
        assert "start" in events
        assert "stop" not in events

        # stop fires when explicitly closed
        LocalBackend.close(func)
        assert "stop" in _read_events(log)
    finally:
        RecordingComponent._log_path = ""
        os.unlink(log)


def test_local_backend_after_call_runs_when_execute_raises():
    """after_call() still fires when execute() raises, so components (e.g.
    metrics/logging) see every invocation, not just successful ones.
    """
    from metaflow import FunctionParameters
    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionUserException,
    )

    log = _tmp_log()
    RecordingComponent._log_path = log

    class FailingFunction(_MockFunction):
        def execute(self, data, params, **kwargs):
            raise ValueError("user error")

    try:
        func = FailingFunction([RecordingComponent()])
        with pytest.raises(MetaflowFunctionUserException):
            LocalBackend.apply(func, "x", params=FunctionParameters())

        events = _read_events(log)
        assert events == ["start", "before_call", "after_call"]
    finally:
        RecordingComponent._log_path = ""
        os.unlink(log)


def test_local_backend_after_call_failure_does_not_mask_user_exception():
    """If after_call() also raises while a user exception is already in
    flight, the original user exception must win — a component failure on
    the error path must not mask it.
    """
    from metaflow import FunctionParameters
    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionUserException,
    )

    class FailingFunction(_MockFunction):
        def execute(self, data, params, **kwargs):
            raise ValueError("user error")

    FailingComponent.fail_after_call = True
    try:
        func = FailingFunction([FailingComponent()])
        with pytest.raises(MetaflowFunctionUserException, match="user error"):
            LocalBackend.apply(func, "x", params=FunctionParameters())
    finally:
        FailingComponent.fail_after_call = False


def test_local_backend_no_components():
    """LocalBackend.apply() works normally when _runtime_components is empty."""
    from metaflow import FunctionParameters
    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )

    func = _MockFunction([])
    result = LocalBackend.apply(func, "world", params=FunctionParameters())
    assert result == "echo:world"


class FailingComponent(AbstractRuntimeComponent):
    """Component whose before_call/after_call can be made to raise on demand."""

    component_id = "failing"
    fail_before_call = False
    fail_after_call = False

    def start(self, *args, **kwargs) -> None:
        pass

    def stop(self, *args, **kwargs) -> None:
        pass

    def before_call(self, *args, **kwargs) -> None:
        if type(self).fail_before_call:
            raise ValueError("before_call blew up")

    def after_call(self, *args, **kwargs) -> None:
        if type(self).fail_after_call:
            raise ValueError("after_call blew up")


def test_local_backend_before_call_component_failure_raises_runtime_exception():
    """A before_call() hook failure surfaces as MetaflowFunctionRuntimeException, not a user error."""
    from metaflow import FunctionParameters
    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionRuntimeException,
    )

    FailingComponent.fail_before_call = True
    try:
        func = _MockFunction([FailingComponent()])
        with pytest.raises(MetaflowFunctionRuntimeException):
            LocalBackend.apply(func, "x", params=FunctionParameters())
    finally:
        FailingComponent.fail_before_call = False


def test_local_backend_after_call_component_failure_raises_runtime_exception():
    """An after_call() hook failure surfaces as MetaflowFunctionRuntimeException, not a user error."""
    from metaflow import FunctionParameters
    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionRuntimeException,
    )

    FailingComponent.fail_after_call = True
    try:
        func = _MockFunction([FailingComponent()])
        with pytest.raises(MetaflowFunctionRuntimeException):
            LocalBackend.apply(func, "x", params=FunctionParameters())
    finally:
        FailingComponent.fail_after_call = False


# ---------------------------------------------------------------------------
# 5. Memory backend — serialisation (no subprocess)
# ---------------------------------------------------------------------------


def test_memory_backend_connection_params_includes_component_names():
    """generate_connection_params stores serialised component specs."""
    from metaflow_extensions.nflx.plugins.functions.backends.memory.memory_backend import (
        MemoryBackend,
    )

    fqn = f"{RecordingComponent.__module__}.{RecordingComponent.__qualname__}"
    params = MemoryBackend.generate_connection_params(
        "test-uuid", runtime_components=[fqn]
    )
    assert params["runtime_components"] == [fqn]


def test_memory_backend_runtime_command_includes_flags():
    """get_runtime_command emits one --runtime-component flag per component."""
    from unittest.mock import patch
    from metaflow_extensions.nflx.plugins.functions.backends.memory.memory_backend import (
        MemoryBackend,
    )

    fqn = f"{RecordingComponent.__module__}.{RecordingComponent.__qualname__}"
    params = MemoryBackend.generate_connection_params(
        "test-uuid",
        input_map="in",
        output_map="out",
        data_watcher="dw",
        runtime_components=[fqn],
    )
    # Patch FunctionSpec.download_to_temp so it doesn't try to hit S3.
    # The method is imported locally inside get_runtime_command so we patch at source.
    with patch(
        "metaflow_extensions.nflx.plugins.functions.core.function_spec.FunctionSpec.download_to_temp",
        return_value="/fake/local/reference.json",
    ):
        cmd = MemoryBackend.get_runtime_command(
            params, "/fake/reference.json", "/usr/bin/python"
        )
    cmd_str = " ".join(cmd)
    assert "--runtime-component" in cmd_str


# ---------------------------------------------------------------------------
# 6. configure()
# ---------------------------------------------------------------------------


class _ConfigA(AbstractRuntimeComponent):
    def start(self, *args, **kwargs):
        pass

    def stop(self, *args, **kwargs):
        pass

    def before_call(self, *args, **kwargs):
        pass

    def after_call(self, *args, **kwargs):
        pass


class _ConfigB(AbstractRuntimeComponent):
    def start(self, *args, **kwargs):
        pass

    def stop(self, *args, **kwargs):
        pass

    def before_call(self, *args, **kwargs):
        pass

    def after_call(self, *args, **kwargs):
        pass


class ConfiguringComponent(AbstractRuntimeComponent):
    """Writes a snapshot of ``_class_config`` (as seen at start() time) to a file."""

    _log_path: str = ""
    component_id = "configuring_component"

    def start(self, *args, **kwargs) -> None:
        with open(type(self)._log_path, "a") as fh:
            fh.write(f"start:{dict(sorted(self._class_config.items()))}\n")

    def stop(self, *args, **kwargs) -> None:
        pass

    def before_call(self, *args, **kwargs) -> None:
        pass

    def after_call(self, *args, **kwargs) -> None:
        pass


def test_configure_stores_kwargs_in_class_config():
    """configure() kwargs land in cls._class_config."""
    try:
        _ConfigA.configure(stream_name="my_stream", app_name="my_app")
        assert _ConfigA._class_config == {
            "stream_name": "my_stream",
            "app_name": "my_app",
        }
    finally:
        _ConfigA._class_config.clear()


def test_configure_accumulates_last_write_wins():
    """Repeated configure() calls accumulate; last write wins per key."""
    try:
        _ConfigA.configure(a=1, b=2)
        _ConfigA.configure(b=3, c=4)
        assert _ConfigA._class_config == {"a": 1, "b": 3, "c": 4}
    finally:
        _ConfigA._class_config.clear()


def test_configure_isolated_per_subclass():
    """configure() on one subclass never leaks into another's _class_config."""
    try:
        _ConfigA.configure(owner="a")
        _ConfigB.configure(owner="b")
        assert _ConfigA._class_config == {"owner": "a"}
        assert _ConfigB._class_config == {"owner": "b"}
    finally:
        _ConfigA._class_config.clear()
        _ConfigB._class_config.clear()


def test_configure_visible_in_start_via_file():
    """configure() called before start_components is visible inside start()."""
    log = _tmp_log()
    ConfiguringComponent._log_path = log
    try:
        ConfiguringComponent.configure(stream_name="my_stream")
        instances = start_components([ConfiguringComponent()])
        assert _read_events(log) == [
            "start:{'stream_name': 'my_stream'}",
        ]
        stop_components(instances)
    finally:
        ConfiguringComponent._log_path = ""
        ConfiguringComponent._class_config.clear()
        os.unlink(log)


def test_configure_after_start_does_not_affect_running_instance():
    """configure() after start() doesn't retroactively change what start() saw."""
    log = _tmp_log()
    ConfiguringComponent._log_path = log
    try:
        ConfiguringComponent.configure(stream_name="my_stream")
        instances = start_components([ConfiguringComponent()])
        assert _read_events(log) == [
            "start:{'stream_name': 'my_stream'}",
        ]

        # configure() after start still updates the class-level dict...
        ConfiguringComponent.configure(stream_name="changed")
        assert ConfiguringComponent._class_config == {"stream_name": "changed"}

        # ...but the already-written start() snapshot is untouched.
        assert _read_events(log) == [
            "start:{'stream_name': 'my_stream'}",
        ]
        stop_components(instances)
    finally:
        ConfiguringComponent._log_path = ""
        ConfiguringComponent._class_config.clear()
        os.unlink(log)


# ---------------------------------------------------------------------------
# 7. collect_output() / after_call_components output routing
# ---------------------------------------------------------------------------


class _NoOutputComponent(AbstractRuntimeComponent):
    component_id = "no_output_component"

    def start(self, *args, **kwargs):
        pass

    def stop(self, *args, **kwargs):
        pass

    def before_call(self, *args, **kwargs):
        pass

    def after_call(self, *args, **kwargs):
        pass


class _OutputComponent(AbstractRuntimeComponent):
    component_id = "output_component"

    def start(self, *args, **kwargs):
        pass

    def stop(self, *args, **kwargs):
        pass

    def before_call(self, *args, **kwargs):
        pass

    def after_call(self, *args, **kwargs):
        pass

    def collect_output(self, *args, **kwargs):
        return {"count": 1}


def test_collect_output_default_returns_none():
    """Default collect_output() is a no-op returning None."""
    inst = _NoOutputComponent()
    assert inst.collect_output() is None
    assert inst.output is None


def test_after_call_components_returns_empty_dict_when_no_output():
    """Instances with default collect_output() contribute nothing to the returned map."""
    inst = _NoOutputComponent()
    collected = after_call_components([inst])
    assert collected == {}
    assert inst.output is None


def test_after_call_components_collects_output_and_sets_output():
    """collect_output() output is both returned (keyed by component_id) and stamped onto output."""
    inst = _OutputComponent()
    collected = after_call_components([inst])

    assert collected == {_OutputComponent.component_id: {"count": 1}}
    assert inst.output == {"count": 1}


def test_after_call_components_mixed_instances():
    """Only instances that report output appear in the returned map; all still get after_call()."""
    log = _tmp_log()
    RecordingComponent._log_path = log
    try:
        recorder = RecordingComponent()
        producer = _OutputComponent()
        collected = after_call_components([recorder, producer])

        assert collected == {_OutputComponent.component_id: {"count": 1}}
        assert recorder.output is None
        assert producer.output == {"count": 1}
        assert _read_events(log) == ["after_call"]
    finally:
        RecordingComponent._log_path = ""
        os.unlink(log)


# ---------------------------------------------------------------------------
# 8. MetaflowFunction.runtime_components / get_runtime_component
# ---------------------------------------------------------------------------


class _StubMetaflowFunction:
    """
    Minimal stand-in exercising only the runtime_components/get_runtime_component
    mixin behavior, without pulling in the full MetaflowFunction ABC machinery
    (spec/backend/task loading, which is out of scope for these tests).
    """

    def __init__(self, components=None):
        if components is not None:
            self._runtime_components = components

    from metaflow_extensions.nflx.plugins.functions.core.function import (
        MetaflowFunction,
    )

    runtime_components = MetaflowFunction.__dict__["runtime_components"]
    get_runtime_component = MetaflowFunction.__dict__["get_runtime_component"]


def test_runtime_components_property_defaults_to_empty_list():
    """runtime_components is [] when _runtime_components was never set."""
    func = _StubMetaflowFunction()
    assert func.runtime_components == []


def test_runtime_components_property_returns_scheduled_instances():
    """runtime_components returns exactly the instances passed at construction."""
    recorder = RecordingComponent()
    producer = _OutputComponent()
    func = _StubMetaflowFunction([recorder, producer])
    assert func.runtime_components == [recorder, producer]


def test_get_runtime_component_returns_matching_instance():
    """get_runtime_component finds the instance whose exact type matches."""
    recorder = RecordingComponent()
    producer = _OutputComponent()
    func = _StubMetaflowFunction([recorder, producer])
    assert func.get_runtime_component(_OutputComponent) is producer
    assert func.get_runtime_component(RecordingComponent) is recorder


def test_get_runtime_component_returns_none_when_absent():
    """get_runtime_component returns None when no instance of that type was scheduled."""
    func = _StubMetaflowFunction([RecordingComponent()])
    assert func.get_runtime_component(_OutputComponent) is None


def test_get_runtime_component_matches_subclass():
    """get_runtime_component matches subclasses via isinstance, not exact type."""

    class SubRecordingComponent(RecordingComponent):
        pass

    func = _StubMetaflowFunction([SubRecordingComponent()])
    assert isinstance(
        func.get_runtime_component(RecordingComponent), SubRecordingComponent
    )
    assert isinstance(
        func.get_runtime_component(SubRecordingComponent), SubRecordingComponent
    )


# ---------------------------------------------------------------------------
# 9. function_from_json — duplicate runtime_components validation
# ---------------------------------------------------------------------------


def test_function_from_json_rejects_duplicate_component_types():
    """Two instances of the same component type raise MetaflowFunctionException."""
    from unittest.mock import patch, MagicMock
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        function_from_json,
    )
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionException,
    )

    fake_spec = MagicMock()
    fake_spec.serializer_configs = None
    fake_spec.class_name = "fake.module.FakeFunction"

    fake_subclass = MagicMock()
    fake_subclass._create_proxy_from_spec.return_value = MagicMock()

    with patch(
        "metaflow_extensions.nflx.plugins.functions.core.function_spec.FunctionSpec.from_json",
        return_value=fake_spec,
    ), patch(
        "metaflow_extensions.nflx.plugins.functions.utils.load_type_from_string",
        return_value=fake_subclass,
    ):
        with pytest.raises(
            MetaflowFunctionException, match="Duplicate runtime component"
        ):
            function_from_json(
                "fake-reference.json",
                start_runtime=False,
                runtime_components=[RecordingComponent(), RecordingComponent()],
            )


def test_function_from_json_allows_distinct_component_types():
    """Distinct component types pass validation and land on func._runtime_components."""
    from unittest.mock import patch, MagicMock
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        function_from_json,
    )

    fake_spec = MagicMock()
    fake_spec.serializer_configs = None
    fake_spec.class_name = "fake.module.FakeFunction"

    fake_func = MagicMock()
    fake_subclass = MagicMock()
    fake_subclass._create_proxy_from_spec.return_value = fake_func

    recorder = RecordingComponent()
    producer = _OutputComponent()

    with patch(
        "metaflow_extensions.nflx.plugins.functions.core.function_spec.FunctionSpec.from_json",
        return_value=fake_spec,
    ), patch(
        "metaflow_extensions.nflx.plugins.functions.utils.load_type_from_string",
        return_value=fake_subclass,
    ):
        func = function_from_json(
            "fake-reference.json",
            start_runtime=False,
            runtime_components=[recorder, producer],
        )

    assert func._runtime_components == [recorder, producer]


def test_function_from_json_runtime_metrics_component():
    """function_from_json wires a real RuntimeMetrics through to the
    backend; after a real call, output carries the expected fields."""
    from unittest.mock import patch, MagicMock
    from metaflow import FunctionParameters
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        function_from_json,
    )
    from metaflow_extensions.nflx.plugins.functions.components.runtime_metrics import (
        RuntimeMetrics,
    )
    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )

    fake_spec = MagicMock()
    fake_spec.serializer_configs = None
    fake_spec.class_name = "fake.module.FakeFunction"

    fake_subclass = MagicMock()
    fake_subclass._create_proxy_from_spec.return_value = _MockFunction([])

    metrics = RuntimeMetrics()

    with patch(
        "metaflow_extensions.nflx.plugins.functions.core.function_spec.FunctionSpec.from_json",
        return_value=fake_spec,
    ), patch(
        "metaflow_extensions.nflx.plugins.functions.utils.load_type_from_string",
        return_value=fake_subclass,
    ):
        func = function_from_json(
            "fake-reference.json",
            start_runtime=False,
            runtime_components=[metrics],
        )

    try:
        result = LocalBackend.apply(func, "hello", params=FunctionParameters())

        assert result == "echo:hello"
        assert metrics.output.keys() == {
            "call_count",
            "last_duration_s",
            "total_duration_s",
        }
        assert metrics.output["call_count"] == 1
        assert metrics.output["last_duration_s"] >= 0
        assert metrics.output["total_duration_s"] >= 0
    finally:
        LocalBackend.close(func)


def test_local_backend_default_use_proxy_path_keeps_runtime_components():
    """End-to-end regression for the default use_proxy=True path: the proxy
    returned by function_from_json() must carry its runtime_components through
    LocalBackend.apply()'s proxy-rehydration step, not lose them.

    Unlike test_function_from_json_runtime_metrics_component (which mocks
    _create_proxy_from_spec to directly return a concrete, non-proxy
    _MockFunction), this exercises the actual proxy object (_func is None)
    that LocalBackend.apply() detects and rehydrates via a second
    function_from_json(..., use_proxy=False, ...) call - the exact code path
    that was silently dropping components.
    """
    from unittest.mock import patch, MagicMock
    from metaflow import FunctionParameters
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        function_from_json,
    )
    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )

    fake_spec = MagicMock()
    fake_spec.serializer_configs = None
    fake_spec.class_name = "fake.module.FakeFunction"
    fake_spec.reference = "s3://fake-bucket/fake-reference.json"

    class _Proxy:
        name = "proxy_fn"
        _func = None
        spec = fake_spec

    fake_subclass = MagicMock()
    fake_subclass._create_proxy_from_spec.return_value = _Proxy()
    fake_subclass.from_spec.return_value = _MockFunction([])

    log = _tmp_log()
    RecordingComponent._log_path = log
    try:
        with patch(
            "metaflow_extensions.nflx.plugins.functions.core.function_spec.FunctionSpec.from_json",
            return_value=fake_spec,
        ), patch(
            "metaflow_extensions.nflx.plugins.functions.utils.load_type_from_string",
            return_value=fake_subclass,
        ), patch(
            "metaflow_extensions.nflx.plugins.functions.core.function_spec.FunctionSpec.download_to_temp",
            return_value="fake-reference.json",
        ):
            # Default use_proxy=True - this is the caller-facing path.
            func = function_from_json(
                "fake-reference.json",
                start_runtime=False,
                runtime_components=[RecordingComponent()],
            )
            assert func._func is None  # sanity: we really got a proxy

            result = LocalBackend.apply(func, "hello", params=FunctionParameters())

        assert result == "echo:hello"
        assert _read_events(log) == ["start", "before_call", "after_call"]
    finally:
        RecordingComponent._log_path = ""
        os.unlink(log)


# ---------------------------------------------------------------------------
# 10. Memory backend — component output routing
# ---------------------------------------------------------------------------


def test_memory_backend_route_component_output_sets_output():
    """_route_component_output stamps output by matching component_id."""
    from metaflow_extensions.nflx.plugins.functions.backends.memory.memory_backend import (
        MemoryBackend,
    )

    producer = _OutputComponent()
    func = _MockFunction([producer])

    runtime_components = {_OutputComponent.component_id: {"count": 5}}

    MemoryBackend._route_component_output(func, runtime_components)

    assert producer.output == {"count": 5}


def test_memory_backend_route_component_output_noop_when_absent():
    """_route_component_output is a no-op when runtime_components is None/empty."""
    from metaflow_extensions.nflx.plugins.functions.backends.memory.memory_backend import (
        MemoryBackend,
    )

    producer = _OutputComponent()
    func = _MockFunction([producer])

    MemoryBackend._route_component_output(func, None)

    assert producer.output is None


# ---------------------------------------------------------------------------
# 11. Ray backend — component output routing
#
# Ray is a real install_requires dependency of metaflow-functions (setup.py),
# so these tests are expected to run in any properly set-up environment. Same
# convention as tests/functions/backends/ray/test_ray_backend.py, which skips
# only as a workaround for incomplete local installs, not because Ray support
# is optional.
# ---------------------------------------------------------------------------

pytest.importorskip("ray")


def test_ray_backend_route_component_output_sets_output():
    """RayBackend._route_component_output stamps output by matching type name."""
    from metaflow_extensions.nflx.plugins.functions.backends.ray.ray_backend import (
        RayBackend,
    )

    producer = _OutputComponent()
    func = _MockFunction([producer])

    RayBackend._route_component_output(
        func, {_OutputComponent.component_id: {"count": 7}}
    )

    assert producer.output == {"count": 7}


def test_ray_backend_route_component_output_ignores_unmatched_entries():
    """Output for a spec name with no matching scheduled component is ignored."""
    from metaflow_extensions.nflx.plugins.functions.backends.ray.ray_backend import (
        RayBackend,
    )

    producer = _OutputComponent()
    func = _MockFunction([producer])

    RayBackend._route_component_output(func, {"some.other.Component": {"count": 1}})

    assert producer.output is None


def test_ray_backend_route_component_output_handles_empty():
    """RayBackend._route_component_output no-ops on falsy component_output."""
    from metaflow_extensions.nflx.plugins.functions.backends.ray.ray_backend import (
        RayBackend,
    )

    producer = _OutputComponent()
    func = _MockFunction([producer])

    RayBackend._route_component_output(func, {})
    RayBackend._route_component_output(func, None)

    assert producer.output is None


# ---------------------------------------------------------------------------
# 12. on_runtime_started + contribute_spec_metadata — deploy-time config
# ---------------------------------------------------------------------------


def test_on_runtime_started_default_is_noop():
    """Base class default implementation does nothing and returns None."""

    class _PlainComponent(AbstractRuntimeComponent):
        component_id = "plain_component"

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

    component = _PlainComponent()
    assert component.on_runtime_started({"anything": 1}) is None
    assert component.on_runtime_started(None) is None


def test_contribute_spec_metadata_default_is_none():
    """A component that needs nothing recorded contributes nothing."""

    class _NoMetadataComponent(AbstractRuntimeComponent):
        component_id = "no_metadata_component"

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

    assert _NoMetadataComponent.contribute_spec_metadata("/some/module/dir") is None


def test_component_meta_registry_tracks_subclasses():
    """Packaging finds components by walking the registry, so every concrete
    component class must land in it. The abstract base must not."""
    from metaflow_extensions.nflx.plugins.functions.components.abstract_component import (
        ComponentMeta,
    )

    class _RegisteredComponent(AbstractRuntimeComponent):
        component_id = "registered_component"

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

    assert _RegisteredComponent in ComponentMeta.registry
    assert AbstractRuntimeComponent not in ComponentMeta.registry


class _MetadataComponent(AbstractRuntimeComponent):
    """Component that records the module dir it was asked about, and reads its
    deploy-time metadata back on the caller side."""

    component_id = "metadata_component"
    asked_with = []

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.received_metadata = "<never called>"

    @classmethod
    def contribute_spec_metadata(cls, function_module_dir):
        cls.asked_with.append(function_module_dir)
        return {"module_dir": function_module_dir, "declared": cls._class_config}

    def on_runtime_started(self, metadata):
        self.received_metadata = metadata

    def start(self, *args, **kwargs):
        pass

    def stop(self, *args, **kwargs):
        pass

    def before_call(self, *args, **kwargs):
        pass

    def after_call(self, *args, **kwargs):
        pass


class TestSpecMetadataCollection:
    """Packaging asks configured components what to record; the caller hands it
    back. Neither side resolves a path inside an extracted code package, which
    is the point -- the caller and the runtime can be on different filesystems.
    """

    def setup_method(self):
        _MetadataComponent.asked_with.clear()
        _MetadataComponent._class_config.clear()

    def teardown_method(self):
        _MetadataComponent.asked_with.clear()
        _MetadataComponent._class_config.clear()

    def _collect(self, module_dir="/src/my_model"):
        """Run the collector the way _build_function_spec does.

        MetaflowFunction is abstract, and the collector only needs
        _function_module_dir(), so call it against a minimal stand-in rather
        than constructing a real function.
        """
        from metaflow_extensions.nflx.plugins.functions.core.function import (
            MetaflowFunction,
        )

        class _Stub:
            def _function_module_dir(self):
                return module_dir

        return MetaflowFunction._collect_runtime_component_metadata(_Stub())

    def test_every_component_is_asked_even_when_unconfigured(self):
        """Packaging asks every registered component, configured or not.

        Gating on _class_config would conflate "did the user configure this"
        with "does this need anything recorded". Only ALBLogger makes the first
        imply the second, because its configure() is where the schema filename
        comes from; a component with no required configuration must still get
        its chance to contribute.
        """
        assert not _MetadataComponent._class_config  # nothing configured
        collected = self._collect(module_dir="/src/my_model")

        assert _MetadataComponent.asked_with == ["/src/my_model"]
        # It chose to contribute anyway -- it needs no user configuration.
        assert collected[_MetadataComponent.component_id]["declared"] == {}

    def test_a_component_needing_no_configuration_can_still_contribute(self):
        """The case the _class_config gate used to break: record something
        derived from the function rather than from user config."""

        class _DerivesFromFunction(_MetadataComponent):
            component_id = "derives_from_function"

            @classmethod
            def contribute_spec_metadata(cls, function_module_dir):
                return {"where": function_module_dir}

        collected = self._collect(module_dir="/src/other")
        assert collected["derives_from_function"] == {"where": "/src/other"}

    def test_configured_component_is_asked_and_recorded_by_component_id(self):
        _MetadataComponent.configure(schema_config="my_config.json")

        collected = self._collect(module_dir="/src/my_model")

        assert _MetadataComponent.asked_with == ["/src/my_model"]
        entry = collected[_MetadataComponent.component_id]
        assert entry["module_dir"] == "/src/my_model"
        assert entry["declared"]["schema_config"] == "my_config.json"

    def test_component_returning_none_records_nothing(self):
        class _DeclinesComponent(_MetadataComponent):
            component_id = "declines_component"

            @classmethod
            def contribute_spec_metadata(cls, function_module_dir):
                return None

        _DeclinesComponent.configure(anything=True)
        try:
            collected = self._collect()
            assert _DeclinesComponent.component_id not in collected
        finally:
            _DeclinesComponent._class_config.clear()


class TestOnRuntimeStartedReceivesSpecMetadata:
    def setup_method(self):
        _MetadataComponent._class_config.clear()

    def test_hook_receives_this_components_entry(self):
        """The caller passes each component only its own entry, keyed by
        component_id -- the same key the backends use to route output back."""
        component = _MetadataComponent()
        metadata = {"schema": {"a": 1}}
        component.on_runtime_started(metadata)
        assert component.received_metadata == metadata

    def test_hook_receives_none_when_the_function_has_no_entry(self):
        """The common platform case: a component installed for a function that
        was not deployed with it configured. Must not be an error."""
        component = _MetadataComponent()
        component.on_runtime_started(None)
        assert component.received_metadata is None


# ---------------------------------------------------------------------------
# 13. Nesting and concurrent-invocation guard
# ---------------------------------------------------------------------------


def test_nested_invocation_restores_the_outer_active_instance():
    """before_call stashes the previous active_instance and after_call restores
    it, so an invocation nested inside another hands routing back.

    Clearing to None instead meant the outer function's *later* log() calls
    silently no-op'd -- demonstrated with ALBLogger: a row lost every field
    logged after the nested call returned.
    """

    class _Nestable(AbstractRuntimeComponent):
        component_id = "nestable"

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

    outer, inner = _Nestable(), _Nestable()

    before_call_components([outer])
    assert _Nestable.active_instance is outer

    before_call_components([inner])  # nested invocation begins
    assert _Nestable.active_instance is inner

    after_call_components([inner])  # nested invocation ends
    assert _Nestable.active_instance is outer  # handed back, not cleared

    after_call_components([outer])
    assert _Nestable.active_instance is None  # top level: no routing


def test_concurrent_local_invocation_with_components_is_refused():
    """Runtime components can't serve overlapping invocations: routing is
    class-level and the per-call buffer is on the instance, so two at once
    interleave both. Local mode is the only backend that can express this
    (memory runs a single-threaded subprocess runloop, a Ray actor is
    single-threaded), so the guard lives there -- and it raises rather than
    serialising, which would silently remove the parallelism the caller asked
    for.
    """
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor

    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionRuntimeException,
    )

    class _Slow(AbstractRuntimeComponent):
        component_id = "slow_component"

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

    class _Func:
        name = "slow_func"
        _component_instances = []
        _runtime_components = [_Slow()]
        spec = None

        def execute(self, data, params, **kwargs):
            time.sleep(0.05)
            return data

    func = _Func()
    errors = []

    def call():
        try:
            LocalBackend.apply(func, 1, params=object())
        except MetaflowFunctionRuntimeException as e:
            errors.append(str(e))
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=2) as ex:
        list(ex.map(lambda _: call(), range(2)))

    assert len(errors) == 1, "exactly one of two overlapping calls must be refused"
    assert "concurrent invocation" in errors[0]


def test_concurrent_local_invocation_without_components_is_allowed():
    """Nothing to interleave when there are no components, so plain concurrent
    local invocation must keep working -- the guard is not a general lock."""
    import time
    from concurrent.futures import ThreadPoolExecutor

    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )

    class _Func:
        name = "plain_func"
        _component_instances = []
        _runtime_components = []
        spec = None

        def execute(self, data, params, **kwargs):
            time.sleep(0.05)
            return data

    func = _Func()
    errors = []

    def call(v):
        try:
            return LocalBackend.apply(func, v, params=object())
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))
            return None

    with ThreadPoolExecutor(max_workers=2) as ex:
        results = list(ex.map(call, [1, 2]))

    assert not errors, f"component-free function was wrongly blocked: {errors}"
    assert sorted(r for r in results if r is not None) == [1, 2]
