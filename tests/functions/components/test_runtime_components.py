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


def test_component_start_activates_class():
    """After start_components the class-level active_instance is set."""
    log = _tmp_log()
    RecordingComponent._log_path = log
    try:
        instances = start_components([RecordingComponent()])
        assert RecordingComponent.active_instance is instances[0]
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
# 12. on_runtime_started — caller-side hook
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
    assert component.on_runtime_started("/some/function/dir") is None


class _RuntimeStartedRecorder(AbstractRuntimeComponent):
    """Component that records every on_runtime_started(function_root_dir) call."""

    component_id = "runtime_started_recorder"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.on_runtime_started_calls = []

    def start(self, *args, **kwargs):
        pass

    def stop(self, *args, **kwargs):
        pass

    def before_call(self, *args, **kwargs):
        pass

    def after_call(self, *args, **kwargs):
        pass

    def on_runtime_started(self, function_root_dir):
        self.on_runtime_started_calls.append(function_root_dir)


def test_function_from_json_invokes_on_runtime_started_with_computed_dir():
    """function_from_json computes function_root_dir from base_path + spec.uuid
    and invokes on_runtime_started on every scheduled component, without the
    backend needing to report the directory back."""
    from unittest.mock import patch, MagicMock
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        function_from_json,
    )
    from metaflow_extensions.nflx.plugins.functions.config import Config

    fake_spec = MagicMock()
    fake_spec.serializer_configs = None
    fake_spec.class_name = "fake.module.FakeFunction"
    fake_spec.uuid = "abc-123"
    expected_dir = os.path.join(
        "/tmp/some-base", f"{Config.RUNTIME_FUNCTION_DIR_PREFIX}abc-123"
    )
    # function_from_json delegates to fs.resolve_function_root_dir(base_path)
    # (see FunctionSpec.resolve_function_root_dir); mock it to match that
    # method's real (non-pipeline) formula rather than asserting on a
    # MagicMock's default return value.
    fake_spec.resolve_function_root_dir.return_value = expected_dir

    fake_func = MagicMock()
    fake_func._runtime_components = []
    fake_subclass = MagicMock()
    fake_subclass._create_proxy_from_spec.return_value = fake_func

    recorder = _RuntimeStartedRecorder()

    with patch(
        "metaflow_extensions.nflx.plugins.functions.core.function_spec.FunctionSpec.from_json",
        return_value=fake_spec,
    ), patch(
        "metaflow_extensions.nflx.plugins.functions.utils.load_type_from_string",
        return_value=fake_subclass,
    ):
        function_from_json(
            "fake-reference.json",
            base_path="/tmp/some-base",
            start_runtime=True,
            runtime_components=[recorder],
        )

    fake_func.backend.start.assert_called_once()
    fake_spec.resolve_function_root_dir.assert_called_once_with("/tmp/some-base")
    assert recorder.on_runtime_started_calls == [expected_dir]


def test_function_from_json_skips_on_runtime_started_when_not_starting():
    """on_runtime_started is only invoked when start_runtime=True."""
    from unittest.mock import patch, MagicMock
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        function_from_json,
    )

    fake_spec = MagicMock()
    fake_spec.serializer_configs = None
    fake_spec.class_name = "fake.module.FakeFunction"
    fake_spec.uuid = "abc-123"

    fake_func = MagicMock()
    fake_func._runtime_components = []
    fake_subclass = MagicMock()
    fake_subclass._create_proxy_from_spec.return_value = fake_func

    recorder = _RuntimeStartedRecorder()

    with patch(
        "metaflow_extensions.nflx.plugins.functions.core.function_spec.FunctionSpec.from_json",
        return_value=fake_spec,
    ), patch(
        "metaflow_extensions.nflx.plugins.functions.utils.load_type_from_string",
        return_value=fake_subclass,
    ):
        function_from_json(
            "fake-reference.json",
            start_runtime=False,
            runtime_components=[recorder],
        )

    assert recorder.on_runtime_started_calls == []


def test_function_from_json_pipeline_resolves_constituent_function_root_dir(
    tmp_path,
):
    """A pipeline's own extraction directory is near-empty (see
    FunctionPipeline._build_pipeline_spec) -- code-package files like schemas
    actually live under a constituent function's own directory. A runtime
    component started against a pipeline must be pointed at that constituent
    directory, not the pipeline's own uuid-named directory, or it can't find
    files it depends on (e.g. avro schemas for a logging component).

    Regression test for FunctionSpec.resolve_function_root_dir /
    FunctionPipelineSpec.resolve_function_root_dir.
    """
    from unittest.mock import patch, MagicMock
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        function_from_json,
    )
    from metaflow_extensions.nflx.plugins.functions.core.function_pipeline_spec import (
        FunctionPipelineSpec,
    )
    from metaflow_extensions.nflx.plugins.functions.config import Config

    base_path = str(tmp_path)

    # Only the constituent function's directory exists and holds the schema
    # file -- the pipeline's own directory (metaflow-function-pipeline-uuid)
    # is deliberately never created, matching production where it would be
    # near-empty even if present.
    child_dir = os.path.join(
        base_path, f"{Config.RUNTIME_FUNCTION_DIR_PREFIX}child-uuid"
    )
    os.makedirs(child_dir)
    with open(os.path.join(child_dir, "schema.avsc"), "w") as f:
        f.write('{"type": "record"}')

    fake_spec = FunctionPipelineSpec(
        uuid="pipeline-uuid",
        class_name="fake.module.FakePipeline",
        system_metadata={"functions": [{"uuid": "child-uuid"}]},
    )

    fake_func = MagicMock()
    fake_func._runtime_components = []
    fake_subclass = MagicMock()
    fake_subclass._create_proxy_from_spec.return_value = fake_func

    class _SchemaReadingComponent(AbstractRuntimeComponent):
        component_id = "schema_reading_component"

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.schema_contents = None

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

        def on_runtime_started(self, function_root_dir):
            with open(os.path.join(function_root_dir, "schema.avsc")) as f:
                self.schema_contents = f.read()

    component = _SchemaReadingComponent()

    with patch(
        "metaflow_extensions.nflx.plugins.functions.core.function_spec.FunctionSpec.from_json",
        return_value=fake_spec,
    ), patch(
        "metaflow_extensions.nflx.plugins.functions.utils.load_type_from_string",
        return_value=fake_subclass,
    ):
        function_from_json(
            "fake-reference.json",
            base_path=base_path,
            start_runtime=True,
            runtime_components=[component],
        )

    assert component.schema_contents == '{"type": "record"}'
