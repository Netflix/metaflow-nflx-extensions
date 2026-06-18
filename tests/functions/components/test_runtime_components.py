"""
Tests for the runtime components system.

Covers three layers:
  1. Lifecycle mechanics — start/stop/before_call/after_call fire in the right order
  2. No-op routing — ClassName(...) is silent when no component is loaded
  3. Local backend integration — lifecycle fires end-to-end through LocalBackend.apply()
  4. Memory backend serialisation — component class names round-trip through
     connection_params and CLI args without spawning a subprocess

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
    load_component_classes,
    start_components,
    stop_components,
    before_call_components,
    after_call_components,
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
        instances = start_components([RecordingComponent])
        before_call_components(instances)
        after_call_components(instances)
        stop_components(instances)

        assert _read_events(log) == ["start", "before_call", "after_call", "stop"]
    finally:
        RecordingComponent._log_path = ""
        os.unlink(log)


def test_component_start_activates_class():
    """After start_components the class-level _active_instance is set."""
    log = _tmp_log()
    RecordingComponent._log_path = log
    try:
        instances = start_components([RecordingComponent])
        assert RecordingComponent._active_instance is instances[0]
        stop_components(instances)
        assert RecordingComponent._active_instance is None
    finally:
        RecordingComponent._log_path = ""
        os.unlink(log)


def test_stop_clears_instance_even_on_error():
    """stop() deactivates the class even if stop() itself raises."""

    class BrokenStop(AbstractRuntimeComponent):
        def start(self, *args, **kwargs): pass
        def stop(self, *args, **kwargs): raise RuntimeError("boom")
        def before_call(self, *args, **kwargs): pass
        def after_call(self, *args, **kwargs): pass
        def __call__(self, *args, **kwargs): pass

    instances = start_components([BrokenStop])
    assert BrokenStop._active_instance is not None

    with pytest.raises(RuntimeError, match="boom"):
        stop_components(instances)

    # _active_instance must be cleared regardless
    assert BrokenStop._active_instance is None


# ---------------------------------------------------------------------------
# 2. No-op / routing tests
# ---------------------------------------------------------------------------

def test_noop_when_not_loaded():
    """Calling RecordingComponent(...) returns None when no instance is active."""
    assert RecordingComponent._active_instance is None
    result = RecordingComponent("hello")
    assert result is None


def test_routes_to_instance_when_active():
    """Calling RecordingComponent(...) invokes __call__ on the active instance."""
    log = _tmp_log()
    RecordingComponent._log_path = log
    try:
        instances = start_components([RecordingComponent])
        RecordingComponent("ping")
        stop_components(instances)

        events = _read_events(log)
        assert "call:ping" in events
    finally:
        RecordingComponent._log_path = ""
        os.unlink(log)


# ---------------------------------------------------------------------------
# 3. load_component_classes round-trip
# ---------------------------------------------------------------------------

def test_load_component_classes_roundtrip():
    """Fully-qualified class name serialises and deserialises back to the same class."""
    fqn = f"{RecordingComponent.__module__}.{RecordingComponent.__qualname__}"
    loaded = load_component_classes([fqn])
    assert len(loaded) == 1
    assert loaded[0] is RecordingComponent


def test_load_component_classes_unknown_raises():
    """load_component_classes raises MetaflowFunctionException for an unknown class."""
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionException,
    )

    # load_type_from_string propagates the ImportError as MetaflowFunctionException
    with pytest.raises(MetaflowFunctionException):
        load_component_classes(["does.not.Exist"])


# ---------------------------------------------------------------------------
# 4. Local backend integration
# ---------------------------------------------------------------------------

class _MockFunction:
    """Minimal stand-in for a MetaflowFunction usable by LocalBackend.apply()."""

    name = "test_mock_function"

    def __init__(self, component_classes):
        self._runtime_components = component_classes

    def execute(self, data, params, **kwargs):
        return f"echo:{data}"


def test_local_backend_fires_lifecycle():
    """LocalBackend.apply() fires the full component lifecycle around execute()."""
    from metaflow import FunctionParameters
    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )

    log = _tmp_log()
    RecordingComponent._log_path = log
    try:
        func = _MockFunction([RecordingComponent])
        result = LocalBackend.apply(func, "hello", params=FunctionParameters())

        assert result == "echo:hello"

        events = _read_events(log)
        assert events == ["start", "before_call", "after_call", "stop"]
    finally:
        RecordingComponent._log_path = ""
        os.unlink(log)


def test_local_backend_stop_runs_on_exception():
    """stop() is called even when execute() raises."""
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
        func = FailingFunction([RecordingComponent])
        with pytest.raises(MetaflowFunctionUserException):
            LocalBackend.apply(func, "x", params=FunctionParameters())

        events = _read_events(log)
        # start must have fired, stop must have fired despite the error
        assert "start" in events
        assert "stop" in events
    finally:
        RecordingComponent._log_path = ""
        os.unlink(log)


def test_local_backend_no_components():
    """LocalBackend.apply() works normally when _runtime_components is empty."""
    from metaflow import FunctionParameters
    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )

    func = _MockFunction([])
    result = LocalBackend.apply(func, "world", params=FunctionParameters())
    assert result == "echo:world"


# ---------------------------------------------------------------------------
# 5. Memory backend — serialisation (no subprocess)
# ---------------------------------------------------------------------------

def test_memory_backend_connection_params_includes_component_names():
    """generate_connection_params stores serialised class names."""
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
        cmd = MemoryBackend.get_runtime_command(params, "/fake/reference.json", "/usr/bin/python")
    cmd_str = " ".join(cmd)
    assert "--runtime-component" in cmd_str
    assert fqn in cmd_str
