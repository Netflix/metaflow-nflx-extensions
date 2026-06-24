"""
Tests for the runtime components system.

Covers:
  1. Lifecycle mechanics — start/stop/before_call/after_call fire in the right order
  2. active_instance — set on start, cleared on stop
  3. Serialisation — serialize_components / load_component_instances round-trip
  4. Local backend integration — lifecycle fires end-to-end through LocalBackend.apply()
  5. Memory backend serialisation — component specs round-trip through connection_params
     and CLI args without spawning a subprocess

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
    """After start_components the class-level active_instance is set."""
    log = _tmp_log()
    RecordingComponent._log_path = log
    try:
        instances = start_components([RecordingComponent])
        assert RecordingComponent.active_instance is instances[0]
        stop_components(instances)
        assert RecordingComponent.active_instance is None
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

    instances = start_components([BrokenStop])
    assert BrokenStop.active_instance is not None

    with pytest.raises(RuntimeError, match="boom"):
        stop_components(instances)

    # active_instance must be cleared regardless
    assert BrokenStop.active_instance is None


# ---------------------------------------------------------------------------
# 2. active_instance / serialisation
# ---------------------------------------------------------------------------

def test_active_instance_none_before_start():
    """active_instance is None before start_components is called."""
    assert RecordingComponent.active_instance is None


def test_serialize_class():
    """serialize_components produces a plain 'module.ClassName' string for a class."""
    specs = serialize_components([RecordingComponent])
    assert len(specs) == 1
    fqn = f"{RecordingComponent.__module__}.{RecordingComponent.__qualname__}"
    assert specs[0] == fqn


def test_serialize_instance_with_kwargs():
    """serialize_components embeds init kwargs as JSON for an instance."""
    import json

    class KwargsComponent(AbstractRuntimeComponent):
        def start(self, *args, **kwargs): pass
        def stop(self, *args, **kwargs): pass
        def before_call(self, *args, **kwargs): pass
        def after_call(self, *args, **kwargs): pass

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
        func = _MockFunction([RecordingComponent])

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
        func = FailingFunction([RecordingComponent])
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
        cmd = MemoryBackend.get_runtime_command(params, "/fake/reference.json", "/usr/bin/python")
    cmd_str = " ".join(cmd)
    assert "--runtime-component" in cmd_str
    assert fqn in cmd_str
