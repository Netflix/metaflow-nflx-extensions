"""
Tests that FunctionPipeline.execute() instruments each constituent function
individually with the pipeline's own runtime_components, not just the
pipeline as a whole.
"""

import pytest

pytestmark = pytest.mark.no_backend_parametrization

from metaflow_extensions.nflx.plugins.functions.components.abstract_component import (
    AbstractRuntimeComponent,
)
from metaflow_extensions.nflx.plugins.functions.core.function_pipeline import (
    FunctionPipeline,
)


class RecordingComponent(AbstractRuntimeComponent):
    """Records (event, function_name) tuples in a shared, class-level list.
    Each test gets its own fresh component instance, but all events land in
    this one list so tests can check the order and content easily."""

    component_id = "recording"
    log: list = []

    def start(self, *args, function=None, **kwargs) -> None:
        self._function_name = getattr(function, "name", None)
        self.log.append(("start", self._function_name))

    def stop(self, *args, **kwargs) -> None:
        self.log.append(("stop", self._function_name))

    def before_call(self, *args, **kwargs) -> None:
        self.log.append(("before_call", self._function_name))

    def after_call(self, *args, exception=None, **kwargs) -> None:
        self.log.append(("after_call", self._function_name, exception))


class _Func:
    """Minimal MetaflowFunction stand-in."""

    def __init__(self, name, raise_error=False):
        self.name = name
        self._raise_error = raise_error

    def execute(self, data, params, **kwargs):
        if self._raise_error:
            raise ValueError(f"boom in {self.name}")
        return data + [self.name]


def _make_pipeline(functions, components):
    """A bare FunctionPipeline with pre-scoped params, bypassing __init__ and
    packaging (mirrors test_parameter_scoping.py's make_pipeline())."""
    pipeline = FunctionPipeline.__new__(FunctionPipeline)
    pipeline.functions = functions
    pipeline._scoped_params = [None] * len(functions)
    pipeline._runtime_components = components
    return pipeline


@pytest.fixture(autouse=True)
def _reset_log():
    RecordingComponent.log = []
    yield
    RecordingComponent.log = []


def test_each_constituent_gets_its_own_before_after_call():
    """Every constituent fires its own start/before_call/after_call/stop,
    tagged with its own name -- not one call for the whole pipeline."""
    functions = [_Func("request_handler"), _Func("score"), _Func("response_handler")]
    pipeline = _make_pipeline(functions, [RecordingComponent()])

    result = pipeline.execute([], None)

    assert result == ["request_handler", "score", "response_handler"]
    assert RecordingComponent.log == [
        ("start", "request_handler"),
        ("before_call", "request_handler"),
        ("after_call", "request_handler", None),
        ("stop", "request_handler"),
        ("start", "score"),
        ("before_call", "score"),
        ("after_call", "score", None),
        ("stop", "score"),
        ("start", "response_handler"),
        ("before_call", "response_handler"),
        ("after_call", "response_handler", None),
        ("stop", "response_handler"),
    ]


def test_constituent_exception_is_forwarded_and_reraised():
    """A failing constituent's exception reaches after_call and still
    propagates -- instrumentation must not swallow or alter it."""
    functions = [_Func("request_handler"), _Func("score", raise_error=True)]
    pipeline = _make_pipeline(functions, [RecordingComponent()])

    with pytest.raises(ValueError, match="boom in score"):
        pipeline.execute([], None)

    events = [e[:2] for e in RecordingComponent.log]
    assert events == [
        ("start", "request_handler"),
        ("before_call", "request_handler"),
        ("after_call", "request_handler"),
        ("stop", "request_handler"),
        ("start", "score"),
        ("before_call", "score"),
        ("after_call", "score"),
        ("stop", "score"),
    ]
    score_after_call = RecordingComponent.log[6]
    assert isinstance(score_after_call[2], ValueError)


def test_no_components_skips_instrumentation_entirely():
    """No runtime_components attached: constituents run exactly as before,
    with zero added overhead."""
    functions = [_Func("request_handler"), _Func("score")]
    pipeline = _make_pipeline(functions, [])

    result = pipeline.execute([], None)

    assert result == ["request_handler", "score"]
    assert RecordingComponent.log == []


def test_pipeline_level_component_instances_are_untouched():
    """Per-constituent calls use fresh component instances, not
    self._component_instances -- the pipeline-level instances the backend
    already starts/stops around the whole execute() call."""
    pipeline_level = RecordingComponent()
    functions = [_Func("request_handler"), _Func("score")]
    pipeline = _make_pipeline(functions, [pipeline_level])
    pipeline._component_instances = [pipeline_level]

    pipeline.execute([], None)

    # pipeline_level itself got none of these calls -- only its clones did.
    assert not hasattr(pipeline_level, "_function_name")
    assert pipeline._component_instances == [pipeline_level]
