"""
Tests for per-constituent component spans in FunctionPipeline.execute().

A span brackets one constituent inside the invocation the backend already
opened -- it is not a second invocation, so `start`/`stop` stay once-per-runtime
and `active_instance` keeps naming the instance the backend activated.
"""

import pytest

pytestmark = pytest.mark.no_backend_parametrization

from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.functions.components.abstract_component import (
    AbstractRuntimeComponent,
)
from metaflow_extensions.nflx.plugins.functions.core.function_pipeline import (
    FunctionPipeline,
)


class SpanComponent(AbstractRuntimeComponent):
    """Records lifecycle and span events, plus who was active at each span."""

    component_id = "span_recorder"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.events = []

    def start(self, *args, **kwargs) -> None:
        self.events.append("start")

    def stop(self, *args, **kwargs) -> None:
        self.events.append("stop")

    def before_call(self, *args, **kwargs) -> None:
        self.events.append("before_call")

    def after_call(self, *args, exception=None, **kwargs) -> None:
        self.events.append(("after_call", exception))

    def on_child_call_start(self, name, **kwargs) -> None:
        self.events.append(("span_start", name))

    def on_child_call_end(self, name, exception=None, **kwargs) -> None:
        self.events.append(("span_end", name, exception))


class RaisingSpanComponent(SpanComponent):
    """Both span hooks raise -- instrumentation must not break the call."""

    component_id = "raising_span_recorder"

    def on_child_call_start(self, name, **kwargs) -> None:
        raise RuntimeError(f"start hook boom for {name}")

    def on_child_call_end(self, name, exception=None, **kwargs) -> None:
        raise RuntimeError(f"end hook boom for {name}")


class _Func:
    """Minimal MetaflowFunction stand-in."""

    def __init__(self, name, raise_error=False, on_execute=None):
        self.name = name
        self._raise_error = raise_error
        self._on_execute = on_execute

    def execute(self, data, params, **kwargs):
        if self._on_execute is not None:
            self._on_execute()
        if self._raise_error:
            raise ValueError(f"boom in {self.name}")
        return data + [self.name]


class _NamelessFunc:
    """A constituent whose `name` raises, like a spec with no name set."""

    @property
    def name(self):
        raise RuntimeError("no name in spec")

    def execute(self, data, params, **kwargs):
        return data + ["nameless"]


def _make_pipeline(functions, instances=None):
    """Bare FunctionPipeline with pre-scoped params, bypassing __init__.

    Mirrors make_pipeline() in test_parameter_scoping.py. `instances` is left
    unset (not empty) when None, so the no-attribute case is exercised too.
    """
    pipeline = FunctionPipeline.__new__(FunctionPipeline)
    pipeline.functions = functions
    pipeline._scoped_params = [None] * len(functions)
    if instances is not None:
        pipeline._component_instances = instances
    return pipeline


def test_span_opens_and_closes_around_each_constituent():
    comp = SpanComponent()
    functions = [_Func("request_handler"), _Func("score"), _Func("response_handler")]
    pipeline = _make_pipeline(functions, [comp])

    result = pipeline.execute([], None)

    assert result == ["request_handler", "score", "response_handler"]
    assert comp.events == [
        ("span_start", "request_handler"),
        ("span_end", "request_handler", None),
        ("span_start", "score"),
        ("span_end", "score", None),
        ("span_start", "response_handler"),
        ("span_end", "response_handler", None),
    ]


def test_spans_do_not_start_stop_or_reroute_components():
    """No extra start/stop, and active_instance still names the installed
    instance while a constituent runs -- the properties a per-constituent
    re-invocation would break."""
    comp = SpanComponent()
    SpanComponent.active_instance = comp
    seen = []
    try:
        functions = [
            _Func("a", on_execute=lambda: seen.append(SpanComponent.active_instance))
        ]
        pipeline = _make_pipeline(functions, [comp])

        pipeline.execute([], None)
    finally:
        SpanComponent.active_instance = None

    assert seen == [comp]
    assert "start" not in comp.events
    assert "stop" not in comp.events


def test_constituent_exception_reaches_span_end_and_propagates():
    comp = SpanComponent()
    functions = [_Func("a"), _Func("b", raise_error=True), _Func("c")]
    pipeline = _make_pipeline(functions, [comp])

    with pytest.raises(ValueError, match="boom in b"):
        pipeline.execute([], None)

    assert [e[:2] for e in comp.events] == [
        ("span_start", "a"),
        ("span_end", "a"),
        ("span_start", "b"),
        ("span_end", "b"),
    ]
    assert isinstance(comp.events[-1][2], ValueError)


def test_span_hook_failure_does_not_break_the_call():
    comp = RaisingSpanComponent()
    functions = [_Func("a"), _Func("b")]
    pipeline = _make_pipeline(functions, [comp])

    assert pipeline.execute([], None) == ["a", "b"]


def test_no_components_runs_constituents_untouched():
    functions = [_Func("a"), _Func("b")]
    pipeline = _make_pipeline(functions, [])

    assert pipeline.execute([], None) == ["a", "b"]


def test_missing_component_instances_attribute_is_not_an_error():
    """A pipeline that never went through a backend has no
    `_component_instances` at all; execute() must not blow up on it."""
    pipeline = _make_pipeline([_Func("a")], instances=None)
    assert not hasattr(pipeline, "_component_instances")

    assert pipeline.execute([], None) == ["a"]


def test_span_name_is_none_when_the_constituent_has_no_name():
    comp = SpanComponent()
    pipeline = _make_pipeline([_NamelessFunc()], [comp])

    pipeline.execute([], None)

    assert comp.events == [("span_start", None), ("span_end", None, None)]


def test_spans_close_in_reverse_instance_order():
    """Opened first-to-last, closed last-to-first, so a component holding a
    stack sees a well-formed nesting."""
    order = []

    class _Ordered(SpanComponent):
        component_id = "ordered_span_recorder"

        def __init__(self, tag, **kwargs):
            super().__init__(**kwargs)
            self.tag = tag

        def on_child_call_start(self, name, **kwargs):
            order.append(("start", self.tag))

        def on_child_call_end(self, name, exception=None, **kwargs):
            order.append(("end", self.tag))

    pipeline = _make_pipeline([_Func("a")], [_Ordered("first"), _Ordered("second")])

    pipeline.execute([], None)

    assert order == [
        ("start", "first"),
        ("start", "second"),
        ("end", "second"),
        ("end", "first"),
    ]


def test_local_backend_end_to_end_span_ordering():
    """Through a real backend: one start/stop for the runtime, before/after_call
    around the whole pipeline, spans nested inside."""
    from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
        LocalBackend,
    )

    comp = SpanComponent()
    pipeline = _make_pipeline([_Func("a"), _Func("b")])
    pipeline._component_instances = []
    pipeline._runtime_components = [comp]

    result = LocalBackend.apply(pipeline, [], params=FunctionParameters())
    LocalBackend.close(pipeline)

    assert result == ["a", "b"]
    assert comp.events == [
        "start",
        "before_call",
        ("span_start", "a"),
        ("span_end", "a", None),
        ("span_start", "b"),
        ("span_end", "b", None),
        ("after_call", None),
        "stop",
    ]
