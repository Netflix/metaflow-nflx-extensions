"""RuntimeMetrics collection and FunctionPipeline constituent scoping."""

import pytest

pytestmark = pytest.mark.no_backend_parametrization

from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
    LocalBackend,
)
from metaflow_extensions.nflx.plugins.functions.components.runtime import (
    after_call_components,
    before_call_components,
    start_components,
    stop_components,
)
from metaflow_extensions.nflx.plugins.functions.components.runtime_metrics import (
    RuntimeMetrics,
)
from metaflow_extensions.nflx.plugins.functions.core.function_pipeline import (
    FunctionPipeline,
)
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionUserException,
)


class _Func:
    """Minimal MetaflowFunction stand-in that can emit metrics."""

    def __init__(self, name, metrics=None, raise_error=False):
        self.name = name
        self._metrics = metrics or {}
        self._raise_error = raise_error

    def execute(self, data, params, **kwargs):
        if self._metrics:
            RuntimeMetrics.metric(self._metrics)
        if self._raise_error:
            raise ValueError(f"boom in {self.name}")
        return data + [self.name]


class _FastPathFunc:
    @property
    def name(self):
        raise AssertionError("inactive fast path must not read constituent names")

    def execute(self, data, params, **kwargs):
        RuntimeMetrics.metric(ignored_without_component=True)
        return data + ["fast"]


def _make_pipeline(functions, name="pipeline"):
    """Build a small executable pipeline without packaging function specs."""
    pipeline = FunctionPipeline.__new__(FunctionPipeline)
    pipeline.functions = functions
    pipeline._scoped_params = [None] * len(functions)
    pipeline._name = name
    pipeline._component_instances = []
    pipeline._runtime_components = []
    return pipeline


def _apply_with_metrics(pipeline, metrics=None):
    metrics = metrics or RuntimeMetrics()
    pipeline._runtime_components = [metrics]
    result = LocalBackend.apply(pipeline, [], params=FunctionParameters())
    return result, metrics


def test_metric_is_noop_when_component_is_not_active():
    assert RuntimeMetrics.active_instance is None
    assert RuntimeMetrics.metric(not_a_mapping=True) is None


def test_metric_calls_merge_and_later_values_win():
    metrics = RuntimeMetrics()
    instances = start_components([metrics])
    try:
        before_call_components(instances)
        RuntimeMetrics.metric({"records": 1, "status": "first"})
        RuntimeMetrics.metric(status="last", cache_hit=True)
        output = after_call_components(instances)[RuntimeMetrics.component_id]
    finally:
        stop_components(instances)

    assert output["metrics"] == {
        "records": 1,
        "status": "last",
        "cache_hit": True,
    }


def test_before_call_resets_only_per_invocation_metrics():
    metrics = RuntimeMetrics()
    instances = start_components([metrics])
    try:
        before_call_components(instances)
        RuntimeMetrics.metric(first_call=True)
        first = after_call_components(instances)[RuntimeMetrics.component_id]

        before_call_components(instances)
        RuntimeMetrics.metric(second_call=True)
        second = after_call_components(instances)[RuntimeMetrics.component_id]
    finally:
        stop_components(instances)

    assert first["metrics"] == {"first_call": True}
    assert second["metrics"] == {"second_call": True}
    assert second["call_count"] == 2


def test_scope_is_nested_and_exception_safe():
    metrics = RuntimeMetrics()
    instances = start_components([metrics])
    try:
        before_call_components(instances)
        RuntimeMetrics.metric(constituents="replaced by the later scope")
        with pytest.raises(ValueError, match="body failed"):
            with RuntimeMetrics.scope("constituents", "0:handler"):
                RuntimeMetrics.metric(inside=True)
                raise ValueError("body failed")
        RuntimeMetrics.metric(outside=True)
        output = after_call_components(instances)[RuntimeMetrics.component_id]
    finally:
        stop_components(instances)

    assert output["metrics"] == {
        "constituents": {"0:handler": {"inside": True}},
        "outside": True,
    }


def test_pipeline_scopes_user_and_duration_metrics_by_constituent():
    pipeline = _make_pipeline(
        [
            _Func("request_handler", {"decoded_records": 1}),
            _Func("model", {"batch_size": 1}),
        ]
    )

    try:
        result, metrics = _apply_with_metrics(pipeline)
    finally:
        LocalBackend.close(pipeline)

    assert result == ["request_handler", "model"]
    constituents = metrics.output["metrics"]["constituents"]
    assert constituents["0:request_handler"]["decoded_records"] == 1
    assert constituents["1:model"]["batch_size"] == 1
    assert constituents["0:request_handler"]["duration_s"] >= 0
    assert constituents["1:model"]["duration_s"] >= 0


def test_nested_pipeline_adds_another_constituent_scope():
    inner = _make_pipeline([_Func("inner", {"rows": 3})], name="nested")
    outer = _make_pipeline([inner], name="outer")

    try:
        _, metrics = _apply_with_metrics(outer)
    finally:
        LocalBackend.close(outer)

    outer_scope = metrics.output["metrics"]["constituents"]["0:nested"]
    inner_scope = outer_scope["constituents"]["0:inner"]
    assert inner_scope["rows"] == 3
    assert inner_scope["duration_s"] >= 0
    assert outer_scope["duration_s"] >= inner_scope["duration_s"]


def test_constituent_exception_keeps_metrics_and_duration():
    pipeline = _make_pipeline([_Func("handler", {"started": True}, raise_error=True)])
    metrics = RuntimeMetrics()
    pipeline._runtime_components = [metrics]

    try:
        with pytest.raises(MetaflowFunctionUserException, match="boom in handler"):
            LocalBackend.apply(pipeline, [], params=FunctionParameters())
    finally:
        LocalBackend.close(pipeline)

    scoped = metrics.output["metrics"]["constituents"]["0:handler"]
    assert scoped["started"] is True
    assert scoped["duration_s"] >= 0


def test_inactive_fast_path_skips_names_and_timing(monkeypatch):
    pipeline = _make_pipeline([_FastPathFunc()])
    monkeypatch.setattr(
        "metaflow_extensions.nflx.plugins.functions.core.function_pipeline.time.monotonic",
        lambda: pytest.fail("inactive fast path must not collect timing"),
    )

    assert pipeline.execute([], FunctionParameters()) == ["fast"]


def test_pipeline_without_runtime_metrics_runs_unchanged():
    pipeline = _make_pipeline([_Func("a"), _Func("b")])

    assert pipeline.execute([], FunctionParameters()) == ["a", "b"]
