from contextlib import contextmanager
import time
from typing import Any, Dict, Iterator, List, Mapping, Optional

from .abstract_component import AbstractRuntimeComponent


class RuntimeMetrics(AbstractRuntimeComponent):
    """
    Records call timing and user-defined metrics for a function's runtime.

    ``metric()`` follows the same active-instance pattern as other interactive
    runtime components: calls are silent no-ops unless ``RuntimeMetrics`` is
    attached to the function, and repeated calls merge into one dictionary for
    the current invocation. Later values win for the same key::

        RuntimeMetrics.metric({"records_processed": 100})
        RuntimeMetrics.metric(cache_hit=True)

    Composites can use ``scope()`` to place their own metrics, and any metrics
    emitted by code they invoke, beneath a well-known path. For example,
    ``FunctionPipeline`` records each constituent beneath
    ``metrics["constituents"]["<index>:<name>"]``.

    Output retains the original aggregate timing fields. The current
    invocation's ``metrics`` dictionary is added when anything recorded a
    metric::

        metrics = RuntimeMetrics()
        func = function_from_json(ref, runtime_components=[metrics])
        func(data)
        func.runtime_components[0].output
        # {
        #     "call_count": 1,
        #     "last_duration_s": 0.0123,
        #     "total_duration_s": 0.0123,
        #     "metrics": {"records_processed": 100, "cache_hit": True},
        # }
    """

    component_id = "runtime_metrics"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._metrics: Dict[str, Any] = {}
        self._metric_targets: List[Dict[str, Any]] = []

    @classmethod
    def metric(cls, data: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
        """Merge values into the current invocation's metric dictionary.

        ``data`` and keyword arguments are merged, with keyword arguments
        winning on duplicate keys. Like ``ALBLogger.log()``, this is a no-op
        when the component is not attached to the running function.
        """
        instance = cls.active_instance
        if instance is not None:
            instance._metric(data, **kwargs)

    def _metric(self, data: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
        if data is None:
            data = {}
        elif not isinstance(data, Mapping):
            raise TypeError("RuntimeMetrics.metric(): data must be a mapping")
        if not data and not kwargs:
            raise ValueError("RuntimeMetrics.metric(): requires at least one metric")

        target = self._metric_targets[-1] if self._metric_targets else self._metrics
        target.update(data)
        target.update(kwargs)

    @classmethod
    @contextmanager
    def scope(cls, *path: str) -> Iterator[None]:
        """Scope metrics emitted in the block beneath ``path``.

        Paths compose, so a nested composite adds another level beneath its
        parent's scope. Like repeated calls to ``metric()``, a scope created at
        a key that already holds a scalar replaces that earlier value; later
        writes win.
        """
        instance = cls.active_instance
        if instance is None:
            yield
            return
        if not path:
            raise ValueError("RuntimeMetrics.scope(): requires at least one key")

        parent = (
            instance._metric_targets[-1]
            if instance._metric_targets
            else instance._metrics
        )
        target = parent
        for part in path:
            child = target.get(part)
            if not isinstance(child, dict):
                child = {}
                target[part] = child
            target = child

        instance._metric_targets.append(target)
        try:
            yield
        finally:
            instance._metric_targets.pop()

    def start(self, *args: Any, **kwargs: Any) -> None:
        self._call_count = 0
        self._total_duration = 0.0
        self._last_duration = 0.0
        self._call_started_at: Optional[float] = None
        self._metrics = {}
        self._metric_targets = []

    def stop(self, *args: Any, **kwargs: Any) -> None:
        pass

    def before_call(self, *args: Any, **kwargs: Any) -> None:
        self._call_started_at = time.monotonic()
        self._metrics = {}
        self._metric_targets = []

    def after_call(self, *args: Any, **kwargs: Any) -> None:
        started_at = self._call_started_at
        if started_at is None:
            raise RuntimeError(
                "RuntimeMetrics.after_call() called before before_call()"
            )
        elapsed = time.monotonic() - started_at
        self._last_duration = elapsed
        self._total_duration += elapsed
        self._call_count += 1

    def collect_output(self, *args: Any, **kwargs: Any) -> Optional[Dict[str, Any]]:
        output: Dict[str, Any] = {
            "call_count": self._call_count,
            "last_duration_s": self._last_duration,
            "total_duration_s": self._total_duration,
        }
        if self._metrics:
            output["metrics"] = self._metrics
        return output
