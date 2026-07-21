import time
from typing import Any, Dict, Optional

from .abstract_component import AbstractRuntimeComponent


class RuntimeMetrics(AbstractRuntimeComponent):
    """
    Records call timing for a function's runtime.

    Purely observational: it has no user-facing hook for calling code to
    interact with directly. It rides the ``before_call``/``after_call``
    lifecycle to time each invocation, and surfaces the accumulated stats via
    ``collect_output()``, which lands on the caller-side handle's
    ``output`` after every call::

        metrics = RuntimeMetrics()
        func = function_from_json(ref, runtime_components=[metrics])
        func(data)
        func.runtime_components[0].output
        # {"call_count": 1, "last_duration_s": 0.0123, "total_duration_s": 0.0123}
    """

    component_id = "runtime_metrics"

    def start(self, *args: Any, **kwargs: Any) -> None:
        self._call_count = 0
        self._total_duration = 0.0
        self._last_duration = 0.0
        self._call_started_at: Optional[float] = None

    def stop(self, *args: Any, **kwargs: Any) -> None:
        pass

    def before_call(self, *args: Any, **kwargs: Any) -> None:
        self._call_started_at = time.monotonic()

    def after_call(self, *args: Any, **kwargs: Any) -> None:
        elapsed = time.monotonic() - self._call_started_at
        self._last_duration = elapsed
        self._total_duration += elapsed
        self._call_count += 1

    def collect_output(self, *args: Any, **kwargs: Any) -> Optional[Dict[str, Any]]:
        return {
            "call_count": self._call_count,
            "last_duration_s": self._last_duration,
            "total_duration_s": self._total_duration,
        }
