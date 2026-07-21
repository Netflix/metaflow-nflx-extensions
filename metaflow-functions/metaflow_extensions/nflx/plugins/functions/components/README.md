# Runtime Components

This adds a plugin-hook system to Metaflow Functions. A **runtime
component** is a class that plugs into a function's lifecycle
(`start`/`stop` once per runtime, `before_call`/`after_call` around every
invocation) to do things like logging, metrics collection, or other
cross-cutting concerns, without the function's own code needing to know
about it.

Components are opt-in and installed at rehydration time: you pass instances
via `runtime_components=[...]` when calling `function_from_json`. If a
component class is referenced from user code (e.g. a classmethod like
`Logger.log(...)`) but was never installed for that particular runtime, the
call is a silent no-op — it doesn't raise. This means the same function code
can run standalone, or with components enabled, with no branching required.
Because installation happens in `function_from_json`, each backend
(local, memory, ray, ...) independently decides whether to enable or disable
a given component for the runtime it starts.

## Example

Suppose we want a function that checks whether a value is above some
threshold, where the threshold comes from an upstream task.

**`my_functions.py`** — a normal avro function:

```python
from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.avro_function import avro_function


@avro_function
def check_above_threshold(
    data: int, params: FunctionParameters = FunctionParameters()
) -> bool:
    """Returns True if data exceeds the configured threshold."""
    return data > params.threshold
```

You can import and call it like any other Python function — the decorator
doesn't change that:

```python
from my_functions import check_above_threshold
from metaflow import FunctionParameters

check_above_threshold(15, params=FunctionParameters(threshold=10))  # True
```

**A flow** that defines the threshold and binds the function to a task, so
the function's parameters and environment travel with it:

```python
from metaflow import current, FlowSpec, step, Flow


class ThresholdFlow(FlowSpec):
    @step
    def start(self):
        self.threshold = 10
        self.next(self.bind_functions)

    @step
    def bind_functions(self):
        from metaflow_extensions.nflx.plugins.avro_function import AvroFunction
        from my_functions import check_above_threshold

        flow = Flow(current.flow_name)
        run = flow[current.run_id]
        start_task = run["start"].task

        # Binds the function to start_task: params.threshold above is
        # resolved from self.threshold on that task.
        self.threshold_function = AvroFunction(check_above_threshold, task=start_task)
        self.next(self.end)

    @step
    def end(self):
        pass
```

Elsewhere (a different step, a different process, a different system
entirely), the function is rehydrated from its reference and can be run
with or without runtime components enabled:

```python
from metaflow_extensions.nflx.plugins.functions.core.function import (
    function_from_json,
    close_function,
)
from metaflow_extensions.nflx.plugins.functions.components.runtime_metrics import (
    RuntimeMetrics,
)

# No components: check_above_threshold behaves exactly as it does when
# called directly.
func = function_from_json(threshold_function_reference)
assert func(15) is True
close_function(func)

# Same function, same reference — this runtime additionally records call
# timing via RuntimeMetrics's before_call/after_call hooks.
metrics = RuntimeMetrics()
func = function_from_json(threshold_function_reference, runtime_components=[metrics])
func(15)
func.runtime_components[0].last_output
# {"call_count": 1, "last_duration_s": ..., "total_duration_s": ...}
close_function(func)
```

Because `runtime_components` is a parameter to `function_from_json`, each
caller decides independently which components (if any) to install for the
runtime it starts — the reference and the function code stay unchanged
either way.
