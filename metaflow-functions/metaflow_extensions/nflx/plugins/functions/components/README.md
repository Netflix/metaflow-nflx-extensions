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

## A component's configuration is part of the model

Two different decisions get made in two different places, and it's worth being
precise about which is which:

- **What a component is configured to do is part of the model.** It's declared
  by the model owner, in the model's own code, and is resolved when the function
  is packaged — so it travels with the function reference, exactly like the
  function's parameters and its environment do. An ALB logging stream is a good
  example: which stream a model logs to, and the schema of the rows it writes,
  are properties *of that model*. They are not something a caller picks, any
  more than a caller picks the model's weights.
- **Whether a component runs is the caller's decision.** That's what
  `runtime_components=[...]` is, and it stays a per-runtime choice.

Concretely: a component declares its configuration by calling
`configure()` **at module level** in the model's code, and implements
`contribute_spec_metadata()` to say what should be recorded. At packaging time
the framework walks the component registry, asks every configured component,
and stores the answers in
`spec.system_metadata["runtime_components"][component_id]`.

Both the caller and every backend already load the spec, so both sides read the
same configuration. That matters because they are frequently not the same
filesystem — the memory backend runs a subprocess, a Ray actor can be on another
node — so resolving anything from a path *inside* an extracted code package would
require both sides to independently agree about that path. Resolving once, at
deploy time, in the process that is already importing the model's module, removes
that whole class of problem.

The practical consequences:

- A function deployed *without* a given component configured carries no entry for
  it. A caller that installs the component anyway gets a component that knows it
  has nothing to do. Components must treat this as normal and stay quiet —
  callers install components without knowing which models use them.
- Because the configuration is in the spec, and the spec feeds the function's
  uuid, changing it changes the function's identity. Reconfiguring a component
  means redeploying the model, which is the same rule that already applies to its
  parameters and code.

## Lifecycle and hooks

| hook | side | when |
|---|---|---|
| `contribute_spec_metadata(function_module_dir)` | packaging | once, at deploy, per configured component class |
| `start` / `stop` | runtime | once per runtime |
| `on_runtime_started(metadata)` | caller | once, after the backend starts |
| `before_call` / `after_call` | runtime | around every invocation |
| `collect_output()` | runtime | after every invocation; result is routed to the caller-side instance's `output` |
| `on_output_received(exception)` | caller | after `output` is routed |

`contribute_spec_metadata` is a **classmethod**, called on the class rather than
an instance, because at packaging time no instance exists — only the declaration
the model's module made via `configure()`. It receives the directory holding the
function's source module, so a component can resolve files the model owner
colocated with their code against the source tree, where they are unambiguously
present.

`on_runtime_started` receives that same recorded metadata back (or `None` when
the model didn't configure this component), which is how a caller-side instance
learns the model's configuration without reading anything off disk.

### Routing user-facing calls: `active_instance`

A component's user-facing entry point is normally a classmethod that routes to
whichever instance is live (`Logger.log(...)` → `cls.active_instance`). That
attribute is set by `before_call_components()` and cleared by
`after_call_components()`, so it names the instance whose invocation is
**currently in flight** — not merely one that has been started.

That scoping matters in both directions. A single loaded function invoked from
more than one thread routes correctly from each, and two separately loaded copies
of the same function don't steal each other's routing. A `log()` outside any
invocation is a no-op rather than landing in the next call's row.

**The invariant it rests on is one invocation at a time per process**, and it is
enforced rather than merely documented. A component instance buffers per-call
state, so it cannot serve overlapping invocations anyway. The memory backend runs
a single-threaded subprocess runloop and a Ray actor is single-threaded, so
neither can express the problem; local mode executes in the caller's thread, so
`LocalBackend.apply` refuses a second concurrent invocation of a function that has
components, with a `MetaflowFunctionRuntimeException` naming the constraint.

It raises rather than serialising on purpose: a lock would silently remove the
parallelism a threaded caller was asking for, trading one silent failure for
another. Functions with *no* components are not guarded — there is nothing to
interleave, so concurrent local invocation stays allowed.

Two things that are explicitly fine:

- **Threads the user's own function spawns.** They're inside the invocation, not
  separate invocations, so the guard never sees them, and `log()` from one of them
  lands in the right row (this is why `active_instance` is a plain class attribute
  and not thread-local — thread-local would silently drop those calls).
- **Nested invocation.** A function whose code invokes another rehydrated function
  in-process re-enters on the same thread, which the guard permits (the lock is
  reentrant), and routing is handed back to the outer call when the inner one
  finishes.

If in-process *concurrent* invocation ever becomes a requirement, the answer is
per-invocation state or one component instance per worker — not a cleverer
`active_instance`.

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
func.runtime_components[0].output
# {"call_count": 1, "last_duration_s": ..., "total_duration_s": ...}
close_function(func)
```

Because `runtime_components` is a parameter to `function_from_json`, each
caller decides independently which components (if any) to install for the
runtime it starts.

Note what does and doesn't change between those two calls. `RuntimeMetrics`
needs no configuration, so the reference and the function code are identical
either way — the only difference is whether the caller installs it. A component
that *is* configured by the model (see "A component's configuration is part of
the model" above) is different: the model's module calls `configure()`, and the
resolved configuration is part of the reference. The caller still chooses whether
to install it, but it no longer chooses what it does.
