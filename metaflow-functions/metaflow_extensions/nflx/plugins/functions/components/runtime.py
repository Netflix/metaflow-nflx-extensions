"""Helpers for serializing, activating, and deactivating runtime components."""

import json
from typing import Any, Dict, List, Tuple, TYPE_CHECKING, cast

from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
    MetaflowFunctionRuntimeException,
)

if TYPE_CHECKING:
    from .abstract_component import AbstractRuntimeComponent


def serialize_components(
    components: List["AbstractRuntimeComponent"],
) -> List[str]:
    """Serialize a list of component instances to strings.

    Produces ``"module.ClassName:json_kwargs"`` using the kwargs captured in
    ``_init_kwargs`` at construction time.
    """
    result = []
    for c in components:
        cls = type(c)
        kwargs_json = json.dumps(c._init_kwargs)
        result.append(f"{cls.__module__}.{cls.__qualname__}:{kwargs_json}")
    return result


def load_component_instances(
    specs: List[str],
) -> List["AbstractRuntimeComponent"]:
    """Reconstruct component instances from serialized spec strings.

    Accepts both ``"module.ClassName"`` (no-arg construction) and
    ``"module.ClassName:json_kwargs"`` (keyword construction) formats.
    """
    from metaflow_extensions.nflx.plugins.functions.utils import load_type_from_string

    instances = []
    for spec in specs:
        if ":" in spec:
            class_name, kwargs_json = spec.split(":", 1)
            kwargs = json.loads(kwargs_json)
        else:
            class_name = spec
            kwargs = {}

        cls = load_type_from_string(class_name)
        if cls is None:
            raise MetaflowFunctionException(
                f"Could not load runtime component class: {class_name}"
            )
        instances.append(cls(**kwargs))
    return instances


def start_components(
    instances: List["AbstractRuntimeComponent"],
    function: Any = None,
    *args,
    **kwargs,
) -> List["AbstractRuntimeComponent"]:
    """Start a list of component instances.

    ``start()`` is called on each and ``active_instance`` is set on the class.
    ``function`` is the ``MetaflowFunction`` instance being started, passed
    through so components can access things like ``function.function_root_dir``
    on the runtime side, where they have no other way to reach it.

    If a component fails to start, the components started so far are stopped
    (best-effort, in reverse start order) before the failure is re-raised as
    a ``MetaflowFunctionRuntimeException``, so a partial startup never leaves
    live, unreferenced components behind.
    """
    started = []
    for instance in instances:
        try:
            instance.start(*args, function=function, **kwargs)
        except Exception as e:
            _stop_components_best_effort(list(reversed(started)), *args, **kwargs)
            raise MetaflowFunctionRuntimeException(
                f"{type(instance).__name__} failed to start: {e!r}"
            ) from e
        type(instance).active_instance = instance
        started.append(instance)
    return instances


def _stop_components_best_effort(
    instances: List["AbstractRuntimeComponent"], *args, **kwargs
) -> List[Tuple[str, Exception]]:
    """Call stop() on every instance and deactivate it from its class.

    Best-effort cleanup: a failure in one component's stop() must not prevent
    the remaining components from being stopped and deactivated. Errors are
    collected and returned (rather than raised) as ``(name, exception)``
    pairs so callers can fold them into their own error reporting.

    Returns:
        A list of ``(component_class_name, exception)`` pairs, one per
        instance whose ``stop()`` raised; empty if all instances stopped
        cleanly.
    """
    errors: List[Tuple[str, Exception]] = []
    for instance in instances:
        try:
            instance.stop(*args, **kwargs)
        except Exception as e:  # noqa: BLE001 - cleanup must not short-circuit
            errors.append((type(instance).__name__, e))
        finally:
            type(instance).active_instance = None
    return errors


def stop_components(
    instances: List["AbstractRuntimeComponent"], *args, **kwargs
) -> None:
    """Call stop() on every instance and deactivate it from its class.

    Best-effort cleanup: a failure in one component's stop() must not prevent
    the remaining components from being stopped and deactivated. Any errors
    are collected and re-raised together after all instances are drained.
    """
    errors = _stop_components_best_effort(instances, *args, **kwargs)
    if errors:
        summary = ", ".join(f"{name}: {err!r}" for name, err in errors)
        raise MetaflowFunctionException(
            f"{len(errors)} runtime component(s) failed to stop: {summary}"
        ) from errors[0][1]


def before_call_components(
    instances: List["AbstractRuntimeComponent"], *args, **kwargs
) -> None:
    for instance in instances:
        instance.before_call(*args, **kwargs)


def after_call_components(
    instances: List["AbstractRuntimeComponent"], *args, **kwargs
) -> Dict[str, Any]:
    """Call after_call() then collect_output() on each instance.

    Returns a ``{component_id: output}`` map for instances whose
    ``collect_output()`` returned non-``None``, keyed by each component's
    ``component_id`` so callers can match output back to the component that
    produced it.
    """
    collected: Dict[str, Any] = {}
    for instance in instances:
        instance.after_call(*args, **kwargs)
        output = instance.collect_output(*args, **kwargs)
        if output is not None:
            instance.output = output
            collected[cast(str, type(instance).component_id)] = output
    return collected
