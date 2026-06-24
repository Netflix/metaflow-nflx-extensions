"""Helpers for serializing, activating, and deactivating runtime components."""

import json
from typing import List, Type, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .abstract_component import AbstractRuntimeComponent


def serialize_components(
    components: List[Union[Type["AbstractRuntimeComponent"], "AbstractRuntimeComponent"]],
) -> List[str]:
    """Serialize a list of component classes or instances to strings.

    Classes produce ``"module.ClassName"``.
    Instances produce ``"module.ClassName:json_kwargs"`` using the kwargs
    captured in ``_init_kwargs`` at construction time.
    """
    result = []
    for c in components:
        if isinstance(c, type):
            result.append(f"{c.__module__}.{c.__qualname__}")
        else:
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
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionException,
    )

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
    components: List[Union[Type["AbstractRuntimeComponent"], "AbstractRuntimeComponent"]],
    *args,
    **kwargs,
) -> List["AbstractRuntimeComponent"]:
    """Start a list of component classes or instances.

    Classes are instantiated with no arguments.  Instances are used directly.
    ``start()`` is called on each and ``active_instance`` is set on the class.
    """
    instances = []
    for item in components:
        instance = item() if isinstance(item, type) else item
        instance.start(*args, **kwargs)
        type(instance).active_instance = instance
        instances.append(instance)
    return instances


def stop_components(
    instances: List["AbstractRuntimeComponent"], *args, **kwargs
) -> None:
    """Call stop() on each instance and deactivate it from its class."""
    for instance in instances:
        try:
            instance.stop(*args, **kwargs)
        finally:
            type(instance).active_instance = None


def before_call_components(
    instances: List["AbstractRuntimeComponent"], *args, **kwargs
) -> None:
    for instance in instances:
        instance.before_call(*args, **kwargs)


def after_call_components(
    instances: List["AbstractRuntimeComponent"], *args, **kwargs
) -> None:
    for instance in instances:
        instance.after_call(*args, **kwargs)
