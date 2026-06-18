"""Helpers for activating/deactivating runtime components inside a subprocess."""

from typing import List, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from .abstract_component import AbstractRuntimeComponent


def load_component_classes(class_names: List[str]) -> List[Type["AbstractRuntimeComponent"]]:
    """Resolve fully-qualified class name strings to component classes."""
    from metaflow_extensions.nflx.plugins.functions.utils import load_type_from_string
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionException,
    )

    classes = []
    for name in class_names:
        cls = load_type_from_string(name)
        if cls is None:
            raise MetaflowFunctionException(
                f"Could not load runtime component class: {name}"
            )
        classes.append(cls)
    return classes


def start_components(
    component_classes: List[Type["AbstractRuntimeComponent"]], *args, **kwargs
) -> List["AbstractRuntimeComponent"]:
    """Instantiate each component class, call start(), and activate it on the class."""
    from .abstract_component import ComponentMeta

    instances = []
    for cls in component_classes:
        instance = ComponentMeta.create_instance(cls)
        instance.start(*args, **kwargs)
        cls._active_instance = instance
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
            type(instance)._active_instance = None


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
