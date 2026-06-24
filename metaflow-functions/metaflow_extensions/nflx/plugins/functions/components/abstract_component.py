from abc import ABCMeta, abstractmethod
from typing import Any, Optional


class ComponentMeta(ABCMeta):
    """
    Metaclass for runtime components.

    Manages the `active_instance` class attribute that the runtime sets after
    `start()` and clears after `stop()`.  Subclasses use it to implement their
    own no-op-when-inactive interaction patterns (e.g. a `log()` classmethod).
    """


class AbstractRuntimeComponent(metaclass=ComponentMeta):
    """
    Base class for runtime components.

    Runtime components plug into the function execution lifecycle.  Pass class
    instances to ``function_from_json`` via ``runtime_components=[...]``::

        logger = Logger(stream_name="my_stream", app_name="my_app")
        func = function_from_json(ref, runtime_components=[logger])

    Constructor keyword arguments are stored in ``_init_kwargs`` so instances
    can be reconstructed across subprocess boundaries.

    Lifecycle (all hooks accept ``*args, **kwargs`` for forward-compatibility):

    * ``start``       — called once when the runtime initialises
    * ``stop``        — called once when the runtime shuts down
    * ``before_call`` — called before each function invocation
    * ``after_call``  — called after each successful function invocation

    Subclasses define their own user-facing interaction pattern.  A common
    pattern is a classmethod that routes through ``active_instance``::

        class Logger(AbstractRuntimeComponent):
            @classmethod
            def log(cls, payload: dict) -> None:
                inst = cls.active_instance
                if inst is not None:
                    inst._entries.update(payload)
    """

    # Set by the runtime after start(); cleared after stop().
    # Declared here so subclasses inherit it as a distinct per-class slot.
    active_instance: Optional["AbstractRuntimeComponent"] = None

    def __init__(self, **kwargs: Any) -> None:
        self._init_kwargs = kwargs

    @abstractmethod
    def start(self, *args: Any, **kwargs: Any) -> None:
        """Called once when the runtime initialises."""

    @abstractmethod
    def stop(self, *args: Any, **kwargs: Any) -> None:
        """Called once when the runtime shuts down."""

    @abstractmethod
    def before_call(self, *args: Any, **kwargs: Any) -> None:
        """Called before each function invocation."""

    @abstractmethod
    def after_call(self, *args: Any, **kwargs: Any) -> None:
        """Called after each function invocation."""
