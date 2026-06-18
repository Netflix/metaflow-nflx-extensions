from abc import ABCMeta, abstractmethod
from typing import Any, Optional


class ComponentMeta(ABCMeta):
    """
    Metaclass that makes calling the class route to the active runtime instance.

    When the runtime activates a component it sets `_active_instance` on the
    class.  After that, ``MyComponent(...)`` routes to the instance's
    ``__call__`` rather than constructing a new object.  If no instance is
    active the call is a silent no-op.

    The runtime creates instances via ``ComponentMeta.create_instance(cls)``
    which bypasses the routing and performs normal object construction.
    """

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        inst = cls.__dict__.get("_active_instance")
        if inst is not None:
            return inst.__call__(*args, **kwargs)
        # No-op when component is not loaded by the runtime.

    @staticmethod
    def create_instance(cls: "AbstractRuntimeComponent") -> "AbstractRuntimeComponent":
        """Create a fresh instance without triggering the user-call routing."""
        instance = cls.__new__(cls)
        instance.__init__()
        return instance


class AbstractRuntimeComponent(metaclass=ComponentMeta):
    """
    Base class for runtime components.

    Runtime components plug into the function execution lifecycle.  They are
    passed by class to ``function_from_json`` via ``runtime_components=[...]``
    and are instantiated inside the subprocess (or the calling process for the
    local backend).

    Lifecycle (all hooks accept ``*args, **kwargs`` for forward-compatibility):

    * ``start``       — called once when the runtime initialises
    * ``stop``        — called once when the runtime shuts down
    * ``before_call`` — called before each function invocation
    * ``after_call``  — called after each successful function invocation
    * ``__call__``    — user-facing entry point; user code calls the *class*
                        directly: ``MyComponent("msg")``

    The class itself is the user-facing API.  ``MyComponent(...)`` routes to
    the active instance's ``__call__`` if the component was loaded, or does
    nothing if it was not.  This means user code never has to guard with
    ``if component_is_loaded``.

    Example
    -------
    ::

        class LoggingComponent(AbstractRuntimeComponent):
            def start(self, *args, **kwargs):
                self._log = []

            def stop(self, *args, **kwargs):
                flush(self._log)

            def before_call(self, *args, **kwargs):
                self._log.append({"event": "before_call"})

            def after_call(self, *args, **kwargs):
                self._log.append({"event": "after_call"})

            def __call__(self, message, **kwargs):
                self._log.append({"event": "user", "message": message})

        # In user function code (no-op if LoggingComponent not loaded):
        LoggingComponent("processing batch", n=len(data))
    """

    # Set by the runtime after start(); cleared after stop().
    # Declared here so subclasses inherit it as a distinct per-class slot.
    _active_instance: Optional["AbstractRuntimeComponent"] = None

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

    @abstractmethod
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """User-facing entry point.  Called via ``MyComponent(...)`` in user code."""
