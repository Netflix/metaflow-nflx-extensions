import contextvars
import weakref
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Optional

# Keyed by component class, not stored in the class's own __dict__: component
# classes get pickled wholesale (e.g. by Ray/cloudpickle, which walks
# cls.__dict__ to reconstruct dynamically-defined classes on the remote side),
# and a ContextVar isn't picklable. Keeping the vars here instead means
# pickling a component class never touches them.
_active_instance_vars: "weakref.WeakKeyDictionary[type, contextvars.ContextVar]" = (
    weakref.WeakKeyDictionary()
)


def _active_instance_var(cls: type) -> "contextvars.ContextVar[Optional[Any]]":
    var = _active_instance_vars.get(cls)
    if var is None:
        var = contextvars.ContextVar(f"{cls.__name__}_active_instance", default=None)
        _active_instance_vars[cls] = var
    return var


class ComponentMeta(ABCMeta):
    """
    Metaclass for runtime components.

    Manages the `active_instance` class attribute that the runtime sets after
    `start()` and clears after `stop()`.  Subclasses use it to implement their
    own no-op-when-inactive interaction patterns (e.g. a `log()` classmethod).

    `active_instance` is backed by a `ContextVar` (one per subclass, see
    `_active_instance_var()` above) rather than a plain class attribute, so
    routing is invocation-local instead of process-global: separate threads
    (each starts with a fresh top-level context) and separate asyncio tasks
    (each gets a copy of the enclosing context) don't see each other's active
    instance, so two runtimes for the same component class running
    concurrently can't cross-route calls into each other. `Cls.active_instance`
    read/write syntax is unchanged for callers -- the property below just
    routes it through the ContextVar instead of `__dict__`.

    Also gives every subclass its own `_class_config` dict (rather than one
    shared dict inherited from the base class) so that `configure()` calls on
    one component class never leak into another's config.
    """

    def __init__(cls, name: str, bases: tuple, namespace: dict) -> None:
        super().__init__(name, bases, namespace)
        cls._class_config = {}

    @property
    def active_instance(cls):
        return _active_instance_var(cls).get()

    @active_instance.setter
    def active_instance(cls, value) -> None:
        _active_instance_var(cls).set(value)


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
    * ``after_call``  — called after each function invocation, whether or not
      it raised (components must not assume the call succeeded)

    Subclasses define their own user-facing interaction pattern.  A common
    pattern is a classmethod that routes through ``active_instance``::

        class Logger(AbstractRuntimeComponent):
            @classmethod
            def log(cls, payload: dict) -> None:
                inst = cls.active_instance
                if inst is not None:
                    inst._entries.update(payload)

    Constructing an instance (or calling ``configure()``) never makes a
    component active. ``active_instance`` is only set by ``start_components()``,
    which runs inside a backend's ``apply()``/runtime bootstrap. Code that
    creates a component directly (e.g. in a notebook) or that imports and
    calls a decorated function directly, bypassing ``function_from_json`` and
    the backend machinery entirely, will never trigger ``start()`` — so
    ``active_instance``-routed calls like ``Logger.log(...)`` are safe no-ops
    in that case, not errors.

    Subclasses may also be configured from user code before the runtime
    starts them, via ``configure()``::

        Logger.configure(stream_name="my_stream", app_name="my_app")

    ``configure()`` kwargs land in ``cls._class_config`` (a dict private to
    each subclass, thanks to ``ComponentMeta``) and act as defaults.  The base
    ``__init__`` merges them under whatever kwargs the constructor receives —
    explicit constructor kwargs win on conflicts::

        Logger.configure(app_name="my_app")
        Logger(app_name="override")._init_kwargs  # {"app_name": "override"}
        Logger()._init_kwargs                      # {"app_name": "my_app"}

    Subclasses with a custom ``__init__`` signature must call
    ``super().__init__(**kwargs)`` (or replicate the merge) to get this
    behavior. Calling ``configure()`` after an instance is constructed has no
    effect on that instance — the merge only happens at construction time.
    """

    # Set by the runtime after start(); cleared after stop(). Backed by a
    # per-subclass ContextVar (see ComponentMeta) rather than this attribute --
    # this annotation is documentation only; the metaclass property shadows it.
    active_instance: Optional["AbstractRuntimeComponent"] = None

    # Populated by configure(). ComponentMeta gives every subclass its own
    # dict, so this annotation is documentation only.
    _class_config: Dict[str, Any] = {}

    # Stable per-call routing key, set by each subclass, e.g.:
    #     class Logger(AbstractRuntimeComponent):
    #         component_id = "logger"
    # Used to match a call's output back to the component that produced it
    # (see components/runtime.py, backends/*/[...]_backend.py) without
    # deriving a module/qualname string on every call.
    component_id: Optional[str] = None

    def __init__(self, **kwargs: Any) -> None:
        if type(self).component_id is None:
            raise NotImplementedError(
                f"{type(self).__name__} must set a class-level `component_id`"
            )
        self._init_kwargs = {**type(self)._class_config, **kwargs}
        self.output: Optional[Any] = None

    @classmethod
    def configure(cls, **kwargs: Any) -> None:
        """
        Set config for this component class from user code, before the
        runtime constructs and starts it.

        Call this at module level so config is available when the platform
        instantiates the component::

            MyComponent.configure(some_option="value")

        Repeated calls accumulate; last write wins per key. Calling
        ``configure()`` after the component has started has no effect on the
        already-running instance.
        """
        cls._class_config.update(kwargs)

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
    def after_call(
        self,
        *args: Any,
        exception: Optional[BaseException] = None,
        **kwargs: Any,
    ) -> None:
        """
        Called after each function invocation, whether or not it raised.

        ``exception`` is the exception the function call raised, or ``None``
        on success. Runs on the process/actor that actually executed the
        call (in-process for the local backend, in the subprocess for the
        memory backend, on the remote actor for the Ray backend).
        """

    def collect_output(
        self,
        *args: Any,
        exception: Optional[BaseException] = None,
        **kwargs: Any,
    ) -> Optional[Any]:
        """
        Called once after each ``after_call()``. Return a value to surface
        back to the caller via the component's ``output`` attribute on the
        caller-side handle returned by ``function_from_json``. Any picklable
        value works (dict, bytes, etc.) — the framework doesn't inspect it.

        ``exception`` is the exception the function call raised, or ``None``
        on success, so an override can decide whether to still surface
        partial output collected before the failure, or suppress it.

        Default implementation returns ``None`` (nothing surfaced). Override
        to report data collected during ``before_call``/``after_call``.
        """
        return None

    def on_runtime_started(self, function_package_dir: str) -> None:
        """
        Called once on the caller side, after the function's runtime has
        started, on the same instance returned by ``function_from_json``
        (not the reconstructed instance the runtime uses).

        ``function_package_dir`` is the directory *inside* the extracted code
        package that holds the function's module, so components can read the
        files the model owner colocated with their code. Note this is not the
        extraction root: files are archived under their dotted module path,
        so a function in ``a.b.c`` gets ``<extraction root>/a/b``. On the
        runtime side the equivalent is ``function.function_package_dir``.

        Default implementation is a no-op.
        """
        pass

    def on_output_received(self, exception: Optional[BaseException] = None) -> None:
        """
        Called once on the caller side after each function invocation
        completes, whether or not it raised, on the same instance returned
        by ``function_from_json``, right after ``output`` has been routed
        for that call (if any was routed).

        ``exception`` is the exception the call raised, or ``None`` on
        success. Override to react to a call's output or failure (e.g.
        decode and display output, or log a failure) without changing what
        the call returns/raises to user code.

        Default implementation is a no-op.
        """
        pass
