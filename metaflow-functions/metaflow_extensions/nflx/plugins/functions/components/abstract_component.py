from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional


class ComponentMeta(ABCMeta):
    """
    Metaclass for runtime components.

    Manages the `active_instance` class attribute that the runtime sets around
    each invocation.  Subclasses use it to implement their own
    no-op-when-inactive interaction patterns (e.g. a `log()` classmethod).

    `active_instance` is a plain per-subclass class attribute, set by
    `before_call_components()` and cleared by `after_call_components()` -- so it
    names the instance whose invocation is *currently in flight*, not merely one
    that has been started.

    **Invariant: one invocation at a time per process**, enforced rather than
    assumed. A component instance buffers per-call state (see e.g. ALBLogger's row
    buffer), so it cannot serve two overlapping invocations, and this attribute
    cannot name two instances at once. The memory backend (single-threaded
    subprocess runloop) and Ray (single-threaded actor) cannot express the
    problem; local mode runs in the caller's thread, so `LocalBackend.apply`
    refuses a concurrent second invocation of a component-bearing function.

    Being a plain attribute rather than thread-local is deliberate: threads the
    user's own function spawns are *inside* the invocation, and `log()` from one
    of them has to land in the current row. Thread-local storage would silently
    drop those calls. A previous version went further and used a per-subclass
    `ContextVar`, which was worse still: thread- and task-confined, so a component
    started on one thread was invisible on every other and logging silently did
    nothing.

    Also gives every subclass its own `_class_config` dict (rather than one
    shared dict inherited from the base class) so that `configure()` calls on
    one component class never leak into another's config.
    """

    # Every concrete component class, in definition order. Populated by
    # __init__ below, which already runs once per subclass. Packaging walks
    # this to ask each component what it wants recorded in the function spec
    # (see contribute_spec_metadata), so core packaging code needs no
    # knowledge of any particular component.
    registry: List[type] = []

    def __init__(cls, name: str, bases: tuple, namespace: dict) -> None:
        super().__init__(name, bases, namespace)
        cls._class_config = {}
        # Per-subclass, so one component class's active instance is never
        # visible as another's.
        cls.active_instance = None
        # Skip the base class itself: it has no component_id and never
        # contributes anything.
        if bases:
            ComponentMeta.registry.append(cls)


class AbstractRuntimeComponent(metaclass=ComponentMeta):
    """
    Base class for runtime components.

    Runtime components plug into the function execution lifecycle.  Pass class
    instances to ``function_from_json`` via ``runtime_components=[...]``::

        logger = Logger(debug=True)
        func = function_from_json(ref, runtime_components=[logger])

    Constructor keyword arguments are stored in ``_init_kwargs`` so instances
    can be reconstructed across subprocess boundaries.

    **A component's configuration belongs to the model, not to the caller.**
    Whether a component runs is the caller's choice (the argument above); what
    it is configured to do is declared by the model, at module level via
    ``configure()``, recorded at packaging time by
    ``contribute_spec_metadata()``, and carried in the function spec. So a
    component's settings travel with the function reference exactly like the
    function's parameters and environment do -- see the README in this
    directory.

    Lifecycle (runtime hooks accept ``*args, **kwargs`` for
    forward-compatibility):

    * ``contribute_spec_metadata`` — classmethod, called at *packaging* time on
      every configured component class; what it returns is recorded in the spec
    * ``start``       — called once when the runtime initialises
    * ``stop``        — called once when the runtime shuts down
    * ``before_call`` — called before each function invocation
    * ``after_call``  — called after each function invocation, whether or not
      it raised (components must not assume the call succeeded)
    * ``collect_output`` — called after each invocation; the result is routed to
      the caller-side instance's ``output``
    * ``on_runtime_started`` — caller side, once, with this component's recorded
      spec metadata (or ``None`` if the model didn't configure it)
    * ``on_output_received`` — caller side, after ``output`` is routed

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

    @classmethod
    def contribute_spec_metadata(
        cls, function_module_dir: str
    ) -> Optional[Dict[str, Any]]:
        """
        Called at **package/deploy time** for every configured component, to
        record whatever this component needs at runtime into the function's
        spec. Whatever is returned lands in
        ``spec.system_metadata["runtime_components"][component_id]`` and is
        then readable by the caller *and* by every backend, since both already
        load the spec.

        ``function_module_dir`` is the directory holding the function's source
        module, in the process doing the packaging -- so a component can resolve
        a file the model owner colocated with their code (a schema, a config)
        against the *source tree*, where it is unambiguously present. Resolving
        such files here rather than at runtime is the point: it removes any need
        for the caller and the runtime to independently agree on a path inside
        an extracted code package.

        Called on **every** component class, whether or not the user configured
        it — a component with no required configuration, or one deriving what it
        records from the function itself, still gets its chance. Components
        self-gate: return ``None`` to record nothing, which is the default. It
        must therefore tolerate being called when the user declared nothing.

        Note this runs in the packaging process, which has necessarily already
        imported the function's module (the decorator had to run for the
        function to exist) -- so a module-level ``configure()`` call has already
        taken effect by the time this is called.
        """
        return None

    def on_runtime_started(self, metadata: Optional[Dict[str, Any]]) -> None:
        """
        Called once on the caller side, after the function's runtime has
        started, on the same instance returned by ``function_from_json``
        (not the reconstructed instance the runtime uses).

        ``metadata`` is whatever this component's
        ``contribute_spec_metadata()`` recorded in the function spec at deploy
        time, or ``None`` if it recorded nothing -- which is the signal that
        this function was not deployed with this component configured. A
        component must treat ``None`` as "not configured for this function" and
        stay quiet: the caller installs components without knowing whether any
        given function uses them.

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
