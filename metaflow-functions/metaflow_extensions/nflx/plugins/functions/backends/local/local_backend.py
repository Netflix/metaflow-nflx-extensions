from typing import Any, Dict, List, Optional
from ..abstract_backend import AbstractBackend
from ..backend_type import BackendType
from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
    get_global_registry,
)
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionRuntimeException,
    MetaflowFunctionException,
    MetaflowFunctionUserException,
)
from metaflow_extensions.nflx.plugins.functions.common.runtime_utils import (
    create_function_parameters,
)
from metaflow_extensions.nflx.plugins.functions.debug import debug
import sys
import threading
import traceback
from contextlib import contextmanager


# Runtime components cannot serve two invocations at once: routing
# (``Cls.active_instance``) is class-level state and a component's per-call
# buffer lives on the instance, so overlapping invocations interleave both. The
# other backends can't hit this -- the memory backend runs a single-threaded
# subprocess runloop and a Ray actor is single-threaded -- but local mode
# executes in the caller's thread, so a threaded caller can.
#
# Reentrant on purpose: acquire(blocking=False) then succeeds for the *same*
# thread, so an invocation nested inside another one (or anything else
# re-entering on one thread) is allowed, while a genuinely concurrent
# invocation from a second thread is refused.
_COMPONENT_INVOCATION_LOCK = threading.RLock()

# Directories start() has put on sys.path, and how many warmed functions still
# want each one there. Refcounted rather than a plain set because two functions
# from the same code package share a root directory: closing one must not break
# the other's deferred imports. close() drops the last reference and the entry
# leaves sys.path with it, so a server that loads and unloads N functions does
# not accumulate N dead entries at the front of every import search.
_SYS_PATH_LOCK = threading.Lock()
_SYS_PATH_REFCOUNTS: Dict[str, int] = {}

# Warm-up and teardown are both check-then-act on the same handle: read
# _local_concrete, hydrate, take the sys.path references, cache -- or the
# reverse. Two threads doing that to one handle concurrently would both pass
# the "already warm?" test, both hydrate, and double-count the refcount, so
# the directory could never be released and one of the two concretes (with
# whatever components it started) would be orphaned. Held per handle rather
# than globally so loading two different functions still overlaps.
_WARMUP_LOCK_REGISTRY_LOCK = threading.Lock()


@contextmanager
def _guard_component_invocation(func_instance):
    """Refuse a concurrent invocation of a function that has runtime components.

    Raises rather than serialising. Serialising would silently remove the
    parallelism a threaded caller was asking for; raising says what the
    constraint is. Functions with no components are unaffected -- there is
    nothing to interleave, so concurrent local invocation stays allowed.
    """
    if not getattr(func_instance, "_runtime_components", None):
        yield
        return

    if not _COMPONENT_INVOCATION_LOCK.acquire(blocking=False):
        raise MetaflowFunctionRuntimeException(
            f"Function '{func_instance.name}' has runtime components and is "
            "already being invoked on another thread. Runtime components do not "
            "support concurrent invocation: routing and per-call buffers are "
            "shared, so overlapping calls would mix rows between invocations. "
            "Invoke it from one thread at a time, or load a separate copy per "
            "thread and serialise calls within each."
        )
    try:
        yield
    finally:
        _COMPONENT_INVOCATION_LOCK.release()


class LocalBackend(AbstractBackend):
    """
    Backend for direct in-process execution.

    Executes functions directly in the same Python process with no isolation.
    This is the simplest backend with minimal overhead, suitable for:
    - Development and debugging
    - Quick prototyping
    - Functions that don't need isolation or parallelization
    """

    @property
    def backend_type(self) -> BackendType:
        return BackendType.LOCAL

    @staticmethod
    def _is_proxy(func_instance) -> bool:
        """True when this handle still needs its code package hydrated.

        Reads the marker ``_create_proxy_from_spec`` sets rather than inferring
        it from ``_func``: a *concrete* FunctionPipeline is also built with
        ``func=None`` (``FunctionPipeline._create_from_spec``), so the
        ``_func is None`` test read one as a proxy and hydrated a second copy
        of every constituent's code package.
        """
        return bool(getattr(func_instance, "_is_proxy_handle", False))

    @staticmethod
    def _root_dirs(concrete) -> List[str]:
        """Extraction directories whose contents the function may import.

        A pipeline's own directory is near-empty -- each constituent extracts
        its own package -- and ``function_root_dir`` reports only the first
        constituent's, so a deferred import in constituent #2 would not
        resolve. Every constituent's directory goes on the path.
        """
        dirs = []
        for handle in [concrete] + list(getattr(concrete, "functions", []) or []):
            try:
                root_dir = handle.function_root_dir
            except MetaflowFunctionException as e:
                # Only raised as "Function root dir is not set", which is the
                # normal state for a handle that owns no extracted code. Left
                # visible: a function whose extraction half-failed shows up
                # here rather than as an opaque ModuleNotFoundError later.
                debug.functions_exec(
                    "LocalBackend: no root dir for '%s' (%s)"
                    % (getattr(handle, "name", handle), e)
                )
                continue
            if root_dir and root_dir not in dirs:
                dirs.append(root_dir)
        return dirs

    @staticmethod
    def _warmup_lock(func_instance) -> threading.Lock:
        """The per-handle lock serializing start()/close() on that handle."""
        with _WARMUP_LOCK_REGISTRY_LOCK:
            lock = getattr(func_instance, "_local_warmup_lock", None)
            if lock is None:
                lock = threading.Lock()
                func_instance._local_warmup_lock = lock
            return lock

    @staticmethod
    def _acquire_sys_path(dirs: List[str]) -> None:
        """Put each directory on sys.path, once, and record the reference.

        Front-inserted to match ``run_in_path``, which is what was on the path
        while the function's own modules were imported at load time; a deferred
        import must resolve to the same module the load-time import would have.
        """
        with _SYS_PATH_LOCK:
            for root_dir in dirs:
                _SYS_PATH_REFCOUNTS[root_dir] = (
                    _SYS_PATH_REFCOUNTS.get(root_dir, 0) + 1
                )
                if root_dir not in sys.path:
                    sys.path.insert(0, root_dir)

    @staticmethod
    def _release_sys_path(dirs: List[str]) -> None:
        """Drop references, removing a directory once nothing wants it."""
        with _SYS_PATH_LOCK:
            for root_dir in dirs:
                remaining = _SYS_PATH_REFCOUNTS.get(root_dir, 0) - 1
                if remaining > 0:
                    _SYS_PATH_REFCOUNTS[root_dir] = remaining
                    continue
                _SYS_PATH_REFCOUNTS.pop(root_dir, None)
                while root_dir in sys.path:
                    sys.path.remove(root_dir)

    @classmethod
    def _hydrate(cls, func_instance):
        """Materialize a proxy handle into a concrete, executable function.

        Downloads and extracts the code package, then loads the decorated
        function out of it. Expensive -- start() calls this once so apply()
        does not have to.
        """
        from metaflow_extensions.nflx.plugins.functions.core.function import (
            function_from_json,
        )
        from metaflow_extensions.nflx.plugins.functions.core.function_spec import (
            FunctionSpec,
        )

        func_spec = func_instance.spec

        if not func_spec.reference:
            raise MetaflowFunctionRuntimeException(
                "Function spec missing reference path"
            )

        # Download S3 reference to local temp file if needed
        local_reference = FunctionSpec.download_to_temp(func_spec.reference)

        # Carry the proxy's runtime_components over to the concrete function -
        # function_from_json() below has no way to see the proxy's, and would
        # otherwise silently default to none.
        runtime_components = getattr(func_instance, "_runtime_components", [])

        # Load concrete function from reference. This handles both regular functions
        # and pipelines by delegating to the appropriate from_spec() implementation.
        # Don't start runtime - function executes directly in this process
        return function_from_json(
            local_reference,
            use_proxy=False,
            backend="local",
            start_runtime=False,
            runtime_components=runtime_components,
        )

    @classmethod
    def start(cls, func_instance, **kwargs):
        """
        Warm up in-process execution so the first call is not the slow one.

        The local backend has no runtime to launch, but it does have per-call
        work that only needs doing once. Without this, ``apply()`` re-hydrates
        the code package on every invocation of a proxy handle and rebuilds the
        function parameters each time -- fine for a script, wrong for a server
        that loads a function once and then calls it under latency SLOs.

        Three things are set up here:

        * the hydrated concrete function, cached on the handle as
          ``_local_concrete``;
        * its ``FunctionParameters``, cached as ``_local_params`` and honouring
          the ``_prefetch_artifacts`` flag that ``function_from_json`` sets when
          called with ``start_runtime=True``, so artifacts are resolved now
          rather than on the first call;
        * the function's root directory on ``sys.path``, *persistently*.
          ``from_spec()`` only adds it for the duration of the load (see
          ``run_in_path``, which restores ``sys.path`` in a ``finally``), so any
          import the user's code defers to call time -- generated protobuf
          stubs are the common case -- fails after the load window closes.

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance to prepare
        """
        with cls._warmup_lock(func_instance):
            # Idempotent: a second start() without an intervening close() must
            # not replace the warmed function. Components start lazily on
            # whichever handle apply() ran, and close() only stops the
            # currently cached one, so overwriting the cache would leave the
            # first concrete's emitters and threads running with nothing left
            # holding them. Under the lock so the check cannot be overtaken by
            # a concurrent start() on the same handle.
            already_warm = getattr(func_instance, "_local_concrete", None)
            if already_warm is not None:
                debug.functions_exec(
                    "LocalBackend.start: '%s' is already warm, nothing to do"
                    % already_warm.name
                )
                return

            concrete = (
                cls._hydrate(func_instance)
                if cls._is_proxy(func_instance)
                else func_instance
            )

            root_dirs = cls._root_dirs(concrete)
            cls._acquire_sys_path(root_dirs)

            prefetch = getattr(func_instance, "_prefetch_artifacts", False)
            params = create_function_parameters(
                concrete.spec, prefetch_artifacts=prefetch
            )

            # Cache on both handles: the caller keeps hold of the proxy, while
            # apply() may be handed either one. The path list rides along so
            # close() can give back exactly what this start() took.
            for handle in (func_instance, concrete):
                handle._local_concrete = concrete
                handle._local_params = params
                handle._local_sys_path = root_dirs

            debug.functions_exec(
                "LocalBackend.start: warmed '%s' (prefetch_artifacts=%s, sys.path+=%s)"
                % (concrete.name, prefetch, root_dirs)
            )

    @classmethod
    async def apply_async(cls, func_instance, data: Any, **kwargs) -> Any:
        return cls.apply(func_instance, data, **kwargs)

    @classmethod
    def apply(cls, func_instance, data: Any, **kwargs) -> Any:
        """
        Execute function directly in the same process.

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance to execute
        data : Any
            Input data for the function
        **kwargs : Any
            Additional keyword arguments

        Returns
        -------
        Any
            Result from function execution
        """
        # If func_instance is still a proxy, convert to a concrete function.
        # start() does this once up front; without it, every call pays for it.
        if cls._is_proxy(func_instance):
            cached = getattr(func_instance, "_local_concrete", None)
            func_instance = (
                cached if cached is not None else cls._hydrate(func_instance)
            )

        # Use params from kwargs if provided, then whatever start() prepared,
        # otherwise create new ones.
        parameters = kwargs.pop("params", None)
        if parameters is None:
            parameters = getattr(func_instance, "_local_params", None)
        if parameters is None:
            parameters = create_function_parameters(func_instance.spec)

        from metaflow_extensions.nflx.plugins.functions.components.runtime import (
            start_components,
            before_call_components,
            after_call_components,
        )

        # One invocation at a time for a function with components -- see
        # _guard_component_invocation above.
        with _guard_component_invocation(func_instance):
            if not func_instance._component_instances:
                func_instance._component_instances = start_components(
                    getattr(func_instance, "_runtime_components", []),
                    function=func_instance,
                )

            try:
                before_call_components(func_instance._component_instances)
            except Exception as e:
                raise MetaflowFunctionRuntimeException(
                    f"Runtime component exception in function '{func_instance.name}': {str(e)}\n{traceback.format_exc()}"
                )

            user_exception: Optional[MetaflowFunctionUserException]
            try:
                result = func_instance.execute(data, parameters, **kwargs)
            except Exception as e:
                user_exception = MetaflowFunctionUserException(
                    f"Exception in function '{func_instance.name}': {str(e)}\n{traceback.format_exc()}"
                )
                result = None
            else:
                user_exception = None

            # after_call must run whether or not the function call itself failed,
            # so components (e.g. metrics/logging) see every invocation.
            # TODO(local-backend exception parity): thread the raw exception
            # through here (`after_call_components(func_instance._component_instances,
            # exception=raw_exception)`) so after_call()/collect_output() can see
            # the failure, matching memory_backend.py. Requires keeping a
            # reference to the raw exception from the `except Exception as e:`
            # block above (currently only its wrapped `MetaflowFunctionUserException`
            # message is kept, not the exception object itself).
            try:
                after_call_components(func_instance._component_instances)
            except Exception as e:
                if user_exception is None:
                    raise MetaflowFunctionRuntimeException(
                        f"Runtime component exception in function '{func_instance.name}': {str(e)}\n{traceback.format_exc()}"
                    )
                # A user exception is already in flight; don't let a component
                # failure on the error path mask it.
                debug.functions_exec(
                    f"Runtime component exception in after_call for '{func_instance.name}' "
                    f"while handling a prior user exception: {e!r}"
                )

            if user_exception is not None:
                raise user_exception

        return result

    @classmethod
    def close(cls, func_instance, clean_dir: bool = True, **kwargs):
        # Same handle lock start() holds: teardown reads the cache it wrote and
        # gives back the sys.path references it took, so the two must not
        # interleave.
        with cls._warmup_lock(func_instance):
            cls._close_locked(func_instance)

    @classmethod
    def _close_locked(cls, func_instance):
        # Components were started on whatever handle apply() ran, which is the
        # concrete function start() cached -- not necessarily the proxy the
        # caller is closing.
        target = getattr(func_instance, "_local_concrete", None) or func_instance

        instances = target._component_instances
        try:
            if instances:
                from metaflow_extensions.nflx.plugins.functions.components.runtime import (
                    stop_components,
                )

                stop_components(instances)
        finally:
            # stop_components() raises when any component's stop() fails, and a
            # component that failed to stop is not a component to keep calling.
            # Clearing in a finally is what stops the next apply() from running
            # before_call/after_call against stopped components, and stops
            # _local_concrete from outliving the function it names.
            target._component_instances = []
            released = None
            for handle in (func_instance, target):
                released = getattr(handle, "_local_sys_path", None) or released
                handle._local_concrete = None
                handle._local_params = None
                handle._local_sys_path = None
            if released:
                cls._release_sys_path(released)

    @classmethod
    def apply_binary(cls, func_instance, data: bytes, **kwargs) -> bytes:
        """
        Execute function with binary serialized data.

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance to execute
        data : bytes
            Serialized input data
        **kwargs : Any
            Additional keyword arguments

        Returns
        -------
        bytes
            Serialized result
        """
        registry = get_global_registry()
        # input_types is a property on MetaflowFunction/FunctionPipeline, not a
        # classmethod taking a spec.
        input_types = func_instance.input_types
        expected_input_type = cls._map_type_info_to_python_type(
            input_types, type(func_instance), func_instance.spec
        )
        deserialized_data = registry.deserialize(data, expected_input_type)

        # apply() takes the user's own input type and returns the user's own
        # output type -- same contract as the memory backend. Wrapping the input
        # in a FunctionPayload here handed the user's function the wrapper
        # instead of its declared input; unwrapping `.data` from the result did
        # the mirror of that on the way out.
        result = cls.apply(func_instance, deserialized_data, **kwargs)

        serializer = registry.get_serializer_for_type(type(result))
        if serializer is None:
            raise MetaflowFunctionException(
                f"No serializer registered for type {type(result)}"
            )

        serialized_data, _ = serializer(result)
        return serialized_data

    @classmethod
    def _map_type_info_to_python_type(cls, type_info, function_cls, func_spec):
        """Helper to map type info to Python type."""
        type_name = type_info.get("type")
        from metaflow_extensions.nflx.plugins.functions.utils import (
            load_type_from_string,
        )

        loaded_class = load_type_from_string(type_name)
        if not loaded_class:
            raise MetaflowFunctionRuntimeException(f"Could not load class {type_name}")
        return loaded_class
