from typing import Any, Dict, List, Optional, Tuple
from ..abstract_backend import AbstractBackend
from ..backend_type import BackendType
from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
    get_global_registry,
)
from metaflow_extensions.nflx.plugins.functions.core.function_payload import (
    FunctionPayload,
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
from dataclasses import dataclass
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

# Identity of the warmed function a handle attaches to. Runtime components are
# part of it for the same reason they are in the Ray backend's key: two handles
# on the same function with different components must not share one warmed
# copy, since components are started against the concrete function.
LocalWarmKey = Tuple[str, Tuple[str, ...]]


@dataclass
class _WarmEntry:
    concrete: Any
    params: Any
    sys_path_dirs: List[str]
    attached: int = 0  # number of handles currently attached to this entry


_WARM_POOL: Dict[LocalWarmKey, _WarmEntry] = {}
_POOL_LOCK = threading.RLock()

# Directories the pool has put on sys.path, and how many entries still want
# each one there. Refcounted separately from the pool because two entries can
# share a root directory -- a pipeline's constituents do -- so tearing one down
# must not break the other's deferred imports.
_SYS_PATH_LOCK = threading.Lock()
_SYS_PATH_REFCOUNTS: Dict[str, int] = {}


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
    def _needs_hydration(func_instance) -> bool:
        """Whether this handle still has to load its code before it can run.

        Not the same as `_func is None`: a pipeline never has a single
        decorated function, so that test calls every pipeline a proxy.
        Re-hydrating a concrete one re-downloads code it already has;
        re-hydrating a locally built one replaces the caller's object with a
        copy from the datastore. A pipeline is ready when its constituents are.
        """
        constituents = getattr(func_instance, "functions", None)
        if constituents is not None:
            return any(LocalBackend._needs_hydration(c) for c in constituents)
        # A handle with no `_func` at all is not a partly-built MetaflowFunction
        # -- it is something else standing in for one, and hydrating it would
        # fail. Only an explicit None means "spec loaded, code not yet".
        return hasattr(func_instance, "_func") and func_instance._func is None

    @classmethod
    def _resolve_key(cls, func_instance) -> Optional[LocalWarmKey]:
        """Identity of the warm entry a handle would attach to.

        ``None`` for a function built in this process: it has no uuid, so
        there is no identity two handles could match on. The other backends
        never see one -- they can only run a published reference -- so this
        case is unique to local and takes the un-pooled path below.
        """
        from metaflow_extensions.nflx.plugins.functions.components.runtime import (
            serialize_components,
        )

        # A handle with no _runtime_id slot is not a MetaflowFunction (which
        # declares it as a class attribute) -- it is something standing in for
        # one, and it has no runtime identity to pool on.
        if not hasattr(func_instance, "_runtime_id"):
            return None

        try:
            uuid = func_instance.uuid
        except (AttributeError, MetaflowFunctionException):
            return None
        if uuid is None:
            return None

        component_specs = tuple(
            serialize_components(getattr(func_instance, "_runtime_components", []))
        )
        return (uuid, component_specs)

    @staticmethod
    def _root_dirs(concrete) -> List[str]:
        """Extraction directories whose contents the function may import.

        A pipeline's own directory is near-empty -- each constituent extracts
        its own package -- and ``function_root_dir`` reports only the first
        constituent's, so a deferred import in constituent #2 would not
        resolve. Every constituent's directory goes on the path.
        """
        dirs: List[str] = []
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
    def _acquire_sys_path(dirs: List[str]) -> None:
        """Put each directory on sys.path, once, and record the reference.

        Front-inserted to match ``run_in_path``, which is what was on the path
        while the function's own modules were imported at load time; a deferred
        import must resolve to the same module the load-time import would have.
        """
        with _SYS_PATH_LOCK:
            for root_dir in dirs:
                _SYS_PATH_REFCOUNTS[root_dir] = _SYS_PATH_REFCOUNTS.get(root_dir, 0) + 1
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
        """Materialize a proxy handle into a concrete, executable function."""
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

    @staticmethod
    def _reject_multiprocess(process) -> None:
        """Refuse a process count this backend cannot honour.

        The memory backend keys a runtime on ``process`` and forks that many
        workers. Local executes in the caller's thread, so there is nothing to
        fork; accepting the argument would report parallelism the caller does
        not get.
        """
        if process is not None and process > 1:
            raise MetaflowFunctionRuntimeException(
                f"The local backend cannot run a function with process={process}: "
                "it executes in the calling thread and has no worker processes. "
                "Use the memory backend for multiple processes, or drop the "
                "argument to run in-process."
            )

    @classmethod
    def _get_or_create_entry(cls, func_instance) -> Optional[_WarmEntry]:
        """Attach the handle to a warm entry, creating one if needed.

        ``None`` when the handle has no poolable identity; the caller falls
        back to running it directly.
        """
        # Once a handle has attached, reuse its resolved key rather than
        # recomputing it (which hashes runtime component specs) on every call.
        already_attached = getattr(func_instance, "_runtime_id", None) is not None

        with _POOL_LOCK:
            key = (
                func_instance._runtime_id
                if already_attached
                else cls._resolve_key(func_instance)
            )
            if key is None:
                return None

            entry = _WARM_POOL.get(key)
            if entry is None:
                concrete = (
                    cls._hydrate(func_instance)
                    if cls._needs_hydration(func_instance)
                    else func_instance
                )
                root_dirs = cls._root_dirs(concrete)
                cls._acquire_sys_path(root_dirs)
                params = create_function_parameters(
                    concrete.spec,
                    prefetch_artifacts=getattr(
                        func_instance, "_prefetch_artifacts", False
                    ),
                )
                entry = _WarmEntry(concrete, params, root_dirs)
                _WARM_POOL[key] = entry
                debug.functions_exec(
                    "LocalBackend: warmed '%s' (sys.path+=%s)"
                    % (concrete.name, root_dirs)
                )

            if not already_attached:
                func_instance._runtime_id = key
                entry.attached += 1

            return entry

    @classmethod
    def _lookup(cls, func_instance) -> Optional[_WarmEntry]:
        """The warm entry this handle is attached to, or None."""
        key = func_instance._runtime_id
        if key is None:
            return None
        with _POOL_LOCK:
            return _WARM_POOL.get(key)

    @classmethod
    def start(cls, func_instance, **kwargs):
        """
        Warm up in-process execution so the first call is not the slow one.

        The local backend has no runtime to launch, but it does have per-call
        work that only needs doing once: hydrating the code package, resolving
        ``FunctionParameters``, and putting the function root on ``sys.path``.
        ``from_spec()`` only adds that root for the duration of the load (see
        ``run_in_path``, which restores ``sys.path`` in a ``finally``), so an
        import the user's code defers to call time -- generated protobuf stubs
        are the common case -- fails after the load window closes.

        Like the memory and Ray backends, this is an optimization and not a
        precondition: ``apply()`` performs the same get-or-create, so a handle
        that never calls ``start()`` still works.
        """
        cls._reject_multiprocess(kwargs.get("process"))
        if cls._get_or_create_entry(func_instance) is None:
            debug.functions_exec(
                "LocalBackend.start: '%s' has no uuid to pool on; it is already "
                "in this process, so there is nothing to warm" % func_instance.name
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
        cls._reject_multiprocess(kwargs.pop("process", None))

        # An explicit params= still wins over anything the pool holds.
        parameters = kwargs.pop("params", None)

        entry = cls._get_or_create_entry(func_instance)
        if entry is not None:
            func_instance = entry.concrete
            if parameters is None:
                parameters = entry.params
        else:
            if cls._needs_hydration(func_instance):
                func_instance = cls._hydrate(func_instance)
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

    @staticmethod
    def _stop_components(target) -> None:
        instances = target._component_instances
        if not instances:
            return
        from metaflow_extensions.nflx.plugins.functions.components.runtime import (
            stop_components,
        )

        try:
            stop_components(instances)
        finally:
            # stop_components() raises when any component's stop() fails, and a
            # component that failed to stop is not a component to keep calling.
            target._component_instances = []

    @classmethod
    def close(cls, func_instance, clean_dir: bool = True, **kwargs):
        """Detach this handle, tearing the warm entry down at the last one."""
        with _POOL_LOCK:
            key = getattr(func_instance, "_runtime_id", None)
            if key is None:
                # Unpooled: apply() ran this handle directly, so its own
                # components are the ones to stop.
                cls._stop_components(func_instance)
                return
            func_instance._runtime_id = None
            entry = _WARM_POOL.get(key)
            if entry is None:
                return
            entry.attached -= 1
            if entry.attached > 0:
                return
            del _WARM_POOL[key]

        try:
            cls._stop_components(entry.concrete)
        finally:
            cls._release_sys_path(entry.sys_path_dirs)

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
        input_types = func_instance.__class__.get_input_types(func_instance.spec)
        expected_input_type = cls._map_type_info_to_python_type(
            input_types, type(func_instance), func_instance.spec
        )
        deserialized_data = registry.deserialize(data, expected_input_type)
        payload_data = FunctionPayload(deserialized_data, kwargs)

        result_payload = cls.apply(func_instance, payload_data, **kwargs)

        serializer = registry.get_serializer_for_type(type(result_payload.data))
        if serializer is None:
            raise MetaflowFunctionException(
                f"No serializer registered for type {type(result_payload.data)}"
            )

        serialized_data, _ = serializer(result_payload.data)
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
