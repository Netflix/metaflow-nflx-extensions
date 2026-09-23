"""In-process runtime supervision for the local backend.

Deliberately the same shape as ``backends/memory/supervisor/supervisor.py``:
the same runtime key, the same ``lease``/``free``/``detach``/``clear`` surface,
the same attached/leased counters, and ``function._runtime_id`` as the handle's
attachment marker. What differs is what a runtime *is* -- memory launches a
subprocess under the resolved conda interpreter, local hydrates the code
package into the caller's own interpreter and keeps it warm.
"""

import sys
import threading
from collections import namedtuple
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from metaflow_extensions.nflx.plugins.functions.common.runtime_utils import (
    create_function_parameters,
)
from metaflow_extensions.nflx.plugins.functions.debug import debug
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
    MetaflowFunctionRuntimeException,
)

# The runtime key is imported from the memory supervisor rather than redefined
# here: two backends disagreeing about what "the same function" means is
# exactly the bug this module exists to avoid. It depends on nothing
# memory-specific and should eventually move to a neutral module.
from metaflow_extensions.nflx.plugins.functions.backends.memory.supervisor.supervisor import (
    _runtime_key,
)
from metaflow_extensions.nflx.plugins.functions.backends.memory.supervisor.utils import (
    RuntimeKey,
)
from metaflow_extensions.nflx.plugins.functions.core.function import MetaflowFunction

# Directories a warm runtime has put on sys.path, and how many runtimes still
# want each one there. Refcounted rather than a plain set because two functions
# from the same code package share a root directory: closing one must not break
# the other's deferred imports.
_SYS_PATH_LOCK = threading.Lock()
_SYS_PATH_REFCOUNTS: Dict[str, int] = {}


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


def _local_runtime_key(function, process: int) -> RuntimeKey:
    """The memory backend's key, with a fallback local alone needs.

    Two handles can only be the same function if there is a content-hash uuid
    saying so. A handle built in this process has none, and a handle that is
    not a MetaflowFunction at all is something local accepts and memory cannot
    -- either way it is its own runtime, so its identity is the key.
    """
    uuid = getattr(getattr(function, "spec", None), "uuid", None)
    if uuid is None or not isinstance(function, MetaflowFunction):
        return ("object:%d" % id(function), process, ())
    return _runtime_key(function, process)


def _root_dirs(concrete) -> List[str]:
    """Extraction directories whose contents the function may import.

    A pipeline's own directory is near-empty -- each constituent extracts its
    own package -- and ``function_root_dir`` reports only the first
    constituent's, so a deferred import in constituent #2 would not resolve.
    Every constituent's directory goes on the path.
    """
    dirs: List[str] = []
    for handle in [concrete] + list(getattr(concrete, "functions", []) or []):
        try:
            root_dir = handle.function_root_dir
        except (AttributeError, MetaflowFunctionException) as e:
            # MetaflowFunctionException is only raised as "Function root dir is
            # not set", the normal state for a handle that owns no extracted
            # code; AttributeError means the handle is not a MetaflowFunction at
            # all, which local alone accepts. Left visible: a function whose
            # extraction half-failed shows up here rather than as an opaque
            # ModuleNotFoundError later.
            debug.functions_exec(
                "LocalRuntime: no root dir for '%s' (%s)"
                % (getattr(handle, "name", handle), e)
            )
            continue
        if root_dir and root_dir not in dirs:
            dirs.append(root_dir)
    return dirs


def _hydrate(func_instance):
    """Materialize a proxy handle into a concrete, executable function."""
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        function_from_json,
    )
    from metaflow_extensions.nflx.plugins.functions.core.function_spec import (
        FunctionSpec,
    )

    func_spec = func_instance.spec
    if not func_spec.reference:
        raise MetaflowFunctionRuntimeException("Function spec missing reference path")

    local_reference = FunctionSpec.download_to_temp(func_spec.reference)

    # Carry the proxy's runtime_components over to the concrete function -
    # function_from_json() below has no way to see the proxy's, and would
    # otherwise silently default to none.
    runtime_components = getattr(func_instance, "_runtime_components", [])

    return function_from_json(
        local_reference,
        use_proxy=False,
        backend="local",
        start_runtime=False,
        runtime_components=runtime_components,
    )


class LocalRuntime(object):
    """The in-process analogue of ``FunctionRuntime``.

    Holds everything a warm function needs that is expensive to rebuild: the
    hydrated concrete function, its ``FunctionParameters``, and the code
    directories it needs on ``sys.path``.
    """

    def __init__(self, function):
        self.function = function
        self.sys_path_dirs: List[str] = []
        self._params: Any = None
        self._prefetch_artifacts = False
        self._started = False

    @property
    def params(self) -> Any:
        """The function's parameters, built once and reused.

        Built on demand rather than in ``start()``: a caller that passes its
        own ``params=`` never needs them, and local accepts handles that have
        no spec to build them from.
        """
        if self._params is None:
            self._params = create_function_parameters(
                self.function.spec,
                prefetch_artifacts=self._prefetch_artifacts,
            )
        return self._params

    def start(self) -> None:
        if self._started:
            return

        from metaflow_extensions.nflx.plugins.functions.backends.local.local_backend import (
            LocalBackend,
        )

        handle = self.function
        concrete = _hydrate(handle) if LocalBackend._needs_hydration(handle) else handle

        # from_spec() puts the function root on sys.path only for the duration
        # of the load (run_in_path drops it in a finally). Any import the user's
        # code defers to call time -- generated protobuf stubs are the common
        # case -- then fails once the load window closes, so the warm runtime
        # holds the entry for as long as it is attached.
        self.sys_path_dirs = _root_dirs(concrete)
        _acquire_sys_path(self.sys_path_dirs)

        self.function = concrete
        self._prefetch_artifacts = getattr(handle, "_prefetch_artifacts", False)
        self._started = True

        if self._prefetch_artifacts:
            # The caller asked for artifacts up front, so resolve them here
            # instead of on the first request.
            _ = self.params

        debug.functions_exec(
            "LocalRuntime: warmed '%s' (sys.path+=%s)"
            % (concrete.name, self.sys_path_dirs)
        )

    def close(self, clean_dir: bool = True) -> None:
        if not self._started:
            return
        self._started = False
        try:
            instances = self.function._component_instances
            if instances:
                from metaflow_extensions.nflx.plugins.functions.components.runtime import (
                    stop_components,
                )

                stop_components(instances)
        finally:
            # stop_components() raises when any component's stop() fails, and a
            # component that failed to stop is not a component to keep calling.
            self.function._component_instances = []
            _release_sys_path(self.sys_path_dirs)
            self.sys_path_dirs = []
            self._params = None


LocalLease = namedtuple("LocalLease", ["key", "runtime"])


@dataclass
class LocalProcess:
    key: Optional[RuntimeKey] = None  # runtime identity backing this entry
    leased: int = 0  # calls currently in flight against this runtime
    attached: int = 0  # handles that currently own this runtime
    function: Optional[Any] = None  # the handle the runtime was created from
    runtime: Optional[LocalRuntime] = None


class LocalSupervisor(object):
    """Manages the lifecycle of warm in-process function runtimes."""

    def __init__(self):
        self._process_map: Dict[RuntimeKey, LocalProcess] = {}
        # Reentrant, unlike the memory supervisor's plain Lock: teardown runs
        # the user's component stop() in this process, and that code may itself
        # invoke a local function.
        self._lock = threading.RLock()

    def lease(self, function, process: int = 1) -> LocalLease:
        """Get or create the warm runtime for ``function`` and claim a call.

        The first successful lease for a handle attaches it to the resolved
        runtime (via ``function._runtime_id``); the runtime is torn down only
        once every attached handle has detached.
        """
        if process > 1:
            raise MetaflowFunctionException(
                "The local backend executes in the calling process and cannot "
                f"provide {process} workers. Use the memory backend for "
                "process > 1."
            )

        already_attached = getattr(function, "_runtime_id", None) is not None
        with self._lock:
            key = (
                function._runtime_id
                if already_attached
                else _local_runtime_key(function, process)
            )
            lrp = self._process_map.get(key)
            if lrp is None:
                lrp = LocalProcess(key=key, function=function)
                lrp.runtime = LocalRuntime(function)
                self._process_map[key] = lrp

            lrp.runtime.start()

            if not already_attached:
                function._runtime_id = key
                lrp.attached += 1
            lrp.leased += 1
            return LocalLease(key=key, runtime=lrp.runtime)

    def free(self, lease: LocalLease) -> None:
        with self._lock:
            lrp = self._process_map.get(lease.key)
            if lrp is None:
                raise MetaflowFunctionException(f"Lease {lease.key} not found")
            lrp.leased -= 1

    def detach(self, function, clean_dir: bool = True) -> None:
        """Release a handle's ownership, tearing down at the last detach."""
        key = getattr(function, "_runtime_id", None)
        if key is None:
            return
        function._runtime_id = None
        with self._lock:
            lrp = self._process_map.get(key)
            if lrp is None:
                return
            lrp.attached -= 1
            if lrp.attached > 0:
                return
            self._cleanup_process(key, clean_dir=clean_dir)

    def clear(self, function=None, clean_dir: bool = True) -> None:
        """Clear runtime(s) from the supervisor, unconditionally."""
        with self._lock:
            if function is None:
                for key in list(self._process_map):
                    self._cleanup_process(key, clean_dir=clean_dir)
                return
            key = getattr(function, "_runtime_id", None)
            if key is not None:
                self._cleanup_process(key, clean_dir=clean_dir)
                function._runtime_id = None

    def is_loaded(self, function) -> bool:
        key = getattr(function, "_runtime_id", None)
        return key is not None and key in self._process_map

    def _cleanup_process(self, key: RuntimeKey, clean_dir: bool = True) -> None:
        lrp = self._process_map.pop(key, None)
        if lrp is not None and lrp.runtime is not None:
            lrp.runtime.close(clean_dir=clean_dir)


local_supervisor = LocalSupervisor()
