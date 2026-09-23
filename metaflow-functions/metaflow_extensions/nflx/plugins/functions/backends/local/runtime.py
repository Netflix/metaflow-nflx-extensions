"""Warm in-process runtimes for the local backend.

A ``LocalRuntime`` holds what is expensive to rebuild for a function that runs
in the caller's own interpreter: the hydrated concrete function, its
``FunctionParameters``, and the code directories its deferred imports need on
``sys.path``. Each handle owns its own; unlike the memory backend there is no
subprocess to pool, so there is nothing to arbitrate between handles.
"""

import sys
import threading
from typing import Any, Dict, List

from metaflow_extensions.nflx.plugins.functions.common.runtime_utils import (
    create_function_parameters,
)
from metaflow_extensions.nflx.plugins.functions.debug import debug
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
    MetaflowFunctionRuntimeException,
)

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

    return function_from_json(
        local_reference,
        use_proxy=False,
        backend="local",
        start_runtime=False,
        # function_from_json() cannot see the proxy's components and would
        # otherwise silently default to none.
        runtime_components=func_instance.runtime_components,
    )


class LocalRuntime(object):
    """A function kept warm in this process."""

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
        # holds the entry for as long as it is alive.
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


def runtime_for(func_instance, process: int = 1) -> LocalRuntime:
    """The handle's warm runtime, created and started on first use."""
    if process > 1:
        raise MetaflowFunctionException(
            "The local backend executes in the calling process and cannot "
            f"provide {process} workers. Use the memory backend for "
            "process > 1."
        )

    runtime = getattr(func_instance, "_local_runtime", None)
    if runtime is None:
        runtime = LocalRuntime(func_instance)
        # A handle local accepts but cannot annotate (``__slots__``, a mock)
        # simply gets a fresh runtime per call. Such a handle is already
        # concrete, so the cost is bounded by the sys.path scan.
        try:
            func_instance._local_runtime = runtime
        except (AttributeError, TypeError):
            debug.functions_exec(
                "LocalRuntime: cannot cache on '%s'; warming per call"
                % getattr(func_instance, "name", func_instance)
            )
    runtime.start()
    return runtime


def close_runtime(func_instance, clean_dir: bool = True) -> None:
    runtime = getattr(func_instance, "_local_runtime", None)
    if runtime is None:
        return
    try:
        func_instance._local_runtime = None
    except (AttributeError, TypeError):
        pass
    runtime.close(clean_dir=clean_dir)
