import threading
from typing import Dict, Optional

from metaflow_extensions.nflx.config.mfextinit_functions import FUNCTION_RUNTIME_PATH
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
)
from metaflow_extensions.nflx.plugins.functions.backends.memory.runtime import (
    FunctionRuntime,
)
from metaflow_extensions.nflx.plugins.functions.backends.memory.supervisor.utils import (
    FunctionProcess,
    FunctionLease,
    RuntimeKey,
)
from metaflow_extensions.nflx.plugins.functions.components.runtime import (
    serialize_components,
)
from metaflow_extensions.nflx.plugins.functions.core.function import (
    MetaflowFunction,
)

from metaflow_extensions.nflx.config.mfextinit_functions import (
    FUNCTION_CLEAN_DIR_ON_DEL,
)

CLEAN_DIR = 1 if FUNCTION_CLEAN_DIR_ON_DEL == "1" else 0


def _runtime_key(function: MetaflowFunction, process: int) -> RuntimeKey:
    """Identity of the runtime a handle would attach to.

    Two handles only share a runtime if their function uuid, process count,
    and runtime component specs (name + kwargs) all match.
    """
    component_specs = tuple(serialize_components(function.runtime_components))
    return (function.uuid, process, component_specs)


class FunctionSupervisor(object):
    def __init__(self, base_path: Optional[str] = None):
        """
        FunctionSupervisor manages the lifecycle of multiple functions

        Parameters
        ----------
        base_path : str, optional, default None
            Base path for the function supervisor. If None, defaults to current directory.
        """
        self._base_path: str = base_path if base_path else FUNCTION_RUNTIME_PATH
        self._process_map: Dict[RuntimeKey, FunctionProcess] = {}
        self._lock = threading.Lock()

    def _create_from_function(
        self, key: RuntimeKey, function: MetaflowFunction, process: int = 1
    ):
        """
        Create a new function process for the given runtime key and add it
        to the process map.

        Parameters
        ----------
        key : RuntimeKey
            Runtime identity to create the process for
        function : MetaflowFunction
            Metaflow Function to create a process from
        process : int, optional, default 1
            Number of processes to create for the function
        """
        fsr = FunctionRuntime(function, self._base_path, process=process)
        fsp = FunctionProcess(
            key=key,
            leased=0,
            attached=0,
            function=function,
            runtime=fsr,
        )
        self._process_map[key] = fsp

    def _get_function(self, key: RuntimeKey) -> FunctionProcess:
        if key not in self._process_map:
            raise MetaflowFunctionException(f"Metaflow Function {key} not found")
        return self._process_map[key]

    def _has_function(self, key: RuntimeKey) -> bool:
        """
        Check if a given runtime key is already running in the supervisor

        Parameters
        ----------
        key : RuntimeKey
            Runtime key to check

        Returns
        -------
        bool
            True if a process for this key is already running, False otherwise
        """
        return key in self._process_map

    def _cleanup_process(self, key: RuntimeKey, clean_dir=False):
        """
        Clean up a process from the supervisor

        Parameters
        ----------
        key : RuntimeKey
            Runtime key of the process to clean up
        """
        if key in self._process_map:
            fsp = self._process_map[key]
            if fsp.runtime:
                fsp.runtime.close(clean_dir=clean_dir)
            del self._process_map[key]

    def _cleanup(self, clean_dir=False):
        for fsp in self._process_map.values():
            if fsp.runtime is not None:
                fsp.runtime.close(clean_dir=clean_dir)
        self._process_map = {}

    def __del__(self):
        if hasattr(self, "_process_map"):
            self._cleanup(clean_dir=CLEAN_DIR)

    def lease(self, function: MetaflowFunction, process: int = 1) -> FunctionLease:
        """
        Lease a function and return its IO paths as a lease object.

        The first successful lease for a given handle attaches that handle
        to the resolved runtime (tracked via ``function._runtime_id``); the
        runtime is only torn down once every attached handle has detached
        (see ``detach``), so closing one handle cannot invalidate another
        handle sharing the same (uuid, process, components) identity.

        Parameters
        ----------
        function : MetaflowFunction
            Metaflow Function to lease
        process : int, default 1
            Number of processes to allocate

        Returns
        -------
        FunctionLease
            A FunctionLease object that contains IO handles,
            connections have already been established

        """
        # Once a handle has attached, reuse its resolved key rather than
        # recomputing it (which hashes runtime component specs) on every
        # call - this method runs in a tight loop for streaming apply().
        already_attached = function._runtime_id is not None

        with self._lock:
            if already_attached:
                key = function._runtime_id
            else:
                key = _runtime_key(function, process)

            # Get the function creating a runtime as needed
            if not self._has_function(key):
                self._create_from_function(key, function, process=process)
            fsp = self._get_function(key)

            if not fsp.runtime:
                raise MetaflowFunctionException("Runtime is not initialized.")

            if not already_attached:
                function._runtime_id = key
                fsp.attached += 1

            # Create a lease object for this function
            f_lease = FunctionLease(key=key, runtime=fsp.runtime)
            fsp.leased += 1
            return f_lease

    def free(self, lease: FunctionLease):
        """
        Free a leased function

        Parameters
        ----------
        lease : FunctionLease
            The lease to free

        """
        with self._lock:
            if lease.key not in self._process_map:
                raise MetaflowFunctionException(f"Lease {lease.key} not found")
            fsp = self._process_map[lease.key]
            fsp.leased -= 1

    def detach(self, function: MetaflowFunction, clean_dir: bool = True) -> None:
        """
        Release a handle's ownership of its runtime.

        Only tears the runtime down once every handle attached to it has
        detached; otherwise this just releases this handle's claim and
        leaves the shared runtime running for the remaining handles.

        Parameters
        ----------
        function : MetaflowFunction
            Metaflow Function handle to detach
        clean_dir : bool, default True
            Whether to clean up the function's directory if this is the
            last handle attached to the runtime
        """
        key = function._runtime_id
        if key is None:
            return
        function._runtime_id = None
        with self._lock:
            if key not in self._process_map:
                return
            fsp = self._process_map[key]
            fsp.attached -= 1
            if fsp.attached <= 0:
                self._cleanup_process(key, clean_dir=clean_dir)

    def clear(
        self, function: Optional[MetaflowFunction] = None, clean_dir: bool = True
    ) -> None:
        """Clear function(s) from the supervisor, unconditionally."""
        with self._lock:
            if function is None:
                self._cleanup(clean_dir=clean_dir)
            else:
                key = function._runtime_id
                if key is not None:
                    self._cleanup_process(key, clean_dir=clean_dir)
                    function._runtime_id = None

    def is_loaded(self, function: MetaflowFunction) -> bool:
        key = function._runtime_id
        return key is not None and key in self._process_map


# Create a singleton instance of FunctionSupervisor
function_supervisor = FunctionSupervisor()
