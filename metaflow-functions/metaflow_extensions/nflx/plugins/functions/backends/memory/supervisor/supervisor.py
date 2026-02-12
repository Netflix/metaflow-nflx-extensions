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
)
from metaflow_extensions.nflx.plugins.functions.core.function import (
    MetaflowFunction,
)
from metaflow_extensions.nflx.plugins.functions.core.function_spec import (
    FunctionSpec,
)

from metaflow_extensions.nflx.config.mfextinit_functions import (
    FUNCTION_CLEAN_DIR_ON_DEL,
)

CLEAN_DIR = 1 if FUNCTION_CLEAN_DIR_ON_DEL == "1" else 0


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
        self._process_map: Dict[str, FunctionProcess] = {}

    def _create_from_function(self, function: MetaflowFunction, process: int = 1):
        """
        Create a new function process from a given Metaflow Function specification
        and add it to the process map.

        Parameters
        ----------
        function : MetaflowFunction
            Metaflow Function to create a process from
        process : int, optional, default 1
            Number of processes to create for the function
        """
        fsr = FunctionRuntime(function, self._base_path, process=process)
        fsp = FunctionProcess(
            uuid=fsr.function_id,  # use the function id, we can expand later to hold multiple invocations of a function
            leased=0,
            function=function,
            runtime=fsr,
        )
        if fsp.uuid is not None:
            self._process_map[fsp.uuid] = fsp

    def _get_function(self, function_spec: FunctionSpec) -> FunctionProcess:
        if function_spec.uuid is None or function_spec.uuid not in self._process_map:
            raise MetaflowFunctionException(
                f"Metaflow Function {function_spec.uuid} not found"
            )
        return self._process_map[function_spec.uuid]

    def _has_function(self, function_spec: FunctionSpec) -> bool:
        """
        Check if a given Metaflow Function is already running in the supervisor

        Parameters
        ----------
        function_spec : FunctionSpec
            Metaflow Function Spec to check

        Returns
        -------
        bool
            True if the Metaflow Function is already running, False otherwise
        """
        return function_spec.uuid in self._process_map

    def _cleanup_process(self, uuid: str, clean_dir=False):
        """
        Clean up a process from the supervisor

        Parameters
        ----------
        uuid : str
            UUID of the process to clean up
        """
        if uuid in self._process_map:
            fsp = self._process_map[uuid]
            if fsp.runtime:
                fsp.runtime.close(clean_dir=clean_dir)
            del self._process_map[uuid]

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
        Lease a function and return its IO paths as a lease object

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
        # Get the function creating a runtime as needed
        if not self._has_function(function.spec):
            self._create_from_function(function, process=process)
        fsp = self._get_function(function.spec)

        # Create a lease object for this function
        if not fsp.runtime:
            raise MetaflowFunctionException("Runtime is not initialized.")

        f_lease = FunctionLease(uuid=fsp.uuid, runtime=fsp.runtime)
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
        if lease.uuid not in self._process_map:
            raise MetaflowFunctionException(f"Lease {lease.uuid} not found")
        fsp = self._process_map[lease.uuid]
        fsp.leased -= 1

    def clear(
        self, function: Optional[MetaflowFunction] = None, clean_dir: bool = True
    ) -> None:
        """Clear function(s) from the supervisor"""
        if function is None:
            self._cleanup(clean_dir=clean_dir)
        else:
            self._cleanup_process(function.uuid, clean_dir=clean_dir)

    def is_loaded(self, function: MetaflowFunction) -> bool:
        return function.uuid in self._process_map


# Create a singleton instance of FunctionSupervisor
function_supervisor = FunctionSupervisor()
