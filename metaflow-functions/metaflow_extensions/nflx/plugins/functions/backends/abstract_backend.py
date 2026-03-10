from abc import ABC, abstractmethod
from typing import Any

from .backend_type import BackendType
from ..environment import get_environment_from_metadata


class AbstractBackend(ABC):
    """
    Base class for all Metaflow Function backends.

    Backends are responsible for executing functions in different environments:
    - MemoryBackend: Subprocess with shared memory IPC
    - LocalBackend: Direct in-process execution
    - RayBackend: Distributed execution on Ray cluster

    Each backend must implement:
    - backend_type: Identify the backend type
    - apply: Execute a function with data

    Optional methods (backends can override if needed):
    - start: Pre-start/warm up resources
    - close: Clean up resources for specific function
    - shutdown: Clean up global backend resources
    """

    @property
    @abstractmethod
    def backend_type(self) -> BackendType:
        """Return the type of this backend."""

    @classmethod
    @abstractmethod
    def apply(cls, func_instance, data: Any, **kwargs) -> Any:
        """
        Execute function with data.

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

    @classmethod
    @abstractmethod
    async def apply_async(cls, func_instance, data: Any, **kwargs) -> Any:
        """
        Execute function with data asynchronously. With asynchronous
        processing the run-loop is yielded at iterations where no
        actions are taking place.

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

    @classmethod
    def start(cls, func_instance, **kwargs):
        """
        Optional: Pre-start or warm up backend resources.

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance to prepare
        """

    @classmethod
    def close(cls, func_instance, clean_dir: bool = True, **kwargs):
        """
        Optional: Clean up resources for a specific function instance.

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance to clean up
        clean_dir : bool
            Whether to clean up directories
        """

    @classmethod
    def get_environment(cls, func_instance) -> str:
        """
        Get the environment specification for this function.

        Parameters
        ----------
        func_instance
            The function instance to get environment for

        Returns
        -------
        str
            Environment identifier string
        """
        if func_instance.spec and func_instance.spec.system_metadata:
            return get_environment_from_metadata(func_instance.spec.system_metadata)
        return ""
