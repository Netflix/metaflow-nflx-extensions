from typing import Dict, Optional, TYPE_CHECKING

from .backend_type import BackendType

if TYPE_CHECKING:
    from .abstract_backend import AbstractBackend

_backend_instances: Dict[str, "AbstractBackend"] = {}


def get_backend(backend_name: Optional[str] = None) -> "AbstractBackend":
    """
    Get a backend instance by name.

    Parameters
    ----------
    backend_name : str, optional
        The name of the backend to get. If not provided, uses the value from
        FUNCTION_BACKEND config/environment variable.

    Returns
    -------
    AbstractBackend
        The requested backend instance
    """
    # Import here to avoid circular imports
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionRuntimeException,
    )

    # Use provided backend name or fall back to config
    if backend_name is None:
        from metaflow_extensions.nflx.config.mfextinit_functions import FUNCTION_BACKEND

        backend_type_str = FUNCTION_BACKEND.lower()
    else:
        backend_type_str = backend_name.lower()

    try:
        backend_type = BackendType(backend_type_str)
    except ValueError:
        valid_options = [bt.value for bt in BackendType]
        raise MetaflowFunctionRuntimeException(
            f"Unknown backend: '{backend_type_str}'. "
            f"Valid options are: {valid_options}"
        )

    if backend_type.value not in _backend_instances:
        if backend_type == BackendType.MEMORY:
            from .memory.memory_backend import MemoryBackend

            _backend_instances[backend_type.value] = MemoryBackend()
        elif backend_type == BackendType.LOCAL:
            from .local.local_backend import LocalBackend

            _backend_instances[backend_type.value] = LocalBackend()
        elif backend_type == BackendType.RAY:
            from .ray.ray_backend import RayBackend

            _backend_instances[backend_type.value] = RayBackend()
        else:
            raise MetaflowFunctionRuntimeException(
                f"Backend type '{backend_type.value}' is not yet implemented"
            )

    return _backend_instances[backend_type.value]
