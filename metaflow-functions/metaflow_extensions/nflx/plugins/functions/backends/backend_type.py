from enum import Enum
from typing import List


class BackendType(Enum):
    MEMORY = "memory"
    LOCAL = "local"
    RAY = "ray"

    @classmethod
    def get_all_backend_names(cls) -> List[str]:
        """Get list of all backend names."""
        return [backend.value for backend in cls]
