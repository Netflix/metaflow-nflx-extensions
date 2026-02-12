"""
Serializer configuration for lazy loading.
"""

from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class SerializerConfig:
    """Configuration for a lazy-loaded serializer."""

    canonical_type: str  # e.g., "builtins.dict", "metaflow.MetaflowDataFrame"
    serializer: str  # e.g., "metaflow_extensions.nflx.plugins.avro_function.serializers.AvroSerializer"
    supports_chunking: bool = False
    extra_kwargs: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate the configuration."""
        if not self.canonical_type:
            raise ValueError("canonical_type cannot be empty")
        if not self.serializer:
            raise ValueError("serializer cannot be empty")
        if "." not in self.serializer:
            raise ValueError("serializer must be in format 'module.ClassName'")

    @property
    def serializer_module(self) -> str:
        """Get the serializer module by splitting the full serializer path."""
        return ".".join(self.serializer.split(".")[:-1])

    @property
    def serializer_class(self) -> str:
        """Get the serializer class name by splitting the full serializer path."""
        return self.serializer.split(".")[-1]


def get_canonical_type_string(type_hint) -> str:
    """Convert a type hint to canonical string representation."""
    if hasattr(type_hint, "__module__") and hasattr(type_hint, "__name__"):
        # Always use module.name format for canonical strings
        return f"{type_hint.__module__}.{type_hint.__name__}"
    if hasattr(type_hint, "__origin__"):
        origin = type_hint.__origin__
        if hasattr(type_hint, "__args__") and type_hint.__args__:
            args = type_hint.__args__
            arg_strs = [get_canonical_type_string(arg) for arg in args]
            return f"{get_canonical_type_string(origin)}[{', '.join(arg_strs)}]"
        return get_canonical_type_string(origin)
    return str(type_hint)


def load_type_from_canonical_string(canonical_type: str):
    """Load a type from its canonical string representation."""
    # Use existing utility from utils.py for consistency
    from metaflow_extensions.nflx.plugins.functions.utils import load_type_from_string

    return load_type_from_string(canonical_type)
