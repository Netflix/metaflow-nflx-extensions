"""
Serializer for FunctionParameters objects.

This module provides serialization for FunctionParameters that works with both
the Metaflow serialization registry (for MemoryBackend) and Ray's serialization
system (for RayBackend).
"""

import types
from typing import Any, List, Tuple, TYPE_CHECKING

from metaflow_extensions.nflx.plugins.functions.serializers.base import BaseSerializer

if TYPE_CHECKING:
    from metaflow_extensions.nflx.plugins.functions.core.function_parameters import (
        FunctionParameters,
    )


class FunctionParametersSerializer(BaseSerializer):
    """
    Serializer for FunctionParameters objects.

    This serializer extracts only cached data from the LazyArtifactMapping to avoid
    triggering lazy artifact loading during serialization. Works with both standard
    Metaflow serialization and Ray serialization (via automatic adapters).

    The serializer extracts cached data and reconstructs FunctionParameters without
    calling __init__, bypassing lazy loading logic.
    """

    @property
    def supported_type(self):
        from metaflow_extensions.nflx.plugins.functions.core.function_parameters import (
            FunctionParameters,
        )

        return FunctionParameters

    def serialize(self, obj: "FunctionParameters") -> Tuple[bytes, List[Any]]:
        """
        Serialize FunctionParameters to bytes.

        Extracts cached artifact data and pickles it as a simple dictionary.
        This avoids serializing the LazyArtifactMapping which would trigger
        artifact loading. This method is used by both standard Metaflow
        serialization and (via adapters) Ray serialization.

        Returns
        -------
        Tuple[bytes, List[Any]]
            Pickled data and empty list (no chunking support)
        """
        import pickle

        # Use object.__getattribute__ to bypass __getattr__ and access internal _lazy_mapping
        lazy_mapping = object.__getattribute__(obj, "_lazy_mapping")

        # Extract only cached data (artifacts already loaded in memory)
        serialized_data = dict(lazy_mapping._cache)

        # Pickle the simple dict instead of the complex FunctionParameters object
        data = pickle.dumps({"_serialized_data": serialized_data})

        return data, []

    def deserialize(self, data: bytes) -> "FunctionParameters":
        """
        Deserialize FunctionParameters from bytes.

        Reconstructs a FunctionParameters object with the cached data. The reconstructed
        object has no Task reference, so it can only access previously cached artifacts.
        This method is used by both standard Metaflow serialization and (via adapters)
        Ray serialization.

        Parameters
        ----------
        data : bytes
            Pickled dictionary containing cached artifact data

        Returns
        -------
        FunctionParameters
            Reconstructed FunctionParameters with cached data
        """
        import pickle
        from metaflow_extensions.nflx.plugins.functions.core.function_parameters import (
            FunctionParameters,
            LazyArtifactMapping,
        )

        state = pickle.loads(data)
        serialized_data = state["_serialized_data"]

        # Create LazyArtifactMapping with no Task - data comes from overrides only
        data_mapping = LazyArtifactMapping(
            task=None, function_spec=None, overrides=serialized_data
        )
        immutable_data = types.MappingProxyType(data_mapping)

        # Manually construct FunctionParameters without calling __init__
        # to avoid validation logic and set internal state directly
        obj = FunctionParameters.__new__(FunctionParameters)
        object.__setattr__(obj, "_data", immutable_data)
        object.__setattr__(obj, "_lazy_mapping", data_mapping)
        object.__setattr__(obj, "_frozen", True)

        return obj


def register_function_parameters_serializer():
    """Register FunctionParameters with the global serialization registry."""
    from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
        register_serializer,
    )

    register_serializer(FunctionParametersSerializer())
