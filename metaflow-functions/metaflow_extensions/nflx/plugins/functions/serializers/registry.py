from typing import (
    Any,
    Callable,
    Dict,
    Type,
    TypeVar,
    Union,
    List,
    Tuple,
    TYPE_CHECKING,
)

from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
)
from metaflow_extensions.nflx.plugins.functions.serializers.config import (
    SerializerConfig,
    get_canonical_type_string,
)
from metaflow_extensions.nflx.plugins.functions.debug import debug

if TYPE_CHECKING:
    from metaflow_extensions.nflx.plugins.functions.serializers.base import (
        BaseSerializer,
    )

T = TypeVar("T")

SerializerFunc = Callable[[Any], Tuple[bytes, List[Any]]]
DeserializerFunc = Callable[[bytes], Any]


class SerializerRegistry:
    """Registry for type-specific serializers with lazy loading using canonical type strings."""

    def __init__(self) -> None:
        # Serializer configurations keyed by canonical type string
        self._serializer_configs: Dict[str, SerializerConfig] = {}
        # Loaded serializer instances keyed by canonical type string
        self._loaded_serializers: Dict[str, "BaseSerializer"] = {}

    def register_serializer_config(self, config: SerializerConfig) -> None:
        """Register a serializer configuration for lazy loading."""
        debug.functions_exec(
            f"Registering serializer config for type: {config.canonical_type}"
        )
        self._serializer_configs[config.canonical_type] = config

    def _load_serializer(self, canonical_type: str) -> "BaseSerializer":
        """Lazily load a serializer from its configuration."""
        if canonical_type in self._loaded_serializers:
            return self._loaded_serializers[canonical_type]

        config = self._serializer_configs.get(canonical_type)
        if config is None:
            raise MetaflowFunctionException(
                f"No serializer configuration registered for type '{canonical_type}'. "
                f"Available types: {list(self._serializer_configs.keys())}"
            )

        debug.functions_exec(f"Loading serializer for type: {canonical_type}")

        try:
            import importlib

            module = importlib.import_module(config.serializer_module)
            serializer_cls = getattr(module, config.serializer_class)

            # Create serializer instance
            if config.extra_kwargs:
                serializer = serializer_cls(**config.extra_kwargs)
            else:
                # Try different constructor patterns based on serializer needs
                try:
                    # First try with no arguments (most serializers)
                    serializer = serializer_cls()
                except TypeError:
                    # If that fails, try with the type class (e.g., AvroSerializer)
                    from metaflow_extensions.nflx.plugins.functions.serializers.config import (
                        load_type_from_canonical_string,
                    )

                    type_cls = load_type_from_canonical_string(canonical_type)
                    if type_cls is not None:
                        serializer = serializer_cls(type_cls)
                    else:
                        raise  # Re-raise the original TypeError

            self._loaded_serializers[canonical_type] = serializer
            return serializer

        except Exception as e:
            raise MetaflowFunctionException(
                f"Failed to load serializer for type '{canonical_type}' from "
                f"module '{config.serializer_module}', class '{config.serializer_class}': {e}"
            )

    def _get_serializer_by_canonical_type(self, canonical_type: str) -> SerializerFunc:
        """Get serializer function by canonical type string."""
        serializer = self._load_serializer(canonical_type)
        return serializer.serialize

    def _get_deserializer_by_canonical_type(
        self, canonical_type: str
    ) -> DeserializerFunc:
        """Get deserializer function by canonical type string."""
        serializer = self._load_serializer(canonical_type)
        return serializer.deserialize

    def get_serializer_for_type(self, type_class: Type[T]) -> SerializerFunc:
        """Get the serializer for a given type."""
        canonical_type = get_canonical_type_string(type_class)
        return self._get_serializer_by_canonical_type(canonical_type)

    def get_deserializer(self, target_type: Type[T]) -> DeserializerFunc:
        """Get the deserializer for a given type."""
        canonical_type = get_canonical_type_string(target_type)
        return self._get_deserializer_by_canonical_type(canonical_type)

    def serialize(self, obj: Any, buf: Union[bytearray, memoryview]) -> Tuple[int, Any]:
        """Serialize an object using the registered serializer."""
        serializer = self.get_serializer_for_type(type(obj))

        data, extra_data = serializer(obj)
        data_len = len(data)

        if data_len > len(buf):
            raise MetaflowFunctionException(
                f"Data size '{data_len}' cannot be written to buffer of size '{len(buf)}'"
            )
        buf[0:data_len] = data
        return data_len, extra_data

    def deserialize(self, data: bytes, target_type: Type[T]) -> T:
        """Deserialize data to a specific type using the registered deserializer."""
        deserializer = self.get_deserializer(target_type)
        return deserializer(data)

    def aggregate_results(self, results: List[Any], target_type: Type[T]) -> Any:
        """Aggregate a list of results using the registered aggregator for the type."""
        if len(results) == 0:
            return target_type()
        if len(results) == 1:
            return results[0]

        canonical_type = get_canonical_type_string(target_type)
        serializer = self._load_serializer(canonical_type)
        return serializer.aggregator(results)

    def is_type_registered(self, type_class: Type[T]) -> bool:
        """Check if a type is registered in the registry."""
        canonical_type = get_canonical_type_string(type_class)
        # Check both loaded serializers and serializer configs
        return (
            canonical_type in self._loaded_serializers
            or canonical_type in self._serializer_configs
        )

    def is_type_chunkable(self, type_class: Type[T]) -> bool:
        """Check if a type supports chunking."""
        canonical_type = get_canonical_type_string(type_class)
        config = self._serializer_configs.get(canonical_type)
        if config is None:
            raise MetaflowFunctionException(
                f"No serializer configuration registered for type '{canonical_type}'. "
                f"Available types: {list(self._serializer_configs.keys())}"
            )
        return config.supports_chunking

    def get_registered_types(self) -> List[str]:
        """Get a list of all registered canonical type strings."""
        return list(self._serializer_configs.keys())


# Global registry instance
_global_registry = SerializerRegistry()


def register_serializer(serializer_obj) -> None:
    """Register a serializer in the global registry by converting to SerializerConfig."""
    from metaflow_extensions.nflx.plugins.functions.serializers.base import (
        BaseSerializer,
    )

    if not isinstance(serializer_obj, BaseSerializer):
        raise MetaflowFunctionException(
            f"Expected BaseSerializer instance, got {type(serializer_obj)}"
        )

    # Convert to config and register
    canonical_type = get_canonical_type_string(serializer_obj.supported_type)
    serializer_full_name = (
        f"{serializer_obj.__class__.__module__}.{serializer_obj.__class__.__name__}"
    )
    config = SerializerConfig(
        canonical_type=canonical_type,
        serializer=serializer_full_name,
        supports_chunking=serializer_obj.supports_chunking,
    )
    register_serializer_config(config)


def register_serializer_config(config: SerializerConfig) -> None:
    """Register a serializer configuration in the global registry."""
    _global_registry.register_serializer_config(config)


# Simple accessors to the global registry
def get_global_registry() -> SerializerRegistry:
    return _global_registry


def is_type_registered(type_class: Type[T]) -> bool:
    return _global_registry.is_type_registered(type_class)


def aggregate_results(results: List[Any], target_type: Type[T]) -> Any:
    return _global_registry.aggregate_results(results, target_type)


def _create_serializer_configs_for_types(
    builtin_types, serializer_module, serializer_class
):
    """Create SerializerConfig objects for multiple builtin types with the same serializer."""
    serializer_full_name = f"{serializer_module}.{serializer_class}"
    return [
        SerializerConfig(
            canonical_type=builtin_type,
            serializer=serializer_full_name,
        )
        for builtin_type in builtin_types
    ]
