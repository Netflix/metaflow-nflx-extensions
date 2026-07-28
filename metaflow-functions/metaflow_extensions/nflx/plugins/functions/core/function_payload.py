from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
)


class FunctionPayload:
    """
    Container for function execution data + kwargs that handles serialization efficiently.

    This class encapsulates both the main data payload and any additional keyword arguments
    that need to be passed through the function execution pipeline. It integrates with
    the existing serialization registry to avoid double serialization.
    """

    def __init__(
        self,
        data: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        is_system_error: bool = False,
        runtime_components: Optional[Dict[str, Any]] = None,
    ):
        """
        Create a FunctionPayload.

        Args:
            data: The main payload data (will be serialized using registry)
            kwargs: JSON-serializable metadata/kwargs
            error: Formatted traceback if function/component execution failed, else None
            is_system_error: True if `error` came from a runtime component, False if from user code
            runtime_components: component_id -> output for components that produced output, else None
        """
        self.data = data
        self.kwargs = kwargs or {}
        self.error = error
        self.is_system_error = is_system_error
        self.runtime_components = runtime_components


def serialize_function_payload(payload: FunctionPayload) -> Tuple[bytes, List[Any]]:
    """
    Custom serializer that writes header + data in one pass to avoid double serialization.

    Format: [header_size:4][header_bytes][data_payload]

    Args:
        payload: FunctionPayload to serialize

    Returns:
        Serialized bytes
    """
    from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
        get_global_registry,
    )

    registry = get_global_registry()

    # Serialize each kwargs value individually using the registry
    serialized_kwargs = {}
    for key, value in payload.kwargs.items():
        value_serializer = registry.get_serializer_for_type(type(value))
        if value_serializer is None:
            raise MetaflowFunctionException(
                f"No serializer registered for type {type(value)} (kwargs key '{key}'). "
                f"Available types: {list(registry._serializers.keys())}"
            )
        value_bytes, _ = value_serializer(value)
        # Store both the serialized bytes and type information
        serialized_kwargs[key] = {
            "type": type(value).__module__ + "." + type(value).__name__,
            "data": value_bytes,
        }

    # Now serialize the header structure (serialized kwargs + system fields)
    header_serializer = registry.get_serializer_for_type(dict)
    if header_serializer is None:
        raise MetaflowFunctionException(
            f"No serializer registered for dict type (header container). "
            f"Available types: {list(registry._serializers.keys())}"
        )
    header = {
        "kwargs": serialized_kwargs,
        "error": payload.error,
        "is_system_error": payload.is_system_error,
        "runtime_components": payload.runtime_components,
    }
    header_bytes, _ = header_serializer(header)

    # Serialize the data using the registry (returns bytes)
    # Special case: if data is already bytes, use it directly (no-op)
    if isinstance(payload.data, bytes):
        data_bytes = payload.data
    else:
        data_serializer = registry.get_serializer_for_type(type(payload.data))
        if data_serializer is None:
            raise MetaflowFunctionException(
                f"No serializer registered for type {type(payload.data)}. "
                f"Available types: {list(registry._serializers.keys())}"
            )
        data_bytes, unwritten = data_serializer(payload.data)

    # Format: [header_size:4][header_bytes][data_bytes]
    header_size_bytes = len(header_bytes).to_bytes(4, "big")

    return header_size_bytes + header_bytes + data_bytes, list()


@dataclass
class PayloadHeader:
    """Result of parsing a serialized FunctionPayload's header + raw data bytes."""

    data_bytes: bytes
    kwargs: Dict[str, Any]
    error: Optional[str]
    is_system_error: bool
    runtime_components: Optional[Dict[str, Any]]


def _parse_function_payload_header(data) -> PayloadHeader:
    """
    Parse FunctionPayload and extract raw data bytes, kwargs, and system fields.

    Args:
        data: Bytes to parse (memoryview, bytes, or bytearray)

    Returns:
        PayloadHeader: Raw data bytes, kwargs dict, error, is_system_error, runtime_components
    """
    # Convert memoryview to bytes if necessary
    if hasattr(data, "tobytes"):
        data = data.tobytes()

    # Ensure we have bytes
    if not isinstance(data, (bytes, bytearray)):
        raise MetaflowFunctionException(
            f"Expected bytes or memoryview for parsing, got {type(data)}"
        )

    # Parse format: [header_size:4][header_bytes][data_bytes]

    # Read header size and header bytes
    header_size = int.from_bytes(data[:4], "big")
    header_bytes = data[4 : 4 + header_size]

    # Extract data bytes from remaining bytes
    data_start = 4 + header_size
    data_bytes = data[data_start:]

    # Deserialize the header (serialized kwargs + system fields) using the registry
    from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
        get_global_registry,
    )

    registry = get_global_registry()
    header = registry.deserialize(header_bytes, dict)
    serialized_kwargs = header["kwargs"]
    error = header.get("error")
    is_system_error = header.get("is_system_error", False)
    runtime_components = header.get("runtime_components")

    # Reconstruct each kwargs value using stored type information
    kwargs = {}
    for key, value_info in serialized_kwargs.items():
        # Get type and data from serialized structure
        type_name = value_info["type"]
        value_bytes = value_info["data"]

        # Import and get the actual type class
        from metaflow_extensions.nflx.plugins.functions.utils import (
            load_type_from_string,
        )

        value_type = load_type_from_string(type_name)
        if value_type is None:
            raise MetaflowFunctionException(
                f"Could not load type '{type_name}' for kwargs key '{key}'"
            )

        # This should never happen due to the check above, but helps mypy with type narrowing
        if value_type is None:
            raise MetaflowFunctionException(
                f"Internal error: value_type is None after successful load for '{type_name}'"
            )

        kwargs[key] = registry.deserialize(value_bytes, value_type)

    return PayloadHeader(
        data_bytes=data_bytes,
        kwargs=kwargs,
        error=error,
        is_system_error=is_system_error,
        runtime_components=runtime_components,
    )


def parse_function_payload(data, expected_data_type=None, return_raw_data=False):
    """
    Unified FunctionPayload parser that handles all use cases.

    Args:
        data: Bytes to parse OR already deserialized FunctionPayload
        expected_data_type: Expected type for data deserialization (required if return_raw_data=False)
        return_raw_data: If True, return raw data bytes; if False, return deserialized FunctionPayload

    Returns:
        - If return_raw_data=True: PayloadHeader (raw data bytes, kwargs, error, is_system_error, runtime_components)
        - If return_raw_data=False: FunctionPayload (deserialized object)
    """
    # Handle case where data is already a FunctionPayload object
    if isinstance(data, FunctionPayload):
        if return_raw_data:
            raise MetaflowFunctionException(
                "Cannot extract raw bytes from already deserialized FunctionPayload"
            )
        return data

    # Parse header and get raw data bytes
    header = _parse_function_payload_header(data)

    if return_raw_data:
        return header
    else:
        if expected_data_type is None:
            raise MetaflowFunctionException(
                "expected_data_type is required when return_raw_data=False"
            )

        # Deserialize data using the expected type
        # Special case: if expected type is bytes, use data_bytes directly (no-op)
        if expected_data_type is bytes:
            reconstructed_data = header.data_bytes
        else:
            from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
                get_global_registry,
            )

            registry = get_global_registry()
            reconstructed_data = registry.deserialize(
                header.data_bytes, expected_data_type
            )
        return FunctionPayload(
            reconstructed_data,
            header.kwargs,
            error=header.error,
            is_system_error=header.is_system_error,
            runtime_components=header.runtime_components,
        )


# Compatibility functions that delegate to the unified parser
def deserialize_function_payload(data) -> FunctionPayload:
    """Deserialize FunctionPayload from binary format."""
    header = _parse_function_payload_header(data)
    return FunctionPayload(
        header.data_bytes,
        header.kwargs,
        error=header.error,
        is_system_error=header.is_system_error,
        runtime_components=header.runtime_components,
    )


def extract_data_bytes_from_function_payload(data) -> PayloadHeader:
    """Extract raw data bytes and kwargs without deserializing data."""
    return parse_function_payload(data, return_raw_data=True)


from metaflow_extensions.nflx.plugins.functions.serializers.base import BaseSerializer


class FunctionPayloadSerializer(BaseSerializer):
    """Serializer for FunctionPayload objects."""

    @property
    def supported_type(self):
        return FunctionPayload

    def serialize(self, obj):
        return serialize_function_payload(obj)

    def deserialize(self, data: bytes):
        return deserialize_function_payload(data)


def register_function_payload_serializer():
    """Register FunctionPayload with the global serialization registry."""
    from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
        register_serializer,
    )

    register_serializer(FunctionPayloadSerializer())
