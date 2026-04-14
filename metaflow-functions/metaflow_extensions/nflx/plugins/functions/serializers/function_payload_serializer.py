from metaflow_extensions.nflx.plugins.functions.core.function_payload import (
    FunctionPayload,
    serialize_function_payload,
    deserialize_function_payload,
)
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
