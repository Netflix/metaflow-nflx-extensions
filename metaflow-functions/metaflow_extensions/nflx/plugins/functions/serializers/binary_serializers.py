from typing import Any, List, Tuple, Type

from metaflow_extensions.nflx.plugins.functions.serializers.base import BaseSerializer


class MemoryViewSerializer(BaseSerializer):
    """Serializer for memoryview objects."""

    @property
    def supported_type(self) -> Type:
        return memoryview

    def serialize(self, obj: memoryview) -> Tuple[bytes, List[Any]]:
        return obj.tobytes(), []

    def deserialize(self, data: bytes) -> memoryview:
        return memoryview(data)


class BytesSerializer(BaseSerializer):
    """Serializer for bytes objects."""

    @property
    def supported_type(self) -> Type:
        return bytes

    def serialize(self, obj: bytes) -> Tuple[bytes, List[Any]]:
        return obj, []

    def deserialize(self, data: bytes) -> bytes:
        return data


class ByteArraySerializer(BaseSerializer):
    """Serializer for bytearray objects."""

    @property
    def supported_type(self) -> Type:
        return bytearray

    def serialize(self, obj: bytearray) -> Tuple[bytes, List[Any]]:
        return bytes(obj), []

    def deserialize(self, data: bytes) -> bytearray:
        return bytearray(data)
