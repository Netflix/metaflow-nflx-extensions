import json
from typing import Any, List, Tuple, Type

from metaflow_extensions.nflx.plugins.functions.serializers.base import BaseSerializer


class JsonSerializer(BaseSerializer):
    """
    Generic serializer for JSON-compatible types (str, dict, list, int, float, bool).
    """

    def __init__(self, json_type: Type):
        """
        Initialize with a specific JSON-compatible type.

        Parameters
        ----------
        json_type : Type
            The JSON-compatible type this serializer handles
        """
        self._json_type = json_type

    @property
    def supported_type(self) -> Type:
        return self._json_type

    def serialize(self, obj: Any) -> Tuple[bytes, List[Any]]:
        """
        Serialize a JSON-compatible object to bytes.

        Parameters
        ----------
        obj : Any
            The JSON-compatible object to serialize

        Returns
        -------
        Tuple[bytes, List[Any]]
            A tuple of (serialized_bytes, empty_list)
        """
        return json.dumps(obj).encode("utf-8"), []

    def deserialize(self, data: bytes) -> Any:
        """
        Deserialize bytes back to a JSON-compatible object.

        Parameters
        ----------
        data : bytes
            The bytes to deserialize

        Returns
        -------
        Any
            The deserialized JSON object
        """
        return json.loads(data.decode("utf-8"))
