from typing import Any, List, Tuple, Type

from metaflow_extensions.nflx.plugins.functions.serializers.base import BaseSerializer
from metaflow_extensions.nflx.plugins.functions.serializers.avro_mixin import (
    AvroSerializationMixin,
)


class AvroSerializer(BaseSerializer, AvroSerializationMixin):
    """
    Safe serializer for Python primitives, dicts, and lists with arbitrary nesting.

    Uses a unique wrapper key '__avro_data__' to avoid collisions with user keys.
    """

    WRAPPER_KEY = "__avro_data__"

    def __init__(self, type_hint: Type):
        self._avro_type = type_hint
        self._schema = self._generate_schema(type_hint)

    @property
    def supported_type(self) -> Type:
        return self._avro_type

    def _generate_schema(self, type_hint: Type) -> dict:
        primitive_types = ["null", "boolean", "long", "double", "string", "bytes"]

        if type_hint in (dict, list):
            # Recursive schema using unique wrapper
            return {
                "type": "record",
                "name": "NestedValue",
                "fields": [
                    {
                        "name": self.WRAPPER_KEY,
                        "type": primitive_types
                        + [
                            {"type": "array", "items": "NestedValue"},
                            {"type": "map", "values": "NestedValue"},
                        ],
                    }
                ],
            }
        else:
            # Primitive wrapper
            type_mapping = {
                str: "string",
                int: "long",
                float: "double",
                bool: "boolean",
                bytes: "bytes",
            }
            avro_type = type_mapping.get(type_hint, "string")
            return {
                "type": "record",
                "name": f"{type_hint.__name__}Wrapper",
                "fields": [{"name": self.WRAPPER_KEY, "type": avro_type}],
            }

    def serialize(self, obj: Any) -> Tuple[bytes, List[Any]]:
        wrapped = {self.WRAPPER_KEY: self._to_nested_value(obj)}
        return self._serialize_with_avro_schema(wrapped, self._schema)

    def deserialize(self, data: bytes) -> Any:
        result = self._deserialize_with_avro_schema(data)
        return self._from_nested_value(result[self.WRAPPER_KEY])

    def _to_nested_value(self, obj: Any) -> Any:
        if obj is None or isinstance(obj, (bool, int, float, str, bytes)):
            return obj
        elif isinstance(obj, dict):
            return {
                k: {self.WRAPPER_KEY: self._to_nested_value(v)} for k, v in obj.items()
            }
        elif isinstance(obj, list):
            return [{self.WRAPPER_KEY: self._to_nested_value(item)} for item in obj]
        else:
            return str(obj)

    def _from_nested_value(self, obj: Any) -> Any:
        if obj is None or isinstance(obj, (bool, int, float, str, bytes)):
            return obj
        elif isinstance(obj, dict):
            # Only unwrap if dict has exactly the WRAPPER_KEY
            if len(obj) == 1 and self.WRAPPER_KEY in obj:
                return self._from_nested_value(obj[self.WRAPPER_KEY])
            else:
                return {k: self._from_nested_value(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._from_nested_value(item) for item in obj]
        else:
            return obj
