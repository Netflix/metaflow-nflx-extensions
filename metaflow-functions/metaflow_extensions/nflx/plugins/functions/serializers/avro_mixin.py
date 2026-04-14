"""Shared Avro serialization utilities."""

import io
from typing import Any, Tuple, List


class AvroSerializationMixin:
    """Mixin providing shared Avro serialization utilities."""

    def _serialize_with_avro_schema(
        self, data: Any, schema: dict
    ) -> Tuple[bytes, List[Any]]:
        """
        Serialize data using the provided Avro schema.

        Parameters
        ----------
        data : Any
            The data to serialize
        schema : dict
            The Avro schema definition

        Returns
        -------
        Tuple[bytes, List[Any]]
            Tuple of (serialized_bytes, empty_list)
        """
        try:
            from fastavro import writer, parse_schema
        except ImportError:
            raise ImportError("fastavro is required for Avro serialization")

        buffer = io.BytesIO()
        parsed_schema = parse_schema(schema)
        writer(buffer, parsed_schema, [data])
        return buffer.getvalue(), []

    def _deserialize_with_avro_schema(self, data: bytes) -> Any:
        """
        Deserialize bytes back to Python object using Avro.

        Parameters
        ----------
        data : bytes
            The bytes to deserialize

        Returns
        -------
        Any
            The deserialized object (first record)
        """
        try:
            from fastavro import reader
        except ImportError:
            raise ImportError("fastavro is required for Avro deserialization")

        buffer = io.BytesIO(data)
        records = list(reader(buffer))
        if not records:
            raise ValueError("No records found in Avro data")
        return records[0]
