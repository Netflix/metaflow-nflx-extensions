import pytest

pytestmark = pytest.mark.no_backend_parametrization
from metaflow_extensions.nflx.plugins.functions.serializers.avro_mixin import (
    AvroSerializationMixin,
)


class TestAvroSerializationMixin:
    """Test the AvroSerializationMixin."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mixin = AvroSerializationMixin()

    def test_serialize_deserialize_simple_record(self):
        """Test serialization and deserialization of a simple record."""
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "message", "type": "string"},
                {"name": "count", "type": "long"},
            ],
        }

        original_data = {"message": "hello", "count": 42}

        # Serialize
        serialized_bytes, artifacts = self.mixin._serialize_with_avro_schema(
            original_data, schema
        )
        assert isinstance(serialized_bytes, bytes)
        assert artifacts == []

        # Deserialize
        deserialized_data = self.mixin._deserialize_with_avro_schema(serialized_bytes)
        assert deserialized_data == original_data

    def test_serialize_deserialize_with_map(self):
        """Test serialization with map types."""
        schema = {
            "type": "record",
            "name": "MapRecord",
            "fields": [
                {"name": "metadata", "type": {"type": "map", "values": "string"}}
            ],
        }

        original_data = {"metadata": {"key1": "value1", "key2": "value2"}}

        # Serialize
        serialized_bytes, artifacts = self.mixin._serialize_with_avro_schema(
            original_data, schema
        )
        assert isinstance(serialized_bytes, bytes)
        assert artifacts == []

        # Deserialize
        deserialized_data = self.mixin._deserialize_with_avro_schema(serialized_bytes)
        assert deserialized_data == original_data

    def test_serialize_deserialize_with_nulls(self):
        """Test serialization with nullable fields."""
        schema = {
            "type": "record",
            "name": "NullableRecord",
            "fields": [
                {"name": "required", "type": "string"},
                {"name": "optional", "type": ["null", "string"]},
            ],
        }

        original_data = {"required": "test", "optional": None}

        # Serialize
        serialized_bytes, artifacts = self.mixin._serialize_with_avro_schema(
            original_data, schema
        )
        assert isinstance(serialized_bytes, bytes)
        assert artifacts == []

        # Deserialize
        deserialized_data = self.mixin._deserialize_with_avro_schema(serialized_bytes)
        assert deserialized_data == original_data

    def test_empty_data_raises_error(self):
        """Test that empty Avro data raises an error."""
        import io

        # Create empty bytes
        empty_buffer = io.BytesIO()
        empty_bytes = empty_buffer.getvalue()

        with pytest.raises(Exception):  # Should raise some kind of parsing error
            self.mixin._deserialize_with_avro_schema(empty_bytes)

    def test_no_records_raises_error(self):
        """Test that data with no records raises ValueError."""
        # Create a valid Avro file with no records
        import io
        from fastavro import writer, parse_schema

        schema = {
            "type": "record",
            "name": "EmptyRecord",
            "fields": [{"name": "field", "type": "string"}],
        }

        buffer = io.BytesIO()
        parsed_schema = parse_schema(schema)
        writer(buffer, parsed_schema, [])  # No records
        empty_records_bytes = buffer.getvalue()

        with pytest.raises(ValueError, match="No records found in Avro data"):
            self.mixin._deserialize_with_avro_schema(empty_records_bytes)
