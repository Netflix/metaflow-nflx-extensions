import pytest

pytestmark = pytest.mark.no_backend_parametrization
from metaflow_extensions.nflx.plugins.avro_function.serializers import AvroSerializer


class TestAvroSerializer:
    """Test the AvroSerializer class."""

    def test_string_serializer(self):
        """Test AvroSerializer for string type."""
        serializer = AvroSerializer(str)
        assert serializer.supported_type == str

        # Test serialization/deserialization
        original = "hello world"
        serialized_bytes, artifacts = serializer.serialize(original)

        assert isinstance(serialized_bytes, bytes)
        assert artifacts == []

        deserialized = serializer.deserialize(serialized_bytes)
        assert deserialized == original
        assert isinstance(deserialized, str)

    def test_int_serializer(self):
        """Test AvroSerializer for int type."""
        serializer = AvroSerializer(int)
        assert serializer.supported_type == int

        # Test serialization/deserialization
        original = 42
        serialized_bytes, artifacts = serializer.serialize(original)

        assert isinstance(serialized_bytes, bytes)
        assert artifacts == []

        deserialized = serializer.deserialize(serialized_bytes)
        assert deserialized == original
        assert isinstance(deserialized, int)

    def test_float_serializer(self):
        """Test AvroSerializer for float type."""
        serializer = AvroSerializer(float)
        assert serializer.supported_type == float

        # Test serialization/deserialization
        original = 3.14159
        serialized_bytes, artifacts = serializer.serialize(original)

        assert isinstance(serialized_bytes, bytes)
        assert artifacts == []

        deserialized = serializer.deserialize(serialized_bytes)
        assert abs(deserialized - original) < 1e-10  # Float precision
        assert isinstance(deserialized, float)

    def test_bool_serializer(self):
        """Test AvroSerializer for bool type."""
        serializer = AvroSerializer(bool)
        assert serializer.supported_type == bool

        # Test True
        serialized_bytes, artifacts = serializer.serialize(True)
        assert isinstance(serialized_bytes, bytes)
        assert artifacts == []
        deserialized = serializer.deserialize(serialized_bytes)
        assert deserialized is True

        # Test False
        serialized_bytes, artifacts = serializer.serialize(False)
        deserialized = serializer.deserialize(serialized_bytes)
        assert deserialized is False

    def test_bytes_serializer(self):
        """Test AvroSerializer for bytes type."""
        serializer = AvroSerializer(bytes)
        assert serializer.supported_type == bytes

        # Test serialization/deserialization
        original = b"binary data \x00\x01\x02"
        serialized_bytes, artifacts = serializer.serialize(original)

        assert isinstance(serialized_bytes, bytes)
        assert artifacts == []

        deserialized = serializer.deserialize(serialized_bytes)
        assert deserialized == original
        assert isinstance(deserialized, bytes)

    def test_dict_serializer(self):
        """Test AvroSerializer for dict type."""
        serializer = AvroSerializer(dict)
        assert serializer.supported_type == dict

        # Test serialization/deserialization
        original = {"key1": "value1", "key2": "value2", "key3": None}
        serialized_bytes, artifacts = serializer.serialize(original)

        assert isinstance(serialized_bytes, bytes)
        assert artifacts == []

        deserialized = serializer.deserialize(serialized_bytes)
        assert deserialized == original
        assert isinstance(deserialized, dict)

    def test_list_serializer(self):
        """Test AvroSerializer for list type."""
        serializer = AvroSerializer(list)
        assert serializer.supported_type == list

        # Test serialization/deserialization
        original = ["item1", "item2", None, 42, 3.14]
        serialized_bytes, artifacts = serializer.serialize(original)

        assert isinstance(serialized_bytes, bytes)
        assert artifacts == []

        deserialized = serializer.deserialize(serialized_bytes)
        assert deserialized == original
        assert isinstance(deserialized, list)

    def test_schema_generation(self):
        """Test that schemas are properly generated for different types."""
        # String schema
        string_serializer = AvroSerializer(str)
        schema = string_serializer._schema
        assert schema["type"] == "record"
        assert schema["name"] == "strWrapper"
        assert len(schema["fields"]) == 1
        assert schema["fields"][0]["name"] == AvroSerializer.WRAPPER_KEY
        assert schema["fields"][0]["type"] == "string"

        # Dict schema using recursive NestedValue record
        dict_serializer = AvroSerializer(dict)
        dict_schema = dict_serializer._schema
        assert dict_schema["type"] == "record"
        assert dict_schema["name"] == "NestedValue"
        assert "fields" in dict_schema
        assert dict_schema["fields"][0]["name"] == AvroSerializer.WRAPPER_KEY
        assert isinstance(dict_schema["fields"][0]["type"], list)  # Union type

        # List schema using recursive NestedValue record
        list_serializer = AvroSerializer(list)
        list_schema = list_serializer._schema
        assert list_schema["type"] == "record"
        assert list_schema["name"] == "NestedValue"
        assert "fields" in list_schema
        assert list_schema["fields"][0]["name"] == AvroSerializer.WRAPPER_KEY
        assert isinstance(list_schema["fields"][0]["type"], list)  # Union type

    def test_round_trip_consistency(self):
        """Test that multiple serialize/deserialize cycles maintain data integrity."""
        test_cases = [
            (str, "test string"),
            (int, 123),
            (float, 45.67),
            (bool, True),
            (bytes, b"test bytes"),
            (dict, {"a": "b", "c": None}),
            (list, [1, "two", None, True]),
        ]

        for type_class, original_value in test_cases:
            serializer = AvroSerializer(type_class)

            # First round trip
            bytes1, _ = serializer.serialize(original_value)
            value1 = serializer.deserialize(bytes1)

            # Second round trip
            bytes2, _ = serializer.serialize(value1)
            value2 = serializer.deserialize(bytes2)

            # Values should be identical
            if isinstance(original_value, float):
                assert abs(original_value - value1) < 1e-10
                assert abs(value1 - value2) < 1e-10
            else:
                assert original_value == value1 == value2

    def test_empty_containers(self):
        """Test serialization of empty containers."""
        # Empty dict
        dict_serializer = AvroSerializer(dict)
        empty_dict = {}
        serialized, _ = dict_serializer.serialize(empty_dict)
        deserialized = dict_serializer.deserialize(serialized)
        assert deserialized == empty_dict

        # Empty list
        list_serializer = AvroSerializer(list)
        empty_list = []
        serialized, _ = list_serializer.serialize(empty_list)
        deserialized = list_serializer.deserialize(serialized)
        assert deserialized == empty_list

    def test_nested_structures(self):
        """Test serialization of nested data structures."""
        # Nested dict
        dict_serializer = AvroSerializer(dict)
        nested_dict = {
            "level1": {"level2": {"level3": "deep value"}},
            "simple": "value",
        }

        serialized, _ = dict_serializer.serialize(nested_dict)
        deserialized = dict_serializer.deserialize(serialized)
        assert deserialized == nested_dict

        # Nested list
        list_serializer = AvroSerializer(list)
        nested_list = [[1, 2, 3], ["a", "b", "c"], [{"nested": "dict"}]]

        serialized, _ = list_serializer.serialize(nested_list)
        deserialized = list_serializer.deserialize(serialized)
        assert deserialized == nested_list

    def test_type_mapping_coverage(self):
        """Test that all expected types are covered in the type mapping."""
        serializer = AvroSerializer(str)  # Just to access the method

        # Test all mapped types
        expected_types = [str, int, float, bool, bytes, dict, list]
        for type_class in expected_types:
            schema = serializer._generate_schema(type_class)
            assert schema is not None
            assert "type" in schema
            assert "name" in schema
            assert "fields" in schema

    def test_unknown_type_defaults_to_string(self):
        """Test that unknown types default to string schema."""

        # Use a type that's not in the mapping
        class CustomType:
            pass

        serializer = AvroSerializer(str)  # Just to access the method
        schema = serializer._generate_schema(CustomType)

        # Should default to string type
        assert schema["fields"][0]["type"] == "string"
        assert schema["name"] == "CustomTypeWrapper"
