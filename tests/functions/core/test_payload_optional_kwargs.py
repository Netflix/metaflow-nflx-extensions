"""Tests for passing None as a function keyword argument.

Kwarg values are serialized by their runtime type, not by the parameter's annotation, so
an argument declared ``ctx: Optional[str] = None`` and called with None dispatches on
``type(None)``. Omitting the argument serializes nothing at all -- the callee's own default
applies -- which is why omitting it worked while passing None did not.
"""

import pytest

pytestmark = pytest.mark.no_backend_parametrization

# Importing a function type registers the core serializers, including NoneType.
import metaflow_extensions.nflx.plugins.avro_function  # noqa: F401
from metaflow_extensions.nflx.plugins.functions.core.function_payload import (
    FunctionPayload,
    _parse_function_payload_header,
    serialize_function_payload,
)
from metaflow_extensions.nflx.plugins.functions.utils import load_type_from_string


def _round_trip(kwargs):
    data, _ = serialize_function_payload(FunctionPayload("hello", kwargs))
    return _parse_function_payload_header(data).kwargs


class TestOptionalKwargSerialization:
    def test_omitted_argument_serializes_nothing(self):
        assert _round_trip({}) == {}

    def test_none_argument_round_trips(self):
        assert _round_trip({"ctx": None}) == {"ctx": None}

    def test_value_argument_round_trips(self):
        assert _round_trip({"ctx": "a value"}) == {"ctx": "a value"}

    def test_none_alongside_other_arguments(self):
        assert _round_trip({"ctx": None, "count": 3}) == {"ctx": None, "count": 3}


class TestNoneTypeCanonicalString:
    def test_canonical_string_is_loadable(self):
        """NoneType claims module 'builtins' but is not an attribute of it.

        The payload header stores this string and reconstructs the type from it, so a
        generic importlib lookup is not enough.
        """
        assert load_type_from_string("builtins.NoneType") is type(None)
