"""
Tests for signature-driven serializer registration in the function-type factory.

A decorated function self-registers serializers for the types in its signature, keyed by the
canonical type string of the annotation. A union annotation has no useful canonical type -- the
value handed to the function at runtime is one of its members -- so an optional parameter such as
``ctx: Optional[SomeProto] = None`` only self-registers if the union is unwrapped first. These
tests pin the unwrapping helper and the end-to-end registration behaviour.
"""

import sys
from typing import Optional, Union

import pytest

pytestmark = pytest.mark.no_backend_parametrization

from metaflow_extensions.nflx.plugins.functions.factory import (
    FunctionTypeConfig,
    _concrete_signature_types,
    create_function_type,
)
from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
    get_global_registry,
)

PROBE_SERIALIZER = (
    "metaflow_extensions.nflx.plugins.avro_function.serializers.AvroSerializer"
)


class OptionalContext:
    """Declared as ``ctx: Optional[OptionalContext] = None`` -- the case the unwrap fixes."""


class Pep604Context:
    """Declared as ``ctx: Pep604Context | None = None``."""


class ImplicitOptionalContext:
    """Declared as ``ctx: ImplicitOptionalContext = None`` (implicit Optional on Python 3.10)."""


class PlainContext:
    """Declared with a bare annotation and no default -- already worked before the unwrap."""


class UnreferencedContext:
    """Never named by a decorated function; must stay unregistered."""


def _probe_resolver(param_type):
    """Resolve a serializer only for this module's probe types, so real types are untouched."""
    probe_types = (
        OptionalContext,
        Pep604Context,
        ImplicitOptionalContext,
        PlainContext,
        UnreferencedContext,
    )
    if param_type in probe_types:
        return {"serializer": PROBE_SERIALIZER}
    return None


# A minimal function type used purely to exercise signature registration. The no-op custom
# validator is what lets these probe functions take an extra parameter: see
# validate_function_signature, where supplying a custom validator skips the parameter-count check.
_ProbeFunction, probe_function = create_function_type(
    FunctionTypeConfig(
        name="signature_probe_function",
        param_validators=[],
        return_validator=lambda type_hint: True,
        custom_validator=lambda func, params, type_hints, decorator_name: None,
        type_serializer_resolvers=[_probe_resolver],
    )
)


def _canonical(cls):
    return f"{cls.__module__}.{cls.__name__}"


def _is_registered(cls):
    return _canonical(cls) in get_global_registry()._serializer_configs


class TestConcreteSignatureTypes:
    """Unit tests for the annotation -> concrete types expansion."""

    def test_plain_type_passes_through(self):
        assert _concrete_signature_types(str) == [str]

    def test_optional_is_unwrapped(self):
        assert _concrete_signature_types(Optional[str]) == [str]

    def test_union_with_none_drops_nonetype(self):
        assert _concrete_signature_types(Union[str, int, None]) == [str, int]

    def test_union_without_none_returns_all_members(self):
        assert _concrete_signature_types(Union[str, int]) == [str, int]

    def test_pep604_optional_is_unwrapped(self):
        assert _concrete_signature_types(str | None) == [str]

    def test_generic_alias_is_not_unwrapped(self):
        # Only unions are expanded; a parameterized generic is still a single concrete annotation.
        assert _concrete_signature_types(list[str]) == [list[str]]


class TestSignatureRegistration:
    """End-to-end: decorating a function registers serializers for its signature types."""

    def test_explicit_optional_registers_the_concrete_type(self):
        @probe_function
        def handler(data: bytes, ctx: Optional[OptionalContext] = None) -> bytes:
            return data

        assert _is_registered(OptionalContext), (
            "Optional[X] carries no usable canonical type, so X itself must be registered or "
            "passing a value for this parameter fails at serialization time"
        )

    def test_pep604_optional_registers_the_concrete_type(self):
        @probe_function
        def handler(data: bytes, ctx: Pep604Context | None = None) -> bytes:
            return data

        assert _is_registered(Pep604Context)

    def test_none_default_registers_the_concrete_type(self):
        # get_type_hints applies implicit Optional on Python 3.10 (so this needs the unwrap) and
        # not on 3.11+ (where the annotation stays bare). Registration must hold on both.
        @probe_function
        def handler(
            data: bytes, ctx: ImplicitOptionalContext = None
        ) -> bytes:  # noqa: RUF013
            return data

        assert _is_registered(
            ImplicitOptionalContext
        ), f"implicit Optional applies on Python <=3.10; running {sys.version_info[:2]}"

    def test_plain_annotation_still_registers(self):
        # Guard that unwrapping did not regress the non-union path.
        @probe_function
        def handler(data: bytes, *, ctx: PlainContext) -> bytes:
            return data

        assert _is_registered(PlainContext)

    def test_unreferenced_type_is_not_registered(self):
        # Guards against the expansion over-registering: only types actually in a signature.
        assert not _is_registered(UnreferencedContext)
