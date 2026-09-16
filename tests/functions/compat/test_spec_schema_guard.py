"""Guards on the reference-JSON schema, for readers we can no longer update.

The JSON is `json.dump(asdict(spec))` and every loader ends in `cls(**desc)` on a
`kw_only` dataclass, so a key that is not a field is a `TypeError` in already-deployed
readers. The committed old reference is the baseline: gaining or losing a field
relative to it is a breaking change for them, while additive data inside
`system_metadata` is free.
"""

import json
from dataclasses import asdict, fields

import pytest

from metaflow_extensions.nflx.plugins.avro_function import AvroFunction
from metaflow_extensions.nflx.plugins.functions.core.function_decorator_spec import (
    FunctionDecoratorSpec,
)
from metaflow_extensions.nflx.plugins.functions.core.function_pipeline import (
    FunctionPipeline,
    PipelineFunctionDecoratorSpec,
)
from metaflow_extensions.nflx.plugins.functions.core.function_spec import FunctionSpec
from metaflow_extensions.nflx.plugins.json_function import JsonFunction

pytestmark = pytest.mark.no_backend_parametrization

SPEC_CLASSES = [
    pytest.param(FunctionSpec, id="FunctionSpec"),
    pytest.param(FunctionPipeline.function_spec_cls, id="FunctionPipelineSpec"),
    pytest.param(AvroFunction.function_spec_cls, id="AvroFunctionSpec"),
    pytest.param(JsonFunction.function_spec_cls, id="JsonFunctionSpec"),
]

DECORATOR_SPEC_CLASSES = [
    pytest.param(FunctionDecoratorSpec, id="FunctionDecoratorSpec"),
    pytest.param(PipelineFunctionDecoratorSpec, id="PipelineFunctionDecoratorSpec"),
]


def _drift(actual, expected):
    return (
        "schema drifted from the committed old reference: "
        f"added {sorted(actual - expected)}, removed {sorted(expected - actual)}. "
        "Read this module's docstring before changing either side."
    )


@pytest.mark.parametrize("spec_cls", SPEC_CLASSES)
def test_spec_top_level_fields_match_the_old_reference(spec_cls, old_reference_data):
    actual = {f.name for f in fields(spec_cls)}
    expected = set(old_reference_data)

    assert actual == expected, _drift(actual, expected)


@pytest.mark.parametrize("deco_cls", DECORATOR_SPEC_CLASSES)
def test_decorator_spec_fields_match_the_old_reference(deco_cls, old_reference_data):
    """Same contract for the nested `function` object."""
    actual = {f.name for f in fields(deco_cls)}
    expected = set(old_reference_data["function"])

    assert actual == expected, _drift(actual, expected)


@pytest.mark.parametrize(
    "function_cls", [AvroFunction, JsonFunction], ids=lambda c: c.__name__
)
def test_generated_decorator_spec_fields_match_the_old_reference(
    function_cls, old_reference_data
):
    """factory.py redeclares these fields on its generated classes; it must not add any."""
    deco_spec = function_cls.function_spec_cls._build_deco_spec(
        {"name": "f", "module": "m"}
    )
    actual = {f.name for f in fields(deco_spec)}
    expected = set(old_reference_data["function"])

    assert actual == expected, _drift(actual, expected)


def test_emitted_json_keys_match_the_old_reference(avro_spec, old_reference_data):
    """What `_export` actually writes, not just what the class declares."""
    emitted = json.loads(json.dumps(asdict(avro_spec), sort_keys=True))

    assert set(emitted) == set(old_reference_data), _drift(
        set(emitted), set(old_reference_data)
    )
    assert set(emitted["function"]) == set(old_reference_data["function"]), _drift(
        set(emitted["function"]), set(old_reference_data["function"])
    )


def test_unknown_top_level_key_is_currently_fatal(tmp_path, avro_spec):
    """If the loaders are ever made lenient, invert this rather than delete it."""
    desc = asdict(avro_spec)
    desc["some_future_field"] = "value"
    reference = tmp_path / "reference.json"
    reference.write_text(json.dumps(desc))

    with pytest.raises(TypeError, match="some_future_field"):
        FunctionSpec.from_json(str(reference))


def test_unknown_system_metadata_key_is_tolerated(tmp_path, avro_spec):
    """The supported way to add data."""
    desc = asdict(avro_spec)
    desc["system_metadata"] = {
        **(desc["system_metadata"] or {}),
        "some_future_section": {"enabled": True},
    }
    reference = tmp_path / "reference.json"
    reference.write_text(json.dumps(desc))

    spec = FunctionSpec.from_json(str(reference))

    assert spec.system_metadata["some_future_section"] == {"enabled": True}
