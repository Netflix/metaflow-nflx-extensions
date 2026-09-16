"""Guards on the reference-JSON schema, for readers we can no longer update.

The JSON is `json.dump(asdict(spec))` and every loader ends in `cls(**desc)` on a
`kw_only` dataclass, so a key that is not a field is a `TypeError` in already-deployed
readers. Adding a name to either baseline below is a breaking change for them;
additive data goes inside `system_metadata`, as #98 did for `runtime_components`.
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

SPEC_FIELDS = {
    "name",
    "uuid",
    "class_name",
    "user",
    "timestamp_utc",
    "reference",
    "function",
    "input_spec",
    "output_spec",
    "code_package",
    "task_pathspec",
    "task_code_path",
    "package_uuid",
    "system_metadata",
    "user_metadata",
    "artifacts",
    "serializer_configs",
}

DECORATOR_SPEC_FIELDS = {
    "name",
    "module",
    "file_name",
    "doc",
    "type",
    "input_schema",
    "parameter_schema",
    "return_schema",
}

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


def _schema_drift(actual, expected):
    return (
        f"reference-JSON schema changed: added {sorted(actual - expected)}, "
        f"removed {sorted(expected - actual)}. Read this module's docstring before "
        "updating the baseline."
    )


@pytest.mark.parametrize("spec_cls", SPEC_CLASSES)
def test_spec_top_level_fields_are_frozen(spec_cls):
    actual = {f.name for f in fields(spec_cls)}

    assert actual == SPEC_FIELDS, _schema_drift(actual, SPEC_FIELDS)


@pytest.mark.parametrize("deco_cls", DECORATOR_SPEC_CLASSES)
def test_decorator_spec_fields_are_frozen(deco_cls):
    """Same contract for the nested `function` object."""
    actual = {f.name for f in fields(deco_cls)}

    assert actual == DECORATOR_SPEC_FIELDS, _schema_drift(actual, DECORATOR_SPEC_FIELDS)


@pytest.mark.parametrize(
    "function_cls", [AvroFunction, JsonFunction], ids=lambda c: c.__name__
)
def test_generated_decorator_spec_fields_are_frozen(function_cls):
    """factory.py redeclares these fields on its generated classes; it must not add any."""
    deco_spec = function_cls.function_spec_cls._build_deco_spec(
        {"name": "f", "module": "m"}
    )

    actual = {f.name for f in fields(deco_spec)}

    assert actual == DECORATOR_SPEC_FIELDS, _schema_drift(actual, DECORATOR_SPEC_FIELDS)


def test_emitted_json_keys_match_the_frozen_fields(avro_spec):
    """What `_export` actually writes, not just what the class declares."""
    emitted = json.loads(json.dumps(asdict(avro_spec), sort_keys=True))

    assert set(emitted) == SPEC_FIELDS, _schema_drift(set(emitted), SPEC_FIELDS)
    assert set(emitted["function"]) == DECORATOR_SPEC_FIELDS, _schema_drift(
        set(emitted["function"]), DECORATOR_SPEC_FIELDS
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
