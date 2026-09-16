"""Guards on the reference-JSON schema, for the benefit of readers we can no longer update.

A bound function's reference JSON is written by `Function._export` as
`json.dump(asdict(func_spec))`, so the JSON's top-level keys *are* the spec
dataclass's field names. Every loader then ends in `cls(**filtered_desc)` on a
`@dataclass(kw_only=True)`: `FunctionSpec._from_json_impl_from_data`,
`FunctionPipelineSpec._from_json_impl_from_data`, and the generated one in
`factory.py`. A key that is not a field is therefore a `TypeError` in every
reader already deployed, which no amount of fixing here can reach.

So: adding a name to either baseline below is a breaking change for readers in
the field. Additive data belongs inside `system_metadata`, which is a free-form
dict -- that is what PR #98 did for `runtime_components`, and why pre-#98
readers can still load specs written after it.
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
    """Every concrete spec emits exactly the agreed top-level key set."""
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
    """factory.py redeclares the schema fields on its generated classes; it must not add any."""
    deco_spec = function_cls.function_spec_cls._build_deco_spec(
        {"name": "f", "module": "m"}
    )

    actual = {f.name for f in fields(deco_spec)}

    assert actual == DECORATOR_SPEC_FIELDS, _schema_drift(actual, DECORATOR_SPEC_FIELDS)


def test_emitted_json_keys_match_the_frozen_fields(avro_spec):
    """What `Function._export` actually writes, not just what the class declares."""
    emitted = json.loads(json.dumps(asdict(avro_spec), sort_keys=True))

    assert set(emitted) == SPEC_FIELDS, _schema_drift(set(emitted), SPEC_FIELDS)
    assert set(emitted["function"]) == DECORATOR_SPEC_FIELDS, _schema_drift(
        set(emitted["function"]), DECORATOR_SPEC_FIELDS
    )


def test_unknown_top_level_key_is_currently_fatal(tmp_path, avro_spec):
    """Documents the constraint above: a reader cannot skip a key it doesn't know.

    If the loaders are ever made lenient, this test should be inverted rather
    than deleted -- it is the only place that states the cost of a new field.
    """
    desc = asdict(avro_spec)
    desc["some_future_field"] = "value"
    reference = tmp_path / "reference.json"
    reference.write_text(json.dumps(desc))

    with pytest.raises(TypeError, match="some_future_field"):
        FunctionSpec.from_json(str(reference))


def test_unknown_system_metadata_key_is_tolerated(tmp_path, avro_spec):
    """The supported way to add data: a new key inside `system_metadata`."""
    desc = asdict(avro_spec)
    desc["system_metadata"] = {
        **(desc["system_metadata"] or {}),
        "some_future_section": {"enabled": True},
    }
    reference = tmp_path / "reference.json"
    reference.write_text(json.dumps(desc))

    spec = FunctionSpec.from_json(str(reference))

    assert spec.system_metadata["some_future_section"] == {"enabled": True}
