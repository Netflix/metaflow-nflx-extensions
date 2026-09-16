"""Today's code must still load reference JSONs written by older versions.

One fixture per era under `fixtures/` (see its README). Nothing here touches S3:
`start_runtime=False` stops after importing the function class.
"""

import pytest

from metaflow_extensions.nflx.plugins.functions.core.function import function_from_json
from metaflow_extensions.nflx.plugins.functions.core.function_spec import FunctionSpec

pytestmark = pytest.mark.no_backend_parametrization

# FunctionSpec's field set at each era's cutoff commit. A fixture that drifts from
# this is no longer evidence about the version it claims to be.
ERA_TOP_LEVEL_KEYS = {
    # f072249 = #98; older shapes are out of support.
    "post-runtime-components": {
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
    },
}


def test_fixture_matches_its_era(old_reference, old_reference_data):
    era = old_reference.split("/")[-2]

    assert set(old_reference_data) == ERA_TOP_LEVEL_KEYS[era]


def test_old_reference_parses(old_reference, old_reference_data):
    spec = FunctionSpec.from_json(old_reference)

    assert spec.uuid == old_reference_data["uuid"]
    assert spec.class_name == old_reference_data["class_name"]
    assert spec.reference == old_reference_data["reference"]
    assert spec.code_package == old_reference_data["code_package"]
    assert spec.task_code_path == old_reference_data["task_code_path"]


def test_old_reference_keeps_its_nested_function(old_reference, old_reference_data):
    spec = FunctionSpec.from_json(old_reference)

    expected = old_reference_data["function"]
    assert spec.function is not None
    assert spec.function.name == expected["name"]
    assert spec.function.module == expected["module"]
    assert spec.function.type == expected["type"]
    assert spec.function.input_schema == expected["input_schema"]
    assert spec.function.return_schema == expected["return_schema"]


def test_old_reference_registers_its_serializers(old_reference, old_reference_data):
    """Serializer configs are import paths, so they must still resolve."""
    from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
        get_global_registry,
    )

    function_from_json(old_reference, start_runtime=False)

    registry = get_global_registry()
    for canonical_type, config in old_reference_data["serializer_configs"].items():
        assert canonical_type in registry._serializer_configs
        assert (
            registry._serializer_configs[canonical_type].serializer
            == config["serializer"]
        )


@pytest.mark.parametrize("backend", ["memory", "local"])
def test_old_reference_builds_a_proxy(old_reference, old_reference_data, backend):
    func = function_from_json(old_reference, backend=backend, start_runtime=False)

    assert func.spec.uuid == old_reference_data["uuid"]
    assert func.name == old_reference_data["name"]
    assert func._runtime_components == []
