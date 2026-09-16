import json
import os

import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")

FIXTURE_ERAS = ["post-runtime-components"]


def fixture_path(era: str) -> str:
    return os.path.join(FIXTURES_DIR, era, "reference.json")


@pytest.fixture(params=FIXTURE_ERAS)
def old_reference(request):
    """Path to one committed reference JSON, once per schema era."""
    return fixture_path(request.param)


@pytest.fixture
def avro_spec():
    """A loaded spec to re-emit, so the schema guard tests the real writer path."""
    from metaflow_extensions.nflx.plugins.functions.core.function_spec import (
        FunctionSpec,
    )

    return FunctionSpec.from_json(fixture_path("post-runtime-components"))


@pytest.fixture
def old_reference_data(old_reference):
    with open(old_reference) as f:
        return json.load(f)
