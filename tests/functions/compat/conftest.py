import json
import os

import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")

FIXTURE_VERSIONS = ["v0.2.7"]


def fixture_path(version: str) -> str:
    return os.path.join(FIXTURES_DIR, version, "reference.json")


@pytest.fixture(params=FIXTURE_VERSIONS)
def old_reference(request):
    """One committed reference JSON, once per pinned old version."""
    return fixture_path(request.param)


@pytest.fixture
def avro_spec():
    """A loaded spec to re-emit, so the guard tests the real writer path."""
    from metaflow_extensions.nflx.plugins.functions.core.function_spec import (
        FunctionSpec,
    )

    return FunctionSpec.from_json(fixture_path(FIXTURE_VERSIONS[0]))


@pytest.fixture
def old_reference_data(old_reference):
    with open(old_reference) as f:
        return json.load(f)
