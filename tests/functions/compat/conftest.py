import json
import os
import sys

import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")

FIXTURE_VERSIONS = ["v0.2.8"]


# The flow under test names its module the same thing, and `get_function_from_path`
# resolves a bare module name through `sys.modules`, so whichever copy is imported
# first wins for the rest of the process.
PACKAGED_MODULE = "function_module"


@pytest.fixture(autouse=True)
def evict_packaged_module():
    """Drop the fixture package's module after each test in this directory.

    Replaying a committed package imports its ``function_module``. Left cached, a later
    in-process load returns that old copy: a function added since the fixture was cut
    reports itself missing, and one that existed then runs the fixture's code instead of
    the code under test.
    """
    yield
    sys.modules.pop(PACKAGED_MODULE, None)


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
