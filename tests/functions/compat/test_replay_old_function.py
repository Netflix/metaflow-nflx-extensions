"""Load a reference written by an older metaflow-functions and actually call it.

The old runtime is not the installed one: the committed task package carries
`.mf_code/metaflow_extensions`, which `update_packaging_env_vars` puts on the runtime
subprocess's `PYTHONPATH`, shadowing site-packages. So this runs today's caller
against that version's code -- the test #98 would have needed.

Three path fields are rewritten into a temp copy, and two parts of the original
environment cannot be carried in a repo. See `fixtures/README.md`.
"""

import json
import os

import pytest

from metaflow_extensions.nflx.plugins.functions.components.runtime_metrics import (
    RuntimeMetrics,
)
from metaflow_extensions.nflx.plugins.functions.core.function import (
    close_function,
    function_from_json,
)
from metaflow_extensions.nflx.plugins.functions.core.function_parameters import (
    FunctionParameters,
)

pytestmark = pytest.mark.no_backend_parametrization

# Committed beside each era's reference.json.
PACKAGE_FILES = {
    "code_package": "function_package.zip",
    "task_code_path": "task_package.tar",
}

# That era's `avro_simple_string`: upper(), spaces stripped, "_" + params.suffix.
INPUT = "hello world"
EXPECTED = "HELLOWORLD_default"


@pytest.fixture
def replayable_reference(old_reference, old_reference_data, tmp_path, monkeypatch):
    """The committed reference, pointed at the committed packages."""
    era_dir = os.path.dirname(old_reference)
    desc = dict(old_reference_data)

    for field, file_name in PACKAGE_FILES.items():
        package = os.path.join(era_dir, file_name)
        if not os.path.exists(package):
            pytest.skip(f"{os.path.basename(era_dir)} has no committed {file_name}")
        desc[field] = package

    desc["artifacts"] = {}

    reference = tmp_path / "reference.json"
    # Rewritten too, not just the package fields: both backends re-read this one
    # and download it themselves.
    desc["reference"] = str(reference)
    reference.write_text(json.dumps(desc))

    monkeypatch.setenv("METAFLOW_FUNCTIONS_TEST_MODE", "1")
    monkeypatch.setenv("METAFLOW_FUNCTION_RUNTIME_PATH", str(tmp_path))

    return str(reference)


@pytest.mark.parametrize("backend", ["memory", "local"])
def test_old_function_still_executes(replayable_reference, backend):
    func = function_from_json(replayable_reference, backend=backend)
    try:
        assert func(INPUT) == EXPECTED
    finally:
        close_function(func)


@pytest.mark.parametrize("backend", ["memory", "local"])
def test_old_function_still_executes_twice(replayable_reference, backend):
    """The second call reuses the started runtime rather than rebinding."""
    func = function_from_json(replayable_reference, backend=backend)
    try:
        assert func(INPUT) == EXPECTED
        assert func("second call") == "SECONDCALL_default"
    finally:
        close_function(func)


@pytest.mark.parametrize("backend", ["memory", "local"])
def test_old_function_accepts_runtime_components(replayable_reference, backend):
    """The #98 surface: the caller appends `--runtime-component <cls>` to the runtime
    command, and a pre-#98 memory_cli has no such option."""
    metrics = RuntimeMetrics()
    func = function_from_json(
        replayable_reference, backend=backend, runtime_components=[metrics]
    )
    try:
        assert func(INPUT) == EXPECTED
        assert metrics.output["call_count"] == 1
        assert metrics.output["total_duration_s"] >= 0
    finally:
        close_function(func)


def test_old_function_accepts_caller_params(replayable_reference):
    """Local only: the memory runtime builds parameters itself inside the subprocess."""
    func = function_from_json(replayable_reference, backend="local")
    try:
        assert func(INPUT, params=FunctionParameters(suffix="ctx")) == "HELLOWORLD_ctx"
    finally:
        close_function(func)
