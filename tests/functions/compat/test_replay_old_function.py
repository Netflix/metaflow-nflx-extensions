"""Load a reference written by an older metaflow-functions and actually call it.

This is the test that would have caught #98. The old runtime code is not the
installed one: `setup_code_packages` extracts the spec's `task_code_path` and
`code_package` into the runtime dir, and `update_packaging_env_vars` puts that
dir's `.mf_code` on the subprocess's `PYTHONPATH`, so the packaged (old)
`metaflow` and `metaflow_extensions` shadow site-packages. Calling an old
reference therefore runs today's caller against that version's runtime.

Three fields have to be rewritten because the captured `s3://` paths resolve
nowhere: the two package paths (`download_s3_packages` passes non-S3 paths
through untouched) and `reference` itself, which both backends re-read and
download on their own (`LocalBackend.apply`, `MemoryBackend.get_runtime_command`).

Two parts of the original environment cannot be carried in the repo:

- The conda env behind `system_metadata.environment.alias`;
  `METAFLOW_FUNCTIONS_TEST_MODE=1` skips resolving it. The old *code* still
  comes from the package, so the compat surface is intact.
- The `artifacts` map, whose entries are metaflow datastore objects
  (`location: ":root:s3://..."`, addressed by sha) rather than paths, so there
  is nothing to redirect. Cleared here, which makes the function fall back to
  its parameter defaults -- note the runtime resolves parameters itself, so
  caller-side `params=` does not substitute for this on the memory backend.
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

# Committed beside each era's reference.json; see fixtures/README.md.
PACKAGE_FILES = {
    "code_package": "function_package.zip",
    "task_code_path": "task_package.tar",
}

# `avro_simple_string` in that era's flows/function_module.py returns
# data.upper().replace(" ", "") + "_" + params.suffix, defaulting to "default".
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
    """The #98 surface: today's caller adds a component to an old function.

    The caller appends `--runtime-component <cls>` to the runtime command, which
    is why specs older than #98 are out of support -- their memory_cli has no
    such option and the subprocess dies on startup.
    """
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
    """Caller-supplied parameters still reach an old function body.

    Local backend only: the memory backend's runtime builds parameters from the
    spec inside the subprocess, so `params=` here would not reach it.
    """
    func = function_from_json(replayable_reference, backend="local")
    try:
        assert func(INPUT, params=FunctionParameters(suffix="ctx")) == "HELLOWORLD_ctx"
    finally:
        close_function(func)
