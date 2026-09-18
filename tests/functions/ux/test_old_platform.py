"""A function bound by today's code, loaded and called by a released older version.

The hermetic half of this lives in `tests/functions/compat/` and only proves the JSON
contract. This one is the whole handoff: the reference comes from a real flow run, so
the old release also downloads the packages, starts a runtime and resolves the bound
task's artifacts (hence `_modified` rather than the default suffix).

Only the caller is old. The runtime subprocess takes its code from the function's
package, which today's binder wrote -- which is exactly what an un-updated platform
does when it loads a new function.
"""

import os
import subprocess

import pytest

from ..compat.conftest import FIXTURE_VERSIONS
from ..old_reader import install

pytestmark = pytest.mark.no_backend_parametrization

CALLER = """
import sys
from metaflow_extensions.nflx.plugins.functions.core.function import (
    close_function,
    function_from_json,
)

func = function_from_json(sys.argv[1], backend=sys.argv[2])
try:
    print(func(sys.argv[3]))
finally:
    close_function(func)
"""


@pytest.fixture(scope="session", params=FIXTURE_VERSIONS)
def old_platform(request, tmp_path_factory):
    version = request.param.lstrip("v")

    try:
        return install(version, tmp_path_factory.mktemp(f"platform-{version}"))
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        pytest.skip(f"cannot install metaflow-functions=={version}: {e}")


@pytest.mark.parametrize("backend", ["memory", "local"])
def test_old_platform_calls_a_function_bound_today(
    old_platform, bound_functions, backend, tmp_path
):
    env = dict(os.environ)
    # The old release cannot resolve this fixture's conda alias; the new runtime code
    # still comes from the package, which is the mixed-version case under test.
    env["METAFLOW_FUNCTIONS_TEST_MODE"] = "1"
    env["METAFLOW_FUNCTION_RUNTIME_PATH"] = str(tmp_path)

    result = subprocess.run(
        [
            old_platform,
            "-c",
            CALLER,
            bound_functions["avro_simple_function"],
            backend,
            "hello",
        ],
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith("HELLO_modified"), result.stdout
