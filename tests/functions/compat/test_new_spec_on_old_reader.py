"""The other direction: a spec today's code writes, read by a released older version.

Nothing in-process can test this -- the reader that matters is a `metaflow-functions`
installed somewhere else, so it gets installed here for real and runs in its own venv.
The versions come from `FIXTURE_VERSIONS`, so one list drives both directions.

Only the *caller* side is old, which is the real shape of an un-updated platform: the
runtime subprocess takes its code from the function's own package, and a new function
carries a new one. So what an old release has to survive is loading and driving, not
executing the function body.
"""

import json
import subprocess
import sys
from dataclasses import asdict

import pytest

from .conftest import FIXTURE_VERSIONS

# Runs inside the old venv; prints what it managed to read back.
READER = """
import json, sys
from metaflow_extensions.nflx.plugins.functions.core.function_spec import FunctionSpec

spec = FunctionSpec.from_json(sys.argv[1])
print(json.dumps({"name": spec.name, "uuid": spec.uuid, "function": spec.function.name}))
"""


@pytest.fixture(scope="session", params=FIXTURE_VERSIONS)
def old_reader(request, tmp_path_factory):
    """A venv with that version of metaflow-functions installed from the index."""
    version = request.param.lstrip("v")
    venv = tmp_path_factory.mktemp(f"reader-{version}")

    try:
        subprocess.run(
            [sys.executable, "-m", "venv", str(venv)], check=True, capture_output=True
        )
        subprocess.run(
            [
                str(venv / "bin" / "pip"),
                "install",
                "-q",
                "metaflow",
                "fastavro",
                "psutil",
                f"metaflow-functions=={version}",
            ],
            check=True,
            capture_output=True,
            timeout=600,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        pytest.skip(f"cannot install metaflow-functions=={version}: {e}")

    return str(venv / "bin" / "python")


def _read_with(python, reference):
    return subprocess.run(
        [python, "-c", READER, str(reference)],
        capture_output=True,
        text=True,
        env={
            "METAFLOW_FUNCTIONS_TEST_MODE": "1",
            "METAFLOW_USER": "test",
            "PATH": "/usr/bin:/bin",
        },
    )


def test_old_reader_loads_a_spec_written_today(old_reader, avro_spec, tmp_path):
    reference = tmp_path / "reference.json"
    reference.write_text(json.dumps(asdict(avro_spec)))

    result = _read_with(old_reader, reference)

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "name": avro_spec.name,
        "uuid": avro_spec.uuid,
        "function": avro_spec.function.name,
    }


def test_old_reader_rejects_an_added_field(old_reader, avro_spec, tmp_path):
    """Why the schema guard exists: the failure is in code we cannot reach or fix."""
    desc = asdict(avro_spec)
    desc["some_future_field"] = "value"
    reference = tmp_path / "reference.json"
    reference.write_text(json.dumps(desc))

    result = _read_with(old_reader, reference)

    assert result.returncode != 0
    assert "some_future_field" in result.stderr
