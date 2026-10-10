"""Offline resolver checks: constraints restrict selection, not installation."""

import json
import subprocess
import sys
from zipfile import ZipFile

import pytest


@pytest.fixture
def wheel_index(tmp_path):
    def wheel(name, version, requires=()):
        normalized = name.replace("-", "_")
        dist_info = "%s-%s.dist-info" % (normalized, version)
        path = tmp_path / ("%s-%s-py3-none-any.whl" % (normalized, version))
        with ZipFile(path, "w") as archive:
            metadata = "Metadata-Version: 2.1\nName: %s\nVersion: %s\n" % (
                name,
                version,
            )
            metadata += "".join("Requires-Dist: %s\n" % r for r in requires)
            archive.writestr(dist_info + "/METADATA", metadata + "\n")
            archive.writestr(
                dist_info + "/WHEEL",
                "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
            )
            archive.writestr(dist_info + "/RECORD", "")

    wheel("pydantic", "1.10.0")
    wheel("pydantic", "2.0.0")
    wheel("nflx-pyiceberg", "0.11.100", ["pydantic>=1"])
    wheel("nflx-pyiceberg", "0.11.102", ["pydantic>=2"])
    wheel("table-client", "1.0.0", ["nflx-pyiceberg>=0.11.100"])
    wheel("old-table-client", "1.0.0", ["nflx-pyiceberg==0.11.100"])
    return tmp_path


@pytest.mark.parametrize(
    "requirements, expected_version, successful",
    [
        (["pydantic<2"], None, True),
        (["nflx-pyiceberg"], "0.11.102", True),
        (["table-client"], "0.11.102", True),
        (["nflx-pyiceberg==0.11.100"], None, False),
        (["old-table-client"], None, False),
    ],
    ids=["unrelated", "direct", "transitive", "exact-conflict", "transitive-conflict"],
)
def test_pip_constraints_only_affect_selected_packages(
    wheel_index, requirements, expected_version, successful
):
    constraints = wheel_index / "constraints.txt"
    constraints.write_text("nflx-pyiceberg>=0.11.102\n")
    report = wheel_index / "report.json"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "--isolated",
            "install",
            "--dry-run",
            "--ignore-installed",
            "--no-index",
            "--find-links",
            str(wheel_index),
            "--constraint",
            str(constraints),
            "--report",
            str(report),
            *requirements,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert (result.returncode == 0) is successful, result.stdout + result.stderr
    if successful:
        selected = {
            p["metadata"]["name"]: p["metadata"]["version"]
            for p in json.loads(report.read_text())["install"]
        }
        assert selected.get("nflx-pyiceberg") == expected_version
        if expected_version is None:
            assert selected == {"pydantic": "1.10.0"}
    else:
        assert "ResolutionImpossible" in result.stderr
