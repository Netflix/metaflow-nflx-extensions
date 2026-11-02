"""Offline resolution tests using local wheels and the real pip resolver."""

import json
import subprocess
import sys
from zipfile import ZipFile

import metaflow  # noqa: F401 -- initialize plugins before importing utilities
import pytest

from metaflow_extensions.netflixext.plugins.conda.utils import (
    dict_to_strlist,
    filter_user_reqs_by_markers,
    get_pypi_requirement,
    merge_pypi_deps,
)


@pytest.mark.parametrize("python, succeeds", [("3.9.20", False), ("3.10.21", True)])
def test_pip_respects_all_active_constraints_without_network(
    tmp_path, python, succeeds
):
    for version in ("1.5", "2.5"):
        dist = "marker_merge_example-%s.dist-info" % version
        wheel = tmp_path / ("marker_merge_example-%s-py3-none-any.whl" % version)
        with ZipFile(wheel, "w") as archive:
            archive.writestr(
                dist + "/METADATA",
                "Metadata-Version: 2.1\nName: marker-merge-example\nVersion: %s\n"
                % version,
            )
            archive.writestr(
                dist + "/WHEEL",
                "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
            )
            archive.writestr(dist + "/RECORD", "")
    deps = merge_pypi_deps(
        {"marker-merge-example": '<2; python_version < "3.10"'},
        {"marker-merge-example": ">=2"},
    )
    active = filter_user_reqs_by_markers(
        {"pypi": dict_to_strlist(deps)}, "python==" + python, "linux-64"
    )["pypi"]
    report = tmp_path / "report.json"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "--isolated",
            "--disable-pip-version-check",
            "install",
            "--dry-run",
            "--ignore-installed",
            "--no-index",
            "--no-deps",
            "--find-links",
            str(tmp_path),
            "--report",
            str(report),
            *(str(get_pypi_requirement(dep)) for dep in active),
        ],
        text=True,
        capture_output=True,
    )
    if succeeds:
        assert result.returncode == 0, result.stdout + result.stderr
        assert (
            json.loads(report.read_text())["install"][0]["metadata"]["version"] == "2.5"
        )
    else:
        assert result.returncode != 0
        assert "ResolutionImpossible" in result.stderr
