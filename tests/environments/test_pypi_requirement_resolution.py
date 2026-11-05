"""Offline resolution tests using local wheels and the real pip resolver."""

import json
import subprocess
import sys
from zipfile import ZipFile
from types import SimpleNamespace

import metaflow  # noqa: F401 -- initialize plugins before importing utilities
import pytest

from metaflow_extensions.netflixext.plugins.conda.utils import (
    dict_to_strlist,
    filter_user_reqs_by_markers,
    get_pypi_requirement,
    merge_pypi_deps,
    merge_pypi_pins,
)


@pytest.fixture
def wheelhouse(tmp_path):
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
    return tmp_path


def _resolve_with_pip(wheelhouse, active):
    report = wheelhouse / "report.json"
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
            str(wheelhouse),
            "--report",
            str(report),
            *(str(get_pypi_requirement(dep)) for dep in active),
        ],
        text=True,
        capture_output=True,
    )
    return result, report


@pytest.mark.parametrize("python, succeeds", [("3.9.20", False), ("3.10.21", True)])
def test_pip_respects_all_active_constraints_without_network(
    wheelhouse, python, succeeds
):
    deps = merge_pypi_deps(
        {"marker-merge-example": '<2; python_version < "3.10"'},
        {"marker-merge-example": ">=2"},
    )
    active = filter_user_reqs_by_markers(
        {"pypi": dict_to_strlist(deps)}, "python==" + python, "linux-64"
    )["pypi"]
    result, report = _resolve_with_pip(wheelhouse, active)
    if succeeds:
        assert result.returncode == 0, result.stdout + result.stderr
        assert (
            json.loads(report.read_text())["install"][0]["metadata"]["version"] == "2.5"
        )
    else:
        assert result.returncode != 0
        assert "ResolutionImpossible" in result.stderr


@pytest.mark.parametrize("user_version, succeeds", [("", True), ("1.5", False)])
def test_config_hook_reaches_flow_dependency_resolution(
    wheelhouse, user_version, succeeds, mocker, monkeypatch
):
    from metaflow import FlowSpec, pypi, step
    from metaflow_extensions.netflixext.plugins.conda.conda_environment import (
        CondaEnvironment,
    )

    module = SimpleNamespace(
        get_pinned_pypi_libs=lambda python, datastore: {"marker-merge-example": ">=2"}
    )
    mocker.patch(
        "metaflow.extension_support.get_modules",
        return_value=[SimpleNamespace(module=module)],
    )
    mocker.patch(
        "metaflow_extensions.netflixext.plugins.conda.conda_environment.get_pinned_conda_libs",
        return_value={},
    )
    monkeypatch.setattr(CondaEnvironment, "_flow_req", None)
    monkeypatch.setattr(CondaEnvironment, "_result_for_step", {})

    class HookDependencyFlow(FlowSpec):
        @pypi(python="3.10.21", packages={"marker-merge-example": user_version})
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    flow = HookDependencyFlow(use_cli=False)
    _, _, req, _ = CondaEnvironment.extract_merged_reqs_for_step(
        SimpleNamespace(
            virtual_packages={}, default_conda_channels=[], default_pypi_sources=[]
        ),
        flow,
        "local",
        flow.start,
        override_arch="linux-64",
    )
    result, report = _resolve_with_pip(wheelhouse, req.packages_as_str["pypi"])
    if succeeds:
        assert result.returncode == 0, result.stdout + result.stderr
        assert (
            json.loads(report.read_text())["install"][0]["metadata"]["version"] == "2.5"
        )
    else:
        assert result.returncode != 0
        assert (
            "No matching distribution found for marker-merge-example" in result.stderr
        )


@pytest.mark.parametrize(
    "user_version, marker, succeeds",
    [
        ("1.5", 'python_version >= "3.10"', False),
        ("2.5", 'python_version >= "3.10"', True),
        ("1.5", 'python_version < "3.10"', True),
    ],
    ids=["active-conflict", "active-compatible", "inactive-constraint"],
)
def test_named_environment_retains_active_user_and_hook_constraints(
    wheelhouse, user_version, marker, succeeds
):
    from metaflow_extensions.netflixext.plugins.conda.env_descr import (
        CondaPackageSpecification,
        EnvType,
        PypiPackageSpecification,
        ResolvedEnvironment,
    )
    from metaflow_extensions.netflixext.plugins.conda.envsresolver import EnvsResolver

    base = ResolvedEnvironment(
        {"pypi": ["marker-merge-example==2.5"]},
        {},
        {},
        "linux-64",
        all_packages=[
            CondaPackageSpecification(
                "python-3.10.21-0", "https://example.org/python-3.10.21-0.conda"
            ),
            PypiPackageSpecification(
                "marker_merge_example-2.5-py3-none-any",
                (wheelhouse / "marker_merge_example-2.5-py3-none-any.whl").as_uri(),
            ),
        ],
        env_type=EnvType.PYPI_ONLY,
    )
    incoming = merge_pypi_pins(
        {"marker-merge-example": "%s; %s" % (user_version, marker)},
        {"marker-merge-example": "2.5"},
    )
    _, _, _, _, deps, _, _ = EnvsResolver.extract_info_from_base(
        SimpleNamespace(default_conda_channels=[], default_pypi_sources=[]),
        base,
        {"pypi": dict_to_strlist(incoming)},
        {},
        {},
        "linux-64",
    )
    result, report = _resolve_with_pip(wheelhouse, deps["pypi"])
    if succeeds:
        assert result.returncode == 0, result.stdout + result.stderr
        assert (
            json.loads(report.read_text())["install"][0]["metadata"]["version"] == "2.5"
        )
    else:
        assert result.returncode != 0, "The user's active constraint was discarded"
        assert (
            "No matching distribution found for marker-merge-example" in result.stderr
        )
