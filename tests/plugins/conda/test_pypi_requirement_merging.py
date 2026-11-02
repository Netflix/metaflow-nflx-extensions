from pathlib import Path
import json
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock
from zipfile import ZipFile

import metaflow  # noqa: F401 -- initialize plugins before importing utilities
import pytest
import tomli

from metaflow._vendor.click.testing import CliRunner
from metaflow._vendor.packaging.specifiers import SpecifierSet
from metaflow_extensions.netflixext.plugins.conda.env_descr import EnvType
from metaflow_extensions.netflixext.plugins.conda.resolvers.conda_lock_resolver import (
    CondaLockResolver,
)
from metaflow_extensions.netflixext.plugins.conda.utils import (
    dict_to_strlist,
    filter_user_reqs_by_markers,
    get_pypi_requirement,
    merge_dep_dicts,
    merge_pypi_deps,
)


def _active_requirements(deps, python="3.10.21", arch="linux-64"):
    return [
        get_pypi_requirement(dep)
        for dep in filter_user_reqs_by_markers(
            {"pypi": dict_to_strlist(deps)}, "python==" + python, arch
        )["pypi"]
    ]


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "python, expected", [("3.9.20", {">=2.21.0"}), ("3.10.21", {">=2.21.0", ">=2.30"})]
)
def test_default_requirement_stays_unconditional(reverse, python, expected):
    user = {"requests": '>=2.30; python_version >= "3.10"'}
    default = {"requests": ">=2.21.0"}
    deps = merge_pypi_deps(*((default, user) if reverse else (user, default)))
    active = _active_requirements(deps, python)
    assert {req.name for req in active} == {"requests"}
    assert {str(spec) for req in active for spec in req.specifier} == expected


def test_matching_markers_combine_versions_and_extras():
    deps = merge_pypi_deps(
        {"My_Package[first]": '>=1; python_version >= "3.10"'},
        {"my-package[second]": '<3; python_version >= "3.10"'},
    )
    (req,) = _active_requirements(deps)
    assert req.name == "my-package"
    assert req.extras == {"first", "second"}
    assert req.specifier == SpecifierSet(">=1,<3")


def test_marked_exact_version_keeps_its_equality_constraint():
    deps = merge_pypi_deps(
        {"example": '2.0; python_version >= "3.10"'}, {"example": ">=1"}
    )
    active = _active_requirements(deps)
    assert {str(spec) for req in active for spec in req.specifier} == {"==2.0", ">=1"}


@pytest.mark.parametrize(
    "arch, expected", [("linux-64", {">=2", "<3"}), ("osx-arm64", {">=2"})]
)
def test_platform_marker_uses_configured_target_instead_of_host(
    arch, expected, monkeypatch
):
    monkeypatch.setattr(
        "metaflow_extensions.netflixext.plugins.conda.utils.CONDA_SYS_MARKERS",
        {
            "linux-64": {"sys_platform": "linux"},
            "osx-arm64": {"sys_platform": "darwin"},
        },
    )
    deps = merge_pypi_deps(
        {"example": '<3; sys_platform == "linux"'}, {"example": ">=2"}
    )
    active = _active_requirements(deps, arch=arch)
    assert {str(spec) for req in active for spec in req.specifier} == expected


def test_active_conflicting_requirements_are_preserved():
    deps = merge_pypi_deps(
        {"example": '<2; python_version < "3.10"'}, {"example": ">=2"}
    )
    active = _active_requirements(deps, python="3.9.20")
    combined = SpecifierSet(",".join(str(req.specifier) for req in active))
    assert not combined.contains("1.9")
    assert not combined.contains("2.0")
    assert {str(spec) for req in active for spec in req.specifier} == {"<2", ">=2"}


def test_marked_direct_reference_keeps_url_extras_and_condition():
    reference = 'example[extra] @ https://example.org/example-2.0-py3-none-any.whl ; python_version >= "3.10"'
    deps = merge_pypi_deps({reference: ""}, {"example": ">=1"})
    active = _active_requirements(deps)
    assert len(active) == 2
    assert any(req.url and req.extras == {"extra"} for req in active)
    assert not any(req.url for req in _active_requirements(deps, python="3.9.20"))


def test_unmarked_requirements_keep_existing_representation():
    user = {"My_Package[extra]": "1.0", "requests": ">=2.30"}
    defaults = {"requests": ">=2.21.0", "other": ""}
    assert merge_pypi_deps(user, defaults) == merge_dep_dicts(user, defaults)


def test_unrelated_requirements_keep_existing_representation_with_marked_package():
    user = {
        "Other_Package[extra]": "1.0",
        "unconstrained": "*",
        "example": '>=1; python_version >= "3.10"',
    }
    deps = merge_pypi_deps(user, {"example": ">=0.5"})
    assert deps["Other_Package[extra]"] == "1.0"
    assert deps["unconstrained"] == ""


def test_marked_hyphenated_package_is_not_parsed_as_conda_build():
    deps = merge_pypi_deps(
        {"my-package": '; python_version >= "3.10"'}, {"my-package": ">=1"}
    )
    assert {req.name for req in _active_requirements(deps)} == {"my-package"}


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
    active = _active_requirements(deps, python)
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
            *(str(req) for req in active),
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


class ResolutionReached(Exception):
    pass


@pytest.mark.parametrize("python", ["3.9.20", "3.10.21"])
@pytest.mark.parametrize("user_version", ["<2", "<3,==2.0"])
def test_conda_lock_receives_every_active_constraint_and_extra(python, user_version):
    conda = Mock(
        default_pypi_sources=["https://pypi.netflix.net/simple"],
        default_conda_channels=[],
        conda_executable_type="micromamba",
        virtual_packages={},
    )
    captured = {}

    def capture_input(*args, cwd, **kwargs):
        captured.update(tomli.loads((Path(cwd) / "pyproject.toml").read_text()))
        raise ResolutionReached

    conda.call_binary.side_effect = capture_input
    deps = merge_pypi_deps(
        {"example[first]": user_version + '; python_version < "3.10"'},
        {"example[second]": ">=2"},
    )
    with pytest.raises(ResolutionReached):
        CondaLockResolver(conda).resolve(
            env_type=EnvType.MIXED,
            python_version_requested="python==" + python,
            deps={"pypi": dict_to_strlist(deps)},
            sources={},
            extras={},
            architecture="linux-64",
        )
    entry = captured["tool"]["poetry"]["dependencies"]["example"]
    assert SpecifierSet(entry["version"]) == SpecifierSet(
        ">=2," + user_version if python.startswith("3.9") else ">=2"
    )
    assert set(entry["extras"]) == (
        {"first", "second"} if python.startswith("3.9") else {"second"}
    )


def test_flow_default_dependency_merge_preserves_user_marker(monkeypatch, mocker):
    from metaflow import FlowSpec, pypi, step
    from metaflow_extensions.netflixext.plugins.conda.conda_environment import (
        CondaEnvironment,
    )

    class MarkedDependencyFlow(FlowSpec):
        @pypi(
            python="3.10.21", packages={"requests": '>=2.30; python_version >= "3.10"'}
        )
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    monkeypatch.setattr(CondaEnvironment, "_flow_req", None)
    monkeypatch.setattr(CondaEnvironment, "_result_for_step", {})
    mocker.patch(
        "metaflow_extensions.netflixext.plugins.conda.conda_environment.get_pinned_conda_libs",
        return_value={"requests": ">=2.21.0"},
    )
    conda = Mock(
        virtual_packages={}, default_conda_channels=[], default_pypi_sources=[]
    )
    flow = MarkedDependencyFlow(use_cli=False)
    env_type, _, req, _ = CondaEnvironment.extract_merged_reqs_for_step(
        conda, flow, "local", flow.start, override_arch="linux-64"
    )
    assert env_type == EnvType.PYPI_ONLY
    active = _active_requirements(req.packages["pypi"])
    assert {str(spec) for req in active for spec in req.specifier} == {
        ">=2.21.0",
        ">=2.30",
    }


def test_cli_default_dependency_merge_preserves_user_marker(tmp_path, mocker):
    from metaflow_extensions.netflixext.cmd.environment.environment_cmd import resolve

    module = "metaflow_extensions.netflixext.cmd.environment.environment_cmd"
    requirements = tmp_path / "requirements.txt"
    requirements.write_text('requests>=2.30; python_version >= "3.10"\n')
    resolver = mocker.patch(module + ".EnvsResolver").return_value
    resolver.add_environment.side_effect = ResolutionReached
    mocker.patch(
        module + ".get_pinned_conda_libs", return_value={"requests": ">=2.21.0"}
    )
    conda = Mock(
        virtual_packages={}, default_conda_channels=[], default_pypi_sources=[]
    )
    result = CliRunner().invoke(
        resolve,
        ["--python", "3.10.21", "-r", str(requirements), "--dry-run"],
        obj=SimpleNamespace(conda=conda, datastore_type="local"),
    )
    assert isinstance(result.exception, ResolutionReached), result.output
    deps = resolver.add_environment.call_args.args[1]["pypi"]
    active = [get_pypi_requirement(dep) for dep in deps]
    assert {str(spec) for req in active for spec in req.specifier} == {
        ">=2.21.0",
        ">=2.30",
    }
