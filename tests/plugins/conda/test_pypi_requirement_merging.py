from pathlib import Path
from unittest.mock import Mock

import metaflow  # noqa: F401 -- initialize plugins before importing utilities
import pytest
import tomli

from metaflow._vendor.packaging.specifiers import SpecifierSet
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow_extensions.netflixext.plugins.conda.conda_common_decorator import (
    StepRequirement,
)
from metaflow_extensions.netflixext.plugins.conda.env_descr import EnvType
from metaflow_extensions.netflixext.plugins.conda.parsers import (
    parse_req_value,
    parse_toml_value,
)
from metaflow_extensions.netflixext.plugins.conda.resolvers.conda_lock_resolver import (
    CondaLockResolver,
)
from metaflow_extensions.netflixext.plugins.conda.utils import (
    dict_to_strlist,
    filter_user_reqs_by_markers,
    get_pypi_requirement,
    merge_pypi_dep_dicts,
)


def _active_requirements(deps, python="3.10.14", arch="linux-64"):
    return [
        get_pypi_requirement(dep)
        for dep in filter_user_reqs_by_markers(
            {"pypi": dict_to_strlist(deps)}, "python==" + python, arch
        )["pypi"]
    ]


@pytest.mark.parametrize("user_version", ["0.11.101", ">=0.11.101"])
def test_pypi_pins_preserve_extras_and_all_version_constraints(user_version):
    merged = merge_pypi_dep_dicts(
        {"Nflx_PyIceberg[pyarrow]": user_version},
        {"nflx-pyiceberg[pandas]": ">=0.11.102"},
    )
    (req,) = _active_requirements(merged)
    assert req.name == "nflx-pyiceberg"
    assert req.extras == {"pyarrow", "pandas"}
    assert not req.specifier.contains("0.11.101")
    assert req.specifier.contains("0.11.102") is (user_version != "0.11.101")


@pytest.mark.parametrize(
    "python, expected",
    [("3.9.20", {"<0.11.102", ">=0.11.102"}), ("3.10.14", {">=0.11.102"})],
)
def test_unconditional_pin_remains_active_with_conditional_user_requirement(
    python, expected
):
    merged = merge_pypi_dep_dicts(
        {"nflx-pyiceberg[pyarrow]": '<0.11.102; python_version < "3.10"'},
        {"nflx-pyiceberg": ">=0.11.102"},
    )
    (req,) = _active_requirements(merged, python)
    assert {str(s) for s in req.specifier} == expected
    assert req.extras == ({"pyarrow"} if python.startswith("3.9") else set())


def test_marked_requirement_merges_constraints_before_the_marker():
    merged = merge_pypi_dep_dicts(
        {"nflx-pyiceberg": '>=0.11.102; python_version >= "3.10"'},
        {"nflx-pyiceberg": ">=0.11.102"},
    )
    (req,) = _active_requirements(merged)
    assert str(req.specifier) == ">=0.11.102"


@pytest.mark.parametrize(
    "arch, expected", [("linux-64", "<2,>=1"), ("osx-arm64", ">=1")]
)
def test_requirement_markers_use_target_platform(arch, expected):
    merged = merge_pypi_dep_dicts(
        {"example": '<2; sys_platform == "linux"'}, {"example": ">=1"}
    )
    (req,) = _active_requirements(merged, arch=arch)
    assert req.specifier == SpecifierSet(expected)


@pytest.mark.parametrize("file_type", ["requirements", "pyproject"])
def test_parsers_preserve_repeated_conditional_requirements(file_type):
    requirements = [
        'example<2; python_version < "3.10"',
        'example>=2; python_version >= "3.10"',
    ]
    deps = {}
    if file_type == "requirements":
        parse_req_value("\n".join(requirements), {}, {}, deps, {}, {})
    else:
        import json

        content = "[project]\ndependencies = " + json.dumps(requirements)
        parse_toml_value(content, {}, {}, deps, {}, {})
    assert str(_active_requirements(deps, "3.9.20")[0].specifier) == "<2"
    assert str(_active_requirements(deps, "3.10.14")[0].specifier) == ">=2"


def test_step_requirement_merge_preserves_conditional_constraints():
    base = StepRequirement()
    base._packages = {"pypi": {"example": '<2; python_version < "3.10"'}}
    incoming = StepRequirement()
    incoming._packages = {"pypi": {"example": ">=1"}}
    base.merge_update(incoming)
    assert str(_active_requirements(base.packages["pypi"])[0].specifier) == ">=1"
    assert _active_requirements(base.packages["pypi"], "3.9.20")[
        0
    ].specifier == SpecifierSet(">=1,<2")


def test_conflicting_direct_urls_are_rejected():
    with pytest.raises(InvalidEnvironmentException, match="Conflicting URLs"):
        merge_pypi_dep_dicts(
            {"example @ https://example.com/one.whl": ""},
            {"example @ https://example.com/two.whl": ""},
        )


def test_conda_lock_input_preserves_extras_and_conflicting_pins():
    conda = Mock(
        default_pypi_sources=["https://pypi.netflix.net/simple"],
        default_conda_channels=[],
        conda_executable_type="micromamba",
        virtual_packages={},
    )

    class SolverReached(Exception):
        pass

    captured = {}

    def capture_input(*args, cwd, **kwargs):
        captured.update(tomli.loads((Path(cwd) / "pyproject.toml").read_text()))
        raise SolverReached

    conda.call_binary.side_effect = capture_input
    with pytest.raises(SolverReached):
        CondaLockResolver(conda).resolve(
            env_type=EnvType.MIXED,
            python_version_requested="python==3.10.14",
            deps={
                "pypi": [
                    "nflx-pyiceberg[pyarrow]==0.11.101",
                    "nflx-pyiceberg==>=0.11.102",
                ]
            },
            sources={},
            extras={},
            architecture="linux-64",
        )
    entry = captured["tool"]["poetry"]["dependencies"]["nflx-pyiceberg"]
    assert entry["extras"] == ["pyarrow"]
    assert SpecifierSet(entry["version"]) == SpecifierSet("==0.11.101,>=0.11.102")
