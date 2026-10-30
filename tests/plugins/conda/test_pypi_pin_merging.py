from pathlib import Path
from unittest.mock import Mock

import metaflow  # noqa: F401 -- initialize plugins before importing utilities
import pytest
import tomli

from metaflow._vendor.packaging.specifiers import SpecifierSet
from metaflow_extensions.netflixext.plugins.conda.env_descr import EnvType
from metaflow_extensions.netflixext.plugins.conda.resolvers.conda_lock_resolver import (
    CondaLockResolver,
)
from metaflow_extensions.netflixext.plugins.conda.utils import (
    dict_to_strlist,
    filter_user_reqs_by_markers,
    get_pypi_requirement,
    merge_pypi_pins,
)


def _active_requirements(deps, python="3.10.14"):
    return [
        get_pypi_requirement(dep)
        for dep in filter_user_reqs_by_markers(
            {"pypi": dict_to_strlist(deps)}, "python==" + python, "linux-64"
        )["pypi"]
    ]


@pytest.mark.parametrize("user_version", ["0.11.101", ">=0.11.101"])
def test_hook_pins_preserve_extras_and_all_version_constraints(user_version):
    merged = merge_pypi_pins(
        {"Nflx_PyIceberg[pyarrow]": user_version},
        {"nflx-pyiceberg[pandas]": ">=0.11.102"},
    )
    (req,) = _active_requirements(merged)
    assert req.name == "nflx-pyiceberg"
    assert req.extras == {"pyarrow", "pandas"}
    assert not req.specifier.contains("0.11.101")
    assert req.specifier.contains("0.11.102") is (user_version != "0.11.101")


@pytest.mark.parametrize("pins", [{}, {"native-package": ">=1"}])
def test_hook_pins_leave_unrelated_requirements_unchanged(pins):
    requirements = {
        "My_Package[EXTRA]": ">=1; python_version >= '3.10'",
        "pandas@https://example.com/pandas-2.2.3-py3-none-any.whl": "",
        "pandas": ">=0.24.0",
    }
    merged = merge_pypi_pins(requirements, pins)
    assert {key: merged[key] for key in requirements} == requirements
    assert set(merged) == set(requirements) | set(pins)


def test_hook_version_pin_preserves_user_direct_reference():
    merged = merge_pypi_pins(
        {"example[extra] @ https://example.com/example-2.0-py3-none-any.whl": ""},
        {"example": ">=1"},
    )
    reqs = _active_requirements(merged)
    assert len(reqs) == 2
    assert any(req.url and req.extras == {"extra"} for req in reqs)
    assert any(req.specifier == SpecifierSet(">=1") for req in reqs)


@pytest.mark.parametrize(
    "python, expected",
    [("3.9.20", {"<2", ">=1"}), ("3.10.14", {">=1"})],
)
def test_hook_pin_remains_unconditional_with_marked_user_requirement(python, expected):
    merged = merge_pypi_pins(
        {"example[extra]": '<2; python_version < "3.10"'},
        {"example": ">=1"},
    )
    active = _active_requirements(merged, python)
    assert {str(spec) for req in active for spec in req.specifier} == expected
    assert set().union(*(req.extras for req in active)) == (
        {"extra"} if python.startswith("3.9") else set()
    )


def test_hyphenated_unversioned_hook_requirement_is_not_conda_parsed():
    req = get_pypi_requirement("nflx-python-eureq")
    assert req.name == "nflx-python-eureq"
    assert not req.specifier


def test_hook_pins_reach_conda_lock_with_extras_and_conflicting_constraints():
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
    requirements = merge_pypi_pins(
        {"nflx-pyiceberg[pyarrow]": "0.11.101"},
        {"nflx-pyiceberg": ">=0.11.102"},
    )
    with pytest.raises(SolverReached):
        CondaLockResolver(conda).resolve(
            env_type=EnvType.MIXED,
            python_version_requested="python==3.10.14",
            deps={"pypi": dict_to_strlist(requirements)},
            sources={},
            extras={},
            architecture="linux-64",
        )
    entry = captured["tool"]["poetry"]["dependencies"]["nflx-pyiceberg"]
    assert entry["extras"] == ["pyarrow"]
    assert SpecifierSet(entry["version"]) == SpecifierSet("==0.11.101,>=0.11.102")
