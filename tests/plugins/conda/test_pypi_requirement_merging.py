from pathlib import Path
from types import SimpleNamespace

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


class ResolutionReached(Exception):
    pass


@pytest.fixture
def conda(mocker):
    return mocker.Mock(
        default_pypi_sources=["https://example.org/simple"],
        default_conda_channels=[],
        conda_executable_type="micromamba",
        virtual_packages={},
    )


@pytest.fixture(
    params=[
        ("requests", '>=2.30; python_version >= "3.10"', set()),
        ("requests[socks]", ">=2.30", {"socks"}),
    ],
    ids=["marker", "extras"],
)
def default_requirement_case(request, mocker):
    for module in (
        "metaflow_extensions.netflixext.plugins.conda.conda_environment",
        "metaflow_extensions.netflixext.cmd.environment.environment_cmd",
    ):
        mocker.patch(
            module + ".get_pinned_conda_libs", return_value={"requests": ">=2.21.0"}
        )
    return request.param


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


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "user_package, default_package, marked, expected_extras",
    [
        ("requests[socks]", "requests", False, {"socks"}),
        ("My_Package[first]", "my-package[second]", False, {"first", "second"}),
        ("My_Package", "my-package", False, set()),
        ("My_Package[first]", "my-package[second]", True, {"first", "second"}),
    ],
    ids=["user-extras", "combined-extras", "normalized-name", "matching-markers"],
)
def test_matching_requirements_combine_constraints_and_extras(
    reverse, user_package, default_package, marked, expected_extras
):
    marker = '; python_version >= "3.10"' if marked else ""
    user = {user_package: ">=1" + marker}
    default = {default_package: "<3" + marker}
    deps = merge_pypi_deps(*((default, user) if reverse else (user, default)))
    (req,) = _active_requirements(deps)
    assert req.name == default_package.split("[")[0]
    assert req.extras == expected_extras
    assert req.specifier == SpecifierSet(">=1,<3")


@pytest.mark.parametrize(
    "pin, marked, extras",
    [
        ("1.0", False, {"extra"}),
        ("2.0", True, set()),
    ],
    ids=["conflicting-extras", "marked-pin"],
)
def test_exact_pins_survive_requirement_merging(pin, marked, extras):
    package = "example[extra]" if extras else "example"
    marker = '; python_version >= "3.10"' if marked else ""
    deps = merge_pypi_deps({package: pin + marker}, {"example": ">=2"})
    active = _active_requirements(deps)
    assert {str(spec) for req in active for spec in req.specifier} == {
        "==" + pin,
        ">=2",
    }
    assert set().union(*(req.extras for req in active)) == extras


@pytest.mark.parametrize(
    "python, expected",
    [
        ("3.9.20", {"<2": {"first"}}),
        ("3.10.21", {"<2": {"first"}, ">=1": {"second"}}),
        ("3.11.0", {">=1": {"second"}}),
    ],
)
def test_distinct_markers_keep_their_own_constraints_and_extras(python, expected):
    deps = merge_pypi_deps(
        {"example[first]": '<2; python_version < "3.11"'},
        {"example[second]": '>=1; python_version >= "3.10"'},
    )
    assert {
        str(req.specifier): req.extras for req in _active_requirements(deps, python)
    } == expected


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


def test_marked_direct_reference_keeps_url_extras_and_condition():
    reference = 'example[extra] @ https://example.org/example-2.0-py3-none-any.whl ; python_version >= "3.10"'
    deps = merge_pypi_deps({reference: ""}, {"example": ">=1"})
    active = _active_requirements(deps)
    assert len(active) == 2
    assert any(req.url and req.extras == {"extra"} for req in active)
    assert not any(req.url for req in _active_requirements(deps, python="3.9.20"))


@pytest.mark.parametrize("marked", [False, True])
def test_unaffected_requirements_keep_existing_representation(marked):
    user = {"Other_Package[extra]": "1.0", "unconstrained": "*", "requests": ">=2.30"}
    defaults = {"requests": ">=2.21.0", "other": ""}
    expected = merge_dep_dicts(user, defaults)
    if marked:
        user["example"] = '>=1; python_version >= "3.10"'
        defaults["example"] = ">=0.5"
    deps = merge_pypi_deps(user, defaults)
    assert {key: deps[key] for key in expected} == expected
    if not marked:
        assert deps == expected


def test_marked_hyphenated_package_is_not_parsed_as_conda_build():
    deps = merge_pypi_deps(
        {"my-package": '; python_version >= "3.10"'}, {"my-package": ">=1"}
    )
    assert {req.name for req in _active_requirements(deps)} == {"my-package"}


@pytest.mark.parametrize("input_mode", ["separate", "reversed", "merged"])
@pytest.mark.parametrize(
    "user_version, expected_version, expected_extras",
    [
        pytest.param("<2", ">=2,<2", {"first", "second"}, id="range"),
        pytest.param(
            "<3,==2.0", ">=2,<3,==2.0", {"first", "second"}, id="compound-pin"
        ),
        pytest.param(
            '<2; python_version < "3.11"',
            ">=2,<2",
            {"first", "second"},
            id="active-marker",
        ),
        pytest.param(
            '<2; python_version < "3.10"', ">=2", {"second"}, id="inactive-marker"
        ),
    ],
)
def test_conda_lock_receives_every_active_constraint_and_extra(
    conda, input_mode, user_version, expected_version, expected_extras
):
    captured = {}

    def capture_input(*args, cwd, **kwargs):
        captured.update(tomli.loads((Path(cwd) / "pyproject.toml").read_text()))
        raise ResolutionReached

    conda.call_binary.side_effect = capture_input
    user = {"example[first]": user_version}
    default = {"example[second]": ">=2"}
    inputs = (default, user) if input_mode == "reversed" else (user, default)
    if input_mode == "merged":
        deps = dict_to_strlist(merge_pypi_deps(*inputs))
    else:
        deps = [dep for requirement in inputs for dep in dict_to_strlist(requirement)]
    with pytest.raises(ResolutionReached):
        CondaLockResolver(conda).resolve(
            env_type=EnvType.MIXED,
            python_version_requested="python==3.10.21",
            deps={"pypi": deps},
            sources={},
            extras={},
            architecture="linux-64",
        )
    entry = captured["tool"]["poetry"]["dependencies"]["example"]
    assert SpecifierSet(entry["version"]) == SpecifierSet(expected_version)
    assert set(entry["extras"]) == expected_extras


def test_flow_default_dependency_merge_preserves_user_requirements(
    monkeypatch, conda, default_requirement_case
):
    from metaflow import FlowSpec, pypi, step
    from metaflow_extensions.netflixext.plugins.conda.conda_environment import (
        CondaEnvironment,
    )

    name, version, expected_extras = default_requirement_case

    class DefaultDependencyFlow(FlowSpec):
        @pypi(python="3.10.21", packages={name: version})
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    monkeypatch.setattr(CondaEnvironment, "_flow_req", None)
    monkeypatch.setattr(CondaEnvironment, "_result_for_step", {})
    flow = DefaultDependencyFlow(use_cli=False)
    env_type, _, req, _ = CondaEnvironment.extract_merged_reqs_for_step(
        conda, flow, "local", flow.start, override_arch="linux-64"
    )
    assert env_type == EnvType.PYPI_ONLY
    active = _active_requirements(req.packages["pypi"])
    assert {str(spec) for req in active for spec in req.specifier} == {
        ">=2.21.0",
        ">=2.30",
    }
    assert set().union(*(req.extras for req in active)) == expected_extras


def test_cli_default_dependency_merge_preserves_user_requirements(
    tmp_path, mocker, conda, default_requirement_case
):
    from metaflow_extensions.netflixext.cmd.environment.environment_cmd import resolve

    module = "metaflow_extensions.netflixext.cmd.environment.environment_cmd"
    name, version, expected_extras = default_requirement_case
    requirements = tmp_path / "requirements.txt"
    requirements.write_text(name + version + "\n")
    resolver = mocker.patch(module + ".EnvsResolver").return_value
    resolver.add_environment.side_effect = ResolutionReached
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
    assert set().union(*(req.extras for req in active)) == expected_extras
