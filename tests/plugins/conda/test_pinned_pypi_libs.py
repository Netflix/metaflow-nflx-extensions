import json
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from metaflow import FlowSpec, named_env, step
from metaflow._vendor.click.testing import CliRunner
from metaflow_extensions.netflixext.cmd.environment.environment_cmd import resolve
from metaflow_extensions.netflixext.plugins.conda.conda_environment import (
    CondaEnvironment,
)
from metaflow_extensions.netflixext.plugins.conda.env_descr import (
    CondaPackageSpecification,
    EnvID,
    EnvType,
    PypiPackageSpecification,
    ResolvedEnvironment,
)
from metaflow_extensions.netflixext.plugins.conda.envsresolver import EnvsResolver
from metaflow_extensions.netflixext.plugins.conda.utils import (
    dict_to_strlist,
    filter_user_reqs_by_markers,
    get_pinned_pypi_libs,
    get_pypi_requirement,
    merge_pypi_pins,
    CondaException,
)

CLI_MODULE = "metaflow_extensions.netflixext.cmd.environment.environment_cmd"
FLOW_MODULE = "metaflow_extensions.netflixext.plugins.conda.conda_environment"


@pytest.fixture
def conda(mocker):
    return mocker.Mock(
        virtual_packages={}, default_conda_channels=[], default_pypi_sources=[]
    )


def _base_environment(env_type, packages):
    python = CondaPackageSpecification(
        "python-3.10.21-0", "https://example.org/python-3.10.21-0.conda"
    )
    pypi = [
        PypiPackageSpecification(
            "%s-%s-py3-none-any" % (name, version),
            "https://example.org/%s-%s-py3-none-any.whl" % (name, version),
        )
        for name, version in packages.items()
    ]
    deps = {} if env_type == EnvType.PYPI_ONLY else {"conda": ["python==3.10.21"]}
    if pypi:
        deps["pypi"] = ["%s==%s" % pair for pair in packages.items()]
    return ResolvedEnvironment(
        deps, {}, {}, "linux-64", all_packages=[python, *pypi], env_type=env_type
    )


@pytest.mark.parametrize(
    "contributions, expected",
    [
        ([], {}),
        ([None, {}], {}),
        (
            [{"shared": ">=1", "one": ""}, {"shared": "<3", "two": "2"}, None],
            {"shared": ">=1,<3", "one": "", "two": "2"},
        ),
    ],
)
def test_config_hooks_merge_contributions_and_receive_target(
    contributions, expected, mocker
):
    extensions, hooks = [], []
    for pins in contributions:
        module = SimpleNamespace()
        if pins is not None:
            module.get_pinned_pypi_libs = mocker.Mock(return_value=pins)
            hooks.append(module.get_pinned_pypi_libs)
        extensions.append(SimpleNamespace(module=module))
    mocker.patch("metaflow.extension_support.get_modules", return_value=extensions)
    assert get_pinned_pypi_libs("3.10.21", "s3") == expected
    for hook in hooks:
        hook.assert_called_once_with("3.10.21", "s3")


@pytest.mark.parametrize("pins", [{}, {"native-package": ">=1"}, {"my-package": ">=2"}])
def test_only_hook_packages_are_regrouped(pins):
    unrelated = {
        "pandas@https://example.org/pandas-2.0-py3-none-any.whl": "",
        "pandas": ">=1",
    }
    user = {"My_Package[extra]": '<3; python_version >= "3.10"'}
    merged = merge_pypi_pins({**unrelated, **user}, pins)
    assert {key: merged[key] for key in unrelated} == unrelated
    if "my-package" not in pins:
        assert merged == {**unrelated, **user, **pins}
    else:
        reqs = [
            get_pypi_requirement(dep)
            for dep in dict_to_strlist(merged)
            if dep.startswith("my-package")
        ]
        assert {str(spec) for req in reqs for spec in req.specifier} == {"<3", ">=2"}
        assert set().union(*(req.extras for req in reqs)) == {"extra"}


def test_deployed_environment_id_does_not_reapply_pypi_pins(monkeypatch, mocker):
    env_id = EnvID("deployed-request", "deployed-full", "linux-64")
    monkeypatch.setattr(CondaEnvironment, "_result_for_step", {})
    monkeypatch.setenv("_METAFLOW_CONDA_ENV", json.dumps(env_id))
    hook = mocker.patch(FLOW_MODULE + ".get_pinned_pypi_libs")
    assert CondaEnvironment.get_env_id(mocker.Mock(), "start") == env_id
    hook.assert_not_called()


@pytest.mark.parametrize(
    "env_type", [EnvType.CONDA_ONLY, EnvType.PYPI_ONLY, EnvType.MIXED]
)
def test_named_environment_pypi_pins_follow_base_type(
    env_type, conda, monkeypatch, mocker
):
    class NamedEnvironmentFlow(FlowSpec):
        @named_env(name="tests/base")
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    packages = {} if env_type == EnvType.CONDA_ONLY else {"requests": "2.32.3"}
    base = _base_environment(env_type, packages)
    conda.env_id_from_alias.return_value = base.env_id
    conda.environment.return_value = base
    monkeypatch.setattr(CondaEnvironment, "_flow_req", None)
    monkeypatch.setattr(CondaEnvironment, "_result_for_step", {})
    hook = mocker.patch(
        FLOW_MODULE + ".get_pinned_pypi_libs", return_value={"example": ">=2"}
    )
    flow = NamedEnvironmentFlow(use_cli=False)
    resolved_type, _, req, resolved_base = (
        CondaEnvironment.extract_merged_reqs_for_step(
            conda, flow, "local", flow.start, override_arch="linux-64"
        )
    )
    assert resolved_type == env_type
    assert resolved_base is base
    if env_type == EnvType.CONDA_ONLY:
        assert "example" not in req.packages.get("pypi", {})
        hook.assert_not_called()
    else:
        assert req.packages["pypi"]["example"] == ">=2"
        hook.assert_called_once_with("3.10.21", "local")


@pytest.mark.parametrize("version", ["1.1.0", "1.2.1"])
def test_reused_direct_url_package_cannot_bypass_hook_pin(version, conda, mocker):
    selected = _base_environment(EnvType.MIXED, {"example_package": version})
    conda.environment.return_value = selected
    conda._datastore_type = "s3"
    module = SimpleNamespace(
        get_pinned_pypi_libs=lambda python, datastore: {"example-package": ">=1.2.0"}
    )
    mocker.patch(
        "metaflow.extension_support.get_modules",
        return_value=[SimpleNamespace(module=module)],
    )
    resolver = EnvsResolver(conda)
    resolver.add_environment(
        "linux-64",
        {
            "conda": ["python==3.10.21"],
            "pypi": [
                "example-package @ https://example.org/example_package-%s-py3-none-any.whl"
                % version
            ],
        },
        {},
        {},
        enforce_pypi_pins=True,
    )
    resolution = mocker.patch.object(resolver, "_resolve_environments")
    with (
        pytest.raises(CondaException, match="example-package>=1.2.0")
        if version == "1.1.0"
        else nullcontext()
    ):
        resolver.resolve_environments(lambda *args: None)
    resolution.assert_not_called()


@pytest.mark.parametrize(
    "option, content, skip, python, specs, extras",
    [
        ("-r", "requests>=1", False, "3.10.21", ">=2", ""),
        ("-r", "requests>=1", True, "3.10.21", "", ""),
        ("-f", "dependencies:\n  - pandas", False, "3.10.21", "", ""),
        (
            "-f",
            "dependencies:\n  - pandas\n  - pip:\n    - requests",
            False,
            "3.10.21",
            ">=2",
            "",
        ),
        ("-r", "example[extra]==1", False, "3.10.21", "==1,>=2", "extra"),
        (
            "-r",
            'example[extra]<2; python_version < "3.10"',
            False,
            "3.9.20",
            "<2,>=2",
            "extra",
        ),
        (
            "-r",
            'example[extra]<2; python_version < "3.10"',
            False,
            "3.10.21",
            ">=2",
            "",
        ),
    ],
    ids=[
        "pypi",
        "skip",
        "conda",
        "mixed",
        "exact-pin-extras",
        "active-marker",
        "inactive-marker",
    ],
)
def test_cli_hook_requirements_follow_target_and_options(
    option, content, skip, python, specs, extras, conda, tmp_path, mocker
):
    class ResolutionReached(Exception):
        pass

    requirements = tmp_path / (
        "requirements.txt" if option == "-r" else "environment.yml"
    )
    requirements.write_text(content + "\n")
    resolver = mocker.patch(CLI_MODULE + ".EnvsResolver").return_value
    resolver.add_environment.side_effect = ResolutionReached
    mocker.patch(CLI_MODULE + ".get_pinned_conda_libs", return_value={})
    hook = mocker.patch(
        CLI_MODULE + ".get_pinned_pypi_libs", return_value={"example": ">=2"}
    )
    args = ["--python", python, option, str(requirements), "--dry-run"]
    result = CliRunner().invoke(
        resolve,
        args + (["--skip-metaflow-deps"] if skip else []),
        obj=SimpleNamespace(conda=conda, datastore_type="local"),
    )
    assert isinstance(result.exception, ResolutionReached), result.output
    assert resolver.add_environment.call_args.kwargs["enforce_pypi_pins"] is (not skip)
    deps = resolver.add_environment.call_args.args[1]
    active = filter_user_reqs_by_markers(deps, "python==" + python, "linux-64")
    reqs = [get_pypi_requirement(dep) for dep in active["pypi"]]
    reqs = [req for req in reqs if req.name == "example"]
    assert {str(spec) for req in reqs for spec in req.specifier} == set(
        filter(None, specs.split(","))
    )
    assert set().union(*(req.extras for req in reqs)) == ({extras} if extras else set())
    if specs:
        hook.assert_called_once_with(python, "local")
    else:
        hook.assert_not_called()


@pytest.mark.parametrize("cached", [False, True], ids=["fresh", "cached"])
@pytest.mark.parametrize("version, succeeds", [("1.1.0", False), ("1.2.1", True)])
def test_flow_environment_cli_enforces_hook_requirements(
    cached, version, succeeds, conda, mocker
):
    from metaflow_extensions.netflixext.plugins import environment_cli

    selected = _base_environment(EnvType.MIXED, {"example_package": version})
    conda.environment.return_value = selected if cached else None
    conda._datastore_type = "local"
    mocker.patch.object(selected, "is_cached", return_value=True)
    mocker.patch.object(environment_cli, "Conda", return_value=conda)
    mocker.patch.object(environment_cli, "_compute_local_instances")
    mocker.patch.object(environment_cli, "_all_local_instances", {})
    req = SimpleNamespace(
        is_disabled=False,
        is_fetch_at_exec=False,
        from_env_name=None,
        packages_as_str={
            "conda": ["python==3.10.21"],
            "pypi": [
                "example-package @ https://example.org/example_package-%s-py3-none-any.whl"
                % version
            ],
        },
        sources={},
    )
    mocker.patch.object(
        environment_cli.CondaEnvironment,
        "extract_merged_reqs_for_step",
        return_value=(EnvType.MIXED, "linux-64", req, None),
    )
    mocker.patch(
        "metaflow_extensions.netflixext.plugins.conda.envsresolver.get_pinned_pypi_libs",
        return_value={"example-package": ">=1.2.0"},
    )

    def resolve(resolver, echo, env_ids):
        for env_id in env_ids:
            resolver._requested_envs[env_id]["resolved"] = selected

    resolution = mocker.patch.object(
        EnvsResolver, "_resolve_environments", autospec=True, side_effect=resolve
    )
    validation = mocker.spy(EnvsResolver, "resolve_environments")
    result = CliRunner().invoke(
        environment_cli.resolve,
        ["--dry-run", "start"],
        obj=SimpleNamespace(
            echo=mocker.Mock(),
            flow_datastore=SimpleNamespace(TYPE="local"),
            flow=[SimpleNamespace(name="start")],
            is_quiet=False,
        ),
    )
    assert validation.call_count == 1
    assert resolution.call_count == (0 if cached else 1)
    if succeeds:
        assert result.exit_code == 0, result.output
    else:
        assert isinstance(result.exception, CondaException), result.output
        assert "example-package>=1.2.0" in str(result.exception)
