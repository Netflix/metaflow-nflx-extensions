from types import SimpleNamespace
import pytest
import metaflow  # noqa: F401 -- initialize plugins before importing their utilities

from metaflow_extensions.netflixext.plugins.conda.utils import (
    get_pypi_constraints,
    pypi_constraint_extras,
    pypi_constraints_satisfied,
    validate_pypi_constraints,
    constrain_pypi_deps,
    CondaException,
)
from metaflow_extensions.netflixext.plugins.conda.env_descr import (
    EnvID,
    EnvType,
    ResolvedEnvironment,
)
from metaflow_extensions.netflixext.plugins.conda.envsresolver import EnvsResolver


def _config_extension(hook=None):
    module = SimpleNamespace()
    if hook is not None:
        module.get_pypi_constraints = hook
    return SimpleNamespace(module=module)


def test_pypi_constraints_merge_all_config_extensions(mocker):
    extensions = [
        _config_extension(lambda python, datastore: {"shared": ">=1", "one": ""}),
        _config_extension(lambda python, datastore: {"shared": "<3", "two": "2"}),
        _config_extension(),
    ]

    mocker.patch("metaflow.extension_support.get_modules", return_value=extensions)
    assert get_pypi_constraints("3.11", "s3") == {
        "shared": ">=1,<3",
        "one": "",
        "two": "2",
    }


def test_pypi_constraints_pass_environment_to_hooks(mocker):
    hook_calls = []

    def hook(python_version, datastore_type):
        hook_calls.append((python_version, datastore_type))
        return {"nflx-pyiceberg": ">=0.11.102"}

    mocker.patch(
        "metaflow.extension_support.get_modules", return_value=[_config_extension(hook)]
    )
    assert get_pypi_constraints("3.10", "s3") == {"nflx-pyiceberg": ">=0.11.102"}

    assert hook_calls == [("3.10", "s3")]


def test_missing_constraint_hooks_leave_environment_identity_unchanged(mocker):
    mocker.patch(
        "metaflow.extension_support.get_modules", return_value=[_config_extension()]
    )
    assert pypi_constraint_extras("3.10", "s3") == {}


def test_constraints_participate_in_cache_identity():
    deps = {"conda": ["python==3.10.*"], "pypi": ["pydantic==<2"]}
    old_id = ResolvedEnvironment.get_req_id(deps, {}, {})
    new_id = ResolvedEnvironment.get_req_id(
        deps, {}, {"pypi_constraints": ["nflx-pyiceberg>=0.11.102"]}
    )
    assert new_id != old_id
    assert deps["pypi"] == ["pydantic==<2"]


@pytest.mark.parametrize(
    "versions, expected",
    [([], True), (["0.11.100"], False), (["0.11.102"], True), (["0.11.104"], True)],
    ids=["absent", "outdated", "minimum", "newer"],
)
def test_constraints_validate_only_selected_packages(versions, expected):
    packages = [
        SimpleNamespace(TYPE="pypi", package_name="nflx_pyiceberg", package_version=v)
        for v in versions
    ]
    extras = {"pypi_constraints": ["nflx-pyiceberg>=0.11.102"]}
    assert pypi_constraints_satisfied(packages, extras) is expected
    if expected:
        validate_pypi_constraints(packages, extras)
    else:
        with pytest.raises(CondaException, match="violate extension constraints"):
            validate_pypi_constraints(packages, extras)


def test_mixed_constraints_do_not_add_absent_packages():
    deps = ["pydantic==<2"]
    assert constrain_pypi_deps(deps, ["nflx-pyiceberg>=0.11.102"]) == deps


def test_mixed_constraints_intersect_existing_exact_pins():
    assert constrain_pypi_deps(
        ["nflx-pyiceberg==0.11.100"], ["nflx-pyiceberg>=0.11.102"]
    ) == ["nflx-pyiceberg====0.11.100,>=0.11.102"]


@pytest.mark.parametrize(
    "version, compatible",
    [(None, True), ("0.11.100", False), ("0.11.104", True)],
    ids=["absent", "outdated", "supported"],
)
def test_named_environment_compatibility_checks_constraints(version, compatible):
    from metaflow.metaflow_config import CONDA_SYS_DEFAULT_PACKAGES
    from metaflow_extensions.netflixext.plugins.conda.utils import dict_to_strlist

    python = SimpleNamespace(
        TYPE="conda",
        package_name="python",
        package_version="3.10.20",
        package_name_with_channel=lambda: "python",
    )
    packages = [python]
    if version:
        packages.append(
            SimpleNamespace(
                TYPE="pypi", package_name="nflx-pyiceberg", package_version=version
            )
        )
    base = ResolvedEnvironment(
        {
            "conda": ["python==3.10.*"],
            "sys": dict_to_strlist(CONDA_SYS_DEFAULT_PACKAGES.get("linux-64", {})),
        },
        {},
        {},
        arch="linux-64",
        env_id=EnvID("base-request", "base-full", "linux-64"),
        all_packages=packages,
        env_type=EnvType.PYPI_ONLY,
    )
    conda = SimpleNamespace(default_conda_channels=[], default_pypi_sources=[])
    result = EnvsResolver.extract_info_from_base(
        conda,
        base,
        {},
        {},
        {"pypi_constraints": ["nflx-pyiceberg>=0.11.102"]},
        "linux-64",
    )
    assert result[-1] is compatible
    assert base.extras == []


def test_cached_environment_cannot_bypass_constraints(mocker):
    cached = SimpleNamespace(
        packages=[
            SimpleNamespace(
                TYPE="pypi", package_name="nflx-pyiceberg", package_version="0.11.100"
            )
        ]
    )
    conda = mocker.Mock()
    conda.environment.return_value = cached
    with pytest.raises(CondaException, match="violate extension constraints"):
        EnvsResolver.find_resolved_environment(
            conda,
            "linux-64",
            {"pypi": ["nflx-pyiceberg"]},
            {},
            {"pypi_constraints": ["nflx-pyiceberg>=0.11.102"]},
        )


def test_pip_resolver_passes_constraints_without_adding_requirements(mocker):
    from pathlib import Path
    from metaflow_extensions.netflixext.plugins.conda.resolvers.pip_resolver import (
        PipResolver,
    )
    from metaflow_extensions.netflixext.plugins.conda.utils import arch_id

    builder = SimpleNamespace(
        env_id=EnvID("builder-request", "builder-full", arch_id()),
        packages=[
            SimpleNamespace(filename="python-3.10.20-build", package_version="3.10.20")
        ],
    )
    conda = mocker.Mock()
    conda.create_builder_env.return_value = "/unused-builder"
    captured = {}

    def capture(args, **kwargs):
        path = args[args.index("--constraint") + 1]
        captured["constraints"] = Path(path).read_text()
        captured["args"] = args
        raise RuntimeError("resolver-input-captured")

    conda.call_binary.side_effect = capture
    with pytest.raises(RuntimeError, match="resolver-input-captured"):
        PipResolver(conda).resolve(
            EnvType.PYPI_ONLY,
            "python==3.10.*",
            {"pypi": ["pydantic==<2"], "sys": ["__glibc==2.27=0"]},
            {"pypi": ["https://pypi.netflix.net/simple"]},
            {"pypi_constraints": ["nflx-pyiceberg>=0.11.102"]},
            arch_id(),
            [builder],
        )
    assert "nflx-pyiceberg>=0.11.102\n" in captured["constraints"]
    assert "pydantic<2" in captured["args"]
    assert not any("nflx-pyiceberg" in arg for arg in captured["args"])


def test_deployed_environment_id_does_not_reapply_constraints(monkeypatch, mocker):
    import json
    from metaflow_extensions.netflixext.plugins.conda.conda_environment import (
        CondaEnvironment,
    )

    env_id = EnvID("deployed-request", "deployed-full", "linux-64")
    monkeypatch.setattr(CondaEnvironment, "_result_for_step", {})
    monkeypatch.setenv("_METAFLOW_CONDA_ENV", json.dumps(env_id))
    hook = mocker.patch(
        "metaflow_extensions.netflixext.plugins.conda.conda_environment.pypi_constraint_extras",
        side_effect=AssertionError("must not resolve an already deployed environment"),
    )
    assert CondaEnvironment.get_env_id(mocker.Mock(), "start") == env_id
    hook.assert_not_called()
