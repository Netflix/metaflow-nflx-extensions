import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import metaflow  # noqa: F401 -- initialize plugins before importing utilities

from metaflow_extensions.netflixext.plugins.conda.utils import (
    get_pinned_pypi_libs,
)


def _config_extension(hook=None):
    module = SimpleNamespace()
    if hook is not None:
        module.get_pinned_pypi_libs = hook
    return SimpleNamespace(module=module)


def test_get_pinned_pypi_libs_merges_all_config_extensions():
    extensions = [
        _config_extension(lambda python, datastore: {"shared": ">=1", "one": ""}),
        _config_extension(lambda python, datastore: {"shared": "<3", "two": "2"}),
        _config_extension(),
    ]

    with patch(
        "metaflow.extension_support.get_modules",
        return_value=extensions,
    ):
        assert get_pinned_pypi_libs("3.11", "s3") == {
            "shared": ">=1,<3",
            "one": "",
            "two": "2",
        }


def test_get_pinned_pypi_libs_passes_environment_to_hooks():
    hook_calls = []

    def hook(python_version, datastore_type):
        hook_calls.append((python_version, datastore_type))
        return {"nflx-pyiceberg": ">=0.11.102"}

    with patch(
        "metaflow.extension_support.get_modules",
        return_value=[_config_extension(hook)],
    ):
        assert get_pinned_pypi_libs("3.10", "s3") == {"nflx-pyiceberg": ">=0.11.102"}

    assert hook_calls == [("3.10", "s3")]


def test_deployed_environment_id_does_not_reapply_pypi_pins(monkeypatch, mocker):
    from metaflow_extensions.netflixext.plugins.conda.conda_environment import (
        CondaEnvironment,
    )
    from metaflow_extensions.netflixext.plugins.conda.env_descr import EnvID

    env_id = EnvID("deployed-request", "deployed-full", "linux-64")
    monkeypatch.setattr(CondaEnvironment, "_result_for_step", {})
    monkeypatch.setenv("_METAFLOW_CONDA_ENV", json.dumps(env_id))
    hook = mocker.patch(
        "metaflow_extensions.netflixext.plugins.conda.conda_environment.get_pinned_pypi_libs",
        side_effect=AssertionError("must not resolve an already deployed environment"),
    )
    assert CondaEnvironment.get_env_id(mocker.Mock(), "start") == env_id
    hook.assert_not_called()


@pytest.mark.parametrize("base_type", ["conda-only", "pypi-only", "mixed"])
def test_named_environment_pypi_pins_follow_base_type(base_type, monkeypatch, mocker):
    from metaflow import FlowSpec, named_env, step
    from metaflow_extensions.netflixext.plugins.conda.conda_environment import (
        CondaEnvironment,
    )
    from metaflow_extensions.netflixext.plugins.conda.env_descr import (
        CondaPackageSpecification,
        EnvType,
        PypiPackageSpecification,
        ResolvedEnvironment,
    )

    env_type = {
        "conda-only": EnvType.CONDA_ONLY,
        "pypi-only": EnvType.PYPI_ONLY,
        "mixed": EnvType.MIXED,
    }[base_type]

    class NamedEnvironmentFlow(FlowSpec):
        @named_env(name="tests/base")
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    packages = [
        CondaPackageSpecification(
            "python-3.10.14-0",
            "https://conda.example/linux-64/python-3.10.14-0.conda",
        )
    ]
    if env_type != EnvType.CONDA_ONLY:
        packages.append(
            PypiPackageSpecification(
                "requests-2.32.3-py3-none-any",
                "https://pypi.example/requests-2.32.3-py3-none-any.whl",
            )
        )
    base_deps = {}
    if env_type != EnvType.PYPI_ONLY:
        base_deps["conda"] = ["python==3.10.14"]
    if env_type != EnvType.CONDA_ONLY:
        base_deps["pypi"] = ["requests==2.32.3"]
    base = ResolvedEnvironment(
        user_dependencies=base_deps,
        user_sources={},
        user_extra_args={},
        arch="linux-64",
        all_packages=packages,
        env_type=env_type,
    )
    conda = mocker.Mock(
        virtual_packages={},
        default_conda_channels=[],
        default_pypi_sources=[],
    )
    conda.env_id_from_alias.return_value = base.env_id
    conda.environment.return_value = base
    monkeypatch.setattr(CondaEnvironment, "_flow_req", None)
    monkeypatch.setattr(CondaEnvironment, "_result_for_step", {})
    hook = mocker.patch(
        "metaflow_extensions.netflixext.plugins.conda.conda_environment.get_pinned_pypi_libs",
        return_value={"nflx-pyiceberg": ">=0.11.102"},
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
        assert "nflx-pyiceberg" not in req.packages.get("pypi", {})
        hook.assert_not_called()
    else:
        assert req.packages["pypi"]["nflx-pyiceberg"] == ">=0.11.102"
        hook.assert_called_once_with("3.10.14", "local")
