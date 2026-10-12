import json
from types import SimpleNamespace
from unittest.mock import patch

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
