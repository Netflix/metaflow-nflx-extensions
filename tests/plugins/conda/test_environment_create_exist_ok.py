"""
Tests for `metaflow environment create --exist-ok`.

Conda is mocked: these tests only check what the command does when the named
environment already exists, not how environments are built.
"""

from unittest.mock import MagicMock, patch

import pytest

from metaflow._vendor.click.testing import CliRunner
from metaflow_extensions.netflixext.cmd.environment.environment_cmd import environment
from metaflow_extensions.netflixext.plugins.conda.env_descr import EnvID
from metaflow_extensions.netflixext.plugins.conda.utils import AliasType

CMD = "metaflow_extensions.netflixext.cmd.environment.environment_cmd"
REQUESTED = EnvID("req", "full", "linux-64")
OTHER = EnvID("req", "other", "linux-64")
PREFIX = "/envs/my_env"


@pytest.fixture
def conda():
    conda = MagicMock()
    env = MagicMock()
    env.env_id = REQUESTED
    conda.env_id_from_alias.return_value = REQUESTED
    conda.environment.return_value = env
    conda.create_for_name.return_value = PREFIX
    with patch("%s.Conda" % CMD, return_value=conda), patch(
        "%s.resolve_env_alias" % CMD, return_value=(AliasType.GENERIC, "my/alias")
    ):
        yield conda


def _create(tmp_path, *flags):
    out = tmp_path / "answer"
    result = CliRunner().invoke(
        environment,
        [
            "--quiet",
            "--quiet-file-output",
            str(out),
            # Pinned so a developer's own metaflow config can't change the defaults.
            "--metadata",
            "local",
            "--datastore",
            "local",
            "create",
            "--name",
            "my_env",
        ]
        + list(flags)
        + ["my/alias"],
    )
    return result, out


def test_creates_when_missing(conda, tmp_path):
    conda.created_environment.return_value = None
    result, out = _create(tmp_path, "--exist-ok")
    assert result.exit_code == 0, result.output
    conda.create_for_name.assert_called_once()
    assert out.read_text().strip() == PREFIX + "/bin/python"


def test_existing_fails_without_exist_ok(conda, tmp_path):
    conda.created_environment.return_value = (REQUESTED, PREFIX)
    result, _ = _create(tmp_path)
    assert result.exit_code != 0
    assert "already exists" in str(result.exception)
    conda.create_for_name.assert_not_called()
    conda.remove_for_name.assert_not_called()


def test_exist_ok_reuses_same_environment(conda, tmp_path):
    conda.created_environment.return_value = (REQUESTED, PREFIX)
    result, out = _create(tmp_path, "--exist-ok")
    assert result.exit_code == 0, result.output
    conda.remove_for_name.assert_not_called()
    conda.create_for_name.assert_called_once()
    assert out.read_text().strip() == PREFIX + "/bin/python"


def test_exist_ok_refuses_a_different_environment(conda, tmp_path):
    conda.created_environment.return_value = (OTHER, PREFIX)
    result, _ = _create(tmp_path, "--exist-ok")
    assert result.exit_code != 0
    assert "holds a different environment" in str(result.exception)
    conda.create_for_name.assert_not_called()
    conda.remove_for_name.assert_not_called()


def test_force_wins_over_exist_ok(conda, tmp_path):
    conda.created_environment.return_value = (REQUESTED, PREFIX)
    result, _ = _create(tmp_path, "--exist-ok", "--force")
    assert result.exit_code == 0, result.output
    conda.remove_for_name.assert_called_once_with("my_env")
    conda.create_for_name.assert_called_once()
