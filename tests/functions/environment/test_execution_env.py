"""Unit tests for the execution-environment prefix a host hands to Triton."""

import os
import stat

import pytest

from metaflow_extensions.nflx.plugins.functions.environment import (
    ensure_activate_script,
)

pytestmark = pytest.mark.local_only


def test_writes_an_activate_script_when_the_env_has_none(tmp_path):
    """Conda.create_for_name() produces no activate script, and Triton's python
    backend sources one before starting its stub."""
    (tmp_path / "bin").mkdir()

    assert ensure_activate_script(str(tmp_path)) == str(tmp_path)

    activate = tmp_path / "bin" / "activate"
    written = activate.read_text()
    assert str(tmp_path) in written
    # The three variables that decide which interpreter, stdlib and shared
    # libraries the activated process gets.
    assert 'export PATH="%s/bin:$PATH"' % tmp_path in written
    assert "LD_LIBRARY_PATH" in written
    assert "unset PYTHONHOME" in written
    assert os.stat(activate).st_mode & stat.S_IXUSR


def test_leaves_an_existing_activate_alone(tmp_path):
    """A conda-pack'd environment comes with its own, which knows more about
    the environment than this shim does."""
    (tmp_path / "bin").mkdir()
    activate = tmp_path / "bin" / "activate"
    activate.write_text("# the environment's own\n")

    ensure_activate_script(str(tmp_path))

    assert activate.read_text() == "# the environment's own\n"


def test_is_idempotent(tmp_path):
    (tmp_path / "bin").mkdir()

    ensure_activate_script(str(tmp_path))
    first = (tmp_path / "bin" / "activate").read_text()
    ensure_activate_script(str(tmp_path))

    assert (tmp_path / "bin" / "activate").read_text() == first


def test_cli_prints_only_the_prefix(monkeypatch, capsys, tmp_path):
    """The JVM reads stdout as the answer, so nothing else may land there."""
    from metaflow_extensions.nflx.plugins.functions import execution_env_cli

    monkeypatch.setattr(
        "metaflow_extensions.nflx.plugins.functions.environment."
        "materialize_conda_environment",
        lambda system_metadata: str(tmp_path),
    )

    assert execution_env_cli.main(["--alias", "env:abc", "--arch", "linux-64"]) == 0
    assert capsys.readouterr().out.strip() == str(tmp_path)
