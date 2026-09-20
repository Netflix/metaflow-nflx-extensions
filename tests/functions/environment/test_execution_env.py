"""Unit tests for the execution-environment prefix a host hands to Triton."""

import json
import os
import stat

import pytest

from metaflow_extensions.nflx.plugins.functions.environment import (
    _pin_local_datastore_root,
    ensure_activate_script,
    environment_python_version,
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


def test_cli_prints_one_json_object(monkeypatch, capsys, tmp_path):
    """The JVM reads stdout as the answer, so nothing else may land there."""
    from metaflow_extensions.nflx.plugins.functions import execution_env_cli

    (tmp_path / "lib" / "python3.10").mkdir(parents=True)
    monkeypatch.setattr(
        "metaflow_extensions.nflx.plugins.functions.environment."
        "materialize_conda_environment",
        lambda system_metadata: str(tmp_path),
    )

    assert execution_env_cli.main(["--alias", "env:abc", "--arch", "linux-64"]) == 0

    assert json.loads(capsys.readouterr().out) == {
        "prefix": str(tmp_path),
        "python": "3.10",
        "arch": "linux-64",
    }


def test_cli_still_prints_a_bare_prefix_on_request(monkeypatch, capsys, tmp_path):
    """A caller pinned to the original one-line contract keeps working."""
    from metaflow_extensions.nflx.plugins.functions import execution_env_cli

    monkeypatch.setattr(
        "metaflow_extensions.nflx.plugins.functions.environment."
        "materialize_conda_environment",
        lambda system_metadata: str(tmp_path),
    )

    assert (
        execution_env_cli.main(["--alias", "env:abc", "--format", "prefix"]) == 0
    )
    assert capsys.readouterr().out.strip() == str(tmp_path)


def test_cli_reports_a_null_python_it_cannot_determine(monkeypatch, capsys, tmp_path):
    """Null rather than a guess: the host decides what to do about it."""
    from metaflow_extensions.nflx.plugins.functions import execution_env_cli

    monkeypatch.setattr(
        "metaflow_extensions.nflx.plugins.functions.environment."
        "materialize_conda_environment",
        lambda system_metadata: str(tmp_path),
    )

    assert execution_env_cli.main(["--alias", "env:abc"]) == 0
    assert json.loads(capsys.readouterr().out)["python"] is None


class TestEnvironmentPythonVersion:
    """The host matches its prebuilt stub against this, so a wrong answer is worse
    than no answer."""

    def test_reads_the_version_off_the_lib_directory(self, tmp_path):
        (tmp_path / "lib" / "python3.10").mkdir(parents=True)

        assert environment_python_version(str(tmp_path)) == "3.10"

    def test_does_not_need_a_shared_libpython(self, tmp_path):
        # A statically linked environment has no libpython3.Y.so to read.
        (tmp_path / "lib" / "python3.12").mkdir(parents=True)
        (tmp_path / "lib" / "libstdc++.so.6").write_text("")

        assert environment_python_version(str(tmp_path)) == "3.12"

    def test_ignores_files_and_near_misses(self, tmp_path):
        lib = tmp_path / "lib"
        lib.mkdir()
        (lib / "python3.10").write_text("a file, not the stdlib directory")
        (lib / "python3").mkdir()
        (lib / "pythonista3.9").mkdir()

        assert environment_python_version(str(tmp_path)) is None

    def test_returns_none_for_a_prefix_with_no_lib(self, tmp_path):
        assert environment_python_version(str(tmp_path)) is None


class TestPinnedDatastoreRoot:
    """Conda's datastore root must not depend on the caller's cwd: a serving host
    execs with an arbitrary one, and the walk creates .metaflow wherever it lands."""

    def test_pins_a_root_when_none_is_configured(self, monkeypatch, tmp_path):
        monkeypatch.delenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", raising=False)
        monkeypatch.setattr(
            "metaflow.metaflow_config.CONDA_LOCAL_PATH", str(tmp_path), raising=False
        )

        _pin_local_datastore_root()

        root = os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"]
        assert root.startswith(str(tmp_path))
        assert os.path.isdir(root)

    def test_leaves_an_explicit_root_alone(self, monkeypatch, tmp_path):
        monkeypatch.setenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", "/somewhere/chosen")

        _pin_local_datastore_root()

        assert os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] == "/somewhere/chosen"


class TestCliContract:
    """The wire contract with python-model-serving, pinned deliberately.

    That repo cannot import metaflow -- its JVM owns the Triton model repository --
    so it runs this CLI as a subprocess and hard-codes the module path, the flag
    names and the keys it reads back out of the answer:

        CLI_MODULE = "metaflow_extensions.nflx.plugins.functions.execution_env_cli"
        ...  "-m", CLI_MODULE, "--alias", alias, "--arch", arch
        root.path("prefix"), root.path("python")

    Renaming any of those is a breaking change for a caller no test over there can
    catch: its unit tests stub this CLI with a shell script, so they keep passing
    against a CLI that no longer exists, and the break surfaces as a failed model
    load on a serving instance. These tests are the tripwire for that -- if one of
    them has to change, a consumer has to change with it.
    """

    MODULE = "metaflow_extensions.nflx.plugins.functions.execution_env_cli"

    def test_the_module_is_runnable_under_its_published_path(self):
        import importlib

        module = importlib.import_module(self.MODULE)
        assert hasattr(module, "main")

    def test_it_accepts_the_flags_the_host_passes(self, monkeypatch, capsys, tmp_path):
        from metaflow_extensions.nflx.plugins.functions import execution_env_cli

        monkeypatch.setattr(
            "metaflow_extensions.nflx.plugins.functions.environment."
            "materialize_conda_environment",
            lambda system_metadata: str(tmp_path),
        )

        # Exactly the argv CondaEnvironmentResolver builds, in that order.
        assert execution_env_cli.main(["--alias", "env:abc", "--arch", "linux-64"]) == 0

    def test_it_answers_with_the_keys_the_host_reads(
        self, monkeypatch, capsys, tmp_path
    ):
        from metaflow_extensions.nflx.plugins.functions import execution_env_cli

        (tmp_path / "lib" / "python3.10").mkdir(parents=True)
        monkeypatch.setattr(
            "metaflow_extensions.nflx.plugins.functions.environment."
            "materialize_conda_environment",
            lambda system_metadata: str(tmp_path),
        )

        execution_env_cli.main(["--alias", "env:abc", "--arch", "linux-64"])
        answer = json.loads(capsys.readouterr().out)

        # "prefix" becomes EXECUTION_ENV_PATH; "python" is matched against the
        # version the shipped triton_python_backend_stub links against.
        assert "prefix" in answer
        assert "python" in answer

    def test_the_alias_flag_is_mandatory(self, capsys):
        """The host always passes it, so this failing loudly beats resolving
        something arbitrary."""
        from metaflow_extensions.nflx.plugins.functions import execution_env_cli

        with pytest.raises(SystemExit):
            execution_env_cli.main(["--arch", "linux-64"])

    def test_nothing_but_the_answer_reaches_stdout(
        self, monkeypatch, capsys, tmp_path
    ):
        """The host parses the whole of stdout as one JSON object, so a stray
        print here is indistinguishable from a broken answer."""
        from metaflow_extensions.nflx.plugins.functions import execution_env_cli

        def _chatty(system_metadata):
            import sys

            print("resolving 47 packages", file=sys.stderr)
            return str(tmp_path)

        monkeypatch.setattr(
            "metaflow_extensions.nflx.plugins.functions.environment."
            "materialize_conda_environment",
            _chatty,
        )

        execution_env_cli.main(["--alias", "env:abc"])
        captured = capsys.readouterr()

        json.loads(captured.out)  # parses whole, or this raises
        assert "resolving 47 packages" in captured.err
