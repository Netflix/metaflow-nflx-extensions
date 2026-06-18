"""
Tests for _ensure_local_conda() path-probing logic.

Covers the flow-of-flows scenario: Runner is called inside a step that ran
_install_remote_conda() during bootstrap, placing micromamba at ../conda_env/
or ./conda_env/ but NOT adding it to PATH.  _ensure_local_conda() must find
it without relying on shutil.which().
"""

import os
import stat
import tempfile
import unittest
from unittest.mock import patch


def _make_executable(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("#!/bin/sh\n")
    os.chmod(path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP)


class TestEnsureLocalCondaPathProbing(unittest.TestCase):
    """_ensure_local_conda() finds micromamba placed by _install_remote_conda()."""

    def setUp(self):
        os.environ.setdefault("METAFLOW_CONDA_PYPI_DEPENDENCY_RESOLVER", "uv")
        from metaflow_extensions.netflixext.plugins.conda.conda import Conda

        self._Conda = Conda
        self._original_cwd = os.getcwd()

    def tearDown(self):
        os.chdir(self._original_cwd)

    def _echo(self, *args, **kwargs):
        pass

    def _conda(self):
        return self._Conda(echo=self._echo, datastore_type="local")

    def test_finds_micromamba_in_parent_conda_env(self):
        """../conda_env/micromamba is found when Runner runs inside a remote step."""
        with tempfile.TemporaryDirectory() as base:
            # Layout _install_remote_conda() produces when parent dir is writable:
            #   base/conda_env/micromamba   ← installed binary
            #   base/task/                  ← CWD of step execution
            task_dir = os.path.join(base, "task")
            os.makedirs(task_dir)
            binary_path = os.path.join(base, "conda_env", "micromamba")
            _make_executable(binary_path)

            os.chdir(task_dir)
            conda = self._conda()

            with patch(
                "metaflow_extensions.netflixext.plugins.conda.conda.CONDA_LOCAL_PATH",
                None,
            ):
                conda._ensure_local_conda()

            self.assertEqual(conda._bins.get("micromamba"), binary_path)
            self.assertEqual(conda._bins.get("conda"), binary_path)
            self.assertEqual(conda._conda_executable_type, "micromamba")
            self.assertTrue(conda.is_non_conda_exec)

    def test_finds_micromamba_in_cwd_conda_env(self):
        """./conda_env/micromamba is found as fallback when ../conda_env doesn't exist."""
        with tempfile.TemporaryDirectory() as base:
            # Layout when parent dir is not writable:
            #   base/conda_env/micromamba   ← installed binary (same dir as CWD)
            binary_path = os.path.join(base, "conda_env", "micromamba")
            _make_executable(binary_path)

            os.chdir(base)
            conda = self._conda()

            with patch(
                "metaflow_extensions.netflixext.plugins.conda.conda.CONDA_LOCAL_PATH",
                None,
            ):
                conda._ensure_local_conda()

            self.assertEqual(conda._bins.get("micromamba"), binary_path)
            self.assertEqual(conda._bins.get("conda"), binary_path)
            self.assertEqual(conda._conda_executable_type, "micromamba")
            self.assertTrue(conda.is_non_conda_exec)

    def test_non_executable_binary_not_used(self):
        """A non-executable file in conda_env/ is skipped (falls through to which())."""
        with tempfile.TemporaryDirectory() as base:
            task_dir = os.path.join(base, "task")
            os.makedirs(task_dir)
            binary_path = os.path.join(base, "conda_env", "micromamba")
            os.makedirs(os.path.dirname(binary_path), exist_ok=True)
            with open(binary_path, "w") as f:
                f.write("#!/bin/sh\n")
            # NOT made executable — os.access(p, os.X_OK) should fail

            os.chdir(task_dir)
            conda = self._conda()

            with patch(
                "metaflow_extensions.netflixext.plugins.conda.conda.CONDA_LOCAL_PATH",
                None,
            ):
                # Should fall through to which(). In the test environment micromamba
                # may or may not be on PATH, so we just assert it didn't pick up
                # the non-executable file.
                try:
                    conda._ensure_local_conda()
                except Exception:
                    pass

            # Even if _ensure_local_conda raised, the non-executable path must not
            # have been selected.
            if conda._bins is not None:
                self.assertNotEqual(conda._bins.get("micromamba"), binary_path)
