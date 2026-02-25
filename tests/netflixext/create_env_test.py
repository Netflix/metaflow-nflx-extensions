import unittest
from unittest.mock import PropertyMock, patch
import os
import tempfile


class TestConda(unittest.TestCase):
    def setUp(self):

        def echo(*args, **kwargs):
            pass

        # Arrange
        os.environ["METAFLOW_CONDA_PYPI_DEPENDENCY_RESOLVER"] = "uv"
        # Import after setting the environment variable
        from metaflow_extensions.netflix_ext.plugins.conda.env_descr import (
            EnvID,
            PypiPackageSpecification,
            ResolvedEnvironment,
        )
        from metaflow_extensions.netflix_ext.plugins.conda.conda import Conda

        self.env_dir = tempfile.mkdtemp()
        self.local_pkg = tempfile.mktemp()
        self.env_id = EnvID(req_id="test_req", full_id="test_full", arch="linux-64")
        pkg = PypiPackageSpecification(
            "test_package-1.0.0-py2-none-any.whl",
            "https://fake/test_package.whl",
            is_real_url=False,
        )
        pkg.add_local_file(".whl", self.local_pkg, pkg_hash="fake_hash")
        self.resolved_env = ResolvedEnvironment(
            user_dependencies={},
            user_sources={},
            user_extra_args={},
            arch="linux-64",
            env_id=self.env_id,
            all_packages=[pkg],
        )
        self.conda = Conda(echo=echo, datastore_type="local")
        self.cached_info = {
            "envs_dirs": [os.path.join(self.env_dir, "envs")],
            "pkgs_dirs": [os.path.join(self.env_dir, "pkgs")],
            "root_prefix": self.env_dir,
        }
        self.conda._found_binaries = True
        self.conda._bins = {
            "conda": os.path.join(self.env_dir, "bin", "conda"),
            "micromamba": os.path.join(self.env_dir, "bin", "micromamba"),
        }

    @patch("subprocess.check_output")
    def test_uv_called(self, mock_check_output):
        # Arrange
        uv_path = os.path.join(self.env_dir, "envs", "test_env", "bin", "uv")
        os.makedirs(os.path.join(self.env_dir, "envs", "test_env", "bin"))
        python_path = os.path.join(self.env_dir, "envs", "test_env", "bin", "python")
        prefix_path = os.path.join(self.env_dir, "envs", "test_env")

        # Act
        with patch.object(
            type(self.conda), "_info_no_lock", new_callable=PropertyMock
        ) as mock_info:
            mock_info.return_value = self.cached_info
            self.conda._create(self.resolved_env, "test_env")

        uv_call = [
            uv_path,
            "pip",
            "install",
            "--python",
            python_path,
            "--prefix",
            prefix_path,
            "--no-deps",
            "--no-verify-hashes",
            "--no-index",
            "--no-cache",
            "--offline",
            "--no-config",
            "--no-progress",
        ]

        pip_call = [
            python_path,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--no-input",
            "--no-index",
            "--no-user",
            "--no-warn-script-location",
            "--no-cache-dir",
            "--root-user-action=ignore",
            "--disable-pip-version-check",
        ]
        # Assert fallback
        ok = False
        for c in mock_check_output.call_args_list:
            if c[0][0][: len(pip_call)] == pip_call:
                ok = True
                break
        assert ok

        # "touch" the uv binary so it can be used
        with open(uv_path, "a"):
            os.utime(uv_path, None)

        # Act "2"
        # Invalidate the created environment above
        os.unlink(os.path.join(self.env_dir, "envs", "test_env", ".metaflowenv"))
        with patch.object(
            type(self.conda), "_info_no_lock", new_callable=PropertyMock
        ) as mock_info:
            mock_info.return_value = self.cached_info
            self.conda._create(self.resolved_env, "test_env")
        # Assert "uv" was called
        ok = False
        for c in mock_check_output.call_args_list:
            if c[0][0][: len(uv_call)] == uv_call:
                ok = True
                break
        assert ok
