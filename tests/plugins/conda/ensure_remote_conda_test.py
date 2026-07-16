import os
import unittest
from unittest.mock import patch

# Import metaflow first so its extension/plugin registration completes before we
# import the nflx Conda submodule (otherwise plugin resolution can fail with a
# circular-import-style "Cannot locate ... plugin" error).
os.environ.setdefault("METAFLOW_DATASTORE_SYSROOT_LOCAL", ".metaflow")
import metaflow  # noqa: F401


class TestEnsureRemoteConda(unittest.TestCase):
    """Codex r16 regression: `_ensure_remote_conda` must treat an EMPTY
    CONDA_REMOTE_INSTALLER as "no datastore installer" (truthy check), not as
    "configured" (`is not None`). The prebuilt build container force-disables the
    datastore installer via METAFLOW_CONDA_REMOTE_INSTALLER="" and runs with
    _storage=None; routing "" to _install_remote_conda (which requires _storage)
    would fail every prebuilt image build."""

    def _make_conda(self):
        def echo(*args, **kwargs):
            pass

        from metaflow_extensions.netflixext.plugins.conda.conda import Conda

        return Conda(echo=echo, datastore_type="local", mode="remote")

    def test_empty_remote_installer_self_installs_micromamba(self):
        c = self._make_conda()
        with patch(
            "metaflow_extensions.netflixext.plugins.conda.conda.CONDA_REMOTE_INSTALLER",
            "",
        ), patch.object(c, "_install_remote_conda") as m_install, patch.object(
            c, "_ensure_micromamba", return_value="/fake/bin/micromamba"
        ) as m_micro:
            c._ensure_remote_conda()
        m_install.assert_not_called()
        m_micro.assert_called_once()
        self.assertEqual(c._conda_executable_type, "micromamba")
        self.assertTrue(c.is_non_conda_exec)

    def test_configured_remote_installer_uses_datastore_path(self):
        c = self._make_conda()
        with patch(
            "metaflow_extensions.netflixext.plugins.conda.conda.CONDA_REMOTE_INSTALLER",
            "conda-{arch}",
        ), patch.object(c, "_install_remote_conda") as m_install, patch.object(
            c, "_ensure_micromamba"
        ) as m_micro:
            c._ensure_remote_conda()
        m_install.assert_called_once()
        m_micro.assert_not_called()


if __name__ == "__main__":
    unittest.main()
