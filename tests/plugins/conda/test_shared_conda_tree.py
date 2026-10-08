"""_open_shared_conda_tree opens a fresh conda install to other users for reading only."""

import os
import stat

import pytest

import metaflow  # noqa: F401  (load metaflow's plugins before the conda module)
from metaflow_extensions.netflixext.plugins.conda.conda import _open_shared_conda_tree

pytestmark = pytest.mark.local_only


def test_tree_is_readable_by_all_but_writable_only_by_its_owner(tmp_path):
    root = tmp_path / "conda"
    for d in ("envs", "pkgs", "bin"):
        (root / d).mkdir(parents=True)
    exe = root / "bin" / "micromamba"
    exe.write_text("")
    exe.chmod(0o700)
    data = root / "pkgs" / "index.json"
    data.write_text("")
    data.chmod(0o600)
    for d in (root, root / "envs", root / "pkgs", root / "bin"):
        d.chmod(0o700)

    _open_shared_conda_tree(str(root))

    for d in (root, root / "envs", root / "pkgs", root / "bin"):
        assert stat.S_IMODE(os.stat(d).st_mode) == 0o755, d
    assert stat.S_IMODE(os.stat(exe).st_mode) == 0o755
    assert stat.S_IMODE(os.stat(data).st_mode) == 0o644


def test_symlink_targets_are_left_alone(tmp_path):
    """chmod follows symlinks, so a link in the tree must not widen its target."""
    outside = tmp_path / "outside"
    outside.write_text("")
    outside.chmod(0o600)
    root = tmp_path / "conda"
    root.mkdir()
    (root / "link").symlink_to(outside)

    _open_shared_conda_tree(str(root))

    assert stat.S_IMODE(os.stat(outside).st_mode) == 0o600
