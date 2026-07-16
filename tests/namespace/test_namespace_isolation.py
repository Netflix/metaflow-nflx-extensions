"""
Regression tests for the pip RECORD blast-radius fix (GitHub issue: namespace
collision between nflx-metaflow and metaflow-netflixext caused non-deterministic
plugin failures during upgrade).

Root cause: both packages installed files at metaflow_extensions/nflx/plugins/conda/*,
so uninstalling the old nflx-metaflow wiped metaflow-netflixext's files too.

Fix: all metaflow-netflixext files moved to metaflow_extensions/netflixext/.
These tests assert the fix stays in place.
"""

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]
NETFLIXEXT_SRC = REPO_ROOT / "metaflow-netflixext" / "metaflow_extensions"


def _installed_py_files():
    """All .py files shipped by metaflow-netflixext (under metaflow_extensions/)."""
    return [
        p.relative_to(NETFLIXEXT_SRC)
        for p in NETFLIXEXT_SRC.rglob("*.py")
        if "__pycache__" not in p.parts
    ]


def test_no_netflixext_files_under_nflx_namespace():
    """metaflow-netflixext must not install any files under metaflow_extensions/nflx/.

    Any file at that path would appear in nflx-metaflow's pip RECORD blast radius
    and get wiped on upgrade, recreating the original bug.
    """
    offenders = [str(p) for p in _installed_py_files() if p.parts[0] == "nflx"]
    assert offenders == [], (
        "metaflow-netflixext ships files under metaflow_extensions/nflx/ — "
        "these are in nflx-metaflow's pip RECORD blast radius and will be "
        "deleted on upgrade:\n" + "\n".join(f"  {f}" for f in offenders)
    )


def test_netflixext_files_exist_under_netflixext_namespace():
    """Sanity check: metaflow-netflixext actually ships files under netflixext/."""
    netflixext_files = [p for p in _installed_py_files() if p.parts[0] == "netflixext"]
    assert len(netflixext_files) > 0, (
        "No files found under metaflow_extensions/netflixext/ — "
        "the namespace move may not have been applied."
    )


def test_conda_plugin_under_netflixext():
    """The conda plugin specifically must be at netflixext/, not nflx/."""
    conda_files = [
        p for p in _installed_py_files() if "plugins" in p.parts and "conda" in p.parts
    ]
    assert len(conda_files) > 0, "No conda plugin files found in metaflow-netflixext"
    for f in conda_files:
        assert f.parts[0] == "netflixext", (
            f"Conda plugin file {f} is not under netflixext/ — "
            "it is still in the pip RECORD blast radius"
        )


def test_no_stale_nflx_imports_in_netflixext_source():
    """metaflow-netflixext source files must not import from metaflow_extensions.nflx.*.

    Stale nflx.* imports in moved files cause ModuleNotFoundError at runtime since
    those paths no longer exist in metaflow-netflixext after the namespace move.
    """
    stale = []
    for py_file in NETFLIXEXT_SRC.rglob("*.py"):
        if "__pycache__" in py_file.parts:
            continue
        # nflx_compat.py legitimately references the old prefix as a string constant
        if py_file.name == "nflx_compat.py":
            continue
        text = py_file.read_text()
        for lineno, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            # Check for actual import statements (not comments or string literals)
            if "metaflow_extensions.nflx." in stripped and (
                stripped.startswith("import ") or stripped.startswith("from ")
            ):
                stale.append(f"{py_file.relative_to(REPO_ROOT)}:{lineno}: {stripped}")
    assert stale == [], (
        "Found stale metaflow_extensions.nflx.* imports inside metaflow-netflixext:\n"
        + "\n".join(f"  {s}" for s in stale)
    )
