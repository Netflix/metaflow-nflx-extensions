"""Unit tests for prebuilt Pass B (in-container deferred-sdist build) and the
setuptools<82 build overlay in ``prebuilt_build_install``.

These exercise ``_run_deferred_sdist_builds`` (the in-container compile path) and
``_SETUPTOOLS_CHECK`` (the overlay's firing condition) directly. No other test
covers them, and the Netflix ``--environment=prebuilt`` E2E flow *cannot* trigger
them: the netflixext resolver constrains ``setuptools<82`` into every builder env
and the shared S3 cache already holds built wheels for common sdists, so sdists
get EMBEDDED at deploy (Pass A) rather than DEFERRED to Pass B. The only way to
exercise Pass B + the overlay is a controlled unit test like this one.
"""

import os
import subprocess
import sys
import textwrap
import types

import pytest

from metaflow_extensions.prebuilt.plugins.conda import prebuilt_build_install as pbi


# --------------------------------------------------------------------------
# _SETUPTOOLS_CHECK probe — decides whether Pass B stages the setuptools<82
# overlay. Exit 0 => env already satisfies <82 (no overlay). Exit 4 => >=82.
# Exit 3 => setuptools/wheel not importable. Run with -S so only the fake
# modules on PYTHONPATH are visible (hermetic, no network, no real setuptools).
# --------------------------------------------------------------------------
def _write_fake(dirpath, name, version=None):
    pkgdir = os.path.join(dirpath, name)
    os.makedirs(pkgdir, exist_ok=True)
    body = "" if version is None else "__version__ = %r\n" % version
    with open(os.path.join(pkgdir, "__init__.py"), "w") as f:
        f.write(body)


def _run_probe(pythonpath):
    env = dict(os.environ)
    if pythonpath is None:
        env.pop("PYTHONPATH", None)
    else:
        env["PYTHONPATH"] = pythonpath
    # -S: skip site-packages so the real setuptools/wheel can't shadow the fakes.
    return subprocess.run(
        [sys.executable, "-S", "-c", pbi._SETUPTOOLS_CHECK],
        env=env,
        capture_output=True,
        text=True,
    )


def test_setuptools_check_exits_0_below_82(tmp_path):
    _write_fake(str(tmp_path), "setuptools", version="50.3.0")
    _write_fake(str(tmp_path), "wheel", version="0.40.0")
    assert _run_probe(str(tmp_path)).returncode == 0


def test_setuptools_check_exits_4_at_or_above_82(tmp_path):
    _write_fake(str(tmp_path), "setuptools", version="82.0.0")
    _write_fake(str(tmp_path), "wheel", version="0.40.0")
    assert _run_probe(str(tmp_path)).returncode == 4


def test_setuptools_check_exits_3_when_missing(tmp_path):
    # No setuptools/wheel anywhere (-S hides site-packages, empty PYTHONPATH dir).
    assert _run_probe(str(tmp_path)).returncode == 3


# --------------------------------------------------------------------------
# _run_deferred_sdist_builds — the actual Pass B in-container compile. Builds a
# real venv as the "env_dir", a trivial sdist as the deferred source, and asserts
# the package is compiled + installed into the env.
# --------------------------------------------------------------------------
def _make_venv(path, *pip_pkgs):
    subprocess.run([sys.executable, "-m", "venv", str(path)], check=True)
    py = os.path.join(str(path), "bin", "python")
    if pip_pkgs:
        r = subprocess.run(
            [
                py,
                "-m",
                "pip",
                "install",
                "-q",
                "--disable-pip-version-check",
                *pip_pkgs,
            ],
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            pytest.skip(
                "could not provision test venv (%s): %s" % (pip_pkgs, r.stderr[-400:])
            )
    return py


def _make_sdist(tmp_path, name, version="0.0.1", import_pkg_resources=False):
    src = tmp_path / ("src_" + name)
    src.mkdir()
    (src / (name + ".py")).write_text("__version__ = %r\n" % version)
    pre = "import pkg_resources  # noqa: F401\n" if import_pkg_resources else ""
    (src / "setup.py").write_text(
        textwrap.dedent(
            """\
            %sfrom setuptools import setup
            setup(name="%s", version="%s", py_modules=["%s"])
            """
        )
        % (pre, name, version, name)
    )
    out = tmp_path / ("dist_" + name)
    out.mkdir()
    r = subprocess.run(
        [sys.executable, "setup.py", "sdist", "--dist-dir", str(out)],
        cwd=str(src),
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        pytest.skip(
            "could not build test sdist (host setuptools?): %s" % r.stderr[-400:]
        )
    tarballs = list(out.glob("*.tar.gz"))
    assert tarballs, "no sdist produced"
    return tarballs[0]


def _defer_one(monkeypatch, name, version, source):
    monkeypatch.setattr(
        pbi,
        "_read_deferred_builds",
        lambda: {
            "schema_version": "2",
            "sdists": [{"name": name, "version": version, "url": str(source)}],
            "wheels": [],
        },
    )


def test_pass_b_builds_and_installs_deferred_sdist(tmp_path, monkeypatch):
    """Happy path: env already has setuptools<82 + wheel; Pass B compiles the
    deferred sdist and installs it (no overlay needed)."""
    py = _make_venv(tmp_path / "env", "setuptools<82", "wheel")
    sdist = _make_sdist(tmp_path, "mfpassbpkg")
    _defer_one(monkeypatch, "mfpassbpkg", "0.0.1", sdist)

    pbi._run_deferred_sdist_builds(
        str(tmp_path / "env"), types.SimpleNamespace(packages=[])
    )

    r = subprocess.run(
        [py, "-c", "import mfpassbpkg; print(mfpassbpkg.__version__)"],
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stderr
    assert "0.0.1" in r.stdout


def test_pass_b_overlay_rescues_incomplete_build_toolchain(tmp_path, monkeypatch):
    """Overlay path: env has setuptools<82 but NO wheel, so the probe fails and
    Pass B must stage the setuptools<82 + wheel overlay onto the build PYTHONPATH;
    a successful build proves the overlay supplied the missing wheel backend."""
    # setuptools but deliberately no wheel -> _SETUPTOOLS_CHECK exits 3.
    py = _make_venv(tmp_path / "env", "setuptools<82")
    # sanity: wheel really is absent in the env
    chk = subprocess.run(
        [py, "-S", "-c", pbi._SETUPTOOLS_CHECK], capture_output=True, text=True
    )
    if chk.returncode == 0:
        pytest.skip("venv unexpectedly already satisfies the setuptools<82+wheel check")

    sdist = _make_sdist(tmp_path, "mfpassboverlay")
    _defer_one(monkeypatch, "mfpassboverlay", "0.0.1", sdist)

    # Without the overlay this raises (no wheel backend); success == overlay worked.
    pbi._run_deferred_sdist_builds(
        str(tmp_path / "env"), types.SimpleNamespace(packages=[])
    )

    r = subprocess.run(
        [py, "-c", "import mfpassboverlay; print('ok')"], capture_output=True, text=True
    )
    assert r.returncode == 0, r.stderr
    assert "ok" in r.stdout


def test_pass_b_noop_when_no_deferred_sdists(tmp_path, monkeypatch):
    """Empty sdists list (the normal Netflix case — everything embedded) is a
    no-op: no env tools are invoked, no error."""
    monkeypatch.setattr(
        pbi,
        "_read_deferred_builds",
        lambda: {"schema_version": "2", "sdists": [], "wheels": []},
    )
    # env_dir doesn't even need to exist for a no-op.
    pbi._run_deferred_sdist_builds(
        str(tmp_path / "nonexistent_env"), types.SimpleNamespace(packages=[])
    )
