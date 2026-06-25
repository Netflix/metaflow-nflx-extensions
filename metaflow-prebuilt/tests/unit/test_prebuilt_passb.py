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
import re
import json
import shutil
import subprocess
import sys
import textwrap
import types

import pytest

from metaflow_extensions.prebuilt.plugins.conda import prebuilt_build_install as pbi


def test_minimal_metaflow_config_uses_core_null_sidecars(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_BUILD_CONTAINER", "1")
    monkeypatch.delenv("METAFLOW_PREBUILT_MINIMAL_PLUGIN_CONFIG", raising=False)

    pbi._install_minimal_metaflow_config()
    config_home = os.environ["METAFLOW_HOME"]
    try:
        with open(os.path.join(config_home, "config.json"), encoding="utf-8") as f:
            config = json.load(f)
    finally:
        pbi._cleanup_minimal_metaflow_config()

    assert config["METAFLOW_DEFAULT_EVENT_LOGGER"] == "nullSidecarLogger"
    assert config["METAFLOW_DEFAULT_MONITOR"] == "nullSidecarMonitor"
    assert config["METAFLOW_ENABLED_LOGGING_SIDECAR"] == ["nullSidecarLogger"]
    assert config["METAFLOW_ENABLED_MONITOR_SIDECAR"] == ["nullSidecarMonitor"]


def test_minimal_metaflow_config_derives_categories_from_metaflow_source(
    tmp_path, monkeypatch
):
    plugins_dir = tmp_path / "metaflow" / "extension_support"
    plugins_dir.mkdir(parents=True)
    (tmp_path / "metaflow" / "__init__.py").write_text("")
    (plugins_dir / "__init__.py").write_text("")
    (plugins_dir / "plugins.py").write_text(
        "_plugin_categories = {\n"
        "    'step_decorator': None,\n"
        "    'environment': None,\n"
        "    'datastore': None,\n"
        "    'logging_sidecar': None,\n"
        "    'monitor_sidecar': None,\n"
        "    'new_plugin_category': None,\n"
        "}\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    config = pbi._minimal_metaflow_config()
    enabled_categories = {
        key.removeprefix("METAFLOW_ENABLED_")
        for key in config
        if key.startswith("METAFLOW_ENABLED_")
    }

    assert enabled_categories == {
        "STEP_DECORATOR",
        "ENVIRONMENT",
        "DATASTORE",
        "LOGGING_SIDECAR",
        "MONITOR_SIDECAR",
        "NEW_PLUGIN_CATEGORY",
    }
    assert set(pbi._MINIMAL_PLUGIN_ALLOWLIST).issubset(enabled_categories)
    assert config["METAFLOW_DEFAULT_EVENT_LOGGER"] == "nullSidecarLogger"
    assert config["METAFLOW_DEFAULT_MONITOR"] == "nullSidecarMonitor"
    assert config["METAFLOW_ENABLED_ENVIRONMENT"] == ["nflx", "conda"]
    assert config["METAFLOW_ENABLED_DATASTORE"] == ["local"]
    assert config["METAFLOW_ENABLED_LOGGING_SIDECAR"] == ["nullSidecarLogger"]
    assert config["METAFLOW_ENABLED_MONITOR_SIDECAR"] == ["nullSidecarMonitor"]
    assert config["METAFLOW_ENABLED_STEP_DECORATOR"] == []
    assert config["METAFLOW_ENABLED_NEW_PLUGIN_CATEGORY"] == []


def test_main_cleans_minimal_metaflow_config_before_fast_exit(monkeypatch, capsys):
    monkeypatch.setenv("METAFLOW_PREBUILT_BUILD_CONTAINER", "1")
    monkeypatch.delenv("METAFLOW_PREBUILT_MINIMAL_PLUGIN_CONFIG", raising=False)

    pbi._install_minimal_metaflow_config()
    config_home = os.environ["METAFLOW_HOME"]
    assert os.path.isdir(config_home)

    monkeypatch.setattr(pbi, "install_env", lambda req_id, full_id: "/env/path")

    assert pbi.main(["prebuilt_build_install", "req", "full"]) == 0

    assert capsys.readouterr().out.strip() == "/env/path"
    assert not os.path.exists(config_home)
    assert os.environ.get("METAFLOW_HOME") != config_home


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


def _make_manual_sdist(tmp_path, name, version, build_requires, setup_imports):
    """Build an sdist tarball BY HAND (no host build backend runs), so its
    setup.py — which imports a build-only dep — is executed only by Pass B in the
    container, not when creating the fixture. pyproject declares the build deps."""
    import io
    import tarfile as _tar

    root = "%s-%s" % (name, version)
    reqs = ", ".join('"%s"' % r for r in list(build_requires) + ["setuptools", "wheel"])
    imports = "".join("import %s  # noqa: F401\n" % m for m in setup_imports)
    files = {
        root + "/" + name + ".py": "__version__ = %r\n" % version,
        root
        + "/pyproject.toml": (
            "[build-system]\nrequires = [%s]\n"
            'build-backend = "setuptools.build_meta"\n' % reqs
        ),
        root
        + "/setup.py": (
            imports
            + "from setuptools import setup\n"
            + "setup(name=%r, version=%r, py_modules=[%r])\n" % (name, version, name)
        ),
        root
        + "/PKG-INFO": "Metadata-Version: 2.1\nName: %s\nVersion: %s\n"
        % (name, version),
    }
    out = tmp_path / ("dist_" + name)
    out.mkdir()
    tarball = out / (root + ".tar.gz")
    with _tar.open(str(tarball), "w:gz") as tf:
        for path, content in files.items():
            b = content.encode()
            ti = _tar.TarInfo(path)
            ti.size = len(b)
            tf.addfile(ti, io.BytesIO(b))
    return tarball


def test_build_requires_extracted_from_sdist(tmp_path):
    """Helper unit: [build-system].requires is read from the sdist, with
    setuptools/wheel/pip filtered out (handled by the overlay/env)."""
    sdist = _make_manual_sdist(
        tmp_path,
        "pkgx",
        "1.0",
        build_requires=["Cython>=3", "setuptools_scm"],
        setup_imports=[],
    )
    reqs = pbi._build_requires_from_sdist(str(sdist))
    assert any(r.startswith("Cython") for r in reqs)
    assert any(r.startswith("setuptools_scm") for r in reqs)
    assert not any(
        re.split(r"[<>=!~ \[;]", r, 1)[0].strip().lower()
        in ("setuptools", "wheel", "pip")
        for r in reqs
    )


def test_pass_b_stages_pyproject_build_requires(tmp_path, monkeypatch):
    """End-to-end Pass B: an sdist whose setup.py imports a build-only dep
    (`tomli`, declared in [build-system].requires) builds because Pass B stages
    that dep into the overlay. Without the fix the build fails (dep not importable
    under --no-build-isolation)."""
    py = _make_venv(tmp_path / "env", "setuptools<82", "wheel")
    # The build dep must NOT already be importable in the env, else the test is moot.
    chk = subprocess.run([py, "-c", "import tomli"], capture_output=True, text=True)
    if chk.returncode == 0:
        pytest.skip("env already has the build dep; can't prove the overlay path")

    sdist = _make_manual_sdist(
        tmp_path,
        "mfpassbbuildreq",
        "0.0.1",
        build_requires=["tomli"],
        setup_imports=["tomli"],
    )
    _defer_one(monkeypatch, "mfpassbbuildreq", "0.0.1", sdist)

    pbi._run_deferred_sdist_builds(
        str(tmp_path / "env"), types.SimpleNamespace(packages=[])
    )

    r = subprocess.run(
        [py, "-c", "import mfpassbbuildreq; print(mfpassbbuildreq.__version__)"],
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stderr
    assert "0.0.1" in r.stdout
