"""Build-time conda env installer for ``--environment=prebuilt``.

Runs INSIDE the Docker build container. Two-pass install:
  Pass A: ``conda.create_for_step(..., only_binary=True)`` installs conda
          packages + pre-built wheels offline (sdists are skipped).
  Pass B: ``_run_deferred_sdist_builds(env_path)`` fetches each deferred sdist
          URL and builds it inside the env on the target architecture.

This module OWNS the deferred-builds wire format (schema_version "2"). Core
conda knows nothing about it — it only exposes the generic ``only_binary``
create primitive (Pass A) and the generic ``defer_pypi_sdist_build`` resolve
primitive (which produced the records this file consumes).

deferred_builds.json schema (version "2"):
  {
    "schema_version": "2",
    "sdists": [{"name": ..., "version": ..., "url": ...}],
    "wheels": [{"name": ..., "version": ..., "filename": ...,
                "wheel_file": ..., "url_format": ...}]
  }
"""

import json
import os
import subprocess
import sys
import time

from metaflow.cli import echo_always

try:
    from metaflow_extensions.netflixext.plugins.conda.conda import Conda
    from metaflow_extensions.netflixext.plugins.conda.env_descr import EnvID
    from metaflow_extensions.netflixext.plugins.conda.remote_bootstrap import (
        setup_conda_manifest,
    )
    from metaflow_extensions.netflixext.plugins.conda.utils import (
        CondaException,
        arch_id,
    )
except ImportError as e:
    print(
        "ERROR: metaflow-netflixext is required in the build container but is "
        "not importable: %s" % e,
        file=sys.stderr,
    )
    sys.exit(1)

# Wire-format container paths — owned by this module (schema_version "2").
# Must match _DEFERRED_BUILDS_CONTAINER_PATH and _DEFERRED_WHEELS_CONTAINER_DIR
# in prebuilt_conda_environment._generate_dockerfile.
_DEFERRED_BUILDS_PATH = "/app/deferred_builds.json"
_DEFERRED_WHEELS_DIR = "/app/deferred_wheels"
_SCHEMA_VERSION = "2"


def _echo(*args, **kwargs):
    kwargs["err"] = False
    echo_always(*args, **kwargs)


def _read_deferred_builds():
    """Return the parsed deferred_builds.json, or None if absent.

    Raises CondaException on malformed JSON or an unexpected schema_version.
    """
    if not os.path.exists(_DEFERRED_BUILDS_PATH):
        return None
    try:
        with open(_DEFERRED_BUILDS_PATH, mode="r", encoding="utf-8") as fh:
            data = json.load(fh)
    except json.JSONDecodeError as e:
        raise CondaException(
            "Malformed %s: %s. Re-deploy to regenerate." % (_DEFERRED_BUILDS_PATH, e)
        )
    schema_version = data.get("schema_version")
    if schema_version != _SCHEMA_VERSION:
        raise CondaException(
            "Unknown deferred_builds.json schema_version: %r (expected %r). "
            "Re-deploy to regenerate." % (schema_version, _SCHEMA_VERSION)
        )
    return data


def _register_embedded_wheels(resolved_env):  # type: ignore[no-untyped-def]
    """Point non-web-downloadable (git/local) packages at their embedded wheels.

    ``_gather_embedded_wheels`` on the deploy machine embedded the already-built
    wheels under ``/app/deferred_wheels`` and recorded them in
    ``deferred_builds.json["wheels"]``. Registering each as the package's local
    file satisfies ``create_for_step``'s lazy fetch so Pass A installs them
    offline (the builder has no S3 / git access).
    """
    data = _read_deferred_builds()
    if data is None:
        return
    wheels = data.get("wheels", [])
    if not wheels:
        return
    by_filename = {p.filename: p for p in resolved_env.packages if p.TYPE == "pypi"}
    for w in wheels:
        filename = w["filename"]
        wheel_file = w.get("wheel_file", filename)
        spec = by_filename.get(filename)
        if spec is None:
            raise CondaException(
                "Embedded wheel '%s' has no matching package in the resolved "
                "environment; deferred_builds.json is inconsistent with the env "
                "manifest." % filename
            )
        whl_path = os.path.join(_DEFERRED_WHEELS_DIR, wheel_file)
        if not os.path.isfile(whl_path):
            raise CondaException(
                "Embedded wheel missing in container: %s (declared in "
                "deferred_builds.json but not present at %s)." % (wheel_file, whl_path)
            )
        spec.add_local_file(w.get("url_format", ".whl"), whl_path)
        _echo("    Registered embedded wheel: %s" % wheel_file)


def _run_deferred_sdist_builds(env_dir: str) -> None:
    """Pass B: build sdists that were deferred from the deploy machine.

    Reads ``/app/deferred_builds.json`` (schema_version "2"). For each entry in
    ``["sdists"]`` runs::

        <env>/bin/uv pip install --no-build-isolation --no-deps <url>

    The sdist's runtime deps were resolved on the deploy machine and installed
    in Pass A, so ``--no-deps`` is correct. Pre-check: assert setuptools+wheel
    import in the env (the build backends); auto-install if missing. A missing
    manifest is a no-op (the env has no deferred sdists).

    ``--no-build-isolation`` is deliberate: by Pass B the env already contains
    the sdist's *runtime* deps, so its ``setup.py`` can import them at build
    time (e.g. ``deepspeed`` imports ``torch``); build isolation would create a
    clean env without them and fail. KNOWN LIMITATION: an sdist whose
    ``pyproject.toml`` ``[build-system].requires`` lists *build* deps beyond
    setuptools/wheel (e.g. ``Cython``, ``setuptools_scm``, ``maturin``) will not
    have those auto-installed here and may fail to build. This matches the
    pre-OSS inline implementation; supplying build requirements is tracked as a
    follow-up (extend deferred_builds.json with a per-sdist ``build_requires``).
    """
    python_bin = os.path.join(env_dir, "bin", "python")
    uv_bin = os.path.join(env_dir, "bin", "uv")

    data = _read_deferred_builds()
    if data is None:
        return  # No deferred builds — sdist-free environment.

    deferred_sdists = data.get("sdists", [])
    if not deferred_sdists:
        return  # Wheels-only or empty; Pass B is a no-op.

    # Pre-check: setuptools + wheel are required as build backends.
    _echo(" (Pass B pre-check: setuptools+wheel) ...", timestamp=False, nl=False)
    check_result = subprocess.run(
        [python_bin, "-c", "import setuptools, wheel"],
        capture_output=True,
        text=True,
    )
    if check_result.returncode != 0:
        if os.path.isfile(uv_bin):
            auto_args = [
                uv_bin,
                "pip",
                "install",
                "--python",
                python_bin,
                "--prefix",
                env_dir,
                "--no-cache",
                "--no-config",
                "setuptools<82",
                "wheel",
            ]
        else:
            auto_args = [
                python_bin,
                "-m",
                "pip",
                "install",
                "--no-cache-dir",
                "--no-input",
                "--disable-pip-version-check",
                "setuptools<82",
                "wheel",
            ]
        auto_result = subprocess.run(auto_args, capture_output=True, text=True)
        if auto_result.returncode != 0:
            raise CondaException(
                "Pass B pre-check: setuptools/wheel not in env %s and "
                "auto-install failed.\nstderr: %s" % (env_dir, auto_result.stderr)
            )
        recheck = subprocess.run(
            [python_bin, "-c", "import setuptools, wheel"],
            capture_output=True,
            text=True,
        )
        if recheck.returncode != 0:
            raise CondaException(
                "Pass B pre-check: setuptools/wheel still not importable after "
                "auto-install in %s.\nstderr: %s" % (env_dir, recheck.stderr)
            )

    _echo(
        " (Pass B: %d deferred sdist(s)) ..." % len(deferred_sdists),
        timestamp=False,
        nl=False,
    )
    for sdist in deferred_sdists:
        name = sdist.get("name", "<unknown>")
        version = sdist.get("version", "")
        url = sdist.get("url", "")
        if not url:
            raise CondaException(
                "Deferred sdist entry for %r has no 'url' in %s. Re-deploy to "
                "regenerate." % (name, _DEFERRED_BUILDS_PATH)
            )
        _echo(" (building %s==%s) ..." % (name, version), timestamp=False, nl=False)
        if os.path.isfile(uv_bin):
            build_args = [
                uv_bin,
                "pip",
                "install",
                "--python",
                python_bin,
                "--prefix",
                env_dir,
                "--no-build-isolation",
                "--no-deps",
                "--no-cache",
                "--no-config",
                "--no-progress",
                url,
            ]
        else:
            build_args = [
                python_bin,
                "-m",
                "pip",
                "install",
                "--no-build-isolation",
                "--no-deps",
                "--no-cache-dir",
                "--no-input",
                "--disable-pip-version-check",
                url,
            ]
        result = subprocess.run(build_args, capture_output=True, text=True)
        if result.returncode != 0:
            raise CondaException(
                "Failed to build deferred sdist %s==%s from %s\n"
                "stdout:\n%s\nstderr:\n%s"
                % (name, version, url, result.stdout, result.stderr)
            )
        _echo(" done.", timestamp=False)


def install_env(req_id: str, full_id: str) -> str:
    """Install the prebuilt conda env inside the Docker build container.

    Pass A: ``conda.create_for_step(..., only_binary=True)``
    Pass B: ``_run_deferred_sdist_builds(env_path)``
    """
    start = time.time()
    _echo("    Setting up Conda (build-time) ...", nl=False)

    setup_conda_manifest()

    my_conda = Conda(_echo, "local", mode="remote")
    my_conda.binary("micromamba")
    _echo(" done in %ds." % int(time.time() - start))

    env_id = EnvID(req_id=req_id, full_id=full_id, arch=arch_id())
    resolved_env = my_conda.environment(env_id)
    if resolved_env is None:
        raise RuntimeError(
            "Cannot find cached environment for %s:%s in the build-time manifest."
            % (req_id, full_id)
        )

    _register_embedded_wheels(resolved_env)

    install_start = time.time()
    # Pass A: install conda packages + pre-built wheels offline. only_binary=True
    # makes _create skip any sdist entries in pypi_paths.
    env_path = my_conda.create_for_step(
        "prebuilt_build",
        resolved_env,
        do_symlink=False,
        only_binary=True,
    )
    _echo("    Pass A done in %ds." % int(time.time() - install_start))

    # Pass B: build the deferred sdists inside the env (on the target arch).
    _run_deferred_sdist_builds(env_path)

    _echo("    Env installed at %s (%ds total)" % (env_path, int(time.time() - start)))
    return env_path


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: python -m metaflow_extensions.prebuilt.plugins.conda."
            "prebuilt_build_install <req_id> <full_id>",
            file=sys.stderr,
        )
        sys.exit(2)
    path = install_env(sys.argv[1], sys.argv[2])
    print(path)
    sys.stdout.flush()
    os._exit(0)
