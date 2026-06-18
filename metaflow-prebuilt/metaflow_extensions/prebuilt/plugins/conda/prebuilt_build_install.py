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
import shutil
import subprocess
import sys
import tempfile
import time

from typing import Optional

# This module runs ONLY inside the prebuilt Docker build container. The image
# was given the resolved-env *manifest* but NOT the deploy machine's package
# cache (the build context copies the manifest, not the cache datastore). So
# force conda to treat the container's "local" datastore as non-caching: with
# no caching datastore, Conda._storage is None and lazy_fetch downloads conda /
# wheel packages from their original web URLs (and installs the embedded wheels
# we register) instead of probing a cache-less LocalStorage and failing.
#
# This must be set BEFORE importing metaflow: from_conf() freezes config values
# at import time, and it parses list-valued configs with json.loads, so the
# value is a JSON array. setdefault() respects an explicit override if one is
# ever set in the generated Dockerfile.
os.environ.setdefault("METAFLOW_CONDA_IGNORE_CACHING_DATASTORES", '["local"]')

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

# Pass B builds sdists with ``--no-build-isolation``, so the build backend is
# the env's OWN setuptools. setuptools 82 removed the bundled ``pkg_resources``,
# which many legacy ``setup.py`` files still import — exactly why the deploy-side
# builder constrains ``setuptools<82``. This probe exits 0 only when setuptools
# (and wheel) are importable AND setuptools' major version is < 82; any other
# outcome (missing, >=82, or unparseable) exits non-zero so the caller installs
# / downgrades to ``setuptools<82``.
_SETUPTOOLS_CHECK = (
    "import sys\n"
    "try:\n"
    "    import setuptools, wheel\n"
    "    major = int(setuptools.__version__.split('.')[0])\n"
    "except Exception:\n"
    "    sys.exit(3)\n"
    "sys.exit(0 if major < 82 else 4)\n"
)


def _echo(*args, **kwargs):
    kwargs["err"] = False
    # echo_always() forwards **kwargs to click.secho(), which has no "timestamp"
    # kwarg (it raises TypeError in style()). The Pass B progress lines pass
    # timestamp=False (a porting artifact from the pre-OSS inline echo helper);
    # drop it so Pass B doesn't crash. This never surfaced in the Netflix flow
    # because Pass B only runs when a deferred sdist has no embedded/cached wheel.
    kwargs.pop("timestamp", None)
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


def _run_deferred_sdist_builds(env_dir, resolved_env):  # type: ignore[no-untyped-def]
    """Pass B: build sdists that were deferred from the deploy machine.

    Reads ``/app/deferred_builds.json`` (schema_version "2"). For each entry in
    ``["sdists"]`` runs::

        <env>/bin/uv pip install --no-build-isolation --no-deps <source>

    where ``<source>`` is the local artifact Pass A's lazy_fetch already fetched
    and hash-verified (preferred — correct bits, no second un-credentialed
    download), falling back to the recorded URL only if no local file is present.

    The sdist's runtime deps were resolved on the deploy machine and installed
    in Pass A, so ``--no-deps`` is correct. Pre-check: the ``--no-build-isolation``
    build below uses the env's OWN ``setuptools``/``wheel`` as the build backend,
    and ``setuptools>=82`` dropped ``pkg_resources`` (which legacy ``setup.py``
    files import; the deploy-side builder applies the same ``<82`` constraint). If
    the env's setuptools is missing or >=82 we stage ``setuptools<82`` + ``wheel``
    into a temp dir and put it on the build subprocess' ``PYTHONPATH`` — a build
    overlay that is NEVER installed into the env, so the shipped image stays
    byte-identical to the resolved conda manifest (no pip-over-conda mutation,
    nothing to restore). A missing deferred-builds manifest is a no-op.

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

    # Pre-check: Pass B builds with --no-build-isolation, so the build backend is
    # the env's OWN setuptools/wheel. setuptools>=82 dropped pkg_resources, which
    # legacy setup.py files import (the deploy-side builder constrains <82 for the
    # same reason). If the env's setuptools is missing or >=82, stage a transient
    # setuptools<82 + wheel into a temp dir and put it FIRST on the build
    # subprocess' PYTHONPATH (a build overlay) — we never install into the env, so
    # the shipped image stays byte-identical to the resolved conda manifest (no
    # pip-over-conda mutation, nothing to restore). The overlay is removed after
    # the builds. build_overlay stays None when the env already satisfies <82.
    build_overlay = None  # type: Optional[str]
    _echo(" (Pass B pre-check: setuptools<82 + wheel) ...", timestamp=False, nl=False)
    check_result = subprocess.run(
        [python_bin, "-c", _SETUPTOOLS_CHECK],
        capture_output=True,
        text=True,
    )
    if check_result.returncode != 0:
        build_overlay = tempfile.mkdtemp(prefix="mf_passb_setuptools_")
        _echo(
            " (staging setuptools<82 + wheel build overlay) ...",
            timestamp=False,
            nl=False,
        )
        if os.path.isfile(uv_bin):
            overlay_args = [
                uv_bin,
                "pip",
                "install",
                "--python",
                python_bin,
                "--target",
                build_overlay,
                "--no-cache",
                "--no-config",
                "setuptools<82",
                "wheel",
            ]
        else:
            overlay_args = [
                python_bin,
                "-m",
                "pip",
                "install",
                "--target",
                build_overlay,
                "--no-cache-dir",
                "--no-input",
                "--disable-pip-version-check",
                "setuptools<82",
                "wheel",
            ]
        overlay_result = subprocess.run(overlay_args, capture_output=True, text=True)
        if overlay_result.returncode != 0:
            shutil.rmtree(build_overlay, ignore_errors=True)
            raise CondaException(
                "Pass B pre-check: could not stage the setuptools<82 + wheel build "
                "overlay in %s.\nstderr: %s" % (build_overlay, overlay_result.stderr)
            )

    # Map filename -> the local artifact Pass A's lazy_fetch already downloaded
    # and hash-verified, so Pass B installs the verified bits instead of
    # re-downloading the raw URL (which lacks creds/hash and may have changed).
    local_by_filename = {}
    for p in resolved_env.packages:
        if getattr(p, "TYPE", None) != "pypi":
            continue
        lf = p.local_file(p.url_format)
        if lf and os.path.isfile(lf):
            local_by_filename[p.filename] = lf

    # Pass B builds invoke the conda env's own tools (compilers, cmake/ninja,
    # console scripts) that Pass A installed; put <env>/bin first on PATH so the
    # build subprocess finds them rather than the Docker build image's PATH.
    build_env = dict(os.environ)
    build_env["PATH"] = (
        os.path.dirname(python_bin) + os.pathsep + build_env.get("PATH", "")
    )
    if build_overlay:
        # Build subprocesses import the transient setuptools<82 + wheel from the
        # overlay first, without it ever being installed into the env.
        build_env["PYTHONPATH"] = (
            build_overlay + os.pathsep + build_env.get("PYTHONPATH", "")
        )

    _echo(
        " (Pass B: %d deferred sdist(s)) ..." % len(deferred_sdists),
        timestamp=False,
        nl=False,
    )
    try:
        for sdist in deferred_sdists:
            name = sdist.get("name", "<unknown>")
            version = sdist.get("version", "")
            # Prefer the verified local artifact; fall back to the recorded URL.
            source = local_by_filename.get(sdist.get("filename")) or sdist.get(
                "url", ""
            )
            if not source:
                raise CondaException(
                    "Deferred sdist %r has neither a fetched local file nor a "
                    "'url' in %s. Re-deploy to regenerate."
                    % (name, _DEFERRED_BUILDS_PATH)
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
                    source,
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
                    source,
                ]
            result = subprocess.run(
                build_args, capture_output=True, text=True, env=build_env
            )
            if result.returncode != 0:
                raise CondaException(
                    "Failed to build deferred sdist %s==%s from %s\n"
                    "stdout:\n%s\nstderr:\n%s"
                    % (name, version, source, result.stdout, result.stderr)
                )
            _echo(" done.", timestamp=False)
    finally:
        # The build overlay is transient build-time tooling; never ship it.
        if build_overlay:
            shutil.rmtree(build_overlay, ignore_errors=True)


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
    _run_deferred_sdist_builds(env_path, resolved_env)

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
