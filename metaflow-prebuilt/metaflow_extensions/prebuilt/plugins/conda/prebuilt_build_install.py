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
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import zipfile
import atexit
import ast
import importlib.machinery

from typing import List, Optional

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


def _truthy(value: str) -> bool:
    return value.strip().lower() not in ("", "0", "false", "no", "off")


_MINIMAL_CONFIG_HOME: Optional[str] = None
_MINIMAL_CONFIG_CLEANUP_REGISTERED = False
_MINIMAL_EVENT_LOGGER = "nullSidecarLogger"
_MINIMAL_MONITOR = "nullSidecarMonitor"
_MINIMAL_PLUGIN_ALLOWLIST = {
    "ENVIRONMENT": ("nflx", "conda"),
    "DATASTORE": ("local",),
    "LOGGING_SIDECAR": (_MINIMAL_EVENT_LOGGER,),
    "MONITOR_SIDECAR": (_MINIMAL_MONITOR,),
}


def _metaflow_plugin_categories() -> List[str]:
    """Return Metaflow plugin categories without importing Metaflow.

    This module writes ``METAFLOW_HOME/config.json`` before importing
    ``metaflow``. Importing ``metaflow.extension_support.plugins`` directly would
    execute Metaflow's package init too early, so read the installed source file
    and parse the source for the ``_plugin_categories`` dict keys instead.
    """

    spec = importlib.machinery.PathFinder.find_spec("metaflow", sys.path)
    locations = list(spec.submodule_search_locations or []) if spec else []
    if not locations:
        raise RuntimeError("Cannot locate installed metaflow package")

    plugins_path = os.path.join(locations[0], "extension_support", "plugins.py")
    try:
        with open(plugins_path, encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=plugins_path)
    except OSError as e:
        raise RuntimeError("Cannot read Metaflow plugin categories: %s" % e) from e

    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "_plugin_categories"
            for target in node.targets
        ):
            continue
        if not isinstance(node.value, ast.Dict):
            break
        categories: List[str] = []
        for key in node.value.keys:
            if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                raise RuntimeError(
                    "Metaflow _plugin_categories contains a non-string key"
                )
            categories.append(key.value.upper())
        return categories

    raise RuntimeError("Cannot find Metaflow _plugin_categories")


def _minimal_metaflow_config() -> dict:
    categories = _metaflow_plugin_categories()
    config = {
        "METAFLOW_DEFAULT_EVENT_LOGGER": _MINIMAL_EVENT_LOGGER,
        "METAFLOW_DEFAULT_MONITOR": _MINIMAL_MONITOR,
    }
    config.update(
        {
            "METAFLOW_ENABLED_%s"
            % category: list(_MINIMAL_PLUGIN_ALLOWLIST.get(category, ()))
            for category in categories
        }
    )
    return config


def _cleanup_minimal_metaflow_config() -> None:
    global _MINIMAL_CONFIG_HOME
    config_home = _MINIMAL_CONFIG_HOME
    _MINIMAL_CONFIG_HOME = None
    if not config_home:
        return
    if os.environ.get("METAFLOW_HOME") == config_home:
        os.environ.pop("METAFLOW_HOME", None)
    shutil.rmtree(config_home, ignore_errors=True)


def _install_minimal_metaflow_config() -> None:
    """Keep build-time Metaflow imports from resolving unrelated plugins.

    The base-image Python runs this installer before the target env exists. Some
    base images intentionally do not include every transitive dependency needed
    by all Netflix runtime plugins, so plugin resolution must be restricted to
    the tiny set needed for Conda environment creation.
    """

    if not _truthy(os.environ.get("METAFLOW_PREBUILT_BUILD_CONTAINER", "0")):
        return
    if not _truthy(os.environ.get("METAFLOW_PREBUILT_MINIMAL_PLUGIN_CONFIG", "1")):
        return

    global _MINIMAL_CONFIG_HOME, _MINIMAL_CONFIG_CLEANUP_REGISTERED
    _cleanup_minimal_metaflow_config()
    config_home = tempfile.mkdtemp(prefix="metaflow-prebuilt-config-")
    try:
        plugin_config = _minimal_metaflow_config()
        with open(os.path.join(config_home, "config.json"), "w", encoding="utf-8") as f:
            json.dump(plugin_config, f)

        # Publish METAFLOW_HOME only after config.json exists. Deploy-side
        # parallelism runs separate Docker builds, so these process globals are
        # not shared across step image builds.
        _MINIMAL_CONFIG_HOME = config_home
        if not _MINIMAL_CONFIG_CLEANUP_REGISTERED:
            atexit.register(_cleanup_minimal_metaflow_config)
            _MINIMAL_CONFIG_CLEANUP_REGISTERED = True
        os.environ["METAFLOW_HOME"] = config_home
        config_home = None
    finally:
        if config_home:
            shutil.rmtree(config_home, ignore_errors=True)


def echo_always(msg: str = "", nl: bool = True, err: bool = True, **_: object) -> None:
    print(msg, end="\n" if nl else "", file=sys.stderr if err else sys.stdout)


_install_minimal_metaflow_config()

try:
    from metaflow.metaflow_config import DATASTORE_LOCAL_DIR, CONDA_MAGIC_FILE_V2
    from metaflow.packaging_sys import ContentType, MetaflowCodeContent
    from metaflow_extensions.netflixext.plugins.conda.conda import Conda
    from metaflow_extensions.netflixext.plugins.conda.env_descr import EnvID
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


def setup_conda_manifest() -> None:
    manifest_folder = os.path.join(os.getcwd(), DATASTORE_LOCAL_DIR)
    os.makedirs(manifest_folder, exist_ok=True)
    path_to_manifest = MetaflowCodeContent.get_filename(
        CONDA_MAGIC_FILE_V2, ContentType.OTHER_CONTENT
    )
    if path_to_manifest is None:
        raise RuntimeError(
            "Cannot find the conda manifest file %s in the package"
            % CONDA_MAGIC_FILE_V2
        )
    shutil.move(path_to_manifest, os.path.join(manifest_folder, CONDA_MAGIC_FILE_V2))


def _parse_build_system_requires(text: str) -> List[str]:
    """Return ``[build-system].requires`` from pyproject.toml text.

    Uses tomllib (py3.11+) or tomli when importable; falls back to a tolerant
    regex for the (well-specified) ``requires = [...]`` array under
    ``[build-system]`` so this works even on an env python without a TOML lib.
    """
    data = None
    for mod in ("tomllib", "tomli"):
        try:
            data = __import__(mod).loads(text)
            break
        except Exception:
            data = None
    if isinstance(data, dict):
        bs = data.get("build-system") or {}
        reqs = bs.get("requires")
        return [str(x) for x in reqs] if isinstance(reqs, list) else []
    # Fallback: pull the requires array out of the [build-system] table textually.
    m = re.search(r"^\[build-system\]\s*(.*?)(?=^\[|\Z)", text, re.S | re.M)
    if not m:
        return []
    rm = re.search(r"requires\s*=\s*\[(.*?)\]", m.group(1), re.S)
    if not rm:
        return []
    return [g.group(1) for g in re.finditer(r"""["']([^"']+)["']""", rm.group(1))]


def _build_requires_from_sdist(source: str) -> List[str]:
    """Best-effort ``[build-system].requires`` for a deferred sdist.

    Pass B builds with ``--no-build-isolation``, so the PEP 517 build backend and
    any extra build requirements (Cython, setuptools_scm, hatchling, pybind11, ...)
    must already be importable — build isolation would normally install them. We
    read them from the sdist's pyproject.toml and stage them into the build
    overlay alongside setuptools<82 + wheel.

    Returns requirement strings EXCLUDING setuptools/wheel/pip (handled by the
    overlay / env). Returns ``[]`` for a URL/non-archive source, a missing or
    unreadable pyproject, or no declared requires — the build then proceeds as
    before (best-effort; a legacy setup.py-only sdist needs nothing beyond the
    setuptools<82 + wheel overlay).
    """
    if not source or not os.path.isfile(source):
        return []  # URL source — can't introspect without downloading.
    text = None
    try:
        if source.endswith((".tar.gz", ".tgz", ".tar.bz2", ".tar")):
            with tarfile.open(source) as tf:
                members = [
                    m
                    for m in tf.getmembers()
                    if m.name.count("/") == 1 and m.name.endswith("/pyproject.toml")
                ]
                if members:
                    fh = tf.extractfile(members[0])
                    text = fh.read().decode("utf-8", "replace") if fh else None
        elif source.endswith(".zip"):
            with zipfile.ZipFile(source) as zf:
                names = [
                    n
                    for n in zf.namelist()
                    if n.count("/") == 1 and n.endswith("/pyproject.toml")
                ]
                if names:
                    text = zf.read(names[0]).decode("utf-8", "replace")
    except Exception:
        return []
    if not text:
        return []
    out = []
    for req in _parse_build_system_requires(text):
        base = re.split(r"[<>=!~ \[;]", req.strip(), 1)[0].strip().lower()
        base = base.replace("_", "-")
        if base in ("setuptools", "wheel", "pip"):
            continue  # handled by the setuptools<82 + wheel overlay / the env
        out.append(req.strip())
    return out


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
    clean env without them and fail. Extra ``[build-system].requires`` (e.g.
    ``Cython``, ``setuptools_scm``, ``maturin``) ARE supported:
    ``_build_requires_from_sdist`` reads them from the sdist's ``pyproject.toml``
    and they are staged into the build overlay (with setuptools<82 + wheel) before
    the build. RESIDUAL LIMITATIONS: (a) the overlay is shared across an image's
    sdists rather than isolated per-sdist, so conflicting build-dep versions across
    sdists could interfere (rare; consistent with --no-build-isolation); and
    (b) build requirements needed as on-PATH *binaries* (not import-only) or
    declared only in a URL-only source whose pyproject can't be read without a
    download are not staged.
    """
    python_bin = os.path.join(env_dir, "bin", "python")
    uv_bin = os.path.join(env_dir, "bin", "uv")

    data = _read_deferred_builds()
    if data is None:
        return  # No deferred builds — sdist-free environment.

    deferred_sdists = data.get("sdists", [])
    if not deferred_sdists:
        return  # Wheels-only or empty; Pass B is a no-op.

    # Pass B builds with --no-build-isolation, so the build backend AND any
    # [build-system].requires must already be importable (build isolation would
    # normally provide them). We stage them into a transient "build overlay" dir
    # placed FIRST on the build subprocess' PYTHONPATH — never installed into the
    # env, so the shipped image stays byte-identical to the resolved conda manifest
    # (no pip-over-conda mutation, nothing to restore). The overlay is removed
    # after the builds. Two sources feed it:
    #   (a) setuptools<82 + wheel, when the env's setuptools is missing or >=82
    #       (>=82 dropped pkg_resources, which legacy setup.py files import; the
    #       deploy-side builder constrains <82 for the same reason); and
    #   (b) each sdist's own [build-system].requires (Cython, setuptools_scm,
    #       hatchling, pybind11, ...) read from its pyproject.toml below.
    # build_env is the subprocess env; <env>/bin goes first on PATH so the build
    # finds the conda env's own tools (compilers, cmake/ninja, console scripts).
    build_env = dict(os.environ)
    build_env["PATH"] = (
        os.path.dirname(python_bin) + os.pathsep + build_env.get("PATH", "")
    )
    # dir: overlay path (lazily created); staged: base-name -> the spec last staged,
    # used to warn when a build dep is re-staged at a different version (below).
    overlay_state = {"dir": None, "staged": {}}

    def _ensure_overlay():
        if overlay_state["dir"] is None:
            overlay_state["dir"] = tempfile.mkdtemp(prefix="mf_passb_overlay_")
            build_env["PYTHONPATH"] = (
                overlay_state["dir"] + os.pathsep + build_env.get("PYTHONPATH", "")
            )
        return overlay_state["dir"]

    def _overlay_install(pkgs, what):
        if not pkgs:
            return
        # The overlay is SHARED across an image's deferred sdists. If two sdists
        # declare the same build dep at different versions, the last writer wins —
        # an earlier sdist may have built against the now-replaced version. We can't
        # isolate per-sdist (that fights --no-build-isolation), but we surface it
        # with a warning instead of a silent miscompilation.
        staged = overlay_state["staged"]
        for pkg in pkgs:
            base = re.split(r"[<>=!~ \[;]", pkg.strip(), 1)[0].strip().lower()
            prev = staged.get(base)
            if prev is not None and prev != pkg.strip():
                _echo(
                    " WARNING: Pass B build dep %r re-staged as %r in the shared "
                    "overlay (was %r); an earlier sdist may have built against the "
                    "previous version." % (base, pkg.strip(), prev),
                    timestamp=False,
                )
            staged[base] = pkg.strip()
        overlay = _ensure_overlay()
        _echo(" (staging %s) ..." % what, timestamp=False, nl=False)
        if os.path.isfile(uv_bin):
            args = [
                uv_bin,
                "pip",
                "install",
                "--python",
                python_bin,
                "--target",
                overlay,
                "--no-cache",
                "--no-config",
            ] + list(pkgs)
        else:
            args = [
                python_bin,
                "-m",
                "pip",
                "install",
                "--target",
                overlay,
                "--no-cache-dir",
                "--no-input",
                "--disable-pip-version-check",
            ] + list(pkgs)
        r = subprocess.run(args, capture_output=True, text=True)
        if r.returncode != 0:
            # Remove the partial overlay now; the outer finally also rmtrees it
            # (ignore_errors=True), so this double-remove is intentional and safe.
            shutil.rmtree(overlay, ignore_errors=True)
            raise CondaException(
                "Pass B: could not stage %s into the build overlay.\nstderr: %s"
                % (what, r.stderr)
            )

    # Everything that can CREATE the overlay runs inside try/finally so the
    # transient overlay is always removed — even if the pre-check staging, build-
    # requires staging, or a build raises before the loop.
    try:
        # (a) setuptools<82 + wheel, only when the env doesn't already satisfy it.
        _echo(
            " (Pass B pre-check: setuptools<82 + wheel) ...", timestamp=False, nl=False
        )
        check_result = subprocess.run(
            [python_bin, "-c", _SETUPTOOLS_CHECK], capture_output=True, text=True
        )
        if check_result.returncode != 0:
            _overlay_install(["setuptools<82", "wheel"], "setuptools<82 + wheel")

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

        _echo(
            " (Pass B: %d deferred sdist(s)) ..." % len(deferred_sdists),
            timestamp=False,
            nl=False,
        )
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
            # (b) Stage this sdist's own [build-system].requires (Cython,
            # setuptools_scm, ...) so --no-build-isolation can import them. We
            # stage them ALONGSIDE the controlled setuptools<82 + wheel (rather
            # than honoring a declared setuptools/wheel pin, which _build_requires_
            # from_sdist filtered out): the <82 cap is the deliberate pkg_resources
            # policy and the latest <82 setuptools / latest wheel satisfy any
            # reasonable lower-bound pin, while guaranteeing the build uses a
            # known-good backend regardless of the env's own setuptools version.
            # NOTE: the overlay is shared across all deferred sdists — fine in
            # practice (build deps rarely conflict) and consistent with the
            # --no-build-isolation design (which deliberately shares the env's
            # runtime deps); it is not per-sdist build isolation.
            build_reqs = _build_requires_from_sdist(source)
            if build_reqs:
                _overlay_install(
                    ["setuptools<82", "wheel"] + build_reqs,
                    "build requires for %s (%s)" % (name, ", ".join(build_reqs)),
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
        if overlay_state["dir"]:
            shutil.rmtree(overlay_state["dir"], ignore_errors=True)


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


def main(argv: Optional[List[str]] = None) -> int:
    args = sys.argv if argv is None else argv
    if len(args) != 3:
        print(
            "Usage: python -m metaflow_extensions.prebuilt.plugins.conda."
            "prebuilt_build_install <req_id> <full_id>",
            file=sys.stderr,
        )
        return 2
    try:
        path = install_env(args[1], args[2])
        print(path)
        sys.stdout.flush()
        return 0
    finally:
        _cleanup_minimal_metaflow_config()


if __name__ == "__main__":
    exit_code = main()
    if exit_code == 0:
        os._exit(0)
    sys.exit(exit_code)
