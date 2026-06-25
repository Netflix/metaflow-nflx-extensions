import atexit
from contextlib import contextmanager
import fcntl
import gzip
import io
import json
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from typing import Any, Callable, Dict, List, Optional, Tuple, cast

from metaflow.exception import MetaflowException
from metaflow.flowspec import FlowSpec

from .build_service import DockerBuildService, DockerfileBuildOptions
from .image_registry import ImageRegistry

try:
    from metaflow.debug import debug
except ImportError:  # pragma: no cover
    debug = None  # type: ignore[assignment]

try:
    # Netflix-extended metaflow — provides the full conda infrastructure.
    from metaflow.metaflow_config import (
        CONDA_MAGIC_FILE_V2,  # type: ignore[attr-defined]
        CONDA_REMOTE_COMMANDS,  # type: ignore[attr-defined]
    )
    from metaflow_extensions.netflixext.plugins.conda.conda import Conda
    from metaflow_extensions.netflixext.plugins.conda.conda_environment import (
        CondaEnvironment,
    )
    from metaflow_extensions.netflixext.plugins.conda.env_descr import (
        EnvID,
        EnvType,
        PypiPackageSpecification,
    )
    from metaflow_extensions.netflixext.plugins.conda.envsresolver import EnvsResolver
    from metaflow_extensions.netflixext.plugins.conda.utils import CondaException
except ImportError:
    # OSS metaflow — these Netflix-specific names are not available.
    # The environment class still registers and its pure functions are usable;
    # the full deploy flow requires the extended metaflow install.
    CONDA_MAGIC_FILE_V2 = None  # type: ignore[assignment]
    CONDA_REMOTE_COMMANDS = set()  # type: ignore[assignment]
    Conda = None  # type: ignore[assignment]
    EnvID = None  # type: ignore[assignment]
    EnvType = None  # type: ignore[assignment]
    PypiPackageSpecification = None  # type: ignore[assignment]
    EnvsResolver = None  # type: ignore[assignment]
    CondaException = RuntimeError  # type: ignore[assignment,misc]
    try:
        from metaflow.plugins.pypi.conda_environment import CondaEnvironment
    except ImportError:
        from metaflow.metaflow_environment import MetaflowEnvironment as CondaEnvironment  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# Schema version prefix on the image tag. Bump when the Dockerfile shape,
# bootstrap activation contract, env_path layout, or tag scheme changes in a way
# that makes pre-existing images at the same env-id incompatible.
# v32: the generated Dockerfile bootstraps tiny build-time Python dependencies
#      when the base image does not provide them, then removes that bootstrap
#      target in the same RUN as env installation.
# v31: the generated Dockerfile removes Conda package caches in the same RUN as
#      env installation, so package archives/extracts never enter the committed
#      install layer.
# v30: the generated Dockerfile removes Conda package caches after installing
#      the runtime env.
# v29: the registry tag now carries a base-image/arch variant suffix (image
#      identity), so images that share an env but differ by base/arch no longer
#      collide on one tag.
PREBUILT_IMAGE_SCHEMA_VERSION = "v32"

# Docker tag components allow [A-Za-z0-9_.-]; everything else is replaced.
_TAG_SAFE_CHARS = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
)

# Where micromamba creates the env inside the build container AND where the
# runtime container finds it. Same path on both sides so conda binary
# shebangs / RPATHs stay valid without any symlink.
PREBUILT_MAMBA_ROOT_PREFIX = "/opt/metaflow/conda-root"
PREBUILT_ENVS_DIR = os.path.join(PREBUILT_MAMBA_ROOT_PREFIX, "envs")

# Where the MetaflowPackage code package gets extracted inside the build
# container — same shape the runtime entry_point uses.
PREBUILT_BUILD_LOCAL_ROOT = "/opt/metaflow/code-package"

# Names under which the MetaflowPackage tarballs live in the docker build context.
_CODE_PACKAGE_TARBALL_NAME = "job.tar"
_INSTALL_SUPPORT_TARBALL_NAME = "install_support.tar.gz"
_INSTALL_SUPPORT_PREFIXES = (
    ".mf_code/metaflow/",
    ".mf_code/metaflow_extensions/",
    ".mf_meta/",
)
_INSTALL_SUPPORT_FILES = (".mf_install",)

# Wire-format names for the deferred-builds hand-off (schema_version "2"),
# OWNED by metaflow-prebuilt. Must stay in sync with the container-side
# constants in prebuilt_build_install (_DEFERRED_BUILDS_PATH / _DEFERRED_WHEELS_DIR).
_DEFERRED_BUILDS_CONTEXT_NAME = "deferred_builds.json"
_DEFERRED_BUILDS_CONTAINER_PATH = "/app/deferred_builds.json"
_DEFERRED_WHEELS_CONTEXT_DIR = "deferred_wheels"
_DEFERRED_WHEELS_CONTAINER_DIR = "/app/deferred_wheels"
_PREBUILT_REGISTRY_CACHE_ENV = "METAFLOW_PREBUILT_REGISTRY_CACHE"


def _env_flag_enabled(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in ("", "0", "false", "no", "off")


def _env_path_for(env_id: EnvID) -> str:
    return os.path.join(
        PREBUILT_ENVS_DIR, "metaflow_%s_%s" % (env_id.req_id, env_id.full_id)
    )


def _sanitize_named_env(name: str) -> str:
    import hashlib  # noqa: PLC0415

    safe_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
    sanitized = "".join(c if c in safe_chars else "_" for c in name)
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:4]
    return "%s__%s" % (sanitized, digest)


def _env_path_for_named(name: str) -> str:
    return os.path.join(PREBUILT_ENVS_DIR, "named_%s" % _sanitize_named_env(name))


def _named_state_key(name: str) -> str:
    return "name:%s" % name


def _step_state_key(step_name: str) -> str:
    return "step:%s" % step_name


@contextmanager
def _without_inherited_pythonpath():
    sentinel = object()
    original = os.environ.get("PYTHONPATH", sentinel)
    os.environ.pop("PYTHONPATH", None)
    try:
        yield
    finally:
        if original is sentinel:
            os.environ.pop("PYTHONPATH", None)
        else:
            os.environ["PYTHONPATH"] = cast(str, original)


@contextmanager
def _isolate_prebuilt_binary_pythonpath():
    """Hide host PYTHONPATH only from conda/uv binary calls.

    Bazel launchers expose runfiles through PYTHONPATH. Letting that leak into a
    conda-managed Python subprocess can mix stdlib/extension modules from two
    Python installs, but clearing it globally also breaks Metaflow helper
    subprocesses that need Bazel runfiles. Keep the mutation scoped to the
    binary call helpers used by Netflixext conda resolution.
    """

    patched: List[Tuple[Any, str, Any]] = []

    def patch_attr(obj: Any, attr: str, replacement: Any) -> None:
        patched.append((obj, attr, getattr(obj, attr)))
        setattr(obj, attr, replacement)

    def isolated(fn: Callable[..., Any]) -> Callable[..., Any]:
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            with _without_inherited_pythonpath():
                return fn(*args, **kwargs)

        return wrapped

    try:
        from metaflow_extensions.netflixext.plugins.conda import (  # noqa: PLC0415
            conda_flow_mutator,
            utils as conda_utils,
        )

        patch_attr(conda_utils, "call_binary", isolated(conda_utils.call_binary))
        patch_attr(
            conda_flow_mutator,
            "call_binary",
            isolated(conda_flow_mutator.call_binary),
        )
        if Conda is not None:
            patch_attr(Conda, "call_binary", isolated(Conda.call_binary))
            patch_attr(Conda, "call_conda", isolated(Conda.call_conda))
        yield
    finally:
        for obj, attr, original in reversed(patched):
            setattr(obj, attr, original)


def _env_cache_key(env_id: EnvID) -> str:
    """Identity of the baked CONDA ENV — keys ``_prebuilt_env_paths`` and the
    runtime bootstrap lookup. Base-image independent ON PURPOSE: the conda env is
    byte-identical whether it sits on a CPU or GPU base, and ``env_path`` is the
    same, so bootstrap (which cannot know the base image at runtime) resolves it
    without that dimension. Image identity (which DOES depend on the base) is a
    separate key — see ``_image_dedup_key`` / ``_image_variant``."""
    return "%s_%s" % (env_id.req_id, env_id.full_id)


# Back-compat alias: callers that meant the env-identity key.
_image_cache_key = _env_cache_key


def _tag_safe(value: str) -> str:
    return "".join(c if c in _TAG_SAFE_CHARS else "_" for c in value)


def _image_variant(base_image: str, arch: str, build_identity: str = "") -> str:
    """Discriminator for images that share an env (same req_id/full_id) but are
    built on a DIFFERENT base image (CPU vs GPU), for a DIFFERENT target arch, or
    with build-service settings that change image output identity.

    Folded into BOTH the in-process image-dedup key and the registry tag so such
    images never collide on one tag — without it, a CPU-first flow could run a
    GPU step on the CPU base, and a GPU build or alternate layer encoding would
    overwrite the CPU/default image at the same registry tag. ``env_id.arch`` is
    included because the env cache key drops it."""
    import hashlib  # noqa: PLC0415

    safe_arch = _tag_safe(arch or "noarch")
    safe_build_identity = _tag_safe(build_identity).strip("._-")
    digest = hashlib.sha256(
        ("%s\x00%s\x00%s" % (base_image or "", arch or "", build_identity)).encode(
            "utf-8"
        )
    ).hexdigest()[:8]
    if safe_build_identity:
        return "%s-%s-%s" % (safe_arch, safe_build_identity[:32], digest)
    return "%s-%s" % (safe_arch, digest)


def _image_dedup_key(env_cache_key: str, variant: str) -> str:
    """Identity of the IMAGE — keys ``_prebuilt_images`` so two steps that share
    an env but need different bases/arches build (and tag) distinct images."""
    return "%s@%s" % (env_cache_key, variant)


def _tag_with_variant(tag: str, variant: str) -> str:
    """Append the base/arch variant to a registry tag so distinct-base images get
    distinct tags (the variant lands in the tag's version component after ':')."""
    return "%s-%s" % (tag, variant)


# conda subdir -> docker --platform, for cross-arch builds (build for the REMOTE
# step's arch, not the deploy machine's). Only linux subdirs are remote-runnable
# as containers; anything else (osx/win/unknown) maps to None => builder default.
_CONDA_ARCH_TO_DOCKER_PLATFORM = {
    "linux-64": "linux/amd64",
    "linux-aarch64": "linux/arm64",
    "linux-ppc64le": "linux/ppc64le",
    "linux-s390x": "linux/s390x",
}


def _docker_platform_for_arch(arch: str) -> Optional[str]:
    """Docker ``--platform`` for a resolved env's conda arch, or None to leave the
    build service on its builder default (same-arch deploy)."""
    return _CONDA_ARCH_TO_DOCKER_PLATFORM.get(arch or "")


def _base_image_for_step(gpu: bool) -> str:
    if gpu:
        return os.environ.get("METAFLOW_PREBUILT_GPU_BASE_IMAGE", "")
    return os.environ.get("METAFLOW_PREBUILT_BASE_IMAGE", "")


def _step_wants_gpu(step: Any) -> bool:
    """True if any decorator on the step requests a GPU (``@resources(gpu=1)`` or
    ``@titus(gpu=1)``) — selects the GPU base image."""
    return any(
        deco.attributes.get("gpu") not in (None, 0, "0")
        for deco in step.decorators
        if hasattr(deco, "attributes") and "gpu" in (deco.attributes or {})
    )


class PrebuiltCondaEnvironment(CondaEnvironment):
    """Environment that pre-bakes resolved conda environments into Docker
    images. When a task runs on a prebuilt image, bootstrap detects the
    pre-baked env, activates it directly, and skips the standard conda
    download + install.

    Opt in with ``--environment=prebuilt``. There is no silent fallback —
    if the image build fails, the deploy fails with MetaflowException.

    The build service and registry are resolved at deploy time from
    ``METAFLOW_PREBUILT_BUILD_SERVICE`` and ``METAFLOW_PREBUILT_IMAGE_REGISTRY``
    via Python entry points (``metaflow_prebuilt.build_services`` and
    ``metaflow_prebuilt.image_registries``).
    """

    TYPE = "prebuilt"

    # The Python package that provides prebuilt_build_install — the RUN step in
    # the generated Dockerfile calls
    # `python -m <_BUILD_INSTALL_MODULE>.prebuilt_build_install`. This package
    # ships the real installer (it imports the Conda class from
    # metaflow-netflixext, which must also be installed in the build image).
    # Subclasses may override.
    _BUILD_INSTALL_MODULE: str = "metaflow_extensions.prebuilt.plugins.conda"

    _prebuilt_images: Dict[str, str] = {}
    _prebuilt_env_paths: Dict[str, str] = {}
    _STATE_FILE_ENV_VAR = "METAFLOW_PREBUILT_STATE_FILE"

    @classmethod
    def _state_file_path(cls) -> Optional[str]:
        return os.environ.get(cls._STATE_FILE_ENV_VAR)

    @classmethod
    def _cleanup_prebuilt_state(cls, path: str) -> None:
        try:
            os.unlink(path)
        except OSError:
            pass
        if os.environ.get(cls._STATE_FILE_ENV_VAR) == path:
            os.environ.pop(cls._STATE_FILE_ENV_VAR, None)

    @classmethod
    def _persist_prebuilt_state(cls) -> None:
        path = cls._state_file_path()
        if path is None:
            parent_dir = os.environ.get("METAFLOW_TEMPDIR", "/tmp")
            try:
                os.makedirs(parent_dir, exist_ok=True)
                fd, path = tempfile.mkstemp(
                    prefix=".metaflow_prebuilt_state_",
                    suffix=".json",
                    dir=parent_dir,
                )
                os.close(fd)
            except OSError as e:
                raise MetaflowException(
                    "Failed to allocate prebuilt state file in %s: %s."
                    % (parent_dir, e)
                ) from e
            os.environ[cls._STATE_FILE_ENV_VAR] = path
            atexit.register(cls._cleanup_prebuilt_state, path)

        data = json.dumps(
            {"images": cls._prebuilt_images, "env_paths": cls._prebuilt_env_paths}
        )
        tmp_path = path + ".tmp"
        try:
            flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC | os.O_NOFOLLOW
            fd = os.open(tmp_path, flags, 0o600)
            with os.fdopen(fd, "w") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.rename(tmp_path, path)
        except OSError as e:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise MetaflowException(
                "Failed to write prebuilt state file %s: %s." % (path, e)
            ) from e

    @classmethod
    def _load_prebuilt_state(cls) -> None:
        if cls._prebuilt_images and cls._prebuilt_env_paths:
            return
        path = cls._state_file_path()
        if path is None:
            return
        if not os.path.isfile(path):
            raise MetaflowException(
                "Prebuilt state file %s referenced by %s does not exist."
                % (path, cls._STATE_FILE_ENV_VAR)
            )
        try:
            flags = os.O_RDONLY | os.O_NOFOLLOW
            fd = os.open(path, flags)
            with os.fdopen(fd, "r") as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                data = json.load(f)
        except OSError as e:
            raise MetaflowException(
                "Failed to read prebuilt state file %s: %s." % (path, e)
            ) from e
        except ValueError as e:
            raise MetaflowException(
                "Prebuilt state file %s is not valid JSON: %s." % (path, e)
            ) from e
        cls._prebuilt_images = data.get("images", {})
        cls._prebuilt_env_paths = data.get("env_paths", {})

    _init_in_progress: bool = False

    def _make_envs_resolver(self) -> "EnvsResolver":
        """Enable sdist-build deferral for the prebuilt resolution path.

        ``defer_pypi_sdist_build=True`` makes the pip resolver keep sdist
        packages as source specs in the resolved environment instead of building
        wheels on the (possibly cross-arch) deploy machine. _get_or_build_image
        derives those sdists from the resolved env (so it also works on cache
        hits) and the container builds them in Pass B (see prebuilt_build_install).

        NOTE: deferral applies to the whole flow's resolution. For a local /
        non-remote step (e.g. ``python flow.py --environment=prebuilt run``) the
        normal create path builds the deferred sdist at create time from the
        lazy-fetched local source — fine for buildable sdists, subject to the
        same build-requires limitation as Pass B.
        """
        return EnvsResolver(cast(Conda, self.conda), defer_pypi_sdist_build=True)

    def init_environment(self, echo: Callable[..., None]):
        if PrebuiltCondaEnvironment._init_in_progress:
            return
        PrebuiltCondaEnvironment._init_in_progress = True
        try:
            with _isolate_prebuilt_binary_pythonpath():
                super().init_environment(echo)
                self._build_prebuilt_images(echo)
        finally:
            PrebuiltCondaEnvironment._init_in_progress = False

    def _build_prebuilt_images(self, echo: Callable[..., None]):
        self.__class__._prebuilt_images = {}
        self.__class__._prebuilt_env_paths = {}

        registry = ImageRegistry.from_config()
        build_svc = DockerBuildService.from_config()
        build_identity = build_svc.image_identity_suffix()
        if not isinstance(build_identity, str):
            build_identity = ""

        # --- Phase 1: Collect build specs for all remote steps (sequential) ---
        step_specs = []
        base_identity_cache: Dict[Tuple[str, str], str] = {}
        for step in self._flow:
            env_id = self.get_env_id(cast(Conda, self.conda), step.name)
            if env_id is None:
                continue

            is_remote = any(
                deco.name in CONDA_REMOTE_COMMANDS for deco in step.decorators
            )
            if not is_remote:
                continue

            if isinstance(env_id, str):
                # fetch_at_exec named env — resolve alias to a concrete env_id.
                # Apply @{VAR} substitution first (as standard conda does) so a
                # name like 'env-@{NAME}' resolves the concrete 'env-prod' alias.
                # NOTE: env_id_from_alias resolves for the deploy-local arch; a
                # cross-arch deploy (deploy arch != remote step arch) could pick
                # the wrong concrete EnvID — a separate netflixext-side limitation
                # (arch-aware alias resolution), independent of the image variant.
                named_alias = self.sub_envvars_in_envname(env_id)
                resolved_id = cast(Conda, self.conda).env_id_from_alias(named_alias)
                if resolved_id is None:
                    raise MetaflowException(
                        "Named env alias %r referenced by step %r does not "
                        "resolve to any cached environment." % (named_alias, step.name)
                    )
                env_id = resolved_id
                env_key = _named_state_key(named_alias)
                is_named = True
            elif EnvID is None or (EnvID is not None and isinstance(env_id, EnvID)):
                # Regular hash-based env_id (duck-typed in OSS mode).
                env_key = _env_cache_key(env_id)
                is_named = False
                named_alias = None
            else:
                continue

            # Image identity = env identity + base-image/arch variant, so two steps
            # that share an env but need a different base (CPU vs GPU) or arch build
            # and TAG distinct images. env_path stays keyed by env_key (the conda
            # env is byte-identical across bases; bootstrap resolves it without the
            # base dimension, which it cannot know at runtime).
            gpu = _step_wants_gpu(step)
            base_image = _base_image_for_step(gpu)
            if not base_image:
                raise MetaflowException(
                    "No base image configured. Set METAFLOW_PREBUILT_GPU_BASE_IMAGE "
                    "(for GPU steps) or METAFLOW_PREBUILT_BASE_IMAGE."
                )
            arch = getattr(env_id, "arch", "")
            base_identity_key = (base_image, arch)
            base_identity = base_identity_cache.get(base_identity_key)
            if base_identity is None:
                base_identity = registry.base_image_identity(base_image, arch)
                base_identity_cache[base_identity_key] = base_identity
            if base_identity != base_image:
                echo(
                    "    Base image resolved for prebuilt tag: %s -> %s"
                    % (base_image, base_identity)
                )
            build_base_image = base_identity or base_image
            variant = _image_variant(build_base_image, arch, build_identity)
            image_key = _image_dedup_key(env_key, variant)
            env_path = (
                _env_path_for_named(named_alias)
                if is_named and named_alias is not None
                else _env_path_for(env_id)
            )

            step_specs.append(
                {
                    "step": step,
                    "env_id": env_id,
                    "env_key": env_key,
                    "env_path": env_path,
                    "named_alias": named_alias,
                    "image_key": image_key,
                    "base_image": build_base_image,
                    "variant": variant,
                }
            )

        if not step_specs:
            self.__class__._persist_prebuilt_state()
            return

        # --- Phase 2: Build unique images concurrently.
        #     Steps that share the same image_key (same env + base/arch) are
        #     deduped — only the first spec triggers a build; the result is
        #     reused by all steps with that key in Phase 3. ---
        unique_specs: Dict[str, dict] = {}
        for spec in step_specs:
            if spec["image_key"] not in unique_specs:
                unique_specs[spec["image_key"]] = spec

        echo_lock = threading.Lock()

        def _locked_echo(msg: str) -> None:
            with echo_lock:
                echo(msg)

        # The code package is flow-level and identical for every step image. Build
        # it lazily so all-registry-hit deploys skip packaging entirely, but share
        # the bytes across concurrent misses so it is still built at most once.
        code_package_lock = threading.Lock()
        code_package: Dict[str, bytes] = {}

        def _shared_code_package_blob() -> bytes:
            blob = code_package.get("blob")
            if blob is not None:
                return blob
            with code_package_lock:
                blob = code_package.get("blob")
                if blob is None:
                    blob = _build_metaflow_code_package(self, _locked_echo)
                    code_package["blob"] = blob
                return blob

        # keyed by image_key; None means the build failed.
        image_results: Dict[str, Optional[Tuple[str, str]]] = {
            ik: None for ik in unique_specs
        }

        def _build_one(image_key: str, spec: dict) -> None:
            result = self._get_or_build_image(
                spec["env_id"],
                spec["step"],
                _locked_echo,
                registry,
                base_image=spec["base_image"],
                variant=spec["variant"],
                named_alias=spec["named_alias"],
                code_package_blob_supplier=_shared_code_package_blob,
            )
            image_results[image_key] = result

        # METAFLOW_PREBUILT_BUILD_WORKERS caps concurrent image builds.
        # Default: unbounded (one worker per unique image) so all independent
        # step images build in parallel. The builds themselves run remotely via
        # `newt build-docker --remote`, so local resource pressure is minimal.
        configured_workers = os.environ.get("METAFLOW_PREBUILT_BUILD_WORKERS")
        try:
            max_workers = (
                int(configured_workers)
                if configured_workers is not None
                else len(unique_specs)
            )
        except ValueError:
            max_workers = len(unique_specs)
        if max_workers <= 0:
            max_workers = len(unique_specs)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_build_one, ik, spec): ik
                for ik, spec in unique_specs.items()
            }
            failed_keys = []
            for future in as_completed(futures):
                image_key = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    _locked_echo(
                        "    ERROR: image build raised exception for %s: %s"
                        % (image_key, exc)
                    )
                    image_results[image_key] = None
                if image_results[image_key] is None:
                    failed_keys.append(image_key)

        if failed_keys:
            raise MetaflowException(
                "Prebuilt image build failed for %d image(s). "
                "--environment=prebuilt does not fall back to standard conda."
                % len(failed_keys)
            )

        # --- Phase 3: Update class state and rewrite step decorators
        #     (sequential — decorator mutation is not thread-safe). ---
        for spec in step_specs:
            image_key = spec["image_key"]
            result = image_results[image_key]
            assert result is not None  # guaranteed: failed_keys raised above.
            pull_tag, env_path = result

            self.__class__._prebuilt_images[image_key] = pull_tag
            self.__class__._prebuilt_env_paths[spec["env_key"]] = env_path
            self.__class__._prebuilt_env_paths[_step_state_key(spec["step"].name)] = (
                env_path
            )

            pull_config = registry.pull_config(pull_tag)
            found_remote_deco = False
            for deco in spec["step"].decorators:
                if deco.name in CONDA_REMOTE_COMMANDS:
                    prev = deco.attributes.get("image")
                    deco.attributes["image"] = pull_tag
                    for k, v in pull_config.items():
                        deco.attributes[k] = v
                    echo(
                        "    @%s image rewritten for step %s: %r -> %r"
                        % (deco.name, spec["step"].name, prev, pull_tag)
                    )
                    found_remote_deco = True

            if not found_remote_deco:
                raise MetaflowException(
                    "Prebuilt image was built (%s) but no remote compute "
                    "decorator found on step %r to rewrite."
                    % (pull_tag, spec["step"].name)
                )

        self.__class__._persist_prebuilt_state()

    def _get_or_build_image(
        self,
        env_id: EnvID,
        step: Any,
        echo: Callable[..., None],
        registry: ImageRegistry,
        base_image: str,
        variant: str,
        named_alias: Optional[str] = None,
        code_package_blob: Optional[bytes] = None,
        code_package_blob_supplier: Optional[Callable[[], bytes]] = None,
    ) -> Optional[Tuple[str, str]]:
        """Build the prebuilt image and return ``(pull_tag, env_path)`` or
        ``None`` on failure. ``base_image`` and ``variant`` are resolved by the
        caller (``_build_prebuilt_images``); the variant is appended to the
        registry tags so distinct-base images get distinct tags."""
        if named_alias is not None:
            push_tag = _tag_with_variant(
                registry.push_tag_for_named(named_alias), variant
            )
            pull_tag = _tag_with_variant(
                registry.pull_tag_for_named(named_alias), variant
            )
            echo(
                "    Building prebuilt image for @named_env=%r "
                "(resolved to %s/%s) ..."
                % (named_alias, env_id.req_id[:8], env_id.full_id[:8])
            )
        else:
            push_tag = _tag_with_variant(registry.push_tag(env_id), variant)
            pull_tag = _tag_with_variant(registry.pull_tag(env_id), variant)
            echo(
                "    Building prebuilt image for %s/%s ..."
                % (env_id.req_id[:8], env_id.full_id[:8])
            )

        resolved_env = cast(Conda, self.conda).environment(env_id)
        if resolved_env is None:
            echo("    WARNING: Could not find resolved environment for %s" % (env_id,))
            return None

        env_type = resolved_env.env_type
        env_path = (
            _env_path_for_named(named_alias)
            if named_alias is not None
            else _env_path_for(env_id)
        )

        # Regular env-id tags are immutable: if a previous deploy already pushed
        # this exact env/base/arch image, reuse it instead of rebuilding and
        # repushing the large conda layer. Named env tags remain mutable and must
        # rebuild so a new deploy can intentionally move the alias.
        registry_cache_enabled = _env_flag_enabled(_PREBUILT_REGISTRY_CACHE_ENV, True)
        if (
            named_alias is None
            and registry_cache_enabled
            and registry.image_exists(push_tag)
        ):
            echo("    Reusing existing prebuilt image: %s" % push_tag)
            return pull_tag, env_path
        if named_alias is None and not registry_cache_enabled:
            echo(
                "    Registry prebuilt image reuse disabled by %s; rebuilding: %s"
                % (_PREBUILT_REGISTRY_CACHE_ENV, push_tag)
            )

        if code_package_blob is None:
            try:
                if code_package_blob_supplier is not None:
                    code_package_blob = code_package_blob_supplier()
                else:
                    code_package_blob = _build_metaflow_code_package(self, echo)
            except Exception as e:
                echo("    ERROR: failed to build metaflow code package: %s" % e)
                return None

        # Derive deferred sdists DIRECTLY from this image's resolved env: a pypi
        # package that is a source dist (url_format != ".whl"), web-downloadable,
        # and has NO pre-built wheel was left unbuilt by the resolver
        # (defer_pypi_sdist_build) and must be built in the container (Pass B).
        # Sourcing from resolved_env (not the resolver's transient list) is also
        # correct on cache hits and is naturally scoped to this image.
        #
        # The two wheel guards exclude packages that ALREADY have a built wheel
        # (cases D/E — e.g. pylock/conda-lock specs that keep a source url_format
        # but carry a built wheel): _gather_embedded_wheels fetches and embeds
        # that wheel below, so they must NOT be deferred (the container would
        # otherwise rebuild from source, and only_binary=True would drop the
        # source archive in Pass A). Non-web and conda packages are excluded here.
        # "url" is the GUARANTEED build source — the is_downloadable_url() filter
        # below ensures it is a real, fetchable URL. "filename" is only a best-effort
        # hint: Pass B prefers the hash-verified local artifact matched by filename
        # and falls back to "url" when there's no match (so a None/empty filename is
        # harmless). Pass B raises only if BOTH are unusable.
        deferred_sdists: List[Any] = [
            {
                "name": p.package_name,
                "version": p.package_version,
                "url": p.url,
                "filename": p.filename,
            }
            for p in resolved_env.packages
            if p.TYPE == "pypi"
            and p.url_format != ".whl"
            and p.is_downloadable_url()
            and p.local_file(".whl") is None
            and p.cached_version(".whl") is None
        ]

        # Materialize wheels for non-web-downloadable (git/local) pypi packages,
        # which the builder can neither fetch (no S3) nor rebuild (no git).
        try:
            embedded_wheels, embedded_wheel_files = _gather_embedded_wheels(
                cast(Conda, self.conda), resolved_env, echo
            )
        except Exception as e:
            echo("    ERROR: failed to gather embedded wheels: %s" % e)
            return None

        build_svc = DockerBuildService.from_config()
        build_options = build_svc.dockerfile_build_options()
        if not isinstance(build_options, DockerfileBuildOptions):
            build_options = DockerfileBuildOptions()
        dockerfile, context_files = _generate_dockerfile(
            base_image,
            env_path,
            env_id,
            env_type,
            resolved_env,
            named_alias=named_alias,
            build_install_module=type(self)._BUILD_INSTALL_MODULE,
            deferred_sdists=deferred_sdists,
            embedded_wheels=embedded_wheels,
            dockerfile_build_options=build_options,
        )
        context_files.update(embedded_wheel_files)
        install_support_blob, runtime_code_blob = (
            _split_code_package_for_prebuilt_build(code_package_blob)
        )
        context_files[_INSTALL_SUPPORT_TARBALL_NAME] = install_support_blob
        context_files[_CODE_PACKAGE_TARBALL_NAME] = runtime_code_blob

        success = build_svc.build_and_push(
            dockerfile,
            context_files,
            push_tag,
            registry.push_credentials(),
            echo,
            target_platform=_docker_platform_for_arch(getattr(env_id, "arch", "")),
        )
        if not success:
            echo("    ERROR: Prebuilt image build/push failed")
            return None

        echo("    Prebuilt image built and pushed: %s" % push_tag)
        return pull_tag, env_path

    def bootstrap_commands(
        self,
        step_name: str,
        datastore_type: str,
    ) -> List[str]:
        self.__class__._load_prebuilt_state()

        env_id = self.get_env_id_noconda(step_name)
        if env_id is None:
            return super().bootstrap_commands(step_name, datastore_type)

        if isinstance(env_id, str):
            # Apply @{VAR} substitution to match how _build_prebuilt_images
            # stored the state (it resolves the alias via sub_envvars_in_envname);
            # otherwise the raw name would miss the registered entry.
            env_id = self.sub_envvars_in_envname(env_id)
            key = _named_state_key(env_id)
            key_descr = "named-alias=%r" % env_id
        elif EnvID is not None and isinstance(env_id, EnvID):
            key = _image_cache_key(env_id)
            key_descr = "env_id=%s" % key
        elif EnvID is None and not isinstance(env_id, str):
            # OSS mode: non-string env_id is treated as a hash-based EnvID duck type
            key = _image_cache_key(env_id)
            key_descr = "env_id=%s" % key
        else:
            return super().bootstrap_commands(step_name, datastore_type)

        # Validate against _prebuilt_env_paths, NOT _prebuilt_images (now keyed by
        # env+base/arch variant, which bootstrap cannot reconstruct at runtime).
        # Prefer the step-scoped path because fetch_at_exec named envs can resolve
        # to the same EnvID as a static named env while needing a different baked
        # path in the image.
        lookup_key = _step_state_key(step_name)
        lookup_descr = "step=%r" % step_name
        if lookup_key not in self.__class__._prebuilt_env_paths:
            lookup_key = key
            lookup_descr = key_descr

        if lookup_key not in self.__class__._prebuilt_env_paths:
            raise MetaflowException(
                "Prebuilt env not registered for step %r %s. "
                "--environment=prebuilt does not fall back to standard conda."
                % (step_name, key_descr)
            )

        env_path = self.__class__._prebuilt_env_paths.get(lookup_key)
        if not env_path:
            raise MetaflowException(
                "Prebuilt image registered for step %r %s but env_path missing."
                % (step_name, lookup_descr)
            )

        return [
            "export CONDA_START=$(date +%s)",
            'python -m %s.prebuilt_runtime_activate "%s"'
            % ("metaflow_extensions.prebuilt.plugins.conda", env_path),
            "export _METAFLOW_CONDA_ENV=$(cat _env_id)",
            # Sanitize host-inherited PYTHONPATH so the conda env uses its own
            # stdlib: keep _escape_trampolines + the first entry + any
            # site-packages/dist-packages entries, dropping bare host stdlib
            # dirs that could shadow the conda stdlib. Save the original to
            # MF_ORIG_PYTHONPATH so env-escape trampolines can restore it.
            # Mirrors metaflow_extensions.netflixext CondaEnvironment.bootstrap_commands.
            "if printenv PYTHONPATH >/dev/null 2>&1; then "
            "export MF_ORIG_PYTHONPATH=$(printenv PYTHONPATH); fi",
            "export PYTHONPATH=$(pwd)/_escape_trampolines:"
            "$(printenv PYTHONPATH | tr ':' '\\n' | "
            "awk 'NR==1 || /site-packages/ || /dist-packages/' | "
            "paste -sd:)",
            "if printenv LD_LIBRARY_PATH >/dev/null 2>&1; then "
            "export MF_ORIG_LD_LIBRARY_PATH=$(printenv LD_LIBRARY_PATH); "
            "export LD_LIBRARY_PATH=$(cat _env_path)/lib:$(printenv LD_LIBRARY_PATH); else "
            "export LD_LIBRARY_PATH=$(cat _env_path)/lib; fi",
            "export CONDA_END=$(date +%s)",
        ]


def _build_metaflow_code_package(
    env: "PrebuiltCondaEnvironment", echo: Callable[..., None]
) -> bytes:
    from metaflow.package import MetaflowPackage  # noqa: PLC0415

    package = MetaflowPackage(env._flow, env, echo)
    blob = package.blob_with_timeout(timeout=600)
    if blob is None:
        raise RuntimeError("MetaflowPackage produced no blob within 600s.")
    data = bytes(blob) if isinstance(blob, (bytes, bytearray)) else blob.read()
    if not data:
        raise RuntimeError("MetaflowPackage blob is empty")
    echo("    Code package: %.1f MB" % (len(data) / (1024 * 1024)))
    return data


def _is_install_support_member(name: str) -> bool:
    return name in _INSTALL_SUPPORT_FILES or name.startswith(_INSTALL_SUPPORT_PREFIXES)


def _write_deterministic_gzip_tar(
    members: List[Tuple[tarfile.TarInfo, bytes]],
) -> bytes:
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="wb", mtime=0) as gz:
        with tarfile.open(fileobj=gz, mode="w") as tar:
            for member, payload in members:
                normalized = tarfile.TarInfo(member.name)
                normalized.size = len(payload) if member.isfile() else 0
                normalized.mode = member.mode
                normalized.type = member.type
                normalized.linkname = member.linkname
                normalized.mtime = 0
                normalized.uid = 0
                normalized.gid = 0
                normalized.uname = ""
                normalized.gname = ""
                if member.isfile():
                    tar.addfile(normalized, io.BytesIO(payload))
                else:
                    tar.addfile(normalized)
    return out.getvalue()


def _split_code_package_for_prebuilt_build(
    code_package_blob: bytes,
) -> Tuple[bytes, bytes]:
    """Split env-install support files from runtime user code.

    The build-time installer only needs packaged Metaflow/extension modules plus
    ``OTHER_CONTENT`` metadata. Extracting that stable subset before the env
    install lets Docker/Newt reuse the expensive env layer when only flow code
    changes. The runtime tarball is extracted after the env install, preserving
    the final image's complete package tree.
    """
    support_members: List[Tuple[tarfile.TarInfo, bytes]] = []
    runtime_members: List[Tuple[tarfile.TarInfo, bytes]] = []
    with tarfile.open(fileobj=io.BytesIO(code_package_blob), mode="r:*") as src:
        for member in src.getmembers():
            if member.isdir():
                continue
            payload = src.extractfile(member).read() if member.isfile() else b""
            if _is_install_support_member(member.name):
                support_members.append((member, payload))
            else:
                runtime_members.append((member, payload))

    if not support_members:
        raise MetaflowException(
            "Failed to split prebuilt code package: no install-support files found."
        )

    return (
        _write_deterministic_gzip_tar(support_members),
        _write_deterministic_gzip_tar(runtime_members),
    )


def _gather_embedded_wheels(
    conda: "Conda",
    resolved_env: Any,
    echo: Callable[..., None],
) -> Tuple[List[Dict[str, Any]], Dict[str, bytes]]:
    """Materialize wheels for pypi packages that need a built wheel embedded.

    Three cases require a wheel to be embedded in the build context:

    B — non-web wheel (git/local, url_format == ".whl"): the build container has
        no S3/git access, so the deploy machine fetches the already-built wheel
        from the conda cache and embeds it.
    D — web-downloadable source WITH a cached built wheel (url_format != ".whl",
        cached_version(".whl") is not None): the container could fetch the source
        but not the wheel, and only_binary=True would drop the source — embed the
        wheel so Pass A installs it offline without a rebuild.
    E — non-web source WITH a cached built wheel: neither source nor wheel is
        web-accessible from the container — embed the wheel as in case D.

    The wheel is retrieved with lazy_fetch_packages on the deploy machine (where
    _storage is live). For a wheel package (B) we fetch it directly; for a
    source package with a cached wheel (D/E) we fetch a synthetic wheel-only,
    cache-only spec — NOT the package itself — because lazy_fetch materializes
    only a package's single most-preferred source and a local source archive
    (which D/E packages may carry from resolution) outranks the cached wheel,
    which would leave the wheel unfetched. The record stores url_format=".whl"
    so _register_embedded_wheels calls add_local_file(".whl", path); _create
    picks .whl first (allowed_formats puts .whl before .tar.gz) and only_binary
    keeps it.

    Returns ``(records, files)`` where ``records`` populates
    ``deferred_builds.json["wheels"]`` and ``files`` maps context paths to bytes.
    """
    candidates = [
        p
        for p in resolved_env.packages
        if p.TYPE == "pypi"
        and (
            # Cases B + E: not web-downloadable (git/local source or wheel).
            not p.is_downloadable_url()
            # Case D: web-downloadable source but a built wheel is in the cache.
            or (p.url_format != ".whl" and p.cached_version(".whl") is not None)
        )
    ]
    records: List[Dict[str, Any]] = []
    files: Dict[str, bytes] = {}
    if not candidates:
        return records, files

    with tempfile.TemporaryDirectory() as tmpdir:
        for p in candidates:
            # We must land the BUILT WHEEL in a local file. lazy_fetch only
            # materializes a package's single most-preferred source, and a local
            # file outranks a cached version (conda.lazy_fetch_packages). So for a
            # source-format package that ALSO carries a local source archive (e.g.
            # a local/git sdist registered during resolution), fetching the
            # package itself would materialize the sdist and leave the cached wheel
            # untouched. We therefore fetch the wheel through a dedicated spec.
            if p.url_format == ".whl":
                # Case B: the package itself IS a (git/local) wheel — fetch it
                # directly (it has no competing source format).
                fetch_spec = p
            else:
                # Cases D/E: a source-format package whose built wheel lives in the
                # conda cache. Fetch a synthetic wheel-only, cache-only spec that
                # carries NO source local file, so the cached ".whl" is the
                # preferred (and only) source and is always materialized.
                cache_whl = p.cached_version(".whl")
                if cache_whl is not None and cache_whl.url:
                    wheel_basename = os.path.basename(cache_whl.url)
                    wheel_fname = wheel_basename[: -len(cache_whl.format)]
                    fetch_spec = PypiPackageSpecification(
                        wheel_fname,
                        # p.url is the SOURCE url and is only a placeholder: with
                        # is_real_url=False + add_cached_version(".whl") below,
                        # lazy_fetch reads the cached wheel and never fetches p.url.
                        p.url,
                        is_real_url=False,  # built wheel: cache-only, never web
                        url_format=".whl",
                        hashes={".whl": cache_whl.hash},
                    )
                    fetch_spec.add_cached_version(".whl", cache_whl)
                else:
                    # Non-web source with no built wheel in cache: it can be
                    # neither fetched nor built in the container — fall through to
                    # the loud failure below.
                    fetch_spec = None

            whl = None
            if fetch_spec is not None:
                conda.lazy_fetch_packages([fetch_spec], None, tmpdir)
                fetched = fetch_spec.local_file(".whl")
                if fetched and os.path.isfile(fetched):
                    whl = fetched
            if whl is None:
                raise CondaException(
                    "Prebuilt: could not materialize a built wheel for package "
                    "'%s' (%s); expected a .whl in the conda cache "
                    "(cached_version('.whl')=%r, url_format=%r). The package must "
                    "have been built during resolution and its wheel be fetchable "
                    "from the conda cache on the deploy machine."
                    % (
                        p.package_name,
                        p.filename,
                        p.cached_version(".whl"),
                        p.url_format,
                    )
                )
            wheel_file = os.path.basename(whl)
            with open(whl, "rb") as fh:
                files["%s/%s" % (_DEFERRED_WHEELS_CONTEXT_DIR, wheel_file)] = fh.read()
            records.append(
                {
                    "name": p.package_name,
                    "version": p.package_version,
                    "filename": p.filename,
                    "wheel_file": wheel_file,
                    # Always ".whl": _register adds it as a .whl local file, which
                    # _create finds first and only_binary keeps.
                    "url_format": ".whl",
                }
            )
            echo(
                "    Embedding prebuilt wheel for %s: %s" % (p.package_name, wheel_file)
            )
    return records, files


def _generate_dockerfile(
    base_image: str,
    env_path: str,
    env_id: EnvID,
    env_type: EnvType,
    resolved_env: Any,
    named_alias: Optional[str] = None,
    build_install_module: str = "metaflow_extensions.prebuilt.plugins.conda",
    deferred_sdists: Optional[List[Any]] = None,
    embedded_wheels: Optional[List[Dict[str, Any]]] = None,
    dockerfile_build_options: Optional[DockerfileBuildOptions] = None,
) -> Tuple[str, Dict[str, Any]]:
    marker_json = json.dumps([env_id.req_id, env_id.full_id, env_id.arch])
    context_files: Dict[str, Any] = {}

    # Write the CURRENT env's deferred-builds hand-off (schema "2") into the
    # build context. It is ALWAYS written (even empty) and ALWAYS COPYed in
    # below, so it OVERWRITES any stale hand-off a base image might already
    # carry at the absolute container path (e.g. a prebuilt-on-prebuilt base set
    # via METAFLOW_PREBUILT_BASE_IMAGE). prebuilt_build_install treats any file
    # at that path as current, so an inherited stale file would otherwise be
    # consumed (wrong env) or fail schema validation. An empty hand-off (no
    # sdists, no wheels) is a valid no-op for the in-container installer.
    sdists = list(deferred_sdists or [])
    wheels = list(embedded_wheels or [])
    context_files[_DEFERRED_BUILDS_CONTEXT_NAME] = json.dumps(
        {"schema_version": "2", "sdists": sdists, "wheels": wheels},
        indent=2,
    ).encode("utf-8")

    lines = [
        "# syntax=docker/dockerfile:1",
        "FROM %s" % base_image,
        "",
        'ENV METAFLOW_CONDA_REMOTE_INSTALLER=""',
        "ENV METAFLOW_DATASTORE_SYSROOT_LOCAL=%s" % PREBUILT_BUILD_LOCAL_ROOT,
        "ENV METAFLOW_EXTRACTED_ROOT=%s" % PREBUILT_BUILD_LOCAL_ROOT,
        "ENV PYTHONPATH=%s/.mf_code" % PREBUILT_BUILD_LOCAL_ROOT,
        "ENV MAMBA_ROOT_PREFIX=%s" % PREBUILT_MAMBA_ROOT_PREFIX,
        "ENV CONDA_ENVS_DIRS=%s" % PREBUILT_ENVS_DIR,
        "",
        "RUN mkdir -p %s" % PREBUILT_BUILD_LOCAL_ROOT,
        "COPY %s /tmp/%s"
        % (_INSTALL_SUPPORT_TARBALL_NAME, _INSTALL_SUPPORT_TARBALL_NAME),
        "RUN tar -xzf /tmp/%s -C %s && rm /tmp/%s"
        % (
            _INSTALL_SUPPORT_TARBALL_NAME,
            PREBUILT_BUILD_LOCAL_ROOT,
            _INSTALL_SUPPORT_TARBALL_NAME,
        ),
        "WORKDIR %s" % PREBUILT_BUILD_LOCAL_ROOT,
        "",
        "RUN mkdir -p %s" % PREBUILT_ENVS_DIR,
    ]

    options = dockerfile_build_options or DockerfileBuildOptions()
    build_install_command = "python -m %s.prebuilt_build_install %s %s" % (
        build_install_module,
        env_id.req_id,
        env_id.full_id,
    )
    bootstrap_command = (
        "BOOTSTRAP=$(mktemp -d) && "
        "(python -c 'import requests' >/dev/null 2>&1 || "
        "(python -m pip --version >/dev/null 2>&1 || "
        "python -m ensurepip --upgrade) && "
        "python -m pip install --disable-pip-version-check --no-cache-dir "
        "--index-url https://pypi.netflix.net/simple --target \"$BOOTSTRAP\" "
        "requests) && "
        "METAFLOW_PREBUILT_BUILD_CONTAINER=1 "
        "PYTHONPATH=\"$BOOTSTRAP:$PYTHONPATH\" %s && "
        "rm -rf \"$BOOTSTRAP\"" % build_install_command
    )
    cleanup_command = "rm -rf %s/pkgs %s/conda-bld /root/.cache/pip" % (
        PREBUILT_MAMBA_ROOT_PREFIX,
        PREBUILT_MAMBA_ROOT_PREFIX,
    )

    if options.buildkit_deferred_input_mounts:
        # Keep the final image clean of any stale hand-off inherited from the
        # base image. The current hand-off and wheels are mounted only for the
        # install RUN step below, so build-only inputs never become pushed image
        # layers.
        lines.append(
            "RUN rm -f %s && rm -rf %s"
            % (_DEFERRED_BUILDS_CONTAINER_PATH, _DEFERRED_WHEELS_CONTAINER_DIR)
        )
        mounts = [
            "--mount=type=bind,source=%s,target=%s,readonly"
            % (_DEFERRED_BUILDS_CONTEXT_NAME, _DEFERRED_BUILDS_CONTAINER_PATH)
        ]
        if wheels:
            mounts.append(
                "--mount=type=bind,source=%s,target=%s,readonly"
                % (_DEFERRED_WHEELS_CONTEXT_DIR, _DEFERRED_WHEELS_CONTAINER_DIR)
            )
        lines.append(
            "RUN %s %s && %s"
            % (" ".join(mounts), bootstrap_command, cleanup_command)
        )
    else:
        # COPY the current deferred-builds hand-off (ALWAYS - this overwrites
        # any stale hand-off inherited from the base image) and any embedded
        # wheels, before the build-install step that consumes them.
        lines.append(
            "COPY %s %s"
            % (_DEFERRED_BUILDS_CONTEXT_NAME, _DEFERRED_BUILDS_CONTAINER_PATH)
        )
        if wheels:
            lines.append(
                "COPY %s %s"
                % (_DEFERRED_WHEELS_CONTEXT_DIR, _DEFERRED_WHEELS_CONTAINER_DIR)
            )
        lines.append("RUN %s && %s" % (bootstrap_command, cleanup_command))
    lines.extend(
        [
            "COPY %s /tmp/%s"
            % (_CODE_PACKAGE_TARBALL_NAME, _CODE_PACKAGE_TARBALL_NAME),
            "RUN tar -xzf /tmp/%s -C %s && rm /tmp/%s"
            % (
                _CODE_PACKAGE_TARBALL_NAME,
                PREBUILT_BUILD_LOCAL_ROOT,
                _CODE_PACKAGE_TARBALL_NAME,
            ),
        ]
    )

    if named_alias is not None:
        real_env_path = _env_path_for(env_id)
        named_env_path = env_path
        lines.append("")
        lines.append("RUN ln -sfT %s %s" % (real_env_path, named_env_path))
        marker_check_path = named_env_path
    else:
        marker_check_path = env_path

    lines.extend(
        [
            "",
            "RUN test -f %s/.metaflowenv || "
            '(echo "Prebuilt env missing .metaflowenv at %s" >&2 && exit 1)'
            % (marker_check_path, marker_check_path),
            "RUN echo '%s' > %s/.metaflowenv" % (marker_json, _env_path_for(env_id)),
        ]
    )

    return "\n".join(lines) + "\n", context_files
