import atexit
import fcntl
import json
import logging
import os
import shutil
import subprocess
import tempfile
import time

from typing import Any, Callable, Dict, List, Optional, Tuple, cast

from metaflow.exception import MetaflowException
from metaflow.flowspec import FlowSpec

from .build_service import DockerBuildService
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
    from .conda import Conda
    from .conda_environment import CondaEnvironment
    from .env_descr import EnvID, EnvType
except ImportError:
    # OSS metaflow — these Netflix-specific names are not available.
    # The environment class still registers and its pure functions are usable;
    # the full deploy flow requires the extended metaflow install.
    CONDA_MAGIC_FILE_V2 = None  # type: ignore[assignment]
    CONDA_REMOTE_COMMANDS = set()  # type: ignore[assignment]
    Conda = None  # type: ignore[assignment]
    EnvID = None  # type: ignore[assignment]
    EnvType = None  # type: ignore[assignment]
    try:
        from metaflow.plugins.pypi.conda_environment import CondaEnvironment
    except ImportError:
        from metaflow.metaflow_environment import MetaflowEnvironment as CondaEnvironment  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# Schema version prefix on the image tag. Bump when the Dockerfile shape,
# bootstrap activation contract, or env_path layout changes in a way that
# makes pre-existing images at the same env-id incompatible.
PREBUILT_IMAGE_SCHEMA_VERSION = "v28"

# Where micromamba creates the env inside the build container AND where the
# runtime container finds it. Same path on both sides so conda binary
# shebangs / RPATHs stay valid without any symlink.
PREBUILT_MAMBA_ROOT_PREFIX = "/opt/metaflow/conda-root"
PREBUILT_ENVS_DIR = os.path.join(PREBUILT_MAMBA_ROOT_PREFIX, "envs")

# Where the MetaflowPackage code package gets extracted inside the build
# container — same shape the runtime entry_point uses.
PREBUILT_BUILD_LOCAL_ROOT = "/opt/metaflow/code-package"

# Name under which the MetaflowPackage tarball lives in the docker build context.
_CODE_PACKAGE_TARBALL_NAME = "job.tar"


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


def _image_cache_key(env_id: EnvID) -> str:
    return "%s_%s" % (env_id.req_id, env_id.full_id)


def _base_image_for_step(gpu: bool) -> str:
    if gpu:
        return os.environ.get("METAFLOW_PREBUILT_GPU_BASE_IMAGE", "")
    return os.environ.get("METAFLOW_PREBUILT_BASE_IMAGE", "")


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

    # Subclasses must set this to the Python package that provides
    # prebuilt_build_install — the RUN step in the generated Dockerfile
    # calls `python -m <_BUILD_INSTALL_MODULE>.prebuilt_build_install`.
    # The default points to the OSS module for documentation purposes;
    # in practice you need a conda stack (nflx-metaflow or similar) to
    # provide the real implementation.
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
                    "Failed to allocate prebuilt state file in %s: %s." % (parent_dir, e)
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

    def __init__(self, flow: FlowSpec):
        super().__init__(flow)

    def init_environment(self, echo: Callable[..., None]):
        if PrebuiltCondaEnvironment._init_in_progress:
            return
        PrebuiltCondaEnvironment._init_in_progress = True
        try:
            super().init_environment(echo)
            self._build_prebuilt_images(echo)
        finally:
            PrebuiltCondaEnvironment._init_in_progress = False

    def _build_prebuilt_images(self, echo: Callable[..., None]):
        self.__class__._prebuilt_images = {}
        self.__class__._prebuilt_env_paths = {}

        registry = ImageRegistry.from_config()

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
                named_alias = env_id
                resolved_id = cast(Conda, self.conda).env_id_from_alias(named_alias)
                if resolved_id is None:
                    raise MetaflowException(
                        "Named env alias %r referenced by step %r does not "
                        "resolve to any cached environment." % (named_alias, step.name)
                    )
                env_id = resolved_id
                key = _named_state_key(named_alias)
                is_named = True
            elif EnvID is None or (EnvID is not None and isinstance(env_id, EnvID)):
                # Regular hash-based env_id (duck-typed in OSS mode).
                key = _image_cache_key(env_id)
                is_named = False
                named_alias = None
            else:
                continue

            if key in self.__class__._prebuilt_images:
                pull_tag = self.__class__._prebuilt_images[key]
            else:
                result = self._get_or_build_image(
                    env_id, step, echo, registry, named_alias=named_alias
                )
                if result is None:
                    raise MetaflowException(
                        "Prebuilt image build failed for step %r. "
                        "--environment=prebuilt does not fall back to "
                        "standard conda." % step.name
                    )
                pull_tag, env_path = result
                self.__class__._prebuilt_images[key] = pull_tag
                self.__class__._prebuilt_env_paths[key] = env_path

            pull_tag = self.__class__._prebuilt_images[key]
            pull_config = registry.pull_config(pull_tag)

            found_remote_deco = False
            for deco in step.decorators:
                if deco.name in CONDA_REMOTE_COMMANDS:
                    prev = deco.attributes.get("image")
                    deco.attributes["image"] = pull_tag
                    for k, v in pull_config.items():
                        deco.attributes[k] = v
                    echo(
                        "    @%s image rewritten for step %s: %r -> %r"
                        % (deco.name, step.name, prev, pull_tag)
                    )
                    found_remote_deco = True

            if not found_remote_deco:
                raise MetaflowException(
                    "Prebuilt image was built (%s) but no remote compute "
                    "decorator found on step %r to rewrite." % (pull_tag, step.name)
                )

        self.__class__._persist_prebuilt_state()

    def _get_or_build_image(
        self,
        env_id: EnvID,
        step: Any,
        echo: Callable[..., None],
        registry: ImageRegistry,
        named_alias: Optional[str] = None,
    ) -> Optional[Tuple[str, str]]:
        """Build the prebuilt image and return ``(pull_tag, env_path)`` or
        ``None`` on failure."""
        if named_alias is not None:
            push_tag = registry.push_tag_for_named(named_alias)
            pull_tag = registry.pull_tag_for_named(named_alias)
            echo(
                "    Building prebuilt image for @named_env=%r "
                "(resolved to %s/%s) ..."
                % (named_alias, env_id.req_id[:8], env_id.full_id[:8])
            )
        else:
            push_tag = registry.push_tag(env_id)
            pull_tag = registry.pull_tag(env_id)
            echo(
                "    Building prebuilt image for %s/%s ..."
                % (env_id.req_id[:8], env_id.full_id[:8])
            )

        resolved_env = cast(Conda, self.conda).environment(env_id)
        if resolved_env is None:
            echo("    WARNING: Could not find resolved environment for %s" % (env_id,))
            return None

        gpu = any(
            deco.attributes.get("gpu") not in (None, 0, "0")
            for deco in step.decorators
            if hasattr(deco, "attributes") and "gpu" in (deco.attributes or {})
        )
        base_image = _base_image_for_step(gpu)
        if not base_image:
            raise MetaflowException(
                "No base image configured. Set METAFLOW_PREBUILT_GPU_BASE_IMAGE "
                "(for GPU steps) or METAFLOW_PREBUILT_BASE_IMAGE."
            )

        env_type = resolved_env.env_type
        env_path = (
            _env_path_for_named(named_alias)
            if named_alias is not None
            else _env_path_for(env_id)
        )

        try:
            code_package_blob = _build_metaflow_code_package(self, echo)
        except Exception as e:
            echo("    ERROR: failed to build metaflow code package: %s" % e)
            return None

        dockerfile, context_files = _generate_dockerfile(
            base_image,
            env_path,
            env_id,
            env_type,
            resolved_env,
            named_alias=named_alias,
            build_install_module=type(self)._BUILD_INSTALL_MODULE,
        )
        context_files[_CODE_PACKAGE_TARBALL_NAME] = code_package_blob

        build_svc = DockerBuildService.from_config()
        success = build_svc.build_and_push(
            dockerfile, context_files, push_tag, registry.push_credentials(), echo
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

        if key not in self.__class__._prebuilt_images:
            raise MetaflowException(
                "Prebuilt image not registered for step %r %s. "
                "--environment=prebuilt does not fall back to standard conda."
                % (step_name, key_descr)
            )

        env_path = self.__class__._prebuilt_env_paths.get(key)
        if not env_path:
            raise MetaflowException(
                "Prebuilt image registered for step %r %s but env_path missing."
                % (step_name, key_descr)
            )

        return [
            "export CONDA_START=$(date +%s)",
            'python -m %s.prebuilt_runtime_activate "%s"'
            % ("metaflow_extensions.prebuilt.plugins.conda", env_path),
            "export _METAFLOW_CONDA_ENV=$(cat _env_id)",
            "export PYTHONPATH=$(pwd)/_escape_trampolines:$(printenv PYTHONPATH)",
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


def _generate_dockerfile(
    base_image: str,
    env_path: str,
    env_id: EnvID,
    env_type: EnvType,
    resolved_env: Any,
    named_alias: Optional[str] = None,
    build_install_module: str = "metaflow_extensions.prebuilt.plugins.conda",
) -> Tuple[str, Dict[str, Any]]:
    marker_json = json.dumps([env_id.req_id, env_id.full_id, env_id.arch])
    context_files: Dict[str, Any] = {}

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
        "COPY %s /tmp/%s" % (_CODE_PACKAGE_TARBALL_NAME, _CODE_PACKAGE_TARBALL_NAME),
        "RUN tar -xzf /tmp/%s -C %s && rm /tmp/%s"
        % (
            _CODE_PACKAGE_TARBALL_NAME,
            PREBUILT_BUILD_LOCAL_ROOT,
            _CODE_PACKAGE_TARBALL_NAME,
        ),
        "WORKDIR %s" % PREBUILT_BUILD_LOCAL_ROOT,
        "",
        "RUN mkdir -p %s" % PREBUILT_ENVS_DIR,
        "RUN python -m %s.prebuilt_build_install %s %s"
        % (build_install_module, env_id.req_id, env_id.full_id),
    ]

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
