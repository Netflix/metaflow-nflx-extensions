import importlib.metadata
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    pass


def _resolve_entry_point(group: str, name: str) -> Any:
    try:
        eps = importlib.metadata.entry_points(group=group)
    except TypeError:
        # Python 3.8 compatibility
        eps = importlib.metadata.entry_points().get(group, [])  # type: ignore[assignment]
    mapping = {ep.name: ep for ep in eps}
    if name not in mapping:
        from metaflow.exception import MetaflowException  # noqa: PLC0415

        raise MetaflowException(
            "No entry point named %r in group %r. "
            "Installed: %s. "
            "Install the package that provides this backend or check "
            "METAFLOW_PREBUILT_BUILD_SERVICE." % (name, group, sorted(mapping))
        )
    return mapping[name].load()


@dataclass(frozen=True)
class DockerfileBuildOptions:
    """Build-backend capabilities that influence Dockerfile generation.

    The default Dockerfile stays maximally portable: files needed during the
    install step are copied into the image before ``RUN`` consumes them. Builders
    that use BuildKit can ask the generator to bind-mount those files instead,
    avoiding build-only artifacts becoming pushed image layers.
    """

    buildkit_deferred_input_mounts: bool = False


class DockerBuildService(ABC):
    """Abstract base class for Docker image build-and-push backends.

    Implement this class and register it under the
    ``metaflow_prebuilt.build_services`` entry point group to make it
    selectable via ``METAFLOW_PREBUILT_BUILD_SERVICE=<name>``.
    """

    @abstractmethod
    def build_and_push(
        self,
        dockerfile: str,
        context_files: Dict[str, Any],
        image_tag: str,
        push_credentials: Dict[str, Any],
        echo: Callable[..., None],
        target_platform: Optional[str] = None,
    ) -> bool:
        """Build a Docker image and push it to the registry.

        Args:
            dockerfile: Full Dockerfile content.
            context_files: Files to include in the build context alongside the
                Dockerfile. Keys are filenames; values are ``str`` (text) or
                ``bytes`` (binary).
            image_tag: Fully-qualified destination tag, e.g.
                ``registry.example.com/ns/name:v28-abc123``.
                Provided by ``ImageRegistry.push_tag()``.
            push_credentials: Opaque dict from ``ImageRegistry.push_credentials()``.
                Schema is defined by the paired registry implementation.
            echo: Callable for user-visible progress output.
            target_platform: Docker platform string for the REMOTE step's arch
                (e.g. ``"linux/amd64"``, ``"linux/arm64"``), derived from the
                resolved env's arch. ``None`` means build for the builder's default
                platform. Local builders (buildx/docker/kaniko) should honor it so a
                cross-arch deploy (e.g. an arm64 laptop deploying a linux-64 step)
                produces an image whose arch matches the resolved manifest; remote
                builders where the platform is fixed by the build environment
                (CodeBuild project compute, Cloudbuild) may ignore it.

        Returns:
            ``True`` on success, ``False`` on recoverable failure.
            Must NOT raise on recoverable failures — return ``False`` and
            call ``echo()`` with a diagnostic. MAY raise on programmer errors.
        """
        ...

    def dockerfile_build_options(self) -> DockerfileBuildOptions:
        """Return Dockerfile-generation options for this build backend.

        Existing third-party build services do not need to implement this; the
        default keeps Dockerfile output unchanged and portable.
        """

        return DockerfileBuildOptions()

    @classmethod
    def from_config(cls) -> "DockerBuildService":
        """Resolve and instantiate the build service selected by
        ``METAFLOW_PREBUILT_BUILD_SERVICE`` (default: ``docker``).

        Raises ``MetaflowException`` if the name is not registered.
        """
        name = os.environ.get("METAFLOW_PREBUILT_BUILD_SERVICE", "docker")
        service_cls = _resolve_entry_point("metaflow_prebuilt.build_services", name)
        return service_cls()
