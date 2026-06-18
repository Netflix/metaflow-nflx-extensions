import os
from abc import ABC, abstractmethod
from typing import Any, Dict, TYPE_CHECKING

from .build_service import _resolve_entry_point

if TYPE_CHECKING:
    from metaflow_extensions.netflixext.plugins.conda.env_descr import EnvID


class ImageRegistry(ABC):
    """Abstract base class for Docker image registries.

    Implement this class and register it under the
    ``metaflow_prebuilt.image_registries`` entry point group to make it
    selectable via ``METAFLOW_PREBUILT_IMAGE_REGISTRY=<name>``.

    For most registries ``push_tag()`` and ``pull_tag()`` both delegate to
    ``image_tag()`` — subclasses only need to implement ``image_tag()``.
    Override ``push_tag()`` and/or ``pull_tag()`` independently only when the
    push address differs from the pull address (e.g. ``LocalRegistry``).
    """

    @abstractmethod
    def image_tag(self, env_id: "EnvID") -> str:
        """Canonical fully-qualified image tag for the given env_id.

        Used as the default return value for both ``push_tag()`` and
        ``pull_tag()``. Format: ``<registry>/<namespace>/<name>:<version>``.
        """
        ...

    def push_tag(self, env_id: "EnvID") -> str:
        """Tag the build service uses to push. Defaults to ``image_tag()``."""
        return self.image_tag(env_id)

    def pull_tag(self, env_id: "EnvID") -> str:
        """Tag baked into the remote runner spec. Defaults to ``image_tag()``."""
        return self.image_tag(env_id)

    def image_tag_for_named(self, name: str) -> str:
        """Full mutable tag for a ``@named_env(fetch_at_exec=True)`` env.

        Tagged by alias name (not env_id hash) so subsequent deploys under the
        same name overwrite the manifest — matching fetch_at_exec semantics.

        Raises ``NotImplementedError`` by default; registries that support named
        envs must override this method.
        """
        raise NotImplementedError(
            "Registry %r does not implement named env tags. "
            "Override image_tag_for_named() to use @named_env(fetch_at_exec=True)."
            % type(self).__name__
        )

    def push_tag_for_named(self, name: str) -> str:
        """Push-side tag for a named env. Defaults to ``image_tag_for_named()``."""
        return self.image_tag_for_named(name)

    def pull_tag_for_named(self, name: str) -> str:
        """Pull-side tag for a named env. Defaults to ``push_tag_for_named()``."""
        return self.push_tag_for_named(name)

    @abstractmethod
    def push_credentials(self) -> Dict[str, Any]:
        """Credentials/config passed to ``DockerBuildService.build_and_push``.

        Return ``{}`` when the build service uses ambient auth (e.g. docker login).
        The schema is build-service-specific; see the ``DockerBuildService``
        contract for which keys each service consumes.
        """
        ...

    @abstractmethod
    def pull_config(self, pull_tag: str) -> Dict[str, Any]:
        """Attributes to inject into the remote runner decorator so it can pull
        the image at ``pull_tag``.

        Return ``{}`` when the runner has ambient pull credentials (e.g. an IAM
        role for ECR + Batch, or a service account for GKE).

        Non-empty example (private registry with K8s imagePullSecrets)::

            {"image_pull_policy": "Always", "image_pull_secrets": "my-secret"}
        """
        ...

    @classmethod
    def from_config(cls) -> "ImageRegistry":
        """Resolve and instantiate the registry selected by
        ``METAFLOW_PREBUILT_IMAGE_REGISTRY`` (default: ``dockerhub``).

        Raises ``MetaflowException`` if the name is not registered.
        """
        name = os.environ.get("METAFLOW_PREBUILT_IMAGE_REGISTRY", "dockerhub")
        registry_cls = _resolve_entry_point("metaflow_prebuilt.image_registries", name)
        return registry_cls()
