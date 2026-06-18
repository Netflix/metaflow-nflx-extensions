import os
from typing import Any, Dict, TYPE_CHECKING

from ..image_registry import ImageRegistry
from ..prebuilt_conda_environment import (
    PREBUILT_IMAGE_SCHEMA_VERSION,
    _sanitize_named_env,
)

if TYPE_CHECKING:
    from metaflow_extensions.netflixext.plugins.conda.env_descr import EnvID


class LocalRegistry(ImageRegistry):
    """Image registry backed by a local ``registry:2`` container.

    Push and pull addresses differ when the runner is in a container
    (e.g. kind, minikube): the deploy machine pushes to ``localhost:5000``
    but the runner pulls from ``host.docker.internal:5000``.

    Configure with:
        METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH  — push address (e.g. localhost:5000)
        METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL  — pull address (e.g. host.docker.internal:5000)
        METAFLOW_PREBUILT_IMAGE_NAMESPACE      — image name (default: metaflow-prebuilt)

    ``image_tag()`` raises ``NotImplementedError`` because there is no single
    canonical address — use ``push_tag()`` / ``pull_tag()`` directly.
    """

    def _namespace(self) -> str:
        return os.environ.get("METAFLOW_PREBUILT_IMAGE_NAMESPACE", "metaflow-prebuilt")

    def _push_host(self) -> str:
        host = os.environ.get("METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH", "")
        if not host:
            raise ValueError(
                "METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH is not set. "
                "Set it to the local registry push address (e.g. localhost:5000)."
            )
        return host

    def _pull_host(self) -> str:
        host = os.environ.get("METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL", "")
        if not host:
            raise ValueError(
                "METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL is not set. "
                "Set it to the local registry pull address (e.g. host.docker.internal:5000)."
            )
        return host

    def _tag_suffix(self, env_id: "EnvID") -> str:
        return "%s-%s_%s" % (
            PREBUILT_IMAGE_SCHEMA_VERSION,
            env_id.req_id,
            env_id.full_id,
        )

    def _named_tag_suffix(self, name: str) -> str:
        return "%s-name-%s" % (PREBUILT_IMAGE_SCHEMA_VERSION, _sanitize_named_env(name))

    def image_tag(self, env_id: "EnvID") -> str:
        raise NotImplementedError(
            "LocalRegistry has separate push and pull addresses. "
            "Use push_tag() and pull_tag() instead of image_tag()."
        )

    def push_tag(self, env_id: "EnvID") -> str:
        return "%s/%s:%s" % (
            self._push_host(),
            self._namespace(),
            self._tag_suffix(env_id),
        )

    def pull_tag(self, env_id: "EnvID") -> str:
        return "%s/%s:%s" % (
            self._pull_host(),
            self._namespace(),
            self._tag_suffix(env_id),
        )

    def image_tag_for_named(self, name: str) -> str:
        raise NotImplementedError(
            "LocalRegistry has separate push and pull addresses. "
            "Use push_tag_for_named() and pull_tag_for_named() instead."
        )

    def push_tag_for_named(self, name: str) -> str:
        return "%s/%s:%s" % (
            self._push_host(),
            self._namespace(),
            self._named_tag_suffix(name),
        )

    def pull_tag_for_named(self, name: str) -> str:
        return "%s/%s:%s" % (
            self._pull_host(),
            self._namespace(),
            self._named_tag_suffix(name),
        )

    def push_credentials(self) -> Dict[str, Any]:
        return {}

    def pull_config(self, pull_tag: str) -> Dict[str, Any]:
        return {}
