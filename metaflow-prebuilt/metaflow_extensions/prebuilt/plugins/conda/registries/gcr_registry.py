import os
from typing import Any, Dict, TYPE_CHECKING

from ..image_registry import ImageRegistry
from ..prebuilt_conda_environment import PREBUILT_IMAGE_SCHEMA_VERSION, _sanitize_named_env

if TYPE_CHECKING:
    from ..env_descr import EnvID

_DEFAULT_GCR_HOST = "gcr.io"


class GCRRegistry(ImageRegistry):
    """Image registry backed by Google Container Registry (GCR).

    GCR uses the standard Docker registry v2 protocol — any Docker registry
    (including ``registry:2``) can substitute for it in tests by setting
    ``METAFLOW_PREBUILT_GCR_HOST``.

    Configure with:
        METAFLOW_PREBUILT_GCR_PROJECT   — GCP project ID (required for real GCR)
        METAFLOW_PREBUILT_GCR_HOST      — override host (default: gcr.io;
                                          set to e.g. localhost:5000 for local testing)
        METAFLOW_PREBUILT_IMAGE_NAMESPACE — image name (default: metaflow-prebuilt)
    """

    def _host(self) -> str:
        return os.environ.get("METAFLOW_PREBUILT_GCR_HOST", _DEFAULT_GCR_HOST)

    def _project(self) -> str:
        return os.environ.get("METAFLOW_PREBUILT_GCR_PROJECT", "")

    def _namespace(self) -> str:
        return os.environ.get("METAFLOW_PREBUILT_IMAGE_NAMESPACE", "metaflow-prebuilt")

    def _repo(self) -> str:
        project = self._project()
        ns = self._namespace()
        host = self._host()
        if host != _DEFAULT_GCR_HOST:
            # Local/fake registry: skip the GCP project segment
            return "%s/%s" % (host, ns)
        if not project:
            raise ValueError(
                "METAFLOW_PREBUILT_GCR_PROJECT must be set when using real GCR."
            )
        return "%s/%s/%s" % (host, project, ns)

    def image_tag(self, env_id: "EnvID") -> str:
        return "%s:%s-%s_%s" % (
            self._repo(),
            PREBUILT_IMAGE_SCHEMA_VERSION,
            env_id.req_id,
            env_id.full_id,
        )

    def image_tag_for_named(self, name: str) -> str:
        return "%s:%s-name-%s" % (
            self._repo(),
            PREBUILT_IMAGE_SCHEMA_VERSION,
            _sanitize_named_env(name),
        )

    def push_credentials(self) -> Dict[str, Any]:
        return {}

    def pull_config(self, pull_tag: str) -> Dict[str, Any]:
        return {}
