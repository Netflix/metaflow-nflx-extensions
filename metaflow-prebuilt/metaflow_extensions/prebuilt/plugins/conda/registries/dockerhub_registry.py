import os
from typing import Any, Dict, TYPE_CHECKING

from ..image_registry import ImageRegistry
from ..prebuilt_conda_environment import (
    PREBUILT_IMAGE_SCHEMA_VERSION,
    _sanitize_named_env,
)

if TYPE_CHECKING:
    from metaflow_extensions.nflx.plugins.conda.env_descr import EnvID


class DockerHubRegistry(ImageRegistry):
    """Image registry backed by Docker Hub (docker.io).

    Configure with:
        METAFLOW_PREBUILT_DOCKERHUB_USER   — Docker Hub username or org (required)
        METAFLOW_PREBUILT_IMAGE_NAMESPACE  — image name under the user (default: metaflow-prebuilt)

    Push authentication is handled by ``docker login`` on the deploy machine;
    ``push_credentials()`` returns ``{}`` (the docker build service uses ambient auth).
    Remote runners pull without explicit credentials for public images, or with
    ``imagePullSecrets`` for private repos (configure separately on the runner).
    """

    def _namespace(self) -> str:
        user = os.environ.get("METAFLOW_PREBUILT_DOCKERHUB_USER", "")
        name = os.environ.get("METAFLOW_PREBUILT_IMAGE_NAMESPACE", "metaflow-prebuilt")
        if user:
            return "%s/%s" % (user, name)
        return name

    def image_tag(self, env_id: "EnvID") -> str:
        return "%s:%s-%s_%s" % (
            self._namespace(),
            PREBUILT_IMAGE_SCHEMA_VERSION,
            env_id.req_id,
            env_id.full_id,
        )

    def image_tag_for_named(self, name: str) -> str:
        return "%s:%s-name-%s" % (
            self._namespace(),
            PREBUILT_IMAGE_SCHEMA_VERSION,
            _sanitize_named_env(name),
        )

    def push_credentials(self) -> Dict[str, Any]:
        return {}

    def pull_config(self, pull_tag: str) -> Dict[str, Any]:
        return {}
