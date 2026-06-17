import os
from typing import Any, Dict, TYPE_CHECKING

from ..image_registry import ImageRegistry
from ..prebuilt_conda_environment import (
    PREBUILT_IMAGE_SCHEMA_VERSION,
    _sanitize_named_env,
)

if TYPE_CHECKING:
    from metaflow_extensions.netflixext.plugins.conda.env_descr import EnvID


class ECRRegistry(ImageRegistry):
    """Image registry backed by Amazon ECR.

    Configure with:
        METAFLOW_PREBUILT_ECR_ACCOUNT   — AWS account ID (required)
        METAFLOW_PREBUILT_ECR_REGION    — AWS region (required)
        METAFLOW_PREBUILT_IMAGE_NAMESPACE — repo name (default: metaflow-prebuilt)

    Authentication is handled by the build service. For ``docker``/``buildx``:
    run ``aws ecr get-login-password | docker login`` before deploying.
    For ``codebuild``: the CodeBuild service role handles ECR push via IAM.
    """

    def _host(self) -> str:
        account = os.environ.get("METAFLOW_PREBUILT_ECR_ACCOUNT", "")
        region = os.environ.get("METAFLOW_PREBUILT_ECR_REGION", "")
        if not account or not region:
            raise ValueError(
                "METAFLOW_PREBUILT_ECR_ACCOUNT and METAFLOW_PREBUILT_ECR_REGION "
                "must be set when using the ecr registry."
            )
        return "%s.dkr.ecr.%s.amazonaws.com" % (account, region)

    def _namespace(self) -> str:
        return os.environ.get("METAFLOW_PREBUILT_IMAGE_NAMESPACE", "metaflow-prebuilt")

    def image_tag(self, env_id: "EnvID") -> str:
        return "%s/%s:%s-%s_%s" % (
            self._host(),
            self._namespace(),
            PREBUILT_IMAGE_SCHEMA_VERSION,
            env_id.req_id,
            env_id.full_id,
        )

    def image_tag_for_named(self, name: str) -> str:
        return "%s/%s:%s-name-%s" % (
            self._host(),
            self._namespace(),
            PREBUILT_IMAGE_SCHEMA_VERSION,
            _sanitize_named_env(name),
        )

    def push_credentials(self) -> Dict[str, Any]:
        # TODO(codebuild-region): CodeBuildService prefers this generic "region"
        # over METAFLOW_PREBUILT_CODEBUILD_REGION, so a CodeBuild project in a
        # different region than ECR would build in the wrong region. Expose a
        # separate codebuild region (or omit this key) when pairing ECR+CodeBuild.
        return {
            "region": os.environ.get("METAFLOW_PREBUILT_ECR_REGION", ""),
            "s3_bucket": os.environ.get("METAFLOW_PREBUILT_CODEBUILD_S3_BUCKET", ""),
            "codebuild_project": os.environ.get(
                "METAFLOW_PREBUILT_CODEBUILD_PROJECT", ""
            ),
        }

    def pull_config(self, pull_tag: str) -> Dict[str, Any]:
        return {}
