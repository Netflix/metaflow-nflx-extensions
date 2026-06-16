import io
import os
import time
import zipfile
from typing import Any, Callable, Dict

from ..build_service import DockerBuildService

_DEFAULT_POLL_INTERVAL = 15
_DEFAULT_TIMEOUT = 1800


class CodeBuildService(DockerBuildService):
    """Build service that runs Docker builds via AWS CodeBuild.

    Zips the build context, uploads to S3, triggers a pre-configured
    CodeBuild project, and polls until the build reaches SUCCEEDED or FAILED.

    The CodeBuild project must be pre-configured by the operator with:
      - A ``BUILD_CONTEXT_KEY`` env var override support
      - ECR push permissions on the service role
      - Docker privileged mode enabled

    Configure with:
        METAFLOW_PREBUILT_CODEBUILD_S3_BUCKET   — S3 bucket for context upload
        METAFLOW_PREBUILT_CODEBUILD_PROJECT     — CodeBuild project name
        METAFLOW_PREBUILT_CODEBUILD_REGION      — AWS region (optional, uses boto3 default)
    """

    def build_and_push(
        self,
        dockerfile: str,
        context_files: Dict[str, Any],
        image_tag: str,
        push_credentials: Dict[str, Any],
        echo: Callable[..., None],
    ) -> bool:
        s3_bucket = push_credentials.get(
            "s3_bucket",
            os.environ.get("METAFLOW_PREBUILT_CODEBUILD_S3_BUCKET", ""),
        )
        project = push_credentials.get(
            "codebuild_project",
            os.environ.get("METAFLOW_PREBUILT_CODEBUILD_PROJECT", ""),
        )
        region = push_credentials.get(
            "region",
            os.environ.get("METAFLOW_PREBUILT_CODEBUILD_REGION"),
        )

        if not s3_bucket or not project:
            echo(
                "    ERROR: METAFLOW_PREBUILT_CODEBUILD_S3_BUCKET and "
                "METAFLOW_PREBUILT_CODEBUILD_PROJECT must be set."
            )
            return False

        try:
            import boto3  # noqa: PLC0415
        except ImportError:
            echo(
                "    ERROR: boto3 is required for CodeBuild. "
                "Install with: pip install 'metaflow-prebuilt[codebuild]'"
            )
            return False

        context_key = "metaflow-prebuilt-context-%d.zip" % int(time.time())
        echo("    Uploading build context to s3://%s/%s ..." % (s3_bucket, context_key))

        try:
            zip_bytes = _make_context_zip(dockerfile, context_files)
            s3 = boto3.client("s3", region_name=region)
            s3.put_object(Bucket=s3_bucket, Key=context_key, Body=zip_bytes)
        except Exception as e:
            echo("    ERROR: S3 upload failed: %s" % e)
            return False

        echo("    Starting CodeBuild project %r ..." % project)
        try:
            cb = boto3.client("codebuild", region_name=region)
            response = cb.start_build(
                projectName=project,
                environmentVariablesOverride=[
                    {"name": "BUILD_CONTEXT_KEY", "value": context_key, "type": "PLAINTEXT"},
                    {"name": "IMAGE_TAG", "value": image_tag, "type": "PLAINTEXT"},
                ],
                sourceTypeOverride="S3",
                sourceLocationOverride="%s/%s" % (s3_bucket, context_key),
            )
            build_id = response["build"]["id"]
        except Exception as e:
            echo("    ERROR: CodeBuild start_build failed: %s" % e)
            return False

        echo("    Polling build %r ..." % build_id)
        deadline = time.time() + _DEFAULT_TIMEOUT
        while time.time() < deadline:
            time.sleep(_DEFAULT_POLL_INTERVAL)
            try:
                status_resp = cb.batch_get_builds(ids=[build_id])
                build = status_resp["builds"][0]
                phase = build["buildStatus"]
            except Exception as e:
                echo("    WARNING: poll error: %s" % e)
                continue

            if phase == "SUCCEEDED":
                echo("    CodeBuild SUCCEEDED (%s)" % build_id)
                return True
            if phase in ("FAILED", "FAULT", "STOPPED", "TIMED_OUT"):
                echo("    ERROR: CodeBuild %s (%s)" % (phase, build_id))
                return False

        echo("    ERROR: CodeBuild timed out after %ds." % _DEFAULT_TIMEOUT)
        return False


def _make_context_zip(dockerfile: str, context_files: Dict[str, Any]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("Dockerfile", dockerfile)
        for fname, content in context_files.items():
            if isinstance(content, str):
                zf.writestr(fname, content)
            else:
                zf.writestr(fname, bytes(content))
    return buf.getvalue()
