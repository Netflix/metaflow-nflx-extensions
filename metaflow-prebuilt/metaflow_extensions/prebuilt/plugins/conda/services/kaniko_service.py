import io
import os
import tarfile
import tempfile
import time
from typing import Any, Callable, Dict

from ..build_service import DockerBuildService

_KANIKO_IMAGE = "gcr.io/kaniko-project/executor:latest"
_DEFAULT_POLL_INTERVAL = 10
_DEFAULT_TIMEOUT = 1800


class KanikoBuildService(DockerBuildService):
    """Build service that runs kaniko inside a Kubernetes Job.

    Uploads the build context as a tarball to a GCS or S3 bucket, then
    creates a Kubernetes Job using the kaniko executor image. Polls until
    the Job completes or fails.

    Configure with:
        METAFLOW_PREBUILT_KANIKO_CONTEXT_BUCKET  — GCS (gs://) or S3 (s3://) bucket
        METAFLOW_PREBUILT_KANIKO_SECRET_NAME     — K8s secret with registry creds
        METAFLOW_PREBUILT_KANIKO_NAMESPACE       — K8s namespace (default: default)
        METAFLOW_PREBUILT_KANIKO_IMAGE           — kaniko executor image (override)
    """

    def build_and_push(
        self,
        dockerfile: str,
        context_files: Dict[str, Any],
        image_tag: str,
        push_credentials: Dict[str, Any],
        echo: Callable[..., None],
    ) -> bool:
        bucket = os.environ.get("METAFLOW_PREBUILT_KANIKO_CONTEXT_BUCKET", "")
        if not bucket:
            echo(
                "    ERROR: METAFLOW_PREBUILT_KANIKO_CONTEXT_BUCKET not set. "
                "Provide a GCS (gs://) or S3 (s3://) bucket for build context upload."
            )
            return False

        namespace = os.environ.get("METAFLOW_PREBUILT_KANIKO_NAMESPACE", "default")
        secret_name = os.environ.get("METAFLOW_PREBUILT_KANIKO_SECRET_NAME", "")
        kaniko_image = os.environ.get("METAFLOW_PREBUILT_KANIKO_IMAGE", _KANIKO_IMAGE)

        context_key = "metaflow-prebuilt-context-%d.tar.gz" % int(time.time())

        echo("    Uploading build context to %s/%s ..." % (bucket, context_key))
        try:
            context_bytes = _make_context_tarball(dockerfile, context_files)
            _upload_context(bucket, context_key, context_bytes)
        except Exception as e:
            echo("    ERROR: context upload failed: %s" % e)
            return False

        context_url = "%s/%s" % (bucket.rstrip("/"), context_key)
        job_name = "mf-prebuilt-%d" % int(time.time())

        echo("    Submitting kaniko Job %r ..." % job_name)
        try:
            success = _run_kaniko_job(
                job_name=job_name,
                namespace=namespace,
                kaniko_image=kaniko_image,
                context_url=context_url,
                image_tag=image_tag,
                secret_name=secret_name,
                echo=echo,
            )
        except Exception as e:
            echo("    ERROR: kaniko Job failed: %s" % e)
            return False

        return success


def _make_context_tarball(dockerfile: str, context_files: Dict[str, Any]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        df_bytes = dockerfile.encode("utf-8")
        info = tarfile.TarInfo(name="Dockerfile")
        info.size = len(df_bytes)
        tar.addfile(info, io.BytesIO(df_bytes))
        for fname, content in context_files.items():
            if isinstance(content, str):
                content = content.encode("utf-8")
            else:
                content = bytes(content)
            info = tarfile.TarInfo(name=fname)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def _upload_context(bucket: str, key: str, data: bytes) -> None:
    if bucket.startswith("gs://"):
        try:
            from google.cloud import storage  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "google-cloud-storage is required for GCS context upload. "
                "Install with: pip install 'metaflow-prebuilt[gcr]'"
            )
        client = storage.Client()
        bucket_name = bucket[5:].split("/")[0]
        # Drop empty path components (e.g. from a trailing slash in
        # gs://bucket/prefix/) so blob_path stays aligned with context_url, which
        # is built from bucket.rstrip("/"); otherwise Kaniko looks for the context
        # at prefix/<key> while we upload to prefix//<key>.
        blob_path = "/".join([p for p in bucket[5:].split("/")[1:] if p] + [key])
        client.bucket(bucket_name).blob(blob_path).upload_from_string(data)
    elif bucket.startswith("s3://"):
        try:
            import boto3  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "boto3 is required for S3 context upload. "
                "Install with: pip install 'metaflow-prebuilt[ecr]'"
            )
        parts = bucket[5:].split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] + "/" if len(parts) > 1 and parts[1] else ""
        boto3.client("s3").put_object(Bucket=bucket_name, Key=prefix + key, Body=data)
    else:
        raise ValueError("Unsupported bucket scheme: %s (use gs:// or s3://)" % bucket)


def _run_kaniko_job(
    job_name: str,
    namespace: str,
    kaniko_image: str,
    context_url: str,
    image_tag: str,
    secret_name: str,
    echo: Callable[..., None],
) -> bool:
    try:
        from kubernetes import (
            client as k8s_client,
            config as k8s_config,
        )  # noqa: PLC0415
    except ImportError:
        raise ImportError(
            "kubernetes is required for kaniko builds. "
            "Install with: pip install 'metaflow-prebuilt[kaniko]'"
        )

    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()

    batch_v1 = k8s_client.BatchV1Api()

    args = [
        "--context=%s" % context_url,
        "--dockerfile=Dockerfile",
        "--destination=%s" % image_tag,
    ]

    volume_mounts = []
    volumes = []
    if secret_name:
        volume_mounts.append(
            k8s_client.V1VolumeMount(
                name="kaniko-secret",
                mount_path="/kaniko/.docker",
            )
        )
        volumes.append(
            k8s_client.V1Volume(
                name="kaniko-secret",
                secret=k8s_client.V1SecretVolumeSource(
                    secret_name=secret_name,
                    items=[
                        k8s_client.V1KeyToPath(
                            key=".dockerconfigjson", path="config.json"
                        )
                    ],
                ),
            )
        )

    job = k8s_client.V1Job(
        metadata=k8s_client.V1ObjectMeta(name=job_name, namespace=namespace),
        spec=k8s_client.V1JobSpec(
            backoff_limit=0,
            template=k8s_client.V1PodTemplateSpec(
                spec=k8s_client.V1PodSpec(
                    restart_policy="Never",
                    containers=[
                        k8s_client.V1Container(
                            name="kaniko",
                            image=kaniko_image,
                            args=args,
                            volume_mounts=volume_mounts or None,
                        )
                    ],
                    volumes=volumes or None,
                )
            ),
        ),
    )

    batch_v1.create_namespaced_job(namespace=namespace, body=job)

    deadline = time.time() + _DEFAULT_TIMEOUT
    while time.time() < deadline:
        time.sleep(_DEFAULT_POLL_INTERVAL)
        status = batch_v1.read_namespaced_job_status(name=job_name, namespace=namespace)
        if status.status.succeeded:
            echo("    kaniko Job %r succeeded." % job_name)
            return True
        if status.status.failed:
            echo("    ERROR: kaniko Job %r failed." % job_name)
            return False

    echo("    ERROR: kaniko Job %r timed out after %ds." % (job_name, _DEFAULT_TIMEOUT))
    return False
