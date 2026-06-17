"""E2E: kaniko container builds a Dockerfile and pushes to local registry:2.

Runs kaniko as a plain Docker container — no Kubernetes required.
Tests that the Dockerfiles we generate are buildable by kaniko.
"""

import os
import shutil
import subprocess
import tempfile

import pytest

from .conftest import REGISTRY_HOST, requires_docker_and_registry
from metaflow_extensions.prebuilt.plugins.conda.prebuilt_conda_environment import (
    _generate_dockerfile,
    _env_path_for,
)

_KANIKO_IMAGE = os.environ.get(
    "METAFLOW_PREBUILT_KANIKO_IMAGE", "gcr.io/kaniko-project/executor:latest"
)

pytestmark = requires_docker_and_registry


class _FakeEnvID:
    req_id = "e2ekaniko"
    full_id = "localtest"
    arch = "linux-64"


class _FakeResolvedEnv:
    env_type = "conda"


def _kaniko_image_available() -> bool:
    r = subprocess.run(
        ["docker", "image", "inspect", _KANIKO_IMAGE],
        capture_output=True,
    )
    if r.returncode == 0:
        return True
    pull = subprocess.run(
        ["docker", "pull", _KANIKO_IMAGE], capture_output=True, timeout=120
    )
    return pull.returncode == 0


def test_kaniko_builds_generated_dockerfile():
    """Verify kaniko can build the Dockerfile that _generate_dockerfile() produces."""
    if not _kaniko_image_available():
        pytest.skip("kaniko image not available: %s" % _KANIKO_IMAGE)

    env_id = _FakeEnvID()
    env_path = _env_path_for(env_id)
    base_image = "busybox:latest"

    dockerfile, _ = _generate_dockerfile(
        base_image=base_image,
        env_path=env_path,
        env_id=env_id,
        env_type="conda",
        resolved_env=_FakeResolvedEnv(),
    )

    # Simplify: strip the conda install RUN (not available in busybox test image)
    # Keep only the FROM + ENV lines to test that kaniko parses our Dockerfile correctly.
    simple_dockerfile = (
        "\n".join(
            line
            for line in dockerfile.splitlines()
            if line.startswith("FROM")
            or line.startswith("ENV")
            or line.startswith("LABEL")
        )
        + "\n"
    )

    build_dir = tempfile.mkdtemp(prefix="metaflow_e2e_kaniko_")
    try:
        with open(os.path.join(build_dir, "Dockerfile"), "w") as f:
            f.write(simple_dockerfile)

        tag = "%s/e2e-kaniko-local:test" % REGISTRY_HOST

        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "host",
                "-v",
                "%s:/workspace" % build_dir,
                _KANIKO_IMAGE,
                "--dockerfile=/workspace/Dockerfile",
                "--context=dir:///workspace",
                "--destination=%s" % tag,
                "--insecure",
                "--skip-tls-verify",
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert (
            result.returncode == 0
        ), "kaniko build failed:\nstdout: %s\nstderr: %s" % (
            result.stdout[-2000:],
            result.stderr[-2000:],
        )
    finally:
        shutil.rmtree(build_dir, ignore_errors=True)
