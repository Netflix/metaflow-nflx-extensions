"""E2E: BuildxBuildService + LocalRegistry against registry:2."""

import subprocess

import pytest

from .conftest import REGISTRY_HOST, requires_docker_and_registry
from metaflow_extensions.prebuilt.plugins.conda.services.buildx_service import (
    BuildxBuildService,
)
from metaflow_extensions.prebuilt.plugins.conda.registries.local_registry import (
    LocalRegistry,
)


def _buildx_available() -> bool:
    try:
        r = subprocess.run(
            ["docker", "buildx", "version"], capture_output=True, timeout=10
        )
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


pytestmark = pytest.mark.skipif(
    not _buildx_available(),
    reason="docker buildx not available",
)


@pytest.fixture
def registry_env(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH", REGISTRY_HOST)
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL", REGISTRY_HOST)
    monkeypatch.setenv("METAFLOW_PREBUILT_IMAGE_NAMESPACE", "e2e-buildx-local")
    # Allow push to insecure local registry
    monkeypatch.delenv("METAFLOW_PREBUILT_BUILDX_BUILDER", raising=False)


class _FakeEnvID:
    req_id = "e2ebuildx"
    full_id = "localtest"
    arch = "linux-64"


def test_buildx_builds_and_pushes_image(registry_env, monkeypatch):
    # Ensure buildx can push to insecure registry
    svc = BuildxBuildService()
    reg = LocalRegistry()
    env_id = _FakeEnvID()

    dockerfile = (
        'FROM busybox:latest\nLABEL test=e2e-buildx-local\nCMD ["echo", "ok"]\n'
    )
    tag = reg.push_tag(env_id)
    messages = []

    result = svc.build_and_push(
        dockerfile=dockerfile,
        context_files={},
        image_tag=tag,
        push_credentials=reg.push_credentials(),
        echo=lambda m, **kw: messages.append(m),
    )

    assert result is True, "Buildx failed: %s" % messages
    subprocess.run(["docker", "rmi", tag], capture_output=True)
