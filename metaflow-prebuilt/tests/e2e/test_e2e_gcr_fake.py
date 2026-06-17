"""E2E: LocalDockerBuildService + GCRRegistry (host overridden to registry:2).

GCR uses the Docker registry v2 protocol — registry:2 is a drop-in substitute.
This test exercises GCRRegistry tag generation and push end-to-end.
"""

import subprocess

import pytest

from .conftest import REGISTRY_HOST, requires_docker_and_registry
from metaflow_extensions.prebuilt.plugins.conda.services.docker_service import (
    LocalDockerBuildService,
)
from metaflow_extensions.prebuilt.plugins.conda.registries.gcr_registry import (
    GCRRegistry,
)


pytestmark = requires_docker_and_registry


@pytest.fixture
def registry_env(monkeypatch):
    # Override GCR host to point at local registry:2
    monkeypatch.setenv("METAFLOW_PREBUILT_GCR_HOST", REGISTRY_HOST)
    monkeypatch.setenv("METAFLOW_PREBUILT_IMAGE_NAMESPACE", "e2e-gcr-fake")
    monkeypatch.delenv("METAFLOW_PREBUILT_GCR_PROJECT", raising=False)


class _FakeEnvID:
    req_id = "e2egcr"
    full_id = "faketest"
    arch = "linux-64"


def test_gcr_fake_builds_and_pushes_image(registry_env):
    svc = LocalDockerBuildService()
    reg = GCRRegistry()
    env_id = _FakeEnvID()

    dockerfile = "FROM scratch\nLABEL test=e2e-gcr-fake\n"
    tag = reg.push_tag(env_id)

    assert REGISTRY_HOST in tag, "Expected registry host in tag, got: %s" % tag

    messages = []
    result = svc.build_and_push(
        dockerfile=dockerfile,
        context_files={},
        image_tag=tag,
        push_credentials=reg.push_credentials(),
        echo=lambda m, **kw: messages.append(m),
    )

    assert result is True, "Build failed: %s" % messages

    pull = subprocess.run(["docker", "pull", tag], capture_output=True, text=True)
    assert pull.returncode == 0, "docker pull failed: %s" % pull.stderr

    subprocess.run(["docker", "rmi", tag], capture_output=True)


def test_gcr_push_and_pull_tags_equal_when_using_fake_host(registry_env):
    reg = GCRRegistry()
    env_id = _FakeEnvID()
    assert reg.push_tag(env_id) == reg.pull_tag(env_id)
