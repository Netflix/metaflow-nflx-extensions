"""E2E: LocalDockerBuildService + LocalRegistry against registry:2."""
import subprocess

import pytest

from .conftest import REGISTRY_HOST, requires_docker_and_registry
from metaflow_extensions.prebuilt.plugins.conda.services.docker_service import (
    LocalDockerBuildService,
)
from metaflow_extensions.prebuilt.plugins.conda.registries.local_registry import LocalRegistry


pytestmark = requires_docker_and_registry


@pytest.fixture
def registry_env(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH", REGISTRY_HOST)
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL", REGISTRY_HOST)
    monkeypatch.setenv("METAFLOW_PREBUILT_IMAGE_NAMESPACE", "e2e-docker-local")


class _FakeEnvID:
    req_id = "e2edocker"
    full_id = "localtest"
    arch = "linux-64"


def test_docker_builds_and_pushes_image(registry_env):
    svc = LocalDockerBuildService()
    reg = LocalRegistry()
    env_id = _FakeEnvID()

    dockerfile = "FROM scratch\nLABEL test=e2e-docker-local\n"
    tag = reg.push_tag(env_id)
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


def test_pull_config_is_empty(registry_env):
    assert LocalRegistry().pull_config("any:tag") == {}
