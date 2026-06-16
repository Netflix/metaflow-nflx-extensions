"""Integration test: LocalDockerBuildService + LocalRegistry.

Requires:
  - Docker daemon running
  - A local registry at localhost:5000 (``docker run -d -p 5000:5000 registry:2``)

Skipped automatically if Docker is unavailable or the registry is unreachable.
"""
import os
import subprocess

import pytest


def _docker_available() -> bool:
    try:
        r = subprocess.run(
            ["docker", "info"], capture_output=True, timeout=10
        )
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _registry_available(host: str = "localhost:5000") -> bool:
    try:
        r = subprocess.run(
            ["curl", "-sf", "http://%s/v2/" % host],
            capture_output=True,
            timeout=5,
        )
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available() or not _registry_available(),
    reason="Docker daemon or local registry (localhost:5000) not available",
)


@pytest.fixture
def local_env(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH", "localhost:5000")
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL", "localhost:5000")
    monkeypatch.setenv("METAFLOW_PREBUILT_IMAGE_NAMESPACE", "metaflow-prebuilt-test")
    monkeypatch.setenv("METAFLOW_PREBUILT_BUILD_SERVICE", "docker")
    monkeypatch.setenv("METAFLOW_PREBUILT_IMAGE_REGISTRY", "local")


def test_docker_build_and_push_to_local_registry(local_env, monkeypatch):
    from metaflow_extensions.prebuilt.plugins.conda.services.docker_service import (
        LocalDockerBuildService,
    )
    from metaflow_extensions.prebuilt.plugins.conda.registries.local_registry import (
        LocalRegistry,
    )

    registry = LocalRegistry()
    svc = LocalDockerBuildService()

    # Minimal env_id stub
    class _FakeEnvID:
        req_id = "testreq0"
        full_id = "testful0"
        arch = "linux-64"

    env_id = _FakeEnvID()
    push_tag = registry.push_tag(env_id)
    pull_tag = registry.pull_tag(env_id)

    # Both should be localhost:5000 in this test setup
    assert push_tag == pull_tag

    dockerfile = "FROM scratch\nLABEL test=metaflow-prebuilt-integration\n"
    messages = []

    result = svc.build_and_push(
        dockerfile=dockerfile,
        context_files={},
        image_tag=push_tag,
        push_credentials=registry.push_credentials(),
        echo=lambda m, **kw: messages.append(m),
    )

    assert result is True, "Build/push failed. Messages: %s" % messages

    # Verify image is pullable
    pull_result = subprocess.run(
        ["docker", "pull", pull_tag], capture_output=True, text=True
    )
    assert pull_result.returncode == 0, (
        "docker pull failed: %s" % pull_result.stderr
    )

    # Cleanup
    subprocess.run(["docker", "rmi", push_tag], capture_output=True)
