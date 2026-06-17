"""Shared fixtures and helpers for e2e tests.

All e2e tests require a running Docker daemon and a local registry.
Set METAFLOW_TEST_REGISTRY_HOST to override the default (localhost:5000).
"""

import os
import subprocess

import pytest

REGISTRY_HOST = os.environ.get("METAFLOW_TEST_REGISTRY_HOST", "localhost:5000")


def docker_available() -> bool:
    try:
        return (
            subprocess.run(
                ["docker", "info"], capture_output=True, timeout=10
            ).returncode
            == 0
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def registry_available() -> bool:
    try:
        return (
            subprocess.run(
                ["curl", "-sf", "http://%s/v2/" % REGISTRY_HOST],
                capture_output=True,
                timeout=5,
            ).returncode
            == 0
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


requires_docker_and_registry = pytest.mark.skipif(
    not docker_available() or not registry_available(),
    reason="Docker daemon or local registry (%s) not available" % REGISTRY_HOST,
)
