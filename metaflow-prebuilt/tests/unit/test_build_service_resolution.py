"""Unit tests for DockerBuildService.from_config() entry point resolution."""

import os
from unittest.mock import MagicMock, patch

import pytest


def _make_ep(name, cls):
    """Return a mock entry point that loads `cls`."""
    ep = MagicMock()
    ep.name = name
    ep.load.return_value = cls
    return ep


class _FakeService:
    def build_and_push(
        self, dockerfile, context_files, image_tag, push_credentials, echo
    ):
        return True


class _OtherService:
    def build_and_push(
        self, dockerfile, context_files, image_tag, push_credentials, echo
    ):
        return True


def _patch_eps(eps_list):
    """Patch importlib.metadata.entry_points to return eps_list for our group."""

    def fake_entry_points(group=None):
        if group == "metaflow_prebuilt.build_services":
            return eps_list
        return []

    return patch(
        "metaflow_extensions.prebuilt.plugins.conda.build_service.importlib.metadata.entry_points",
        side_effect=fake_entry_points,
    )


def test_from_config_resolves_registered_name(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_BUILD_SERVICE", "fake")
    eps = [_make_ep("fake", _FakeService), _make_ep("other", _OtherService)]
    with _patch_eps(eps):
        from metaflow_extensions.prebuilt.plugins.conda.build_service import (
            DockerBuildService,
        )

        svc = DockerBuildService.from_config()
    assert isinstance(svc, _FakeService)


def test_from_config_defaults_to_docker(monkeypatch):
    monkeypatch.delenv("METAFLOW_PREBUILT_BUILD_SERVICE", raising=False)
    docker_cls = MagicMock()
    docker_cls.return_value = MagicMock()
    eps = [_make_ep("docker", docker_cls)]
    with _patch_eps(eps):
        from metaflow_extensions.prebuilt.plugins.conda.build_service import (
            DockerBuildService,
        )

        DockerBuildService.from_config()
    docker_cls.assert_called_once()


def test_from_config_raises_on_unknown_name(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_BUILD_SERVICE", "nonexistent")
    eps = [_make_ep("docker", _FakeService), _make_ep("kaniko", _OtherService)]
    with _patch_eps(eps):
        from metaflow_extensions.prebuilt.plugins.conda.build_service import (
            DockerBuildService,
        )

        with pytest.raises(Exception) as exc_info:
            DockerBuildService.from_config()
    msg = str(exc_info.value)
    assert "nonexistent" in msg
    assert "docker" in msg
    assert "kaniko" in msg


def test_from_config_raises_with_empty_group(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_BUILD_SERVICE", "docker")
    with _patch_eps([]):
        from metaflow_extensions.prebuilt.plugins.conda.build_service import (
            DockerBuildService,
        )

        with pytest.raises(Exception) as exc_info:
            DockerBuildService.from_config()
    assert "docker" in str(exc_info.value)
