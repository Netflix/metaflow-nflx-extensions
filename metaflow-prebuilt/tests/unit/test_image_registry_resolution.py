"""Unit tests for ImageRegistry.from_config() entry point resolution."""

from unittest.mock import MagicMock, patch

import pytest


def _make_ep(name, cls):
    ep = MagicMock()
    ep.name = name
    ep.load.return_value = cls
    return ep


class _FakeRegistry:
    def image_tag(self, env_id):
        return "fake/registry:tag"

    def push_credentials(self):
        return {}

    def pull_config(self, pull_tag):
        return {}


def _patch_eps(eps_list):
    def fake_entry_points(group=None):
        if group == "metaflow_prebuilt.image_registries":
            return eps_list
        return []

    return patch(
        "metaflow_extensions.prebuilt.plugins.conda.build_service.importlib.metadata.entry_points",
        side_effect=fake_entry_points,
    )


def test_from_config_resolves_registered_registry(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_IMAGE_REGISTRY", "fake")
    eps = [_make_ep("fake", _FakeRegistry)]
    with _patch_eps(eps):
        from metaflow_extensions.prebuilt.plugins.conda.image_registry import (
            ImageRegistry,
        )

        reg = ImageRegistry.from_config()
    assert isinstance(reg, _FakeRegistry)


def test_from_config_defaults_to_dockerhub(monkeypatch):
    monkeypatch.delenv("METAFLOW_PREBUILT_IMAGE_REGISTRY", raising=False)
    dockerhub_cls = MagicMock()
    dockerhub_cls.return_value = MagicMock()
    eps = [_make_ep("dockerhub", dockerhub_cls)]
    with _patch_eps(eps):
        from metaflow_extensions.prebuilt.plugins.conda.image_registry import (
            ImageRegistry,
        )

        ImageRegistry.from_config()
    dockerhub_cls.assert_called_once()


def test_from_config_raises_on_unknown_registry(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_IMAGE_REGISTRY", "unknown-reg")
    eps = [_make_ep("ecr", _FakeRegistry), _make_ep("gcr", _FakeRegistry)]
    with _patch_eps(eps):
        from metaflow_extensions.prebuilt.plugins.conda.image_registry import (
            ImageRegistry,
        )

        with pytest.raises(Exception) as exc_info:
            ImageRegistry.from_config()
    msg = str(exc_info.value)
    assert "unknown-reg" in msg
    assert "ecr" in msg
    assert "gcr" in msg
