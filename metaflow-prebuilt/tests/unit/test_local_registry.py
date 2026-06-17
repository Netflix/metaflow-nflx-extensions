"""Unit tests for LocalRegistry push/pull address split."""

import pytest

from metaflow_extensions.prebuilt.plugins.conda.registries.local_registry import (
    LocalRegistry,
)


def _fake_env_id(req="aaa", full="bbb"):
    class _E:
        req_id = req
        full_id = full
        arch = "linux-64"

    return _E()


def test_push_tag_uses_push_host(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH", "localhost:5000")
    monkeypatch.setenv(
        "METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL", "host.docker.internal:5000"
    )
    r = LocalRegistry()
    tag = r.push_tag(_fake_env_id())
    assert tag.startswith("localhost:5000/")


def test_pull_tag_uses_pull_host(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH", "localhost:5000")
    monkeypatch.setenv(
        "METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL", "host.docker.internal:5000"
    )
    r = LocalRegistry()
    tag = r.pull_tag(_fake_env_id())
    assert tag.startswith("host.docker.internal:5000/")


def test_push_and_pull_tags_differ(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH", "localhost:5000")
    monkeypatch.setenv(
        "METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL", "host.docker.internal:5000"
    )
    r = LocalRegistry()
    env_id = _fake_env_id()
    assert r.push_tag(env_id) != r.pull_tag(env_id)


def test_image_tag_raises(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH", "localhost:5000")
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL", "localhost:5000")
    r = LocalRegistry()
    with pytest.raises(NotImplementedError):
        r.image_tag(_fake_env_id())


def test_missing_push_host_raises(monkeypatch):
    monkeypatch.delenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH", raising=False)
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL", "localhost:5000")
    r = LocalRegistry()
    with pytest.raises(ValueError, match="METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH"):
        r.push_tag(_fake_env_id())


def test_missing_pull_host_raises(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH", "localhost:5000")
    monkeypatch.delenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL", raising=False)
    r = LocalRegistry()
    with pytest.raises(ValueError, match="METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL"):
        r.pull_tag(_fake_env_id())


def test_push_credentials_empty(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH", "localhost:5000")
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL", "localhost:5000")
    assert LocalRegistry().push_credentials() == {}


def test_pull_config_empty(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PUSH", "localhost:5000")
    monkeypatch.setenv("METAFLOW_PREBUILT_LOCAL_REGISTRY_PULL", "localhost:5000")
    assert LocalRegistry().pull_config("any:tag") == {}
