"""Unit tests for GCRRegistry."""

import pytest

from metaflow_extensions.prebuilt.plugins.conda.registries.gcr_registry import (
    GCRRegistry,
)
from metaflow_extensions.prebuilt.plugins.conda.prebuilt_conda_environment import (
    PREBUILT_IMAGE_SCHEMA_VERSION,
)


def _fake_env_id(req="abc", full="def"):
    class _E:
        req_id = req
        full_id = full
        arch = "linux-64"

    return _E()


def test_image_tag_format_real_gcr(monkeypatch):
    monkeypatch.delenv("METAFLOW_PREBUILT_GCR_HOST", raising=False)
    monkeypatch.setenv("METAFLOW_PREBUILT_GCR_PROJECT", "my-project")
    tag = GCRRegistry().image_tag(_fake_env_id(req="rrr", full="fff"))
    assert (
        tag
        == "gcr.io/my-project/metaflow-prebuilt:%s-rrr_fff"
        % PREBUILT_IMAGE_SCHEMA_VERSION
    )


def test_image_tag_with_host_override(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_GCR_HOST", "localhost:5000")
    monkeypatch.delenv("METAFLOW_PREBUILT_GCR_PROJECT", raising=False)
    tag = GCRRegistry().image_tag(_fake_env_id())
    assert tag.startswith("localhost:5000/")
    assert "gcr.io" not in tag


def test_missing_project_raises_for_real_gcr(monkeypatch):
    monkeypatch.delenv("METAFLOW_PREBUILT_GCR_HOST", raising=False)
    monkeypatch.delenv("METAFLOW_PREBUILT_GCR_PROJECT", raising=False)
    with pytest.raises(ValueError, match="METAFLOW_PREBUILT_GCR_PROJECT"):
        GCRRegistry().image_tag(_fake_env_id())


def test_missing_project_ok_with_host_override(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_GCR_HOST", "localhost:5000")
    monkeypatch.delenv("METAFLOW_PREBUILT_GCR_PROJECT", raising=False)
    tag = GCRRegistry().image_tag(_fake_env_id())
    assert "localhost:5000" in tag


def test_push_and_pull_tags_equal(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_GCR_HOST", "localhost:5000")
    r = GCRRegistry()
    env_id = _fake_env_id()
    assert r.push_tag(env_id) == r.pull_tag(env_id)


def test_pull_config_empty(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_GCR_HOST", "localhost:5000")
    assert GCRRegistry().pull_config("any") == {}
