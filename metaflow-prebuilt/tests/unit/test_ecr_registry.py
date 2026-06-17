"""Unit tests for ECRRegistry."""

import pytest

from metaflow_extensions.prebuilt.plugins.conda.registries.ecr_registry import (
    ECRRegistry,
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


def test_image_tag_format(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_ECR_ACCOUNT", "123456789012")
    monkeypatch.setenv("METAFLOW_PREBUILT_ECR_REGION", "us-east-1")
    r = ECRRegistry()
    tag = r.image_tag(_fake_env_id(req="rrr", full="fff"))
    assert (
        tag
        == "123456789012.dkr.ecr.us-east-1.amazonaws.com/metaflow-prebuilt:%s-rrr_fff"
        % PREBUILT_IMAGE_SCHEMA_VERSION
    )


def test_image_tag_custom_namespace(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_ECR_ACCOUNT", "111")
    monkeypatch.setenv("METAFLOW_PREBUILT_ECR_REGION", "eu-west-1")
    monkeypatch.setenv("METAFLOW_PREBUILT_IMAGE_NAMESPACE", "my-repo")
    tag = ECRRegistry().image_tag(_fake_env_id())
    assert "my-repo:" in tag


def test_missing_account_raises(monkeypatch):
    monkeypatch.delenv("METAFLOW_PREBUILT_ECR_ACCOUNT", raising=False)
    monkeypatch.setenv("METAFLOW_PREBUILT_ECR_REGION", "us-east-1")
    with pytest.raises(ValueError, match="METAFLOW_PREBUILT_ECR_ACCOUNT"):
        ECRRegistry().image_tag(_fake_env_id())


def test_missing_region_raises(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_ECR_ACCOUNT", "123")
    monkeypatch.delenv("METAFLOW_PREBUILT_ECR_REGION", raising=False)
    with pytest.raises(ValueError, match="METAFLOW_PREBUILT_ECR_REGION"):
        ECRRegistry().image_tag(_fake_env_id())


def test_pull_config_empty(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_ECR_ACCOUNT", "123")
    monkeypatch.setenv("METAFLOW_PREBUILT_ECR_REGION", "us-east-1")
    assert ECRRegistry().pull_config("any") == {}
