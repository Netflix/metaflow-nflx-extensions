"""Unit tests for CodeBuildService — mocks boto3, no real AWS needed."""

from unittest.mock import MagicMock, patch

import pytest

from metaflow_extensions.prebuilt.plugins.conda.services.codebuild_service import (
    CodeBuildService,
    _make_context_zip,
)


def _echo(msg, **kw):
    pass


def _make_boto3_mock(build_status="SUCCEEDED"):
    boto3_mock = MagicMock()
    s3_client = MagicMock()
    cb_client = MagicMock()
    cb_client.start_build.return_value = {"build": {"id": "proj:abc123"}}
    cb_client.batch_get_builds.return_value = {
        "builds": [{"buildStatus": build_status}]
    }
    boto3_mock.client.side_effect = lambda svc, **kw: (
        s3_client if svc == "s3" else cb_client
    )
    return boto3_mock, s3_client, cb_client


def test_make_context_zip_contains_dockerfile():
    import zipfile, io

    data = _make_context_zip("FROM scratch", {"hello.txt": "hi"})
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = zf.namelist()
    assert "Dockerfile" in names
    assert "hello.txt" in names


def test_build_and_push_calls_s3_and_codebuild(monkeypatch):
    import sys

    svc = CodeBuildService()
    creds = {
        "s3_bucket": "my-bucket",
        "codebuild_project": "my-project",
        "region": "us-east-1",
    }
    boto3_mock, s3_client, cb_client = _make_boto3_mock("SUCCEEDED")
    with patch.dict(sys.modules, {"boto3": boto3_mock}):
        result = svc.build_and_push("FROM scratch", {}, "tag", creds, _echo)
    assert result is True
    s3_client.put_object.assert_called_once()
    cb_client.start_build.assert_called_once()
    cb_client.batch_get_builds.assert_called()


def test_build_and_push_returns_false_on_failure(monkeypatch):
    svc = CodeBuildService()
    creds = {"s3_bucket": "b", "codebuild_project": "p", "region": "us-east-1"}
    boto3_mock, _, _ = _make_boto3_mock("FAILED")
    import sys

    with patch.dict(sys.modules, {"boto3": boto3_mock}):
        assert svc.build_and_push("FROM scratch", {}, "tag", creds, _echo) is False


def test_build_and_push_returns_false_when_missing_config():
    svc = CodeBuildService()
    assert svc.build_and_push("FROM scratch", {}, "tag", {}, _echo) is False
