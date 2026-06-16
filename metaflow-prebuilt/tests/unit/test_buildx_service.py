"""Unit tests for BuildxBuildService."""
from unittest.mock import MagicMock, patch

import pytest

from metaflow_extensions.prebuilt.plugins.conda.services.buildx_service import BuildxBuildService


def _echo(msg, **kw):
    pass


def _ok():
    r = MagicMock()
    r.returncode = 0
    r.stdout = r.stderr = ""
    return r


def _fail():
    r = MagicMock()
    r.returncode = 1
    r.stdout = "out"
    r.stderr = "err"
    return r


def test_build_and_push_uses_push_flag():
    svc = BuildxBuildService()
    with patch("subprocess.run", return_value=_ok()) as mock_run:
        result = svc.build_and_push("FROM scratch", {}, "localhost:5000/test:tag", {}, _echo)
    assert result is True
    cmd = mock_run.call_args.args[0]
    assert "--push" in cmd
    assert "--tag" in cmd


def test_build_and_push_no_builder_by_default(monkeypatch):
    monkeypatch.delenv("METAFLOW_PREBUILT_BUILDX_BUILDER", raising=False)
    svc = BuildxBuildService()
    with patch("subprocess.run", return_value=_ok()) as mock_run:
        svc.build_and_push("FROM scratch", {}, "tag", {}, _echo)
    cmd = mock_run.call_args.args[0]
    assert "--builder" not in cmd


def test_build_and_push_adds_builder_when_set(monkeypatch):
    monkeypatch.setenv("METAFLOW_PREBUILT_BUILDX_BUILDER", "mybuilder")
    svc = BuildxBuildService()
    with patch("subprocess.run", return_value=_ok()) as mock_run:
        svc.build_and_push("FROM scratch", {}, "tag", {}, _echo)
    cmd = mock_run.call_args.args[0]
    assert "--builder" in cmd
    assert "mybuilder" in cmd


def test_returns_false_on_failure():
    svc = BuildxBuildService()
    with patch("subprocess.run", return_value=_fail()):
        assert svc.build_and_push("FROM scratch", {}, "tag", {}, _echo) is False


def test_returns_false_when_docker_not_found():
    svc = BuildxBuildService()
    with patch("subprocess.run", side_effect=FileNotFoundError()):
        assert svc.build_and_push("FROM scratch", {}, "tag", {}, _echo) is False
