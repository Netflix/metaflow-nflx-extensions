"""Unit tests for LocalDockerBuildService."""

import subprocess
from unittest.mock import MagicMock, call, patch

import pytest

from metaflow_extensions.prebuilt.plugins.conda.services.docker_service import (
    LocalDockerBuildService,
)


def _echo(msg, **kwargs):
    pass


def _run_success(**kwargs):
    r = MagicMock()
    r.returncode = 0
    r.stdout = ""
    r.stderr = ""
    return r


def _run_fail(**kwargs):
    r = MagicMock()
    r.returncode = 1
    r.stdout = "some stdout"
    r.stderr = "some error"
    return r


def test_build_and_push_calls_docker_build_and_push(tmp_path):
    svc = LocalDockerBuildService()
    with patch(
        "subprocess.run", side_effect=[_run_success(), _run_success()]
    ) as mock_run:
        result = svc.build_and_push(
            dockerfile="FROM scratch",
            context_files={"hello.txt": "hello"},
            image_tag="localhost:5000/test:latest",
            push_credentials={},
            echo=_echo,
        )
    assert result is True
    cmds = [c.args[0] for c in mock_run.call_args_list]
    assert any("build" in cmd for cmd in cmds)
    assert any("push" in cmd for cmd in cmds)


def test_build_and_push_returns_false_on_build_failure():
    svc = LocalDockerBuildService()
    with patch("subprocess.run", return_value=_run_fail()):
        result = svc.build_and_push(
            dockerfile="FROM scratch",
            context_files={},
            image_tag="localhost:5000/test:latest",
            push_credentials={},
            echo=_echo,
        )
    assert result is False


def test_build_and_push_returns_false_on_push_failure():
    svc = LocalDockerBuildService()
    with patch(
        "subprocess.run",
        side_effect=[_run_success(), _run_fail()],
    ):
        result = svc.build_and_push(
            dockerfile="FROM scratch",
            context_files={},
            image_tag="localhost:5000/test:latest",
            push_credentials={},
            echo=_echo,
        )
    assert result is False


def test_build_and_push_calls_echo_on_failure():
    messages = []
    svc = LocalDockerBuildService()
    with patch("subprocess.run", return_value=_run_fail()):
        svc.build_and_push(
            dockerfile="FROM scratch",
            context_files={},
            image_tag="localhost:5000/test:latest",
            push_credentials={},
            echo=lambda msg, **kw: messages.append(msg),
        )
    assert any("ERROR" in m for m in messages)


def test_build_and_push_returns_false_when_docker_not_found():
    svc = LocalDockerBuildService()
    with patch("subprocess.run", side_effect=FileNotFoundError("docker not found")):
        result = svc.build_and_push(
            dockerfile="FROM scratch",
            context_files={},
            image_tag="localhost:5000/test:latest",
            push_credentials={},
            echo=_echo,
        )
    assert result is False


def test_build_and_push_writes_context_files(tmp_path):
    svc = LocalDockerBuildService()
    written_dirs = []

    def fake_run(cmd, cwd=None, **kwargs):
        if cwd:
            written_dirs.append(cwd)
        r = MagicMock()
        r.returncode = 0
        r.stdout = r.stderr = ""
        return r

    with patch("subprocess.run", side_effect=fake_run):
        svc.build_and_push(
            dockerfile="FROM scratch",
            context_files={"data.bin": b"\x00\x01\x02"},
            image_tag="localhost:5000/test:latest",
            push_credentials={},
            echo=_echo,
        )

    assert written_dirs, "docker build was not called with a cwd"
