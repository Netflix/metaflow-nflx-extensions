"""Unit tests for _generate_dockerfile()."""

import os
import subprocess
from unittest.mock import MagicMock

import pytest

import json

from metaflow_extensions.prebuilt.plugins.conda.build_service import (
    DockerfileBuildOptions,
)
from metaflow_extensions.prebuilt.plugins.conda.prebuilt_conda_environment import (
    _generate_dockerfile,
    _make_bootstrap_command,
    PREBUILT_BUILD_LOCAL_ROOT,
    PREBUILT_MAMBA_ROOT_PREFIX,
    PREBUILT_ENVS_DIR,
    PREBUILT_IMAGE_SCHEMA_VERSION,
    _DEFERRED_BUILDS_CONTEXT_NAME,
    _env_path_for,
    _env_path_for_named,
)


def _make_env_id(req_id="abc123", full_id="def456", arch="linux-64"):
    env_id = MagicMock()
    env_id.req_id = req_id
    env_id.full_id = full_id
    env_id.arch = arch
    return env_id


def _make_resolved_env(env_type="conda"):
    env = MagicMock()
    env.env_type = env_type
    return env


def _bootstrap_shell_command(dockerfile):
    install_run = next(
        line for line in dockerfile.splitlines() if "prebuilt_build_install" in line
    )
    assert install_run.startswith("RUN ")
    cleanup_marker = " && rm -rf %s/pkgs" % PREBUILT_MAMBA_ROOT_PREFIX
    return install_run[len("RUN ") :].split(cleanup_marker, 1)[0]


def test_generate_dockerfile_regular_has_required_instructions():
    env_id = _make_env_id()
    resolved_env = _make_resolved_env()
    base_image = "ubuntu:22.04"
    env_path = _env_path_for(env_id)

    dockerfile, context_files = _generate_dockerfile(
        base_image, env_path, env_id, "conda", resolved_env
    )

    assert "FROM %s" % base_image in dockerfile
    assert "ENV METAFLOW_CONDA_REMOTE_INSTALLER" in dockerfile
    assert "ENV METAFLOW_EXTRACTED_ROOT=%s" % PREBUILT_BUILD_LOCAL_ROOT in dockerfile
    assert "ENV MAMBA_ROOT_PREFIX=%s" % PREBUILT_MAMBA_ROOT_PREFIX in dockerfile
    assert "ENV CONDA_ENVS_DIRS=%s" % PREBUILT_ENVS_DIR in dockerfile
    assert "COPY job.tar" in dockerfile
    assert "prebuilt_build_install" in dockerfile
    cleanup = "rm -rf %s/pkgs %s/conda-bld /root/.cache/pip" % (
        PREBUILT_MAMBA_ROOT_PREFIX,
        PREBUILT_MAMBA_ROOT_PREFIX,
    )
    install_run = next(
        line for line in dockerfile.splitlines() if "prebuilt_build_install" in line
    )
    assert " && %s" % cleanup in install_run
    assert "RUN %s" % cleanup not in dockerfile
    assert env_id.req_id in dockerfile
    assert env_id.full_id in dockerfile
    assert ".metaflowenv" in dockerfile

    # The deferred-builds hand-off is ALWAYS written (empty here) and ALWAYS
    # COPYed in, so it overwrites any stale hand-off a base image might carry.
    # (The code package itself is added by the caller, not here.)
    assert set(context_files) == {_DEFERRED_BUILDS_CONTEXT_NAME}
    assert json.loads(context_files[_DEFERRED_BUILDS_CONTEXT_NAME]) == {
        "schema_version": "2",
        "sdists": [],
        "wheels": [],
    }
    assert "COPY %s" % _DEFERRED_BUILDS_CONTEXT_NAME in dockerfile


def test_generate_dockerfile_installs_env_before_runtime_code_package():
    env_id = _make_env_id()
    resolved_env = _make_resolved_env()

    dockerfile, _ = _generate_dockerfile(
        "ubuntu:22.04", _env_path_for(env_id), env_id, "conda", resolved_env
    )

    install_support_pos = dockerfile.index("COPY install_support.tar.gz")
    build_install_pos = dockerfile.index("prebuilt_build_install")
    runtime_code_pos = dockerfile.index("COPY job.tar")

    assert install_support_pos < build_install_pos < runtime_code_pos


def test_generate_dockerfile_deferred_handoff_and_wheels():
    """Deferred sdists + embedded wheels land in the hand-off and both COPYs."""
    env_id = _make_env_id()
    resolved_env = _make_resolved_env()
    sdists = [{"name": "deepspeed", "version": "0.14.0", "url": "https://x/d.tar.gz"}]
    wheels = [
        {
            "name": "mypkg",
            "filename": "mypkg-1.0.tar.gz",
            "wheel_file": "mypkg-1.0-py3-none-any.whl",
        }
    ]

    dockerfile, context_files = _generate_dockerfile(
        "ubuntu:22.04",
        _env_path_for(env_id),
        env_id,
        "conda",
        resolved_env,
        deferred_sdists=sdists,
        embedded_wheels=wheels,
    )

    handoff = json.loads(context_files[_DEFERRED_BUILDS_CONTEXT_NAME])
    assert handoff == {"schema_version": "2", "sdists": sdists, "wheels": wheels}
    # Hand-off COPY (always) + the embedded-wheels dir COPY (only when wheels).
    assert "COPY %s" % _DEFERRED_BUILDS_CONTEXT_NAME in dockerfile
    assert dockerfile.count("COPY ") >= 3  # code tarball + hand-off + wheels dir


def test_generate_dockerfile_can_mount_deferred_inputs_with_buildkit():
    env_id = _make_env_id()
    resolved_env = _make_resolved_env()
    wheels = [
        {
            "name": "mypkg",
            "filename": "mypkg-1.0.tar.gz",
            "wheel_file": "mypkg-1.0-py3-none-any.whl",
        }
    ]

    dockerfile, context_files = _generate_dockerfile(
        "ubuntu:22.04",
        _env_path_for(env_id),
        env_id,
        "conda",
        resolved_env,
        embedded_wheels=wheels,
        dockerfile_build_options=DockerfileBuildOptions(
            buildkit_deferred_input_mounts=True
        ),
    )

    assert _DEFERRED_BUILDS_CONTEXT_NAME in context_files
    assert "COPY %s" % _DEFERRED_BUILDS_CONTEXT_NAME not in dockerfile
    assert "COPY deferred_wheels" not in dockerfile
    assert (
        "RUN rm -f /app/deferred_builds.json && rm -rf /app/deferred_wheels"
        in dockerfile
    )

    # Verify structural invariants on the RUN step rather than the full literal
    # string so minor additions (like new flags or index-url changes) don't
    # require updating this test.
    run_line = next(l for l in dockerfile.splitlines() if "--mount=type=bind" in l)
    # Both mounts are present.
    assert (
        "--mount=type=bind,source=deferred_builds.json,"
        "target=/app/deferred_builds.json,readonly" in run_line
    )
    assert (
        "--mount=type=bind,source=deferred_wheels,"
        "target=/app/deferred_wheels,readonly" in run_line
    )
    # The requests check comes before pip install in the RUN line.
    requests_check_pos = run_line.index("python -c 'import requests'")
    pip_install_pos = run_line.index("pip install")
    assert requests_check_pos < pip_install_pos
    # The pip install is in the grouped fallback so it only runs when requests
    # is absent.
    assert "|| ((" in run_line
    # The build-install module runs after the requests bootstrap.
    assert "prebuilt_build_install" in run_line
    assert run_line.index("prebuilt_build_install") > pip_install_pos
    # Conda cache cleanup runs in the same RUN step.
    assert "rm -rf %s/pkgs" % PREBUILT_MAMBA_ROOT_PREFIX in run_line
    assert "pypi.netflix.net" not in dockerfile


def test_generate_dockerfile_can_configure_bootstrap_pip_options():
    env_id = _make_env_id()
    resolved_env = _make_resolved_env()

    dockerfile, _ = _generate_dockerfile(
        "ubuntu:22.04",
        _env_path_for(env_id),
        env_id,
        "conda",
        resolved_env,
        dockerfile_build_options=DockerfileBuildOptions(
            bootstrap_pip_install_options=(
                "--index-url",
                "https://pypi.example/simple",
            )
        ),
    )

    assert (
        "python -m pip install --disable-pip-version-check --no-cache-dir "
        "--index-url https://pypi.example/simple "
        '--target "$BOOTSTRAP" requests'
    ) in dockerfile
    assert "|| ((python -m pip --version" in dockerfile
    assert "requests)) && METAFLOW_PREBUILT_BUILD_CONTAINER=1" in dockerfile


def test_generate_dockerfile_skips_requests_install_when_import_succeeds(tmp_path):
    env_id = _make_env_id()
    resolved_env = _make_resolved_env()

    dockerfile, _ = _generate_dockerfile(
        "ubuntu:22.04", _env_path_for(env_id), env_id, "conda", resolved_env
    )
    command = _bootstrap_shell_command(dockerfile)

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    pip_marker = tmp_path / "pip_was_called"
    build_install_marker = tmp_path / "build_install_was_called"
    python_calls = tmp_path / "python_calls"
    fake_python = fake_bin / "python"
    fake_python.write_text(
        """#!/bin/sh
printf '%s\\n' "$*" >> "$PYTHON_CALLS"
if [ "$1" = "-c" ] && [ "$2" = "import requests" ]; then
    exit 0
fi
if [ "$1" = "-m" ] && [ "$2" = "pip" ]; then
    echo pip >> "$PIP_MARKER"
    exit 42
fi
if [ "$1" = "-m" ] && [ "$2" = "ensurepip" ]; then
    echo ensurepip >> "$PIP_MARKER"
    exit 42
fi
if [ "$1" = "-m" ]; then
    case "$2" in
        *.prebuilt_build_install)
            echo build-install >> "$BUILD_INSTALL_MARKER"
            exit 0
            ;;
    esac
fi
echo "unexpected python args: $*" >&2
exit 2
"""
    )
    fake_python.chmod(0o755)

    env = os.environ.copy()
    env.update(
        {
            "PATH": "%s%s%s" % (fake_bin, os.pathsep, env.get("PATH", "")),
            "PIP_MARKER": str(pip_marker),
            "BUILD_INSTALL_MARKER": str(build_install_marker),
            "PYTHON_CALLS": str(python_calls),
        }
    )

    result = subprocess.run(
        ["sh", "-c", command],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert not pip_marker.exists(), python_calls.read_text()
    assert build_install_marker.read_text() == "build-install\n"


def test_generate_dockerfile_named_alias_adds_symlink():
    env_id = _make_env_id()
    resolved_env = _make_resolved_env()
    base_image = "ubuntu:22.04"
    named_alias = "my/team/env:v1"
    env_path = _env_path_for_named(named_alias)

    dockerfile, _ = _generate_dockerfile(
        base_image, env_path, env_id, "conda", resolved_env, named_alias=named_alias
    )

    assert "ln -sfT" in dockerfile
    assert _env_path_for(env_id) in dockerfile
    assert env_path in dockerfile


def test_generate_dockerfile_marker_uses_env_id_json():
    env_id = _make_env_id(req_id="rrr", full_id="fff", arch="linux-64")
    resolved_env = _make_resolved_env()
    base_image = "ubuntu:22.04"
    env_path = _env_path_for(env_id)

    dockerfile, _ = _generate_dockerfile(
        base_image, env_path, env_id, "conda", resolved_env
    )

    import json

    expected_marker = json.dumps(["rrr", "fff", "linux-64"])
    assert expected_marker in dockerfile


def test_generate_dockerfile_uses_prebuilt_module_path():
    env_id = _make_env_id()
    resolved_env = _make_resolved_env()
    dockerfile, _ = _generate_dockerfile(
        "ubuntu:22.04", _env_path_for(env_id), env_id, "conda", resolved_env
    )
    assert "metaflow_extensions.prebuilt" in dockerfile
    assert "metaflow_extensions.nflx" not in dockerfile


def test_generate_dockerfile_custom_build_install_module():
    """build_install_module is forwarded into the RUN step."""
    env_id = _make_env_id()
    resolved_env = _make_resolved_env()
    custom_module = "metaflow_extensions.netflixext.plugins.conda"
    dockerfile, _ = _generate_dockerfile(
        "ubuntu:22.04",
        _env_path_for(env_id),
        env_id,
        "conda",
        resolved_env,
        build_install_module=custom_module,
    )
    assert custom_module + ".prebuilt_build_install" in dockerfile
    assert (
        "metaflow_extensions.prebuilt.plugins.conda.prebuilt_build_install"
        not in dockerfile
    )


def test_make_bootstrap_command_groups_pip_fallback_correctly():
    """Verify the fallback pip install stays inside the requests-missing path."""
    cmd = _make_bootstrap_command(
        "python -m some.module",
        "python -m pip install --disable-pip-version-check --no-cache-dir "
        "--index-url https://pypi.example/simple "
        '--target "$BOOTSTRAP" requests',
    )

    # The requests check is present.
    assert "python -c 'import requests'" in cmd
    # The pip install is present.
    assert "pip install" in cmd
    assert "--index-url https://pypi.example/simple" in cmd
    assert "pypi.netflix.net" not in cmd
    # The fallback block is grouped inside the request-check subshell.
    assert "|| ((" in cmd
    assert cmd.index("import requests") < cmd.index("|| ((")
    # The build-install command is present and follows the bootstrap setup.
    assert "python -m some.module" in cmd
    assert cmd.index("python -m some.module") > cmd.index("pip install")
    # Bootstrap tempdir is cleaned up.
    assert 'rm -rf "$BOOTSTRAP"' in cmd
