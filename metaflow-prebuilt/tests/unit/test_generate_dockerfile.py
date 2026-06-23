"""Unit tests for _generate_dockerfile()."""

from unittest.mock import MagicMock

import pytest

import json

from metaflow_extensions.prebuilt.plugins.conda.prebuilt_conda_environment import (
    _generate_dockerfile,
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
    assert (
        "RUN rm -rf %s/pkgs %s/conda-bld /root/.cache/pip"
        % (PREBUILT_MAMBA_ROOT_PREFIX, PREBUILT_MAMBA_ROOT_PREFIX)
        in dockerfile
    )
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
