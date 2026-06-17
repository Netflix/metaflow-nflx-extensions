"""Unit tests for PrebuiltCondaEnvironment state file machinery and bootstrap_commands."""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from metaflow_extensions.prebuilt.plugins.conda.prebuilt_conda_environment import (
    PrebuiltCondaEnvironment,
    _image_cache_key,
    _named_state_key,
)


@pytest.fixture(autouse=True)
def reset_class_state():
    """Reset class-level state between tests."""
    PrebuiltCondaEnvironment._prebuilt_images = {}
    PrebuiltCondaEnvironment._prebuilt_env_paths = {}
    if PrebuiltCondaEnvironment._STATE_FILE_ENV_VAR in os.environ:
        del os.environ[PrebuiltCondaEnvironment._STATE_FILE_ENV_VAR]
    yield
    PrebuiltCondaEnvironment._prebuilt_images = {}
    PrebuiltCondaEnvironment._prebuilt_env_paths = {}
    if PrebuiltCondaEnvironment._STATE_FILE_ENV_VAR in os.environ:
        del os.environ[PrebuiltCondaEnvironment._STATE_FILE_ENV_VAR]


def _make_env_id(req="abc", full="def"):
    # Prefer the real nflx EnvID when it is importable (the package couples to
    # metaflow-netflixext), so the `isinstance(env_id, EnvID)` check in
    # bootstrap_commands takes the prebuilt path. Fall back to a duck-typed mock
    # on a pure-OSS install, where EnvID is None and the hash-based branch runs.
    from metaflow_extensions.prebuilt.plugins.conda.prebuilt_conda_environment import (
        EnvID,
    )

    if EnvID is not None:
        return EnvID(req_id=req, full_id=full, arch="linux-64")

    env_id = MagicMock()
    env_id.req_id = req
    env_id.full_id = full
    env_id.arch = "linux-64"
    return env_id


class TestStateFilePersistLoad:
    def test_persist_creates_file_and_sets_env_var(self, tmp_path, monkeypatch):
        monkeypatch.setenv("METAFLOW_TEMPDIR", str(tmp_path))
        PrebuiltCondaEnvironment._prebuilt_images = {"k": "tag"}
        PrebuiltCondaEnvironment._prebuilt_env_paths = {"k": "/some/path"}

        PrebuiltCondaEnvironment._persist_prebuilt_state()

        path = PrebuiltCondaEnvironment._state_file_path()
        assert path is not None
        assert os.path.isfile(path)
        with open(path) as f:
            data = json.load(f)
        assert data["images"] == {"k": "tag"}
        assert data["env_paths"] == {"k": "/some/path"}

    def test_load_round_trips_state(self, tmp_path, monkeypatch):
        monkeypatch.setenv("METAFLOW_TEMPDIR", str(tmp_path))
        PrebuiltCondaEnvironment._prebuilt_images = {"k": "mytag"}
        PrebuiltCondaEnvironment._prebuilt_env_paths = {"k": "/env/path"}
        PrebuiltCondaEnvironment._persist_prebuilt_state()

        # Clear in-memory state; simulate subprocess load
        PrebuiltCondaEnvironment._prebuilt_images = {}
        PrebuiltCondaEnvironment._prebuilt_env_paths = {}

        PrebuiltCondaEnvironment._load_prebuilt_state()

        assert PrebuiltCondaEnvironment._prebuilt_images == {"k": "mytag"}
        assert PrebuiltCondaEnvironment._prebuilt_env_paths == {"k": "/env/path"}

    def test_load_returns_silently_when_no_state_file_env_var(self):
        # No env var set → no-op, class state stays empty
        PrebuiltCondaEnvironment._load_prebuilt_state()
        assert PrebuiltCondaEnvironment._prebuilt_images == {}

    def test_load_raises_on_missing_file(self, tmp_path, monkeypatch):
        fake_path = str(tmp_path / "nonexistent.json")
        monkeypatch.setenv(PrebuiltCondaEnvironment._STATE_FILE_ENV_VAR, fake_path)
        with pytest.raises(Exception, match="does not exist"):
            PrebuiltCondaEnvironment._load_prebuilt_state()

    def test_load_raises_on_corrupt_json(self, tmp_path, monkeypatch):
        p = tmp_path / "state.json"
        p.write_text("not valid json{{{")
        monkeypatch.setenv(PrebuiltCondaEnvironment._STATE_FILE_ENV_VAR, str(p))
        with pytest.raises(Exception):
            PrebuiltCondaEnvironment._load_prebuilt_state()


class TestBuildInstallModule:
    def test_default_build_install_module(self):
        # This package ships the real prebuilt_build_install (it imports Conda
        # from metaflow-netflixext at runtime), so the container runs it from
        # the prebuilt namespace.
        assert (
            PrebuiltCondaEnvironment._BUILD_INSTALL_MODULE
            == "metaflow_extensions.prebuilt.plugins.conda"
        )

    def test_subclass_can_override_build_install_module(self):
        class CustomPrebuiltEnvironment(PrebuiltCondaEnvironment):
            _BUILD_INSTALL_MODULE = "some.other.conda.stack"

        assert (
            CustomPrebuiltEnvironment._BUILD_INSTALL_MODULE == "some.other.conda.stack"
        )
        # Base class is unchanged
        assert (
            PrebuiltCondaEnvironment._BUILD_INSTALL_MODULE
            == "metaflow_extensions.prebuilt.plugins.conda"
        )


class TestBootstrapCommands:
    def _make_env(self):
        flow = MagicMock()
        env = PrebuiltCondaEnvironment.__new__(PrebuiltCondaEnvironment)
        env._flow = flow
        env.conda = MagicMock()
        return env

    def test_bootstrap_commands_returns_prebuilt_commands_when_image_registered(
        self, tmp_path, monkeypatch
    ):
        env_id = _make_env_id()
        key = _image_cache_key(env_id)
        pull_tag = "localhost:5000/metaflow-prebuilt:v28-abc_def"
        env_path = "/opt/metaflow/conda-root/envs/metaflow_abc_def"

        monkeypatch.setenv("METAFLOW_TEMPDIR", str(tmp_path))
        PrebuiltCondaEnvironment._prebuilt_images = {key: pull_tag}
        PrebuiltCondaEnvironment._prebuilt_env_paths = {key: env_path}
        PrebuiltCondaEnvironment._persist_prebuilt_state()
        PrebuiltCondaEnvironment._prebuilt_images = {}
        PrebuiltCondaEnvironment._prebuilt_env_paths = {}

        env = self._make_env()
        env.get_env_id_noconda = MagicMock(return_value=env_id)

        cmds = env.bootstrap_commands("start", "local")

        assert any("prebuilt_runtime_activate" in c for c in cmds)
        assert any(env_path in c for c in cmds)
        assert any("_env_id" in c for c in cmds)
        assert any("LD_LIBRARY_PATH" in c for c in cmds)

    def test_bootstrap_commands_raises_if_image_not_registered(
        self, tmp_path, monkeypatch
    ):
        env_id = _make_env_id()
        monkeypatch.setenv("METAFLOW_TEMPDIR", str(tmp_path))
        PrebuiltCondaEnvironment._prebuilt_images = {}
        PrebuiltCondaEnvironment._prebuilt_env_paths = {}
        PrebuiltCondaEnvironment._persist_prebuilt_state()
        PrebuiltCondaEnvironment._prebuilt_images = {}
        PrebuiltCondaEnvironment._prebuilt_env_paths = {}

        env = self._make_env()
        env.get_env_id_noconda = MagicMock(return_value=env_id)

        with pytest.raises(Exception, match="not registered"):
            env.bootstrap_commands("start", "local")

    def test_bootstrap_commands_falls_back_for_step_without_env(self):
        env = self._make_env()
        env.get_env_id_noconda = MagicMock(return_value=None)

        with patch.object(
            PrebuiltCondaEnvironment.__bases__[0],
            "bootstrap_commands",
            return_value=["standard"],
        ):
            cmds = env.bootstrap_commands("local_step", "local")
        assert cmds == ["standard"]

    def test_bootstrap_commands_uses_prebuilt_module_path(self, tmp_path, monkeypatch):
        env_id = _make_env_id()
        key = _image_cache_key(env_id)
        monkeypatch.setenv("METAFLOW_TEMPDIR", str(tmp_path))
        PrebuiltCondaEnvironment._prebuilt_images = {key: "tag"}
        PrebuiltCondaEnvironment._prebuilt_env_paths = {key: "/env"}
        PrebuiltCondaEnvironment._persist_prebuilt_state()
        PrebuiltCondaEnvironment._prebuilt_images = {}
        PrebuiltCondaEnvironment._prebuilt_env_paths = {}

        env = self._make_env()
        env.get_env_id_noconda = MagicMock(return_value=env_id)
        cmds = env.bootstrap_commands("start", "local")

        activate_cmd = next(c for c in cmds if "prebuilt_runtime_activate" in c)
        assert "metaflow_extensions.prebuilt" in activate_cmd
        assert "metaflow_extensions.nflx" not in activate_cmd


def test_gather_embedded_wheels_prefers_cached_wheel_over_local_sdist():
    """Codex r13 regression: a source-format package that carries a competing
    local sdist AND has a cached built wheel must embed the WHEEL — not be
    defeated by lazy_fetch preferring the local source archive (it only
    materializes a package's single most-preferred source, and a local file
    outranks a cached version). _gather must fetch a wheel-only spec, not the
    package itself."""
    from metaflow_extensions.prebuilt.plugins.conda.prebuilt_conda_environment import (
        _gather_embedded_wheels,
    )

    try:
        from metaflow_extensions.nflx.plugins.conda.env_descr import (
            PypiCachePackage,
            PypiPackageSpecification,
        )
    except ImportError:
        pytest.skip("requires metaflow-netflixext")

    src_url = "https://pypi.example/foo-1.0.tar.gz"
    whl_hash = "0" * 64
    # Cache URL layout: .../<filename.whl>/<hash>/<filename.whl>
    cache_url = (
        "pypi/example/foo-1.0-py3-none-any.whl/%s/foo-1.0-py3-none-any.whl" % whl_hash
    )

    with tempfile.TemporaryDirectory() as d:
        cand = PypiPackageSpecification("foo-1.0", src_url, url_format=".tar.gz")
        # The competing local source archive that would wrongly win the race.
        sdist_path = os.path.join(d, "foo-1.0.tar.gz")
        with open(sdist_path, "wb") as f:
            f.write(b"sdist-bytes")
        cand.add_local_file(".tar.gz", sdist_path)
        # A built wheel for it lives in the conda cache.
        cand.add_cached_version(".whl", PypiCachePackage(cache_url))

        captured = {}

        def fake_lazy_fetch(pkgs, auth, dest, **kwargs):
            spec = list(pkgs)[0]
            captured["spec"] = spec
            # Faithfully reproduce lazy_fetch: it materializes only the single
            # most-preferred source, and a LOCAL file outranks a cached version.
            # So if a local source archive is already present, the cached wheel
            # is NOT fetched (this is exactly the bug). Fetching the package
            # itself (old behavior) thus leaves no .whl; fetching a wheel-only
            # spec (the fix) materializes it.
            if any(spec.local_file(f) for f in spec.allowed_formats()):
                return
            if spec.cached_version(".whl") is not None:
                os.makedirs(os.path.join(dest, "pypi"), exist_ok=True)
                whl_path = os.path.join(dest, "pypi", "foo-1.0-py3-none-any.whl")
                with open(whl_path, "wb") as f:
                    f.write(b"wheel-bytes")
                spec.add_local_file(".whl", whl_path, pkg_hash=whl_hash)

        conda = MagicMock()
        conda.lazy_fetch_packages.side_effect = fake_lazy_fetch
        resolved_env = MagicMock()
        resolved_env.packages = [cand]

        records, files = _gather_embedded_wheels(
            conda, resolved_env, lambda *a, **k: None
        )

    # It embedded the WHEEL (not the sdist) ...
    assert len(records) == 1
    assert records[0]["wheel_file"] == "foo-1.0-py3-none-any.whl"
    assert records[0]["url_format"] == ".whl"
    assert any(k.endswith("foo-1.0-py3-none-any.whl") for k in files)
    assert list(files.values()) == [b"wheel-bytes"]
    # ... by fetching a wheel-only spec, NOT the source candidate carrying the
    # local sdist (which is what previously defeated this).
    assert captured["spec"] is not cand
    assert captured["spec"].url_format == ".whl"
    assert captured["spec"].local_file(".tar.gz") is None
