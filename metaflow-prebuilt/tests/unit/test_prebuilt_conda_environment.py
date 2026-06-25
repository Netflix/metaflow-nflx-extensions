"""Unit tests for PrebuiltCondaEnvironment state file machinery and bootstrap_commands."""

import json
import os
import tempfile
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from metaflow_extensions.prebuilt.plugins.conda import prebuilt_conda_environment
from metaflow_extensions.prebuilt.plugins.conda.prebuilt_conda_environment import (
    PrebuiltCondaEnvironment,
    _image_cache_key,
    _named_state_key,
    _env_cache_key,
    _step_state_key,
    _image_variant,
    _image_dedup_key,
    _tag_with_variant,
)


@pytest.fixture(autouse=True)
def reset_class_state():
    """Reset class-level state between tests."""
    PrebuiltCondaEnvironment._prebuilt_images = {}
    PrebuiltCondaEnvironment._prebuilt_env_paths = {}
    PrebuiltCondaEnvironment._init_in_progress = False
    if PrebuiltCondaEnvironment._STATE_FILE_ENV_VAR in os.environ:
        del os.environ[PrebuiltCondaEnvironment._STATE_FILE_ENV_VAR]
    yield
    PrebuiltCondaEnvironment._prebuilt_images = {}
    PrebuiltCondaEnvironment._prebuilt_env_paths = {}
    PrebuiltCondaEnvironment._init_in_progress = False
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


class TestInitEnvironment:
    def test_init_environment_preserves_pythonpath_but_isolates_binary_calls(
        self, monkeypatch
    ):
        from metaflow_extensions.netflixext.plugins.conda import (
            conda_flow_mutator,
            utils as conda_utils,
        )

        env = PrebuiltCondaEnvironment.__new__(PrebuiltCondaEnvironment)
        seen_pythonpath = []

        class FakeConda:
            def call_binary(self, *args, **kwargs):
                seen_pythonpath.append(
                    ("conda.call_binary", os.environ.get("PYTHONPATH"))
                )
                return b""

            def call_conda(self, *args, **kwargs):
                seen_pythonpath.append(
                    ("conda.call_conda", os.environ.get("PYTHONPATH"))
                )
                return b""

        fake_conda = FakeConda()

        def fake_utils_call_binary(*args, **kwargs):
            seen_pythonpath.append(("utils.call_binary", os.environ.get("PYTHONPATH")))
            return b""

        def fake_flow_mutator_call_binary(*args, **kwargs):
            seen_pythonpath.append(
                ("flow_mutator.call_binary", os.environ.get("PYTHONPATH"))
            )
            return b""

        def fake_base_init_environment(_self, _echo):
            seen_pythonpath.append(("base", os.environ.get("PYTHONPATH")))
            conda_utils.call_binary([], "uv")
            conda_flow_mutator.call_binary([], "uv")
            fake_conda.call_binary([], binary="pip")
            fake_conda.call_conda([], binary="mamba")

        def fake_build_prebuilt_images(_self, _echo):
            seen_pythonpath.append(("build", os.environ.get("PYTHONPATH")))

        monkeypatch.setenv("PYTHONPATH", "/bazel/runfiles:/tmp/host-stdlib")
        monkeypatch.setattr(conda_utils, "call_binary", fake_utils_call_binary)
        monkeypatch.setattr(
            conda_flow_mutator, "call_binary", fake_flow_mutator_call_binary
        )
        monkeypatch.setattr(prebuilt_conda_environment, "Conda", FakeConda)

        with patch.object(
            PrebuiltCondaEnvironment.__bases__[0],
            "init_environment",
            new=fake_base_init_environment,
        ), patch.object(
            PrebuiltCondaEnvironment,
            "_build_prebuilt_images",
            new=fake_build_prebuilt_images,
        ):
            env.init_environment(lambda *args, **kwargs: None)

        assert seen_pythonpath == [
            ("base", "/bazel/runfiles:/tmp/host-stdlib"),
            ("utils.call_binary", None),
            ("flow_mutator.call_binary", None),
            ("conda.call_binary", None),
            ("conda.call_conda", None),
            ("build", "/bazel/runfiles:/tmp/host-stdlib"),
        ]
        assert os.environ["PYTHONPATH"] == "/bazel/runfiles:/tmp/host-stdlib"
        assert PrebuiltCondaEnvironment._init_in_progress is False


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

    def test_bootstrap_commands_resolves_named_alias_string(
        self, tmp_path, monkeypatch
    ):
        alias = "mlp/metaflow/npow/prebuilt_allcases:v1"
        env_path = "/opt/metaflow/conda-root/envs/named_mlp_alias__1234"

        monkeypatch.setenv("METAFLOW_TEMPDIR", str(tmp_path))
        PrebuiltCondaEnvironment._prebuilt_env_paths = {
            _named_state_key(alias): env_path
        }
        PrebuiltCondaEnvironment._persist_prebuilt_state()
        PrebuiltCondaEnvironment._prebuilt_images = {}
        PrebuiltCondaEnvironment._prebuilt_env_paths = {}

        env = self._make_env()
        env.get_env_id_noconda = MagicMock(return_value=alias)
        env.sub_envvars_in_envname = MagicMock(return_value=alias)

        cmds = env.bootstrap_commands("named_dynamic", "local")

        activate_cmd = next(c for c in cmds if "prebuilt_runtime_activate" in c)
        assert env_path in activate_cmd

    def test_bootstrap_commands_prefers_step_path_for_dynamic_named_env(
        self, tmp_path, monkeypatch
    ):
        """fetch_at_exec named envs may look like EnvIDs at runtime.

        When a static and dynamic @named_env share the same alias, their concrete
        EnvID is the same but their baked env paths differ. The step-scoped entry
        preserves the dynamic alias path even if get_env_id_noconda returns the
        resolved EnvID.
        """
        env_id = _make_env_id()
        env_key = _env_cache_key(env_id)
        step_key = _step_state_key("named_dynamic")
        static_path = "/opt/metaflow/conda-root/envs/metaflow_abc_def"
        dynamic_path = "/opt/metaflow/conda-root/envs/named_mlp_alias__1234"

        monkeypatch.setenv("METAFLOW_TEMPDIR", str(tmp_path))
        PrebuiltCondaEnvironment._prebuilt_env_paths = {
            env_key: static_path,
            step_key: dynamic_path,
        }
        PrebuiltCondaEnvironment._persist_prebuilt_state()
        PrebuiltCondaEnvironment._prebuilt_images = {}
        PrebuiltCondaEnvironment._prebuilt_env_paths = {}

        env = self._make_env()
        env.get_env_id_noconda = MagicMock(return_value=env_id)

        cmds = env.bootstrap_commands("named_dynamic", "local")

        activate_cmd = next(c for c in cmds if "prebuilt_runtime_activate" in c)
        assert dynamic_path in activate_cmd
        assert static_path not in activate_cmd

    def test_build_prebuilt_images_registers_step_path_for_dynamic_named_env(
        self, tmp_path, monkeypatch
    ):
        alias = "mlp/metaflow/npow/prebuilt_allcases:v1"
        env_id = _make_env_id()
        env_path = "/opt/metaflow/conda-root/envs/named_mlp_alias__1234"
        remote_deco = SimpleNamespace(name="titus", attributes={"image": "base"})
        step = SimpleNamespace(name="named_dynamic", decorators=[remote_deco])

        monkeypatch.setenv("METAFLOW_TEMPDIR", str(tmp_path))
        monkeypatch.setenv("METAFLOW_PREBUILT_BASE_IMAGE", "cpu-base")
        monkeypatch.setattr(
            prebuilt_conda_environment, "CONDA_REMOTE_COMMANDS", {"titus"}
        )

        registry = MagicMock()
        registry.pull_config.return_value = {}
        registry.base_image_identity.side_effect = lambda base, _arch: base
        env = self._make_env()
        env._flow = [step]
        env.get_env_id = MagicMock(return_value=alias)
        env.sub_envvars_in_envname = MagicMock(return_value=alias)
        env.conda.env_id_from_alias.return_value = env_id

        with patch.object(
            prebuilt_conda_environment.ImageRegistry,
            "from_config",
            return_value=registry,
        ), patch.object(
            PrebuiltCondaEnvironment,
            "_get_or_build_image",
            return_value=("pull-tag", env_path),
        ), patch.object(
            prebuilt_conda_environment,
            "_build_metaflow_code_package",
            return_value=b"fake-tarball",
        ):
            env._build_prebuilt_images(lambda *args, **kwargs: None)

        assert (
            PrebuiltCondaEnvironment._prebuilt_env_paths[_named_state_key(alias)]
            == env_path
        )
        assert (
            PrebuiltCondaEnvironment._prebuilt_env_paths[
                _step_state_key("named_dynamic")
            ]
            == env_path
        )
        assert remote_deco.attributes["image"] == "pull-tag"


def test_get_or_build_image_reuses_existing_immutable_tag(monkeypatch):
    env_id = _make_env_id()
    env = PrebuiltCondaEnvironment.__new__(PrebuiltCondaEnvironment)
    env.conda = MagicMock()
    env.conda.environment.return_value = SimpleNamespace(env_type="conda")

    registry = MagicMock()
    registry.push_tag.return_value = "registry.example/prebuilt:v29-abc_def"
    registry.pull_tag.return_value = "prebuilt:v29-abc_def"
    registry.image_exists.return_value = True

    monkeypatch.setattr(
        prebuilt_conda_environment,
        "_build_metaflow_code_package",
        MagicMock(side_effect=AssertionError("should not package code")),
    )
    build_service = MagicMock()
    monkeypatch.setattr(
        prebuilt_conda_environment.DockerBuildService,
        "from_config",
        MagicMock(return_value=build_service),
    )

    result = env._get_or_build_image(
        env_id,
        SimpleNamespace(name="start"),
        lambda *args, **kwargs: None,
        registry,
        base_image="cpu-base",
        variant="linux-64-1234",
    )

    assert result == (
        "prebuilt:v29-abc_def-linux-64-1234",
        "/opt/metaflow/conda-root/envs/metaflow_%s_%s"
        % (env_id.req_id, env_id.full_id),
    )
    registry.image_exists.assert_called_once_with(
        "registry.example/prebuilt:v29-abc_def-linux-64-1234"
    )
    build_service.build_and_push.assert_not_called()


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
        from metaflow_extensions.netflixext.plugins.conda.env_descr import (
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


class TestImageIdentity:
    """Regression for the image-identity fix: two steps that share an env
    (same req_id/full_id) but differ by base image (CPU vs GPU) or arch must
    build and TAG distinct images, while the runtime env lookup stays
    base-independent so bootstrap (which can't know the base) still resolves it."""

    def test_variant_differs_by_base_image(self):
        arch = "linux-64"
        cpu = _image_variant("registry.example/big_data:stable", arch)
        gpu = _image_variant("registry.example/big_data_gpu:stable", arch)
        assert cpu != gpu

    def test_variant_differs_by_arch(self):
        base = "registry.example/big_data:stable"
        assert _image_variant(base, "linux-64") != _image_variant(base, "linux-aarch64")

    def test_variant_is_stable(self):
        assert _image_variant("b:1", "linux-64") == _image_variant("b:1", "linux-64")

    def test_dedup_key_separates_cpu_and_gpu_for_same_env(self):
        env_id = _make_env_id()
        env_key = _env_cache_key(env_id)
        arch = getattr(env_id, "arch", "linux-64")
        cpu = _image_dedup_key(env_key, _image_variant("cpu_base", arch))
        gpu = _image_dedup_key(env_key, _image_variant("gpu_base", arch))
        # The bug: keying on env alone collided (one image for both). Now distinct.
        assert cpu != gpu
        # ...but both still carry the shared env identity.
        assert env_key in cpu and env_key in gpu

    def test_tag_carries_variant_and_distinguishes_base(self):
        base_tag = "metaflow/prebuilt:v29-abc_def"
        cpu = _tag_with_variant(base_tag, _image_variant("cpu_base", "linux-64"))
        gpu = _tag_with_variant(base_tag, _image_variant("gpu_base", "linux-64"))
        assert cpu != gpu  # distinct tags -> GPU build can't overwrite the CPU image
        assert cpu.startswith(base_tag + "-")
        appended = cpu[len(base_tag) + 1 :]
        assert appended and all(c.isalnum() or c in "_.-" for c in appended)

    def test_env_cache_key_is_base_independent(self):
        env_id = _make_env_id()
        assert _env_cache_key(env_id) == "%s_%s" % (env_id.req_id, env_id.full_id)

    def test_bootstrap_resolves_env_when_images_keyed_by_variant(
        self, tmp_path, monkeypatch
    ):
        # Mirror the real build: _prebuilt_images keyed by env+variant (image
        # identity), _prebuilt_env_paths keyed by env (env identity). Bootstrap
        # cannot know the variant at runtime, so it must resolve via the env key.
        # Under the OLD code (validating _prebuilt_images by the env key) this
        # raised; it must now succeed.
        env_id = _make_env_id()
        env_key = _env_cache_key(env_id)
        image_key = _image_dedup_key(
            env_key, _image_variant("gpu_base", getattr(env_id, "arch", "linux-64"))
        )
        env_path = "/opt/metaflow/conda-root/envs/metaflow_abc_def"
        monkeypatch.setenv("METAFLOW_TEMPDIR", str(tmp_path))
        PrebuiltCondaEnvironment._prebuilt_images = {image_key: "tag-gpu-variant"}
        PrebuiltCondaEnvironment._prebuilt_env_paths = {env_key: env_path}
        PrebuiltCondaEnvironment._persist_prebuilt_state()
        PrebuiltCondaEnvironment._prebuilt_images = {}
        PrebuiltCondaEnvironment._prebuilt_env_paths = {}

        env = PrebuiltCondaEnvironment.__new__(PrebuiltCondaEnvironment)
        env._flow = MagicMock()
        env.conda = MagicMock()
        env.get_env_id_noconda = MagicMock(return_value=env_id)

        cmds = env.bootstrap_commands("start", "local")
        assert any("prebuilt_runtime_activate" in c for c in cmds)
        assert any(env_path in c for c in cmds)


def test_docker_platform_for_arch():
    from metaflow_extensions.prebuilt.plugins.conda.prebuilt_conda_environment import (
        _docker_platform_for_arch,
    )

    assert _docker_platform_for_arch("linux-64") == "linux/amd64"
    assert _docker_platform_for_arch("linux-aarch64") == "linux/arm64"
    # non-linux / unknown / empty -> None (builder default, no --platform forced)
    assert _docker_platform_for_arch("osx-arm64") is None
    assert _docker_platform_for_arch("") is None
    assert _docker_platform_for_arch(None) is None
