import os
import sys
import types
from typing import Tuple
from unittest.mock import patch

import pytest

from metaflow import current
from metaflow.exception import MetaflowException
import metaflow.metaflow_config as mf_config
import metaflow_extensions.nflx.plugins.huggingface.huggingface_decorator as hf_dec
from metaflow_extensions.nflx.plugins.huggingface.env_auth_provider import (
    EnvHuggingFaceAuthProvider,
)
from metaflow_extensions.nflx.plugins.huggingface.huggingface_decorator import (
    HuggingFaceDecorator,
    _LazyRepoMap,
    _build_spec_map,
    _fill_huggingface_maps,
    _parse_repo_spec,
    _safe_path_component,
)


_NO_FLOW_STEP_INIT: Tuple[object, ...] = (None, None, "start", [], None, None, None)
_HF_TOKEN_ENV_KEYS = ("HF_TOKEN", "HUGGING_FACE_TOKEN", "HUGGING_FACE_HUB_TOKEN")


def _fake_hf_api_class(error_message):
    class _FakeApi:
        def __init__(self, token=None, endpoint=None):
            pass

        def model_info(self, repo_id, revision=None, token=None):
            raise RuntimeError(error_message)

    return _FakeApi


def test_parse_repo_spec_repo_only():
    assert _parse_repo_spec("bert-base-uncased") == ("bert-base-uncased", "main")
    assert _parse_repo_spec("meta-llama/Llama-2-7b") == (
        "meta-llama/Llama-2-7b",
        "main",
    )


def test_parse_repo_spec_with_revision():
    assert _parse_repo_spec("meta-llama/Llama-2-7b@main") == (
        "meta-llama/Llama-2-7b",
        "main",
    )
    assert _parse_repo_spec("bert-base-uncased@v1.0") == (
        "bert-base-uncased",
        "v1.0",
    )


@pytest.mark.parametrize("spec", ["", "   ", "repo_only@", "@revision_only"])
def test_parse_repo_spec_invalid(spec):
    with pytest.raises(MetaflowException):
        _parse_repo_spec(spec)


def test_build_spec_map_list():
    specs = _build_spec_map(["bert-base-uncased", "meta-llama/Llama-2-7b@main"])
    assert specs["bert-base-uncased"] == ("bert-base-uncased", "main")
    assert specs["meta-llama/Llama-2-7b"] == ("meta-llama/Llama-2-7b", "main")


def test_build_spec_map_list_duplicate_repo_raises():
    with pytest.raises(MetaflowException, match="duplicate model repo"):
        _build_spec_map(["org/model@main", "org/model@v2"])


def test_build_spec_map_dict():
    specs = _build_spec_map(
        {"llama": "meta-llama/Llama-2-7b@main", "bert": "bert-base-uncased"}
    )
    assert specs["llama"] == ("meta-llama/Llama-2-7b", "main")
    assert specs["bert"] == ("bert-base-uncased", "main")


def test_build_spec_map_dict_allows_duplicate_repo_with_aliases():
    specs = _build_spec_map({"stable": "org/model@main", "candidate": "org/model@v2"})
    assert specs["stable"] == ("org/model", "main")
    assert specs["candidate"] == ("org/model", "v2")


def test_build_spec_map_dict_empty_alias_raises():
    with pytest.raises(MetaflowException, match="aliases cannot be empty"):
        _build_spec_map({"  ": "org/model"})


def test_build_spec_map_invalid_type():
    with pytest.raises(MetaflowException):
        _build_spec_map("not-a-list-or-dict")


def test_build_spec_map_list_non_string():
    with pytest.raises(MetaflowException):
        _build_spec_map(["ok", 123])


def test_step_init_no_models_raises():
    dec = HuggingFaceDecorator(attributes={"models": None})
    with pytest.raises(MetaflowException, match="models"):
        dec.step_init(*_NO_FLOW_STEP_INIT)


def test_step_init_empty_models_raises():
    dec = HuggingFaceDecorator(attributes={"models": []})
    with pytest.raises(MetaflowException):
        dec.step_init(*_NO_FLOW_STEP_INIT)


def test_step_init_metadata_only_non_bool_raises():
    dec = HuggingFaceDecorator(
        attributes={"models": ["bert-base-uncased"], "metadata_only": "false"}
    )
    with pytest.raises(MetaflowException, match="metadata_only must be a boolean"):
        dec.step_init(*_NO_FLOW_STEP_INIT)


def test_step_init_lazy_non_bool_raises():
    dec = HuggingFaceDecorator(
        attributes={"models": ["bert-base-uncased"], "lazy": "false"}
    )
    with pytest.raises(MetaflowException, match="lazy must be a boolean"):
        dec.step_init(*_NO_FLOW_STEP_INIT)


def test_step_init_local_dir_not_string_raises():
    dec = HuggingFaceDecorator(
        attributes={"models": ["bert-base-uncased"], "local_dir": 99}
    )
    with pytest.raises(MetaflowException, match="local_dir"):
        dec.step_init(*_NO_FLOW_STEP_INIT)


def test_env_auth_provider_token_precedence(monkeypatch):
    provider = EnvHuggingFaceAuthProvider()
    cases = (
        (
            {
                "HF_TOKEN": "a",
                "HUGGING_FACE_TOKEN": "b",
                "HUGGING_FACE_HUB_TOKEN": "c",
            },
            "a",
        ),
        (
            {
                "HF_TOKEN": "",
                "HUGGING_FACE_TOKEN": "b",
                "HUGGING_FACE_HUB_TOKEN": "c",
            },
            "b",
        ),
        (
            {
                "HF_TOKEN": "",
                "HUGGING_FACE_TOKEN": "",
                "HUGGING_FACE_HUB_TOKEN": "c",
            },
            "c",
        ),
    )
    for env_overlay, expected in cases:
        for key, value in env_overlay.items():
            monkeypatch.setenv(key, value)
        assert provider.get_token() == expected


def test_env_auth_provider_all_keys_absent(monkeypatch):
    for key in _HF_TOKEN_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    assert EnvHuggingFaceAuthProvider().get_token() is None


def test_get_auth_provider_uses_builtin_env(monkeypatch):
    monkeypatch.setattr(mf_config, "HUGGINGFACE_AUTH_PROVIDER", "env", raising=False)
    monkeypatch.setattr(mf_config, "HUGGINGFACE_AUTH_PROVIDERS", {}, raising=False)
    assert isinstance(hf_dec._get_auth_provider(), EnvHuggingFaceAuthProvider)


def test_get_auth_provider_uses_configured_import_path(monkeypatch):
    module = types.ModuleType("test_hf_provider_module")

    class CustomProvider:
        TYPE = "custom"

        def get_token(self):
            return "custom-token"

    module.CustomProvider = CustomProvider
    monkeypatch.setitem(sys.modules, "test_hf_provider_module", module)
    monkeypatch.setattr(mf_config, "HUGGINGFACE_AUTH_PROVIDER", "custom", raising=False)
    monkeypatch.setattr(
        mf_config,
        "HUGGINGFACE_AUTH_PROVIDERS",
        {"custom": "test_hf_provider_module.CustomProvider"},
        raising=False,
    )

    provider = hf_dec._get_auth_provider()
    assert provider.get_token() == "custom-token"


def test_get_auth_provider_unknown_configured_provider_raises(monkeypatch):
    monkeypatch.setattr(
        mf_config, "HUGGINGFACE_AUTH_PROVIDER", "no-such-id", raising=False
    )
    monkeypatch.setattr(mf_config, "HUGGINGFACE_AUTH_PROVIDERS", {}, raising=False)
    with pytest.raises(MetaflowException, match="unknown auth provider"):
        hf_dec._get_auth_provider()


def test_get_auth_provider_invalid_mapping_type_raises(monkeypatch):
    monkeypatch.setattr(mf_config, "HUGGINGFACE_AUTH_PROVIDER", "env", raising=False)
    monkeypatch.setattr(
        mf_config, "HUGGINGFACE_AUTH_PROVIDERS", ["not", "a", "dict"], raising=False
    )
    with pytest.raises(MetaflowException, match="must be a dict"):
        hf_dec._get_auth_provider()


def test_get_auth_provider_import_failure_raises(monkeypatch):
    monkeypatch.setattr(mf_config, "HUGGINGFACE_AUTH_PROVIDER", "custom", raising=False)
    monkeypatch.setattr(
        mf_config,
        "HUGGINGFACE_AUTH_PROVIDERS",
        {"custom": "missing_module.CustomProvider"},
        raising=False,
    )
    with pytest.raises(MetaflowException, match="failed to import"):
        hf_dec._get_auth_provider()


def test_get_auth_provider_non_string_import_path_raises(monkeypatch):
    monkeypatch.setattr(mf_config, "HUGGINGFACE_AUTH_PROVIDER", "custom", raising=False)
    monkeypatch.setattr(
        mf_config,
        "HUGGINGFACE_AUTH_PROVIDERS",
        {"custom": 123},
        raising=False,
    )
    with pytest.raises(MetaflowException, match="must be a string"):
        hf_dec._get_auth_provider()


def test_get_auth_provider_type_mismatch_raises(monkeypatch):
    module = types.ModuleType("test_hf_provider_mismatch")

    class CustomProvider:
        TYPE = "other"

        def get_token(self):
            return "custom-token"

    module.CustomProvider = CustomProvider
    monkeypatch.setitem(sys.modules, "test_hf_provider_mismatch", module)
    monkeypatch.setattr(mf_config, "HUGGINGFACE_AUTH_PROVIDER", "custom", raising=False)
    monkeypatch.setattr(
        mf_config,
        "HUGGINGFACE_AUTH_PROVIDERS",
        {"custom": "test_hf_provider_mismatch.CustomProvider"},
        raising=False,
    )
    with pytest.raises(MetaflowException, match="declares TYPE"):
        hf_dec._get_auth_provider()


def test_get_model_info_wraps_not_found_when_token_set():
    fake = _fake_hf_api_class("404 repository not found")
    with patch.object(hf_dec, "_import_hf_api", return_value=fake):
        with pytest.raises(MetaflowException) as ctx:
            hf_dec._get_model_info("acme/model", "main", "secret-token")
    assert "acme/model" in str(ctx.value)
    assert "Token was obtained" in str(ctx.value)


def test_get_model_info_propagates_when_no_token():
    fake = _fake_hf_api_class("404 not found")
    with patch.object(hf_dec, "_import_hf_api", return_value=fake):
        with pytest.raises(RuntimeError):
            hf_dec._get_model_info("acme/model", "main", None)


def test_fill_maps_metadata_only_uses_get_model_info():
    sentinel = object()
    with patch.object(hf_dec, "_get_model_info", return_value=sentinel) as m_info:
        path_map, info_map = _fill_huggingface_maps(
            {"a": ("r1", "v1"), "b": ("r2", "v2")},
            True,
            "tok",
            "https://hub.example",
            "/base",
        )
    assert path_map == {}
    assert info_map["a"] is sentinel
    assert info_map["b"] is sentinel
    assert m_info.call_count == 2
    m_info.assert_any_call("r1", "v1", "tok", endpoint="https://hub.example")
    m_info.assert_any_call("r2", "v2", "tok", endpoint="https://hub.example")


def test_fill_maps_download_uses_download_to_task_dir():
    with patch.object(
        hf_dec, "_download_to_task_dir", return_value="/snap/path"
    ) as m_dl:
        path_map, info_map = _fill_huggingface_maps(
            {"alias": ("org/repo", "rev")},
            False,
            "tok",
            None,
            "/parent",
        )
    assert info_map == {}
    assert path_map["alias"] == "/snap/path"
    m_dl.assert_called_once_with("org/repo", "rev", "tok", None, "/parent")


def test_download_model_calls_snapshot_download_with_endpoint():
    calls = []

    def fake_snapshot_download(**kwargs):
        calls.append(kwargs)
        return "/downloaded"

    with patch.object(
        hf_dec, "_import_snapshot_download", return_value=fake_snapshot_download
    ):
        path = hf_dec._download_model(
            "org/repo", "rev", "tok", "/local", endpoint="https://hub.example"
        )
    assert path == "/downloaded"
    assert calls == [
        {
            "repo_id": "org/repo",
            "revision": "rev",
            "token": "tok",
            "local_dir": "/local",
            "endpoint": "https://hub.example",
        }
    ]


def test_download_to_task_dir_sanitizes_repo_with_double_dash(tmp_path):
    with patch.object(hf_dec, "_download_model", return_value="/downloaded") as m_dl:
        path = hf_dec._download_to_task_dir(
            "org/repo", "main", None, None, str(tmp_path)
        )
    assert path == "/downloaded"
    m_dl.assert_called_once_with(
        "org/repo",
        "main",
        None,
        os.path.join(str(tmp_path), "org--repo_main"),
        endpoint=None,
    )


def test_download_to_task_dir_sanitizes_revision_with_slashes(tmp_path):
    with patch.object(hf_dec, "_download_model", return_value="/downloaded") as m_dl:
        path = hf_dec._download_to_task_dir(
            "org/repo", "refs/pr/1", None, None, str(tmp_path)
        )
    expected_local_dir = os.path.join(str(tmp_path), "org--repo_refs%2Fpr%2F1")
    assert path == "/downloaded"
    m_dl.assert_called_once_with(
        "org/repo",
        "refs/pr/1",
        None,
        expected_local_dir,
        endpoint=None,
    )


def test_download_to_task_dir_keeps_traversal_revision_inside_base(tmp_path):
    with patch.object(hf_dec, "_download_model", return_value="/downloaded") as m_dl:
        path = hf_dec._download_to_task_dir(
            "org/repo", "../../escape", None, None, str(tmp_path)
        )
    local_dir = m_dl.call_args.args[3]
    assert path == "/downloaded"
    assert os.path.commonpath([str(tmp_path), local_dir]) == str(tmp_path)
    assert local_dir == os.path.join(str(tmp_path), "org--repo_..%2F..%2Fescape")
    m_dl.assert_called_once_with(
        "org/repo",
        "../../escape",
        None,
        local_dir,
        endpoint=None,
    )


def test_download_to_task_dir_distinguishes_encoded_revision_collisions(tmp_path):
    with patch.object(hf_dec, "_download_model", return_value="/downloaded") as m_dl:
        hf_dec._download_to_task_dir("org/repo", "refs/pr/1", None, None, str(tmp_path))
        hf_dec._download_to_task_dir(
            "org/repo", "refs--pr--1", None, None, str(tmp_path)
        )
    slash_revision_dir = m_dl.call_args_list[0].args[3]
    literal_revision_dir = m_dl.call_args_list[1].args[3]
    assert slash_revision_dir != literal_revision_dir
    assert slash_revision_dir.endswith("org--repo_refs%2Fpr%2F1")
    assert literal_revision_dir.endswith("org--repo_refs--pr--1")


@pytest.mark.parametrize("component", ["", "   ", ".", ".."])
def test_safe_path_component_rejects_empty_or_unsafe_values(component):
    with pytest.raises(MetaflowException, match="download path|empty"):
        _safe_path_component(component, "revision")


def test_lazy_download_deferred_until_getitem():
    calls = []

    def record_download(repo_id, revision, token, endpoint, local_dir_base):
        calls.append((repo_id, revision, local_dir_base))
        return "/fake/%s" % repo_id.replace("/", "--")

    with patch.object(hf_dec, "_download_to_task_dir", side_effect=record_download):
        mapping = _LazyRepoMap(
            {"a": ("org/m1", "main"), "b": ("org/m2", "v1")},
            False,
            None,
            None,
            "/fakebase",
        )
        assert calls == []
        assert mapping["a"] == "/fake/org--m1"
        assert calls == [("org/m1", "main", "/fakebase")]
        assert mapping["a"] == "/fake/org--m1"
        assert calls == [("org/m1", "main", "/fakebase")]
        assert mapping["b"] == "/fake/org--m2"
        assert calls == [
            ("org/m1", "main", "/fakebase"),
            ("org/m2", "v1", "/fakebase"),
        ]


def test_lazy_metadata_deferred_until_getitem():
    calls = []

    def record_info(repo_id, revision, token, endpoint=None):
        calls.append((repo_id, revision, endpoint))
        return object()

    with patch.object(hf_dec, "_get_model_info", side_effect=record_info):
        mapping = _LazyRepoMap(
            {"k": ("org/m", "rev")},
            True,
            "tok",
            "https://hf.example",
            "/base",
        )
        assert calls == []
        _ = mapping["k"]
        assert calls == [("org/m", "rev", "https://hf.example")]


def test_lazy_unknown_key_raises():
    mapping = _LazyRepoMap({"a": ("x", "main")}, False, None, None, "/tmp")
    with pytest.raises(KeyError):
        _ = mapping["missing"]


def test_lazy_contains_does_not_download():
    with patch.object(hf_dec, "_download_to_task_dir", side_effect=AssertionError):
        mapping = _LazyRepoMap({"a": ("x", "main")}, False, None, None, "/tmp")
        assert "a" in mapping
        assert "z" not in mapping


def test_lazy_get_returns_value_or_default():
    info = {"id": "m"}
    with patch.object(hf_dec, "_get_model_info", return_value=info):
        mapping = _LazyRepoMap({"k": ("x/y", "main")}, True, None, None, "/tmp")
        assert mapping.get("k") is info
        assert mapping.get("missing") is None
        assert mapping.get("missing", "default") == "default"


def test_resolve_local_dir_explicit_path():
    assert hf_dec._resolve_local_dir_base("/data/hf_cache") == os.path.abspath(
        "/data/hf_cache"
    )


def test_resolve_local_dir_explicit_path_strips_whitespace():
    assert hf_dec._resolve_local_dir_base("  /data/hf_cache  ") == os.path.abspath(
        "/data/hf_cache"
    )


def test_resolve_local_dir_default_joins_task_temp(monkeypatch):
    monkeypatch.setattr(current, "_tempdir", "/var/mf_tmp")
    monkeypatch.setattr(mf_config, "HUGGINGFACE_LOCAL_DIR", None, raising=False)
    assert hf_dec._resolve_local_dir_base(None) == os.path.join(
        "/var/mf_tmp", "metaflow_huggingface"
    )


def test_resolve_local_dir_config_when_decorator_unset(monkeypatch):
    monkeypatch.setattr(
        mf_config, "HUGGINGFACE_LOCAL_DIR", "/mnt/shared/hf", raising=False
    )
    assert hf_dec._resolve_local_dir_base(None) == os.path.abspath("/mnt/shared/hf")


def test_current_huggingface_sentinel_raises_without_decorator():
    with pytest.raises(RuntimeError, match="@huggingface"):
        _ = current.huggingface


def test_huggingface_decorator_promoted_to_metaflow_top_level():
    import metaflow

    assert hasattr(metaflow, "huggingface")
