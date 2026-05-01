from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from types import ModuleType
from typing import List

import pytest


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
DEMO_PATH = os.path.join(ROOT, "demos", "huggingface", "run_huggingface_demo.py")


def _load_demo_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("hf_demo_for_tests", DEMO_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load Hugging Face demo module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _run_demo_cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, DEMO_PATH, *args],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def test_demo_run_help_exits_successfully() -> None:
    result = _run_demo_cli("run", "--help")

    assert result.returncode == 0
    assert "--fetch" in result.stdout
    assert "--only-read-first-model" in result.stdout


def test_demo_test_help_exits_successfully() -> None:
    result = _run_demo_cli("test", "--help")

    assert result.returncode == 0
    assert "--live" in result.stdout
    assert "--download" in result.stdout


def test_demo_invalid_only_read_first_with_download_fails() -> None:
    result = _run_demo_cli("run", "--fetch", "download", "--only-read-first-model")

    assert result.returncode == 2
    assert "--only-read-first-model requires --fetch metadata" in result.stderr


def test_demo_invalid_only_read_first_with_env_fails() -> None:
    result = _run_demo_cli("run", "--auth", "env", "--only-read-first-model")

    assert result.returncode == 2
    assert "--only-read-first-model requires --auth public" in result.stderr


def test_demo_invalid_only_read_first_with_prefetch_fails() -> None:
    result = _run_demo_cli("run", "--prefetch", "--only-read-first-model")

    assert result.returncode == 2
    assert "omit --prefetch" in result.stderr


def test_demo_invalid_demo_cache_with_metadata_fails() -> None:
    result = _run_demo_cli("run", "--fetch", "metadata", "--use-demo-cache")

    assert result.returncode == 2
    assert "--use-demo-cache only applies to --fetch download" in result.stderr


def test_demo_config_persist_and_reload(monkeypatch: pytest.MonkeyPatch) -> None:
    demo = _load_demo_module()
    config = demo.DemoConfig(
        auth="public",
        fetch="download",
        only_read_first_model=False,
        prefetch=True,
        use_demo_cache=True,
        local_dir="/tmp/hf-demo",
    )
    monkeypatch.delenv(demo.MF_HF_DEMO_ENV, raising=False)

    demo._persist_demo_env(config)
    loaded = demo._load_demo_config_from_env()

    assert loaded == config


def test_demo_default_config_when_env_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    demo = _load_demo_module()
    monkeypatch.delenv(demo.MF_HF_DEMO_ENV, raising=False)

    loaded = demo._load_demo_config_from_env()

    assert loaded == demo.DemoConfig()


def test_public_metadata_defaults_build_expected_config() -> None:
    demo = _load_demo_module()
    config = demo.DemoConfig(auth="public", fetch="metadata")
    model_pairs = demo._resolve_model_list(config)

    kwargs = demo._build_hf_kwargs(
        model_pairs,
        metadata_only=config.metadata_only,
        lazy=config.lazy,
        local_dir=None,
    )

    assert kwargs == {
        "models": {"gpt2": "openai-community/gpt2@main"},
        "metadata_only": True,
        "lazy": True,
    }


def test_lazy_two_model_public_config() -> None:
    demo = _load_demo_module()
    config = demo.DemoConfig(
        auth="public", fetch="metadata", only_read_first_model=True
    )
    model_pairs = demo._resolve_model_list(config)

    kwargs = demo._build_hf_kwargs(
        model_pairs,
        metadata_only=config.metadata_only,
        lazy=config.lazy,
        local_dir=None,
    )

    assert kwargs["models"] == {
        "used": "openai-community/gpt2@main",
        "not_accessed": "google-bert/bert-base-uncased",
    }
    assert kwargs["metadata_only"] is True
    assert kwargs["lazy"] is True


def test_demo_local_dir_included_when_set() -> None:
    demo = _load_demo_module()

    kwargs = demo._build_hf_kwargs(
        [("gpt2", "openai-community/gpt2@main")],
        metadata_only=False,
        lazy=False,
        local_dir=os.path.abspath("/tmp/hf-demo"),
    )

    assert kwargs["local_dir"] == os.path.abspath("/tmp/hf-demo")


def test_demo_configures_demo_local_datastore(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    demo = _load_demo_module()
    monkeypatch.delenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", raising=False)

    demo._configure_metaflow_datastore_env()

    assert (
        os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"]
        == demo.DEFAULT_METAFLOW_DATASTORE
    )


def test_demo_preserves_user_local_datastore(monkeypatch: pytest.MonkeyPatch) -> None:
    demo = _load_demo_module()
    custom_datastore = os.path.abspath("/tmp/metaflow-demo")
    monkeypatch.setenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", custom_datastore)

    demo._configure_metaflow_datastore_env()

    assert os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] == custom_datastore


def test_python_test_command_runs_no_network_suite_first() -> None:
    demo = _load_demo_module()

    commands = demo._build_test_commands(include_live=False, include_download=False)

    assert len(commands) == 1
    assert commands[0].label == "Running no-network Hugging Face tests"
    assert commands[0].command[:6] == (
        "uv",
        "run",
        "--with",
        "pytest",
        "--with",
        "requests",
    )
    assert "tests/plugins/huggingface/test_huggingface_demo.py" in commands[0].command


def test_python_test_command_adds_live_metadata_modes() -> None:
    demo = _load_demo_module()

    commands = demo._build_test_commands(include_live=True, include_download=False)
    labels: List[str] = [command.label for command in commands]

    assert labels == [
        "Running no-network Hugging Face tests",
        "Running live public metadata demo",
        "Running live lazy two-model metadata demo",
    ]
    assert commands[1].command[-5:] == (
        "run",
        "--auth",
        "public",
        "--fetch",
        "metadata",
    )
    assert commands[2].command[-1] == "--only-read-first-model"


def test_python_test_command_adds_download_mode() -> None:
    demo = _load_demo_module()

    commands = demo._build_test_commands(include_live=False, include_download=True)

    assert len(commands) == 2
    assert commands[1].label == "Running live public download demo"
    assert commands[1].command[-3:] == (
        "--fetch",
        "download",
        "--use-demo-cache",
    )
