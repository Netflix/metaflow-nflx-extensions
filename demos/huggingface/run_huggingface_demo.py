#!/usr/bin/env python
"""Demo and test entrypoint for auth-managed Hugging Face access."""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from argparse import Namespace
from dataclasses import asdict, dataclass
from typing import Dict, List, NoReturn, Optional, Sequence, Tuple, Type

AUTH_PUBLIC = "public"
AUTH_ENV = "env"
FETCH_METADATA = "metadata"
FETCH_DOWNLOAD = "download"

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(DEMO_DIR, os.pardir, os.pardir))
DEMO_SCRIPT = os.path.join("demos", "huggingface", "run_huggingface_demo.py")
DEFAULT_DEMO_CACHE = os.path.abspath(os.path.join(DEMO_DIR, ".demo_hf_cache"))
DEFAULT_METAFLOW_DATASTORE = os.path.abspath(os.path.join(DEMO_DIR, ".metaflow"))
MF_HF_DEMO_ENV = "METAFLOW_HF_DEMO_CONFIG"

DEFAULT_PRIVATE_SPEC = "your-org/private-model@main"
DEFAULT_PUBLIC_SPEC = "openai-community/gpt2@main"
DEFAULT_SECOND_PUBLIC_SPEC = "google-bert/bert-base-uncased"
TOKEN_ENV_VARS = ("HF_TOKEN", "HUGGING_FACE_TOKEN", "HUGGING_FACE_HUB_TOKEN")
PYTEST_TARGETS = (
    "tests/plugins/huggingface/test_huggingface_decorator.py",
    "tests/plugins/huggingface/test_huggingface_demo.py",
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
LOGGER = logging.getLogger("huggingface_demo")


@dataclass(frozen=True)
class DemoConfig:
    """Serializable configuration for a Hugging Face demo run.

    Args:
        auth: Demo auth mode. `public` exercises no-token Hub access through the
            env provider, and `env` exercises env-token access for a private repo.
        fetch: `metadata` for Hub metadata calls, or `download` for snapshots.
        only_read_first_model: When true, lists two public models but reads only
            the first key to demonstrate lazy access.
        prefetch: When true, sets `lazy=False` on the decorator.
        use_demo_cache: When true, downloads into the demo cache directory.
        local_dir: Optional download parent directory.
    """

    auth: str = AUTH_PUBLIC
    fetch: str = FETCH_METADATA
    only_read_first_model: bool = False
    prefetch: bool = False
    use_demo_cache: bool = False
    local_dir: Optional[str] = None

    @property
    def metadata_only(self) -> bool:
        """Return whether this run should fetch Hub metadata without files."""
        return self.fetch == FETCH_METADATA

    @property
    def lazy(self) -> bool:
        """Return whether the decorator should resolve models lazily."""
        return not self.prefetch


@dataclass(frozen=True)
class TestCommand:
    """A labeled command in the local Hugging Face demo test matrix.

    Args:
        label: Human-readable command description.
        command: Command arguments passed to `subprocess.run`.
    """

    label: str
    command: Tuple[str, ...]


def _exit_with_error(message: str) -> NoReturn:
    LOGGER.error(message)
    raise SystemExit(2)


def _has_env_token() -> bool:
    return any(os.environ.get(key) for key in TOKEN_ENV_VARS)


def _default_model_pairs(config: DemoConfig) -> List[Tuple[str, str]]:
    if config.auth == AUTH_PUBLIC:
        if config.only_read_first_model:
            return [
                ("used", DEFAULT_PUBLIC_SPEC),
                ("not_accessed", DEFAULT_SECOND_PUBLIC_SPEC),
            ]
        return [("gpt2", DEFAULT_PUBLIC_SPEC)]
    return [("gpt2", DEFAULT_PRIVATE_SPEC)]


def _resolve_model_list(config: DemoConfig) -> List[Tuple[str, str]]:
    return _default_model_pairs(config)


def _build_models_dict(model_pairs: Sequence[Tuple[str, str]]) -> Dict[str, str]:
    return {key: spec for key, spec in model_pairs}


def _build_hf_kwargs(
    model_pairs: Sequence[Tuple[str, str]],
    metadata_only: bool,
    lazy: bool,
    local_dir: Optional[str],
) -> Dict[str, object]:
    kwargs: Dict[str, object] = {
        "models": _build_models_dict(model_pairs),
        "metadata_only": metadata_only,
        "lazy": lazy,
    }
    if local_dir is not None:
        kwargs["local_dir"] = local_dir
    return kwargs


def _validate_run_config(config: DemoConfig) -> None:
    if config.only_read_first_model and config.fetch != FETCH_METADATA:
        _exit_with_error("--only-read-first-model requires --fetch metadata.")
    if config.only_read_first_model and config.auth != AUTH_PUBLIC:
        _exit_with_error("--only-read-first-model requires --auth public.")
    if config.only_read_first_model and config.prefetch:
        _exit_with_error(
            "--only-read-first-model demonstrates lazy=True; omit --prefetch."
        )
    if config.use_demo_cache and config.fetch != FETCH_DOWNLOAD:
        _exit_with_error("--use-demo-cache only applies to --fetch download.")


def _validate_env_token_if_needed(
    config: DemoConfig, model_pairs: Sequence[Tuple[str, str]]
) -> None:
    if config.auth != AUTH_ENV:
        return
    expected = _default_model_pairs(DemoConfig(auth=AUTH_ENV, fetch=config.fetch))
    if list(model_pairs) != expected or _has_env_token():
        return
    _exit_with_error(
        "--auth env with the demo private repo requires HF_TOKEN, "
        "HUGGING_FACE_TOKEN, or HUGGING_FACE_HUB_TOKEN. Use --auth public for "
        "the public default, set a token, or edit Hub repo ids in "
        "run_huggingface_demo.py."
    )


def _configure_auth_env() -> None:
    os.environ["METAFLOW_HUGGINGFACE_AUTH_PROVIDER"] = "env"


def _configure_metaflow_datastore_env() -> None:
    os.environ.setdefault(
        "METAFLOW_DATASTORE_SYSROOT_LOCAL", DEFAULT_METAFLOW_DATASTORE
    )


def _resolve_local_dir(config: DemoConfig) -> Optional[str]:
    if config.local_dir is not None:
        return os.path.abspath(os.path.expanduser(config.local_dir))
    if config.use_demo_cache:
        return DEFAULT_DEMO_CACHE
    return None


def _build_flow_class(
    config: DemoConfig,
    model_pairs: Sequence[Tuple[str, str]],
    local_dir: Optional[str],
) -> Type[object]:
    import metaflow
    from metaflow import FlowSpec, step

    huggingface = getattr(metaflow, "huggingface")

    models = _build_models_dict(model_pairs)
    hf_kwargs = _build_hf_kwargs(
        model_pairs,
        metadata_only=config.metadata_only,
        lazy=config.lazy,
        local_dir=local_dir,
    )

    class HuggingFaceDemoFlow(FlowSpec):
        @huggingface(**hf_kwargs)
        @step
        def start(self) -> None:
            if config.metadata_only:
                _print_metadata_results(models, config.only_read_first_model)
            else:
                _print_download_results(models, config.lazy, local_dir)
            self.next(self.end)

        @step
        def end(self) -> None:
            print("Done.")

    HuggingFaceDemoFlow.__doc__ = (
        "Dynamic @huggingface demo flow built by run_huggingface_demo.py."
    )
    return HuggingFaceDemoFlow


def _print_metadata_results(
    models: Dict[str, str], only_read_first_model: bool
) -> None:
    from metaflow import current

    hf_context = getattr(current, "huggingface")
    if only_read_first_model:
        keys = list(models)
        first_key = keys[0]
        info = hf_context.model_info[first_key]
        _print_model_metadata(info)
        print(
            f"(lazy=True) Listed two models but only read model_info for "
            f"{first_key!r}; {keys[1]!r} was never accessed so it was not fetched."
        )
        return

    for key in models:
        print(f"--- key: {key} ---")
        info = hf_context.model_info[key]
        _print_model_metadata(info)


def _print_download_results(
    models: Dict[str, str], lazy: bool, local_dir: Optional[str]
) -> None:
    from metaflow import current

    hf_context = getattr(current, "huggingface")
    for key in models:
        path = hf_context.models[key]
        print(f"{key} -> {path}")
    if not lazy:
        print("(lazy=False) All listed models were resolved before the step body.")
    if local_dir is not None:
        print(f"local_dir parent: {local_dir}")


def _print_model_metadata(info: object) -> None:
    siblings = getattr(info, "siblings", None)
    sibling_count = len(siblings) if siblings else 0
    print(f"Model id: {getattr(info, 'id', 'N/A')}")
    print(f"Revision (sha): {getattr(info, 'sha', 'N/A')}")
    print(f"Files (siblings): {sibling_count}")


def _persist_demo_env(config: DemoConfig) -> None:
    os.environ[MF_HF_DEMO_ENV] = json.dumps(asdict(config), sort_keys=True)


def _load_demo_config_from_env() -> DemoConfig:
    raw_config = os.environ.get(MF_HF_DEMO_ENV)
    if raw_config is None:
        return DemoConfig()
    try:
        payload = json.loads(raw_config)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"Invalid {MF_HF_DEMO_ENV}: {error}") from error
    if not isinstance(payload, dict):
        raise RuntimeError(f"Invalid {MF_HF_DEMO_ENV}: expected a JSON object")
    try:
        return DemoConfig(**payload)
    except TypeError as error:
        raise RuntimeError(f"Invalid {MF_HF_DEMO_ENV}: {error}") from error


def _execute_hf_demo(config: DemoConfig) -> None:
    model_pairs = _resolve_model_list(config)
    _validate_env_token_if_needed(config, model_pairs)
    _configure_auth_env()
    _configure_metaflow_datastore_env()

    flow_class = _build_flow_class(config, model_pairs, _resolve_local_dir(config))
    flow_class()


def _config_from_namespace(args: Namespace) -> DemoConfig:
    return DemoConfig(
        auth=args.auth,
        fetch=args.fetch,
        only_read_first_model=args.only_read_first_model,
        prefetch=args.prefetch,
        use_demo_cache=args.use_demo_cache,
        local_dir=args.local_dir,
    )


def _run_cmd(args: Namespace) -> None:
    config = _config_from_namespace(args)
    _validate_run_config(config)
    _persist_demo_env(config)

    saved_argv = sys.argv[:]
    try:
        sys.argv = [saved_argv[0], "run"]
        _execute_hf_demo(config)
    finally:
        sys.argv = saved_argv


def _uv_run_command(packages: Sequence[str], command: Sequence[str]) -> Tuple[str, ...]:
    uv_command: List[str] = ["uv", "run"]
    for package in packages:
        uv_command.extend(("--with", package))
    uv_command.extend(("--with-editable", "./metaflow-netflixext"))
    uv_command.extend(command)
    return tuple(uv_command)


def _no_network_test_command() -> TestCommand:
    command = _uv_run_command(
        ("pytest", "requests"),
        ("python", "-m", "pytest", *PYTEST_TARGETS, "-q"),
    )
    return TestCommand("Running no-network Hugging Face tests", command)


def _live_demo_command(label: str, demo_args: Sequence[str]) -> TestCommand:
    command = _uv_run_command(
        ("requests", "huggingface_hub"),
        ("python", DEMO_SCRIPT, "run", *demo_args),
    )
    return TestCommand(label, command)


def _build_test_commands(
    include_live: bool, include_download: bool
) -> List[TestCommand]:
    commands = [_no_network_test_command()]
    if include_live:
        commands.extend(
            [
                _live_demo_command(
                    "Running live public metadata demo",
                    ("--auth", AUTH_PUBLIC, "--fetch", FETCH_METADATA),
                ),
                _live_demo_command(
                    "Running live lazy two-model metadata demo",
                    (
                        "--auth",
                        AUTH_PUBLIC,
                        "--fetch",
                        FETCH_METADATA,
                        "--only-read-first-model",
                    ),
                ),
            ]
        )
    if include_download:
        commands.append(
            _live_demo_command(
                "Running live public download demo",
                (
                    "--auth",
                    AUTH_PUBLIC,
                    "--fetch",
                    FETCH_DOWNLOAD,
                    "--use-demo-cache",
                ),
            )
        )
    return commands


def _run_subprocess(command: Sequence[str]) -> None:
    try:
        subprocess.run(list(command), cwd=REPO_ROOT, check=True)
    except FileNotFoundError:
        _exit_with_error(f"Required command not found: {command[0]}")
    except subprocess.CalledProcessError as error:
        raise SystemExit(error.returncode) from error


def _test_cmd(args: Namespace) -> None:
    for test_command in _build_test_commands(args.live, args.download):
        LOGGER.info(test_command.label)
        _run_subprocess(test_command.command)
    LOGGER.info("Hugging Face demo checks passed.")


def _add_run_parser(subparsers: argparse._SubParsersAction) -> None:
    run = subparsers.add_parser(
        "run",
        help="Run a one-step demo flow with auth-managed @huggingface access",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_parser_epilog(),
    )
    run.add_argument(
        "--auth",
        choices=(AUTH_PUBLIC, AUTH_ENV),
        default=AUTH_PUBLIC,
        help="Select demo repo/auth mode. Both choices use the env auth provider. "
        "Default: %(default)s.",
    )
    run.add_argument(
        "--fetch",
        choices=(FETCH_METADATA, FETCH_DOWNLOAD),
        default=FETCH_METADATA,
        help="metadata = Hub API only; download = snapshot_download to disk. "
        "Default: %(default)s.",
    )
    run.add_argument(
        "--prefetch",
        action="store_true",
        help="Set lazy=False so every listed model is resolved before the step body.",
    )
    run.add_argument(
        "--use-demo-cache",
        action="store_true",
        help="For --fetch download, set local_dir to demos/huggingface/.demo_hf_cache.",
    )
    run.add_argument(
        "--local-dir",
        metavar="PATH",
        default=None,
        help="Parent directory for downloads. Overrides --use-demo-cache.",
    )
    run.add_argument(
        "--only-read-first-model",
        action="store_true",
        dest="only_read_first_model",
        help="List two public models but only read the first alias. Requires "
        "--auth public --fetch metadata and conflicts with --prefetch.",
    )
    run.set_defaults(func=_run_cmd)


def _add_test_parser(subparsers: argparse._SubParsersAction) -> None:
    test = subparsers.add_parser(
        "test",
        help="Run the local Hugging Face decorator and demo test matrix",
    )
    test.add_argument(
        "--live",
        action="store_true",
        help="Also run live public metadata demo modes against Hugging Face Hub.",
    )
    test.add_argument(
        "--download",
        action="store_true",
        help="Also run the optional live public download demo with the demo cache.",
    )
    test.set_defaults(func=_test_cmd)


def _parser_epilog() -> str:
    return f"""
defaults (if you run: {os.path.basename(sys.argv[0])} run with no flags)
  --auth public
  --fetch metadata
  lazy=True for @huggingface (each repo resolves on first access unless --prefetch)
  Models: default public gpt2 metadata-only demo (no token).

To use different Hub repos or aliases, edit constants in this file.

Demo repos (selected by --auth)
  --auth public: {DEFAULT_PUBLIC_SPEC}
  --auth env: {DEFAULT_PRIVATE_SPEC}

Both --auth public and --auth env set METAFLOW_HUGGINGFACE_AUTH_PROVIDER to env;
they differ only in which demo repos are used.
"""


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run or test the auth-managed @huggingface demo flow.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_parser_epilog(),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_run_parser(subparsers)
    _add_test_parser(subparsers)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Run the Hugging Face demo CLI.

    Args:
        argv: Optional argument list. When omitted, arguments are read from
            `sys.argv`.
    """
    parser = _build_parser()
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    args.func(args)


def _metaflow_worker_entry() -> None:
    """Reconstruct the configured flow when Metaflow re-invokes this file."""
    _execute_hf_demo(_load_demo_config_from_env())


if __name__ == "__main__":
    cli_args = sys.argv[1:]
    if cli_args and cli_args[0] in {"run", "test"}:
        main()
    elif not cli_args:
        _exit_with_error(
            f"usage: {os.path.basename(sys.argv[0])} "
            "run|test [--help | ...]. Start with `run` for the demo CLI."
        )
    else:
        _metaflow_worker_entry()
