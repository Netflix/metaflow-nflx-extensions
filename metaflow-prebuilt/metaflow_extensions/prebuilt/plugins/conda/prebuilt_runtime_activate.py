# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
"""Runtime activation for `--environment=prebuilt`.

Counterpart to `remote_bootstrap.bootstrap_environment` for the
prebuilt case. The env was already installed at build time in the
docker image; this just does the small set of file writes /
symlinks the runtime task entrypoint relies on:

  1. Verify the pre-baked env exists at `env_path` (with `.metaflowenv`
     stamp). NO silent fallback — exit code 17 if missing.
  2. Read the image's `.metaflowenv` marker to get the AUTHORITATIVE
     env_id and write it to `_env_id`. We deliberately don't trust
     args from bash here — for `@named_env(fetch_at_exec=True)` the
     image at task launch may be a different deploy than the one
     that generated the spec, so the image's marker is the only
     reliable source of "what env is actually in this container."
  3. Write `_env_path` so the standard bash chain `cat _env_path`
     resolves correctly.
  4. Generate env_escape trampolines under `_escape_trampolines/`.
  5. Symlink `__conda_python -> env_path/bin/python` (same effect as
     `Conda.create_for_step(do_symlink=True)`).

After this, `PrebuiltCondaEnvironment.bootstrap_commands` follows the
exact shape of `CondaEnvironment.bootstrap_commands` — `cat _env_id`,
`cat _env_path`, the standard PYTHONPATH+LD_LIBRARY_PATH lines — so
the bash plumbing is shared, not re-implemented.
"""
import json
import os
import sys
from typing import Any

from metaflow.cli import echo_always
from metaflow.plugins.env_escape import generate_trampolines, ENV_ESCAPE_PY


_PREBUILT_MISSING_EXIT_CODE = 17


def _echo(*args: Any, **kwargs: Any) -> None:
    kwargs["err"] = False
    echo_always(*args, **kwargs)


def activate_env(env_path: str) -> None:
    """Activate the pre-baked conda env at `env_path`.

    `env_path` may be a symlink (the `@named_env(fetch_at_exec=True)`
    case — symlink points at whichever env_id the current image
    baked); we follow it to find the real env. The `.metaflowenv`
    marker at the (resolved) path is the authoritative source for
    req_id/full_id — args via bash would be stale if the image was
    overwritten by a later deploy under the same alias.

    Raises SystemExit(17) if the env isn't present — mirrors the
    `assert_env_present` contract in
    `PrebuiltCondaEnvironment.bootstrap_commands` so the runtime
    fails loudly when the @titus image somehow isn't the prebuilt one.
    """
    marker = os.path.join(env_path, ".metaflowenv")
    if not os.path.isdir(env_path) or not os.path.isfile(marker):
        print(
            "ERROR: prebuilt env not found at %s — image is not the "
            "prebuilt one. NO silent fallback to conda; task will exit." % env_path,
            file=sys.stderr,
        )
        sys.exit(_PREBUILT_MISSING_EXIT_CODE)

    _echo("Prebuilt environment detected — skipping conda bootstrap")

    with open(marker, mode="r", encoding="utf-8") as f:
        env_id_list = json.load(f)
    with open("_env_id", mode="w", encoding="utf-8") as f:
        json.dump(env_id_list, f)
    with open("_env_path", mode="w", encoding="utf-8") as f:
        f.write(env_path)

    if ENV_ESCAPE_PY is not None:
        cwd = os.getcwd()
        trampoline_dir = os.path.join(cwd, "_escape_trampolines")
        os.makedirs(trampoline_dir, exist_ok=True)
        generate_trampolines(trampoline_dir)

    sym = os.path.join(os.getcwd(), "__conda_python")
    target = os.path.join(env_path, "bin", "python")
    if os.path.lexists(sym):
        os.unlink(sym)
    os.symlink(target, sym)

    _echo("Prebuilt environment activated.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(
            "Usage: python -m metaflow_extensions.prebuilt.plugins.conda."
            "prebuilt_runtime_activate <env_path>",
            file=sys.stderr,
        )
        sys.exit(2)
    activate_env(sys.argv[1])
