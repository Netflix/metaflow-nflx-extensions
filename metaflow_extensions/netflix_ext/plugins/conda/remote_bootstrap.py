# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
import json
import os
import shutil
import sys
import time
from typing import Any, Callable

from metaflow.metaflow_config import DATASTORE_LOCAL_DIR, CONDA_MAGIC_FILE_V2

from metaflow.cli import echo_always

from metaflow.debug import debug

from metaflow.plugins.env_escape import generate_trampolines, ENV_ESCAPE_PY

from .conda import Conda
from .conda_environment import CondaEnvironment
from .env_descr import EnvID
from .utils import arch_id, plural_marker


def my_echo_always(*args: Any, **kwargs: Any) -> Callable[..., None]:
    kwargs["err"] = False
    return echo_always(*args, **kwargs)


def bootstrap_environment(
    flow_name: str, step_name: str, req_id: str, full_id: str, datastore_type: str
):
    start = time.time()
    my_echo_always("    Setting up Conda ...", nl=False)
    setup_conda_manifest()
    my_conda = Conda(my_echo_always, datastore_type, mode="remote")
    # Access a binary to force the downloading of the remote conda
    my_conda.binary("micromamba")
    delta_time = int(time.time() - start)
    my_echo_always(" done in %d second%s." % (delta_time, plural_marker(delta_time)))

    # Resolve a late environment if full_id is a special string _fetch_exec
    if full_id == "_fetch_exec":
        alias_to_fetch = CondaEnvironment.sub_envvars_in_envname(req_id)
        env_id = my_conda.env_id_from_alias(alias_to_fetch)
        if env_id is None:
            raise RuntimeError(
                "Cannot find environment '%s' (from '%s') for arch '%s'"
                % (alias_to_fetch, req_id, arch_id())
            )
        req_id = env_id.req_id
        full_id = env_id.full_id
    resolved_env = my_conda.environment(
        EnvID(req_id=req_id, full_id=full_id, arch=arch_id())
    )
    if resolved_env is None:
        raise RuntimeError(
            "Cannot find cached environment for hash %s:%s" % (req_id, full_id)
        )
    # Install the environment; this will fetch packages as well.
    python_bin = os.path.join(
        my_conda.create_for_step(step_name, resolved_env, do_symlink=True),
        "bin",
        "python",
    )
    # Setup anything needed by the escape hatch
    if ENV_ESCAPE_PY is not None:
        cwd = os.getcwd()
        trampoline_dir = os.path.join(cwd, "_escape_trampolines")
        os.makedirs(trampoline_dir)
        generate_trampolines(trampoline_dir)
        # print("Environment escape will use %s as the interpreter" % ENV_ESCAPE_PY)
    else:
        pass
        # print("Could not find a environment escape interpreter")
    # We write out the env_id to _env_id so it can be read by the outer bash script
    with open("_env_id", mode="w", encoding="utf-8") as f:
        json.dump(EnvID(req_id, full_id, arch_id()), f)

    # Same thing for env path (used to set lib for example)
    with open("_env_path", mode="w", encoding="utf-8") as f:
        if python_bin is None:
            raise RuntimeError("Environment was not created properly")
        json.dump(os.path.dirname(os.path.dirname(python_bin)), f)


def setup_conda_manifest():
    manifest_folder = os.path.join(os.getcwd(), DATASTORE_LOCAL_DIR)
    if not os.path.exists(manifest_folder):
        os.makedirs(manifest_folder)
    shutil.move(
        os.path.join(os.getcwd(), CONDA_MAGIC_FILE_V2),
        os.path.join(manifest_folder, CONDA_MAGIC_FILE_V2),
    )


if __name__ == "__main__":
    start = time.time()
    bootstrap_environment(*sys.argv[1:])
    debug.conda_exec("Conda bootstrap took %f seconds" % (time.time() - start))
