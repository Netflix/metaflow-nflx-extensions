# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
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
from .env_descr import EnvID
from .utils import arch_id


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
    my_echo_always(" done in %d seconds." % int(time.time() - start))

    resolved_env = my_conda.environment(
        EnvID(req_id=req_id, full_id=full_id, arch=arch_id())
    )
    if resolved_env is None:
        raise RuntimeError(
            "Cannot find cached environment for hash %s:%s" % (req_id, full_id)
        )
    # Install the environment; this will fetch packages as well.
    my_conda.create_for_step(step_name, resolved_env, do_symlink=True)

    # Setup anything needed by the escape hatch
    if ENV_ESCAPE_PY is not None:
        cwd = os.getcwd()
        generate_trampolines(cwd)
        # print("Environment escape will use %s as the interpreter" % ENV_ESCAPE_PY)
    else:
        pass
        # print("Could not find a environment escape interpreter")


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
