from __future__ import annotations

import os
import re
import sys
import time
import tempfile
import tarfile
import zipfile
import contextlib
import fcntl
from typing import TYPE_CHECKING, Dict, Any, Callable, List, Optional

if TYPE_CHECKING:
    from metaflow import S3

from metaflow.plugins.env_escape import generate_trampolines

from metaflow_extensions.nflx.plugins.functions.debug import debug
from metaflow_extensions.nflx.plugins.functions.utils import (
    is_s3,
)
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionRuntimeException,
)

INTERPRETER_PATH_FILE = "interpreter_path"


@contextlib.contextmanager
def cd(new_dir: str):
    prev_dir = os.getcwd()
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(prev_dir)


def atomic_write(msg: str, fname: str, path: str):
    """
    Atomically write a file

    Parameters
    ----------
    msg: str
        The contents of the file
    fname : str
        The file name without extension
    path : str
        The path without without the filename

    """
    with tempfile.NamedTemporaryFile(mode="w", dir=path, delete=False) as tmp:
        tmp.write(msg)
        tmp.flush()
        os.fsync(tmp.fileno())
        os.rename(tmp.name, os.path.join(path, fname))


def atomic_read(path: str, timeout: float = 5):
    """
    Atomically read a file

    Parameters
    ----------
    path : str
        Path to the file
    timeout : float
        Time to wait for file creation
    """
    now = time.time()
    while (time.time() - now) < timeout:
        try:
            with open(path, "r") as file:
                content = file.read()
                return content.strip()
        except FileNotFoundError:
            pass

    raise MetaflowFunctionRuntimeException(f"Timeout while waiting on {path}")


def setup_code_packages(code_package: str, task_code_path: str, directory: str):
    """
    Set up code packages by downloading necessary files and preparing the environment.

    If the task package is present, we extract it into the specified directory and add
    the function package to the system path.

    If the task package is NOT present, we extract the function package into the specified
    directory instead. We need to extract it since the environment will look for the INFO
    file to set up the extensions properly.

    Uses file-based locking to ensure thread-safe extraction when multiple processes
    try to extract to the same directory simultaneously.

    Parameters
    ----------
    code_package : str
        Metaflow function code package
    task_code_path : str
        Path to the task code package
    directory : str
        The directory where the code should be extracted

    Returns
    -------
    bool
        True if packages were expanded
    """
    if not is_s3(code_package):
        MetaflowFunctionRuntimeException("The code package path must be an S3 path.")

    # Use parent directory for lock file to avoid race in directory creation
    parent_dir = os.path.dirname(directory)
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir, exist_ok=True)

    completion_marker = os.path.join(directory, ".extraction_complete")

    # Fast path: if extraction is already complete, return immediately
    # without creating any lock files
    if os.path.exists(completion_marker):
        debug.functions_exec(
            f"Working directory {directory} already extracted (marker found)."
        )
        return False

    # Extraction not complete - acquire lock to extract or wait for another process
    lock_file_path = os.path.join(parent_dir, f".{os.path.basename(directory)}.lock")

    # Use 'a' mode to avoid truncating on each open, and to create if doesn't exist
    with open(lock_file_path, "a") as lock_file:
        debug.functions_exec(f"Acquiring lock for {directory}")
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            # Double-check if extraction completed while we were waiting for lock
            if os.path.exists(completion_marker):
                debug.functions_exec(
                    f"Working directory {directory} already extracted (marker found after lock)."
                )
                return False

            # Create directory if it doesn't exist
            already_created = False
            try:
                os.mkdir(directory)
                debug.functions_exec(f"Working directory {directory} created.")
            except FileExistsError:
                debug.functions_exec(f"Working directory {directory} already created.")
                already_created = True
            debug.functions_exec(f"Function working directory: {directory}")

            # Setup code packages only if marker doesn't exist
            # (This handles both new directories and partial extraction scenarios)
            if not os.path.exists(completion_marker):
                from metaflow import S3

                with S3() as s3:
                    if task_code_path is None:
                        raise MetaflowFunctionRuntimeException(
                            "Attribute `task_code_path` is not available in FunctionSpec and is required "
                            "for executing a bound Metaflow Function"
                        )
                    paths_dict = download_s3_packages(code_package, s3, task_code_path)
                    function_package_path = paths_dict.get(code_package)
                    if not function_package_path:
                        raise MetaflowFunctionRuntimeException(
                            "Missing function package."
                        )

                    task_package_path = None
                    if task_code_path is not None:
                        task_package_path = paths_dict.get(task_code_path)

                    if task_package_path:
                        extract_tar_file(task_package_path, directory)

                    extract_zip_file(function_package_path, directory)

                # Create completion marker to indicate extraction is done
                with open(completion_marker, "w") as f:
                    f.write(f"Extraction completed at {time.time()}\n")
                debug.functions_exec(f"Extraction complete for {directory}")

                # Clean up lock file - no longer needed once marker exists
                try:
                    os.unlink(lock_file_path)
                    debug.functions_exec(f"Cleaned up lock file {lock_file_path}")
                except OSError:
                    pass  # Ignore errors - lock file cleanup is best-effort

            return not already_created
        finally:
            # Lock is automatically released when file is closed
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            debug.functions_exec(f"Released lock for {directory}")


def download_s3_packages(
    code_package: str, s3: S3, task_code_path: str
) -> Dict[str, str]:
    """
    Download packages from S3 and prepare local paths.

    This function identifies which packages need to be downloaded from S3 and
    which are already local. It downloads the necessary packages and returns
    a dictionary mapping original paths to local paths.

    Parameters
    ----------
    code_package : str
        Metaflow function code package
    s3 : S3
        The S3 client to use for downloading packages.
    task_code_path : str
        The task code path to avoid reloading Task object

    Returns
    -------
    Dict[str, str]
        A dictionary mapping original package paths to their local paths.
    """
    to_download: List[str] = []
    paths_dict: Dict[str, str] = {}

    if not task_code_path:
        raise MetaflowFunctionRuntimeException(
            f"Expected `task_code_path` to be provided, but got `{task_code_path}`"
        )

    # Consolidate paths and determine which are local vs. S3
    for path in [code_package, task_code_path]:
        if path:
            if is_s3(path):
                to_download.append(path)
            else:
                paths_dict[path] = path

    # Download S3 objects and update paths dictionary
    for s3obj in s3.get_many(to_download):
        if s3obj.path:
            paths_dict[s3obj.url] = os.path.abspath(s3obj.path)
        else:
            raise MetaflowFunctionRuntimeException(f"Failed to download {s3obj.url}")

    return paths_dict


def extract_zip_file(zip_path: str, directory: str):
    """
    Extract a ZIP file to the specified directory.

    Parameters
    ----------
    zip_path : str
        The path to the ZIP file to extract.
    directory : str
        The directory where the ZIP file should be extracted.
    """
    with cd(directory):
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall()


def extract_tar_file(tar_path: str, directory: str):
    """
    Extract a tar file to the specified directory.

    Parameters
    ----------
    tar_path : str
        The path to the tar file to extract.
    directory : str
        The directory where the tar file should be extracted.
    """
    with cd(directory):
        with tarfile.open(tar_path, mode="r") as tar:
            tar.extractall()


def run_in_path(loader_func: Callable[[], Any], root_path: str) -> Any:
    """
    Execute a function in a root path

    This utility function handles the common pattern of:
    1. Change to temp directory
    2. Execute loader function
    3. Restore original directory

    Parameters
    ----------
    loader_func : Callable[[], Any]
        Function to execute in the package context (e.g., load/reconstruct functions)
    root_path : str
        Directory to run function

    Returns
    -------
    Any
        The result of the loader function
    """
    # Change to the temporary directory to load the function
    original_cwd = os.getcwd()

    # Add temp directory to Python path so imports work
    os.chdir(root_path)
    sys.path.insert(0, root_path)

    try:
        # Execute the loader function
        return loader_func()
    finally:
        # Always restore the original directory and Python path
        os.chdir(original_cwd)
        # Remove the entry this call added, rather than restoring a snapshot
        # of the whole list. A snapshot restore also deletes every sys.path
        # change made by anyone else while the load was open: another thread
        # loading concurrently loses the entry it is importing through, and a
        # persistent entry LocalBackend.start() added disappears while its
        # refcount still claims it is there, so that function's deferred
        # imports start failing and its close() silently removes nothing.
        try:
            sys.path.remove(root_path)
        except ValueError:
            # Already gone -- loaded code removed it, or another holder of the
            # same directory took the copy we inserted. Either way there is
            # nothing of ours left to take back.
            pass


def get_environment_from_metadata(system_metadata: Dict[str, Any]) -> str:
    """
    Get the environment specification from system metadata.

    Parameters
    ----------
    system_metadata : Dict[str, Any]
        The system metadata containing environment information

    Returns
    -------
    str
        Environment identifier string
    """
    if system_metadata:
        env_info = system_metadata.get("environment", {})
        return env_info.get("alias", "")
    return ""


def extract_code_packages(
    code_package: str, task_code_path: str, base_path: str
) -> str:
    """
    Extract code packages to a directory.

    Simple utility - just downloads and extracts code, nothing else.

    Parameters
    ----------
    code_package : str
        S3 path to function code package
    task_code_path : str
        S3 path to task code package
    base_path : str
        Base directory to extract into

    Returns
    -------
    str
        Path to the directory containing extracted code
    """
    # Reuse existing setup_code_packages logic
    setup_code_packages(code_package, task_code_path, base_path)
    return base_path


ACTIVATE_SHIM = """# Generated by metaflow-functions for hosts that activate an environment.
#
# An environment created by Conda.create_for_name() has no activate script --
# conda-pack writes one, create_for_name does not -- and a host that starts a
# process *in* this environment rather than with its python binary needs one.
# Triton's python backend is the case this exists for: it sources
# $EXECUTION_ENV_PATH/bin/activate and then execs its stub with whatever
# environment that left behind.
export VIRTUAL_ENV="{prefix}"
export PATH="{prefix}/bin:$PATH"
export LD_LIBRARY_PATH="{prefix}/lib:${{LD_LIBRARY_PATH:-}}"
unset PYTHONHOME
"""


def ensure_activate_script(prefix: str) -> str:
    """Write an ``activate`` script into ``prefix/bin`` if it has none.

    Returns the prefix. Idempotent, and never overwrites: an environment that
    came with its own activate (a conda-pack'd one, say) keeps it.
    """
    activate = os.path.join(prefix, "bin", "activate")
    if not os.path.exists(activate):
        with open(activate, "w") as f:
            f.write(ACTIVATE_SHIM.format(prefix=prefix))
        os.chmod(activate, 0o755)
    return prefix


def _pin_local_datastore_root() -> None:
    """Give the conda machinery a datastore root that does not depend on the caller.

    ``Conda.__init__`` calls ``LocalStorage.get_datastore_root_from_config``, which,
    with no ``METAFLOW_DATASTORE_SYSROOT_LOCAL`` set, walks *up from the current
    working directory* looking for a ``.metaflow`` and creates one wherever it runs
    out of parents. That is reasonable for a CLI a user runs inside their project,
    and wrong for a host process that materialises an environment on someone else's
    behalf: a serving host's cwd is arbitrary (``/`` for a stub Triton exec'd), and
    when it is not writable the failure surfaces as a ``PermissionError`` from a
    directory creation nobody asked for, which reads like a conda problem and is not.

    So pin it, and pin it next to the conda tree this is about to write to: those two
    belong on the same volume, and ``CONDA_LOCAL_PATH`` is already the host's answer
    to "where does metaflow keep big things". Only ever a default -- an explicit
    ``METAFLOW_DATASTORE_SYSROOT_LOCAL`` still wins, so a caller that does have a
    project root keeps it.
    """
    if os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL"):
        return

    from metaflow.metaflow_config import CONDA_LOCAL_PATH

    base = CONDA_LOCAL_PATH or tempfile.gettempdir()
    root = os.path.join(base, "metaflow-functions-datastore")
    try:
        os.makedirs(root, exist_ok=True)
    except OSError as e:
        # Nothing to gain by failing here: leaving the variable unset just restores
        # the cwd walk, which is what the caller would have got anyway.
        debug.functions_exec("Could not pin a local datastore root at %s: %s" % (root, e))
        return
    os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = root
    debug.functions_exec("Pinned local datastore root: %s" % root)


def environment_python_version(prefix: str) -> Optional[str]:
    """The ``major.minor`` python an environment at ``prefix`` carries.

    Reported from here rather than left to the caller to work out. A host that
    execs something else *into* this environment has to know the version to
    decide whether it can -- Triton's python backend ships a stub built against
    one specific ``libpython``, and loading an environment built against another
    fails inside the stub with nothing useful in the message. The caller owns
    the policy ("which versions can I run"); this owns the fact, because the
    layout it is read off is conda's and therefore metaflow's.

    ``lib/pythonX.Y/`` is the thing to read: conda always creates it, it is
    cheap to list, and it does not depend on a shared libpython existing --
    a static-python environment has no ``libpython3.Y.so`` at all.

    A conda environment has exactly one such directory, so the choice below only
    matters for a prefix that is not one. Sorting numerically rather than by name
    is what keeps that case sane: a system prefix like ``/usr`` carries
    python2.7, python3.10 and python3.11 together, and sorting the names as
    strings answers "2.7", because ``"python2.7" < "python3.10"``. Asked about
    /usr in a jammy image whose python is 3.10.12, this returned 2.7 -- which as
    a stub-compatibility answer would refuse a perfectly good model.
    """
    lib = os.path.join(prefix, "lib")
    try:
        entries = os.listdir(lib)
    except OSError:
        return None
    found = []
    for entry in entries:
        match = re.fullmatch(r"python(\d+)\.(\d+)", entry)
        if match and os.path.isdir(os.path.join(lib, entry)):
            found.append((int(match.group(1)), int(match.group(2))))
    if not found:
        return None
    major, minor = max(found)
    return "%d.%d" % (major, minor)


def materialize_conda_environment(system_metadata: Dict[str, Any]) -> str:
    """Create the environment this function was published against; return its prefix.

    Split out of :func:`resolve_conda_environment`, which wants the python
    binary inside it. A caller that has to hand the whole environment to
    something else -- a Triton ``EXECUTION_ENV_PATH``, a container image build
    -- wants the prefix, and wants it to have an activate script.

    The environment is content-addressed by its alias, so two functions
    published against the same one share a single local copy and only the first
    pays to create it.
    """
    if os.environ.get("METAFLOW_FUNCTIONS_TEST_MODE") == "1":
        debug.functions_exec("Test mode: using current prefix: %s" % sys.prefix)
        return sys.prefix

    if not system_metadata:
        raise MetaflowFunctionRuntimeException("System metadata is missing")

    environment = system_metadata.get("environment")
    if not isinstance(environment, dict):
        raise MetaflowFunctionRuntimeException(
            "Environment metadata is missing or not a dictionary"
        )
    alias = environment.get("alias")
    arch = environment.get("arch")
    if alias is None:
        raise MetaflowFunctionRuntimeException("Environment alias is missing")

    def no_echo(*args, **kwargs):
        pass

    _pin_local_datastore_root()

    # Imported here rather than at module scope: this is the only use of Conda,
    # and a *serving* environment carries the serving stack but not the conda
    # builder -- it was created from a resolved environment and never resolves
    # one. At module scope the import made the whole backend chain
    # (local_backend -> abstract_backend -> this module) unimportable there:
    #   ModuleNotFoundError: No module named 'metaflow_extensions.netflix_ext'
    # seen loading a function inside its own environment on Triton.
    try:
        from metaflow_extensions.netflixext.plugins.conda.conda import (  # type: ignore
            Conda,
        )
    except ImportError:
        from metaflow_extensions.netflix_ext.plugins.conda.conda import (  # type: ignore
            Conda,
        )

    c = Conda(no_echo, "s3")
    resolved_env = c.environment_from_alias(alias, arch)
    if not resolved_env:
        raise MetaflowFunctionRuntimeException(
            f"Cannot recreate the environment. "
            f"Environment {alias} does not refer to a known environment "
            f"for machine architecture {arch}"
        )

    prefix = c.create_for_name(alias.replace(":", "_"), resolved_env, do_symlink=False)
    debug.functions_exec("Materialized conda environment: %s" % prefix)
    return ensure_activate_script(prefix)


def resolve_conda_environment(system_metadata: Dict[str, Any]) -> str:
    """
    Resolve conda environment and return python binary path.

    Simple utility - just resolves the environment, no trampolines.

    Parameters
    ----------
    system_metadata : Dict[str, Any]
        System metadata containing environment info

    Returns
    -------
    str
        Path to python binary in the conda environment
    """
    # In test mode, skip expensive conda resolution and use current Python
    if os.environ.get("METAFLOW_FUNCTIONS_TEST_MODE") == "1":
        debug.functions_exec(f"Test mode: using current Python: {sys.executable}")
        return sys.executable

    python_path = os.path.join(
        materialize_conda_environment(system_metadata), "bin", "python"
    )
    debug.functions_exec(f"Resolved conda environment: {python_path}")
    return python_path


def generate_trampolines_for_directory(directory: str):
    """
    Generate escape trampolines in a directory.

    Simple utility - just generates trampolines.

    Parameters
    ----------
    directory : str
        Directory to generate trampolines in
    """
    debug.functions_exec(f"Generating trampolines in {directory}")
    generate_trampolines(directory)
