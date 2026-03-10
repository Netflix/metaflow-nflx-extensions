from __future__ import annotations

import os
import sys
import time
import tempfile
import tarfile
import zipfile
import contextlib
import fcntl
from typing import TYPE_CHECKING, Dict, Any, Callable, List

if TYPE_CHECKING:
    from metaflow import S3

from metaflow.plugins.env_escape import generate_trampolines

try:
    from metaflow_extensions.nflx.plugins.conda.conda import Conda  # type: ignore
except ImportError:
    from metaflow_extensions.netflix_ext.plugins.conda.conda import Conda  # type: ignore
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
    original_sys_path = sys.path.copy()

    # Add temp directory to Python path so imports work
    os.chdir(root_path)
    sys.path.insert(0, root_path)

    try:
        # Execute the loader function
        return loader_func()
    finally:
        # Always restore the original directory and Python path
        os.chdir(original_cwd)
        sys.path[:] = original_sys_path


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
    import os
    import sys

    if os.environ.get("METAFLOW_FUNCTIONS_TEST_MODE") == "1":
        debug.functions_exec(f"Test mode: using current Python: {sys.executable}")
        return sys.executable

    if not system_metadata:
        raise MetaflowFunctionRuntimeException("System metadata is missing")

    environment = system_metadata.get("environment")
    if not isinstance(environment, dict):
        raise MetaflowFunctionRuntimeException(
            "Environment metadata is missing or not a dictionary"
        )
    alias = environment.get("alias")
    arch = environment.get("arch")

    def no_echo(*args, **kwargs):
        pass

    c = Conda(no_echo, "s3")
    if alias is None:
        raise MetaflowFunctionRuntimeException("Environment alias is missing")
    resolved_env = c.environment_from_alias(alias, arch)
    if not resolved_env:
        raise MetaflowFunctionRuntimeException(
            f"Cannot recreate the environment. "
            f"Environment {alias} does not refer to a known environment "
            f"for machine architecture {arch}"
        )

    env_name = alias.replace(":", "_")
    binary_path = c.create_for_name(env_name, resolved_env, do_symlink=False)
    python_path = binary_path + "/bin/python"

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
