import os
import sys
import traceback
from typing import TYPE_CHECKING, Dict, Callable


if TYPE_CHECKING:
    from metaflow_extensions.nflx.plugins.functions.core.function_parameters import (
        FunctionParameters,
    )

from metaflow_extensions.nflx.plugins.functions.debug import debug
from metaflow_extensions.nflx.plugins.functions.core.function_spec import (
    FunctionSpec,
)
from metaflow_extensions.nflx.plugins.functions.backends.memory.runtime_config import (
    RuntimeConfig,
)
from metaflow_extensions.nflx.plugins.functions.config import Config
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionUserException,
)


def print_and_exit(
    message: str,
    exit_code: int = RuntimeConfig.SYSTEM_EXIT_CODE,
    file=sys.stderr,
    keep_alive: bool = False,
):
    debug.functions_exec(
        f"Exit interpreter (code={exit_code}, keep_alive={keep_alive})."
    )
    print(message, file=file, flush=True)
    if not keep_alive:
        print(f"Process exit code: {exit_code}", file=file, flush=True)
        sys.exit(exit_code)


def set_shell_env(function_dir: str) -> Dict[str, str]:
    """
    Set up shell environment variables for function execution.

    Returns
    -------
    Dict[str, str]
        Dictionary of environment variables
    """
    env = os.environ.copy()

    # Add any function-specific environment variables here if needed
    update_packaging_env_vars(env, function_dir)
    debug.functions_exec(f"Env variables set: {env}.")

    return env


def update_packaging_env_vars(env: Dict[str, str], function_dir: str) -> None:
    from metaflow.packaging_sys import MetaflowCodeContent

    # explicitly add function_dir to PYTHONPATH for the trampolines
    env["PYTHONPATH"] = f"{function_dir}"

    for key, value in MetaflowCodeContent.get_env_vars_for_packaged_metaflow(
        function_dir
    ).items():
        if key.endswith(":"):
            # If the key ends with a colon, we override the existing value
            env[key[:-1]] = value
        elif key not in env:
            env[key] = value
        else:
            # If the key already exists, we append the value to the existing one
            env[key] = f"{env[key]}:{value}"


def create_function_parameters(
    func_spec: FunctionSpec,
    prefetch_artifacts: bool = False,
) -> "FunctionParameters":
    """
    Create FunctionParameters from a function spec.

    Parameters
    ----------
    func_spec : FunctionSpec
        The function specification containing task information
    prefetch_artifacts : bool, default False
        If True, pre-fetch all artifacts from S3 during initialization to populate
        the cache. This improves first execute() call latency at the cost of slower
        initialization. If any artifact fails to download, initialization will fail
        with an exception.

    Returns
    -------
    FunctionParameters
        The function parameters object (or a subclass if specified in parameter_schema)
    """
    # Lazy import to avoid circular imports
    from metaflow_extensions.nflx.plugins.functions.core.function_parameters import (
        FunctionParameters,
    )
    from metaflow_extensions.nflx.plugins.functions.utils import load_type_from_string
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionException,
    )

    # Check if the function spec has a parameter_schema specifying a concrete type
    parameter_class = FunctionParameters
    if (
        func_spec.function
        and func_spec.function.parameter_schema
        and "type" in func_spec.function.parameter_schema
    ):
        type_string = func_spec.function.parameter_schema["type"]
        # Load the concrete parameter class
        loaded_class = load_type_from_string(type_string)
        if not loaded_class:
            raise MetaflowFunctionException(
                f"Failed to load parameter class '{type_string}' specified in parameter_schema"
            )
        if not issubclass(loaded_class, FunctionParameters):
            raise MetaflowFunctionException(
                f"Parameter class '{type_string}' must be a subclass of FunctionParameters"
            )
        parameter_class = loaded_class

    return parameter_class(
        function_spec=func_spec, prefetch_artifacts=prefetch_artifacts
    )


def run_with_exception_handling(
    func: Callable[[], None], keep_alive: bool = False
) -> None:
    """
    Run a function with standardized exception handling for the runtime.

    Parameters
    ----------
    func : Callable[[], None]
        The function to run with exception handling
    keep_alive: bool
        Do not terminate the process on error
    """
    try:
        func()
    except MetaflowFunctionUserException as e:
        debug.functions_exec(f"User exception thrown (keep_alive={keep_alive}).")
        print_and_exit(
            traceback.format_exc(),
            exit_code=RuntimeConfig.USER_EXIT_CODE,
            file=sys.stderr,
            keep_alive=keep_alive,
        )

    except Exception as e:
        debug.functions_exec(f"System exception thrown (keep_alive={keep_alive}).")
        print_and_exit(
            traceback.format_exc(),
            exit_code=RuntimeConfig.SYSTEM_EXIT_CODE,
            keep_alive=keep_alive,
        )


def is_runtime_directory(directory: str) -> bool:
    """
    Check if a directory appears to be a runtime function directory.

    Parameters
    ----------
    directory : str
        The directory path to check

    Returns
    -------
    bool
        True if this appears to be a runtime function directory
    """
    dir_name = os.path.basename(directory)
    return dir_name.startswith(Config.RUNTIME_FUNCTION_DIR_PREFIX)
