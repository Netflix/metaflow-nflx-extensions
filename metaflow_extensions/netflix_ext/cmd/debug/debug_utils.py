import os
import sys
import json
import shutil
import traceback
import subprocess
from metaflow import Task, S3
from typing import Any, Dict, Optional, Tuple
from metaflow.exception import CommandException
from metaflow.plugins.env_escape import generate_trampolines
from metaflow_extensions.nflx.cmd.debug.constants import Constants


def get_code_package_location(task: Task) -> str:
    """
    Gets the code package location for the task.

    Parameters
    ----------
    task : Task
        The task object.

    Returns
    -------
    str
        The code package location for the task.
    """
    code_package = task.metadata_dict.get("code-package", "").strip()
    code_package = json.loads(code_package)
    return code_package.get("location")


def get_python_version(task: Task) -> str:
    """
    Gets the python version for the task.

    Parameters
    ----------
    task : Task
        The task object.

    Returns
    -------
    str
        The python version for the task.
    """
    python_version = task.metadata_dict.get(
        "python_version", Constants.DEFAULT_PYTHON_VERSION
    )
    return ".".join(python_version.split(".")[:2])


def copy_stub_generator_to_metaflow_root_dir(metaflow_root_dir: str):
    """
    Copies the debug and current stub generator to the metaflow root directory.

    Parameters
    ----------
    metaflow_root_dir : str
        The metaflow root directory.
    """
    debug_stub_generator_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "debug_stub_generator.py",
    )
    current_stub_generator_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "current_stub_generator.py",
    )
    shutil.copy(debug_stub_generator_path, metaflow_root_dir)
    shutil.copy(current_stub_generator_path, metaflow_root_dir)


def fetch_environment_type(task: Task) -> str:
    """
    Fetches the conda environment type for the task.

    Parameters
    ----------
    task : Task
        The task object.

    Returns
    -------
    str
        The conda environment type for the task.
    """
    conda_env_id = task.metadata_dict.get("conda_env_id", "").strip()
    if conda_env_id == "":
        return "default"
    elif conda_env_id.startswith("[") and conda_env_id.endswith("]"):
        return "new_conda"
    raise CommandException(
        "The conda environment type for the task is not supported. Please use the new conda decorators avaailable in "
        "Metaflow."
    )


def _set_python_environment(
    obj,
    task: Task,
    task_pathspec: str,
    override_env: Optional[str] = None,
    override_env_from_pathspec: Optional[str] = None,
) -> str:
    """
    Sets the python environment for the debug task.

    Parameters
    ----------
    obj : Any
        The command object.
    task : Task
        The task object.
    task_pathspec : str
        The pathspec of the task.
    override_env : Optional[str], optional, default None
        Name of the named environment to use for debugging the task. Overrides the environment used in the Task.
    override_env_from_pathspec : Optional[str], optional, default None
        Pathspec to use as environment for debugging the task. Overrides the environment used in the Task.

    Returns
    -------
    str
        The path to the python executable for the debug task.

    """
    if override_env is not None and override_env_from_pathspec is not None:
        raise CommandException(
            "Only one of --override-env and --override-env-from-pathspec can be specified"
        )

    conda_environment_type = fetch_environment_type(task)
    path_spec = "/".join(task_pathspec.split("/")[:-1])
    path_spec_formatted = path_spec.replace("/", "_").replace("-", "_")
    mf_python_path, mf_env, mf_env_name = sys.executable, None, None
    obj.echo(f"Conda environment type: {conda_environment_type}")

    if conda_environment_type == "new_conda":
        mf_env_name = override_env if override_env else path_spec_formatted.lower()
        path_spec = (
            override_env_from_pathspec if override_env_from_pathspec else path_spec
        )
        mf_env = [override_env] if override_env else ["--pathspec", f"{path_spec}"]
        obj.echo(
            f"Creating conda environment: {mf_env_name} with pathspec: {path_spec}"
        )
        conda_command = [
            "metaflow",
            "environment",
            "--quiet",
            "create",
            "--name",
            mf_env_name,
            "--install-notebook",
            "--force",
        ]
        conda_command.extend(mf_env)
        # Check if the pathspec is a conda environment
        try:
            result = subprocess.run(
                conda_command,
                check=True,
                capture_output=True,
            )
            # We have to parse the stderr and find the exact line where the python path is present
            for line in result.stderr.decode().split("\n"):
                if "/bin/python" in line:
                    mf_python_path = line
                    break
            return mf_python_path
        except subprocess.CalledProcessError as e:
            raise CommandException(
                f"Error creating conda environment: {e.stderr.decode()}"
            )
        except Exception as e:
            raise CommandException("Setting python environment failed: {}".format(e))
    return mf_python_path


def _extract_code_package(obj, task: Task, metaflow_root_dir: str):
    """
    Extracts the code package for the task in the metaflow root directory.

    Parameters
    ----------
    obj : Any
        The command object.
    task : Task
        The task object.
    metaflow_root_dir : str
        The metaflow root directory.
    """
    os.makedirs(metaflow_root_dir, exist_ok=True)
    code_package_path = os.path.join(metaflow_root_dir, "code_package.tar.gz")
    code_package_location = get_code_package_location(task)
    try:
        with S3() as s3:
            res = s3.get(code_package_location)
            with open(code_package_path, "wb") as f:
                f.write(res.blob)

        # We now untar the code package
        shutil.unpack_archive(code_package_path, metaflow_root_dir)
        obj.echo(f"Code package extracted to {metaflow_root_dir}")
    except Exception as e:
        raise CommandException(
            "Error in downloading or extracting code package: {}".format(e)
        )


def _find_kernel_name(
    python_executable: Optional[str] = None,
) -> Optional[Tuple[str, str]]:
    """
    Finds the jupyter kernel name for the python executable. This is a best effort
    function and may potentially fail in some cases. We return None in those cases
    and will not set the kernel in the generated notebook.

    Parameters
    ----------
    python_executable : Optional[str], optional, default None
        The python executable path.

    Returns
    -------
    Optional[Tuple[str, str]]
        The kernel name and the kernel's display name if found, None otherwise.
    """
    try:
        output = subprocess.check_output(["jupyter", "kernelspec", "list", "--json"])
        kernelspecs = json.loads(output)
        for kernel_name, kernel_spec in kernelspecs["kernelspecs"].items():
            if kernel_spec["spec"]["argv"][0] == python_executable:
                return kernel_name, kernel_spec["spec"]["display_name"]
    except Exception as e:
        # Ignore the exception and return None as it is a best effort function
        print(f"Error finding kernel name: {traceback.format_exc()}")
        pass
    return None


def _generate_escape_trampolines(metaflow_root_dir: str):
    """
    Generates the escape trampolines for the task.
    """
    trampoline_dir = os.path.join(metaflow_root_dir, "_escape_trampolines")
    os.makedirs(trampoline_dir, exist_ok=True)
    generate_trampolines(trampoline_dir)
