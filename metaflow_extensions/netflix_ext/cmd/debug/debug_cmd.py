import os
import tarfile
import datetime

from metaflow import namespace, Flow, Run, Step, Task
from metaflow._vendor import click
from typing import Any, Dict, Optional, Union
from metaflow.exception import CommandException
from metaflow.cli import echo_dev_null, echo_always
from .debug_script_generator import DebugScriptGenerator
from .debug_utils import (
    fetch_environment_type,
    copy_stub_generator_to_metaflow_root_dir,
    _set_python_environment,
    _extract_code_package,
    _find_kernel_name,
    _generate_escape_trampolines,
)


class CommandObj:
    def __init__(self):
        pass


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.group(help="Post/Pre Step debugging commands")
@click.option(
    "--quiet/--no-quiet",
    show_default=True,
    default=False,
    help="Suppress unnecessary messages",
)
@click.pass_context
def debug(
    ctx: Any,
    quiet: bool,
):
    if quiet:
        echo = echo_dev_null
    else:
        echo = echo_always

    obj = CommandObj()
    obj.quiet = quiet
    obj.echo = echo
    obj.echo_always = echo_always
    ctx.obj = obj


@debug.command(hidden=True)
@click.option(
    "--task-pathspec",
    help="Pathspec for the task to generate the escape trampolines for",
    required=True,
)
@click.option(
    "--metaflow-root-dir",
    help="Root directory where the escape trampolines should be inserted",
    required=True,
)
@click.option(
    "--inspect",
    help="If 'true' or '1', generates script to inspect the state of the task after it has finished running",
    type=str,
    default="false",
)
@click.option(
    "--generate-notebook",
    help="If 'true' or '1', setup a notebook with debugging params",
    type=str,
    default="true",
)
@click.option(
    "--python-executable",
    help="The python executable path for the debug task",
    required=False,
    default=None,
)
@click.pass_obj
def generate_debug_scripts(
    obj,
    task_pathspec: str,
    metaflow_root_dir: str,
    inspect: str,
    generate_notebook: str,
    python_executable: Optional[str] = None,
):
    """
    Generates the debug scripts for the task.
    """
    # Move logic to _generate_debug_scripts to make it reusable
    _generate_debug_scripts(
        obj,
        task_pathspec,
        metaflow_root_dir,
        inspect,
        generate_notebook,
        python_executable,
    )


@debug.command()
@click.option(
    "--metaflow-root-dir",
    help="Root directory where the code package and debug scripts/notebooks should be generated",
    required=True,
)
@click.option(
    "--override-env",
    help="Name of the named environment to use for debugging the task. Overrides the Conda environment from this task",
)
@click.option(
    "--override-env-from-pathspec",
    help="Pathspec to use as environment for debugging the task. Overrides the Conda environment from this task",
)
@click.option(
    "--inspect",
    help="If true, allows the user to inspect the state of the task after it has finished running",
    is_flag=True,
    default=False,
    show_default=True,
)
@click.argument(
    "pathspec",
)
@click.pass_obj
def task(
    obj,
    pathspec: str,
    metaflow_root_dir: str,
    override_env: Optional[str] = None,
    override_env_from_pathspec: Optional[str] = None,
    inspect: bool = False,
):
    """
    Create a new debugging notebook based on named_env name or pathspec.
    """
    # Move logic to _execute_local_backend to make it reusable
    _execute_local_backend(
        obj,
        pathspec,
        metaflow_root_dir,
        override_env,
        override_env_from_pathspec,
        inspect,
    )


def _execute_local_backend(
    obj,
    pathspec: str,
    metaflow_root_dir: str,
    override_env: Optional[str] = None,
    override_env_from_pathspec: Optional[str] = None,
    inspect: bool = False,
):
    """
    Executes the local backend for the task.

    Parameters
    ----------
    obj : CommandObj
        The command object.
    pathspec : str
        The pathspec to a task (can be Flow, Run or Step as well if using
        latest_successful_run, end step or unique task in step leads to a unique task).
    metaflow_root_dir : str
        The root directory where the debug scripts should be generated.
    override_env : Optional[str], optional, default None
        Name of the named environment to use for debugging the task. Overrides the environment used in the Task.
    override_env_from_pathspec : Optional[str], optional, default None
        Pathspec to use as environment for debugging the task. Overrides the environment used in the Task.
    inspect : bool, optional, default False
        If true, allows the user to inspect the state of the task after it has finished running.
    """
    cur_task = None
    path_components = pathspec.split("/")
    if not path_components:
        raise CommandException("Provide either a flow, run, step or task to debug")
    if len(path_components) < 2:
        # Enforce namespace here as we are getting the latest run (so we don't want
        # to cross boundaries as that could be confusing)
        r = Flow(path_components[0]).latest_successful_run
        if r is None:
            raise CommandException(
                "Flow {} can only be specified if there is a successful run in the "
                "current namespace. Please specify a run, step or task as this is not "
                "the case".format(path_components[0])
            )
        path_components.append(r.id)
    else:
        r = Run("/".join(path_components[:2]), _namespace_check=False)

    if len(path_components) < 3:
        path_components.append("end")
    if len(path_components) < 4:
        # Enforce single task
        cur_task = None
        for t in r[path_components[2]]:
            if cur_task is not None:
                raise CommandException(
                    "Step {} does not refer to a single task -- please specify the "
                    "task unambiguously".format("/".join(path_components))
                )
            cur_task = t
        if cur_task is None:
            raise CommandException(
                "Step {} does not contain any tasks".format("/".join(path_components))
            )
        path_components.append(cur_task.id)
    else:
        cur_task = r[path_components[2]][path_components[3]]
    task_pathspec = "/".join(path_components)

    if "code-package" not in cur_task.metadata_dict:
        raise CommandException(
            "Task {} does not have code-package. `debug task command only supports "
            "tasks that were executed remotely.".format(task_pathspec)
        )
    python_executable = _set_python_environment(
        obj, cur_task, task_pathspec, override_env, override_env_from_pathspec
    )
    obj.echo(
        "Python executable path for debugging is set to {}".format(python_executable)
    )
    obj.echo(
        "Generating debug scripts for task {} using local backend".format(task_pathspec)
    )
    # We download the code package from the task, extract it, generate the escape trampolines,
    # and then generate the debug scripts
    _extract_code_package(obj, cur_task, metaflow_root_dir)
    _generate_escape_trampolines(metaflow_root_dir)
    _generate_debug_scripts(
        obj,
        task_pathspec,
        metaflow_root_dir,
        inspect,
        generate_notebook="true",
        python_executable=python_executable,
    )


def _generate_debug_scripts(
    obj,
    task_pathspec: str,
    metaflow_root_dir: str,
    inspect: Union[str, bool],
    generate_notebook: str,
    python_executable: Optional[str] = None,
):
    """
    Generates the debug scripts for the task.

    Parameters
    ----------
    obj : CommandObj
        The command object.
    task_pathspec : str
        The pathspec of the task.
    metaflow_root_dir : str
        The root directory where the debug scripts should be generated.
    inspect : Union[str, bool]
        If True, 'true' or '1', generates script to inspect the state of the task after
        it has finished running.
    generate_notebook : str
        If 'true' or '1', setup a notebook with debugging params.
    python_executable : Optional[str], optional, default None
        The python executable path for the debug task.
    """
    os.makedirs(metaflow_root_dir, exist_ok=True)
    python_executable = python_executable.strip() if python_executable else None
    flow_name = task_pathspec.split("/")[0]
    debug_file_name = task_pathspec.replace("/", "_").replace("-", "_") + "_debug.py"
    debug_script_generator = DebugScriptGenerator(task_pathspec, inspect)
    script = debug_script_generator.generate_debug_script(metaflow_root_dir, flow_name)
    debug_script_generator.write_debug_script(
        metaflow_root_dir, script, debug_file_name
    )

    generate_notebook = generate_notebook.lower() in ["true", "1"]
    if generate_notebook:
        kernel_def = _find_kernel_name(python_executable)
        if kernel_def:
            obj.echo(
                f"Jupyter kernel name: {kernel_def[0]} with display name: {kernel_def[1]}"
            )
        notebook_json = debug_script_generator.generate_debug_notebook(
            metaflow_root_dir, debug_file_name, kernel_def
        )
        debug_script_generator.write_debug_notebook(metaflow_root_dir, notebook_json)

    obj.echo(
        f"Debug scripts and notebook generated at {metaflow_root_dir}",
        fg="green",
        bold=True,
    )

    # We copy the stub generators to the metaflow root directory as the stub generators
    # may not be present in the metaflow version that the user is using.
    copy_stub_generator_to_metaflow_root_dir(metaflow_root_dir)
