import os
import json
import textwrap
from typing import List, Dict, Any, Optional, Tuple, Union
from metaflow import Step
from metaflow_extensions.nflx.cmd.debug.constants import Constants
from metaflow_extensions.nflx.cmd.debug.debug_utils import fetch_environment_type
from metaflow_extensions.nflx.cmd.debug.debug_stub_generator import DebugStubGenerator


class DebugScriptGenerator(object):
    def __init__(self, task_pathspec: str, inspect: Union[str, bool]):
        """
        Initializes the debugScriptGenerator.

        Parameters
        ----------
        task_pathspec : str
            The pathspec of the task.
        inspect : Union[str, bool]
            The inspect flag.
        """
        self.task_pathspec = task_pathspec
        if isinstance(inspect, bool):
            self.inspect = inspect
        else:
            self.inspect = True if inspect.lower() in ("true", "1") else False
        self.debug_stub_generator = DebugStubGenerator(task_pathspec)

    def namespace_imports(self, script: str) -> str:
        """
        Appends the namespace imports to the script.

        Parameters
        ----------
        script : str
            The script for the step.

        Returns
        -------
        str
            The script with the namespace imports appended.
        """
        script += textwrap.dedent(
            Constants.CURRENT_TASK_NAMESPACE_STUB.format(
                Namespace=self.debug_stub_generator.task_namespace
            )
        )
        return script

    def append_imports(self, metaflow_root_dir: str, script: str) -> str:
        """
        Appends the imports to the script.

        Parameters
        ----------
        metaflow_root_dir : str
            The root directory of the Metaflow installation.
        script : str
            The script for the step.

        Returns
        -------
        str
            The script with the imports appended.
        """
        script += textwrap.dedent(
            Constants.IMPORTS_STUB.format(
                MetaflowEnvEscapeDir=os.path.join(
                    metaflow_root_dir, "_escape_trampolines"
                ),
                MetaflowFileName=self.debug_stub_generator.file_name.split(".")[0],
            )
        )
        return script

    def append_workbench_instantiation(self, script: str) -> str:
        """
        Appends the debug stub generator instantiation to the script.

        Parameters
        ----------
        script : str
            The script for the step.

        Returns
        -------
        str
            The script with the debug stub generator instantiation appended.
        """
        script += textwrap.dedent(
            Constants.DEBUG_STUB_GENERATOR_INSTANTIATION_TEMPLATE.format(
                TaskPathspec=self.task_pathspec
            )
        )
        return script

    @staticmethod
    def append_stubbed_classes(flow_name: str, script: str) -> str:
        """
        Appends the stubs to the script.

        Parameters
        ----------
        flow_name : str
            The name of the flow.
        script : str
            The script for the step.

        Returns
        -------
        str
            The script with the stubs appended.
        """
        script += textwrap.dedent(
            Constants.DEBUG_INPUT_ITEM_STUB.format(
                FlowSpecClass=flow_name,
            )
        )
        script += textwrap.dedent(
            Constants.DEBUG_INPUT_STUB.format(
                FlowSpecClass=flow_name,
            )
        )
        script += textwrap.dedent(
            Constants.DEBUG_LINEAR_STEP_STUB.format(
                FlowSpecClass=flow_name,
            )
        )
        script += textwrap.dedent(
            Constants.DEBUG_JOIN_STEP_STUB.format(
                FlowSpecClass=flow_name,
            )
        )
        return script

    def append_join_step_instantiation(self, script: str) -> str:
        """
        Appends the join step instantiation to the script.

        Parameters
        ----------
        script : str
            The script for the step.

        Returns
        -------
        str
            The script with the join step instantiation appended.
        """
        template = (
            Constants.JOIN_STUB_INSPECT_INSTANTIATION_TEMPLATE
            if self.inspect
            else Constants.JOIN_STUB_INSTANTIATION_TEMPLATE
        )
        script += textwrap.dedent(template)
        return script

    def append_linear_step_instantiation(self, script: str) -> str:
        """
        Appends the linear step instantiation to the script.

        Parameters
        ----------
        script : str
            The script for the step.

        Returns
        -------
        str
            The script with the linear step instantiation appended.
        """
        template = (
            Constants.LINEAR_STUB_INSPECT_INSTANTIATION_TEMPLATE
            if self.inspect
            else Constants.LINEAR_STUB_INSTANTIATION_TEMPLATE
        )
        script += textwrap.dedent(template)
        return script

    def append_start_step_instantiation(self, script: str) -> str:
        """
        Appends the start step instantiation to the script.

        Parameters
        ----------
        script : str
            The script for the step.

        Returns
        -------
        str
            The script with the start step instantiation appended.
        """
        _run_pathspec = "/".join(self.task_pathspec.split("/")[:2])
        prev_task = Step(
            os.path.join(_run_pathspec, "_parameters"), _namespace_check=False
        ).task
        template = (
            Constants.START_STUB_INSPECT_INSTANTIATION_TEMPLATE
            if self.inspect
            else Constants.START_STUB_INSTANTIATION_TEMPLATE
        )
        script += textwrap.dedent(template.format(ParameterTask=prev_task.pathspec))
        return script

    def append_current_stub_instantiation(self, script: str) -> str:
        """
        Appends the current stub instantiation to the script.

        Parameters
        ----------
        script : str
            The script for the step.

        Returns
        -------
        str
            The script with the current stub instantiation appended.
        """
        script += textwrap.dedent(
            Constants.CURRENT_STUB_INSTANTIATION_TEMPLATE.format(
                TaskPathspec=self.task_pathspec
            )
        )
        return script

    @staticmethod
    def write_debug_script(
        metaflow_root_dir: str, debug_script: str, debug_file_name: str
    ) -> None:
        """
        Writes the debug script for the step.

        Parameters
        ----------
        metaflow_root_dir : str
            The root directory of the Metaflow installation.
        debug_script : str
            The debug script for the step.
        debug_file_name : str
            The name of the debug script.
        """
        debug_script_path = os.path.join(metaflow_root_dir, debug_file_name)
        with open(debug_script_path, "w") as f:
            f.write(debug_script)

    def generate_debug_script(self, metaflow_root_dir: str, flow_name: str) -> str:
        """
        Generates the debug script for the step.

        Parameters
        ----------
        metaflow_root_dir : str
            The root directory of the Metaflow installation.
        flow_name : str
            The name of the flow.

        Returns
        -------
        str
            The debug script for the step.
        """
        print("Generating debug script for task: {}".format(self.task_pathspec))
        # We first write out the imports
        debug_script = self.append_imports(metaflow_root_dir, "")

        # We then write out the namespace imports
        debug_script = self.namespace_imports(debug_script)

        # We then write out the stubbed classes
        debug_script = self.append_stubbed_classes(flow_name, debug_script)

        # We instantiate the current stub generator
        debug_script = self.append_current_stub_instantiation(debug_script)

        # We instantiate the debug stub generator
        debug_script = self.append_workbench_instantiation(debug_script)

        # We then write out the stubs instantiation
        if self.debug_stub_generator.step_type == "start":
            debug_script = self.append_start_step_instantiation(debug_script)
        elif self.debug_stub_generator.step_type == "join":
            debug_script = self.append_join_step_instantiation(debug_script)
        else:
            debug_script = self.append_linear_step_instantiation(debug_script)

        return debug_script

    def generate_debug_notebook(
        self,
        metaflow_root_dir: str,
        debug_file_name: str,
        kernel_def: Optional[Tuple[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Generates the debug notebook for the task.

        Parameters
        ----------
        metaflow_root_dir : str
            The root directory of the Metaflow installation.
        debug_file_name : str
            The name of the debug script
        kernel_def : Optional[Tuple[str, str]], optional, default None
            The kernel definition containing the kernel name and the kernel display name.

        Returns
        -------
        Dict[str, Any]
            The dictionary representing the debug notebook for the task.
        """
        # We first generate the notebook cells
        notebook_cells = self.generate_notebook_cells(
            metaflow_root_dir, debug_file_name
        )

        # We then generate the notebook JSON
        notebook_json = self.generate_notebook_json(notebook_cells, kernel_def)
        return notebook_json

    @staticmethod
    def write_debug_notebook(
        metaflow_root_dir: str, notebook_json: Dict[str, Any]
    ) -> None:
        """
        Writes the debug notebook for the task.

        Parameters
        ----------
        metaflow_root_dir : str
            The root directory of the Metaflow installation.
        notebook_json : Dict[str, Any]
            The name of the debug notebook.
        """
        # We then write out the notebook JSON
        debug_notebook_name = "00_DEBUG_ME.ipynb"
        debug_notebook_path = os.path.join(metaflow_root_dir, debug_notebook_name)
        with open(debug_notebook_path, "w") as f:
            json.dump(notebook_json, f, indent=4)

    @staticmethod
    def generate_notebook_json(
        notebook_cells: List[Dict[str, Any]],
        kernel_def: Optional[Tuple[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Generates the notebook JSON for the task.

        Parameters
        ----------
        notebook_cells : List[Dict[str, Any]]
            The list of notebook cells.
        kernel_def : Optional[Tuple[str, str]], optional, default None
            The kernel definition containing the kernel name and the kernel display name.

        Returns
        -------
        Dict[str, Any]
            The notebook JSON for the task.
        """
        # Create an empty notebook
        nb = {
            "cells": [],
            "metadata": {}
            if kernel_def is None
            else {
                "kernelspec": {
                    "name": kernel_def[0],
                    "display_name": kernel_def[1],
                    "language": "python",
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Add cells to the notebook
        for cell in notebook_cells:
            nb_cell = {
                "cell_type": cell.get("format"),
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [cell.get("content")],
            }
            nb["cells"].append(nb_cell)

        return nb

    def get_function_definition(self, metaflow_root_dir: str) -> Tuple[str, int]:
        """
        Returns the function definition and the end line number for the task.

        Parameters
        ----------
        metaflow_root_dir : str
            The root directory of the Metaflow installation.

        Returns
        -------
        Tuple[str, int]
            The function definition and the end line number for the task.
        """
        with open(
            os.path.join(metaflow_root_dir, self.debug_stub_generator.file_name), "r"
        ) as f:
            step_started = False
            function_definition = ""
            for i, line in enumerate(f):
                if i == self.debug_stub_generator.task_line_num:
                    step_started = True

                if step_started:
                    function_definition += line
                    if "self.next(self." in line:
                        return function_definition, i

    def generate_notebook_cells(
        self, metaflow_root_dir: str, debug_file_name: str
    ) -> List[Dict[str, Any]]:
        """
        Generates the notebook cells for the task.

        Parameters
        ----------
        metaflow_root_dir : str
            The root directory of the Metaflow installation.
        debug_file_name : str
            The name of the debug notebook.

        Returns
        -------
        str
            The notebook cells for the task.
        """
        # We first add a markdown cell stating that it is a debugging notebook
        notebook_cells = [
            {
                "format": "markdown",
                "content": Constants.JUPYTER_TITLE_MARKDOWN.format(
                    TaskPathSpec=self.task_pathspec,
                    DebugType="Post-Execution State"
                    if self.inspect
                    else "Pre-Execution State",
                ),
            }
        ]

        # We now add a code cell that incorporates the Metaflow escape hatch
        # We only add this if the user code ran in a conda environment
        if fetch_environment_type(self.debug_stub_generator.task) != "default":
            script = textwrap.dedent(
                Constants.ESCAPE_HATCH_STUB.format(
                    MetaflowEnvEscapeDir=os.path.abspath(
                        os.path.join(metaflow_root_dir, "_escape_trampolines")
                    )
                )
            )
            notebook_cells.append({"format": "code", "content": script.strip()})

        # We now add a code cell that imports the debugging modules
        script = "from {} import * ".format(debug_file_name.split(".")[0])
        notebook_cells.append({"format": "code", "content": script})

        # We now add a markdown cell that tells the user to add their own code
        flow_file_name = self.debug_stub_generator.file_name
        start_line_num = self.debug_stub_generator.task_line_num
        function_definition, end_line_num = self.get_function_definition(
            metaflow_root_dir
        )
        markdown_content = Constants.JUPYTER_INSTRUCTIONS_MARKDOWN.format(
            FileName=flow_file_name,
            StepStartLine=start_line_num,
            StepEndLine=end_line_num,
            FunctionDefinition=textwrap.dedent(function_definition),
        )
        notebook_cells.append(
            {"format": "markdown", "content": textwrap.dedent(markdown_content.strip())}
        )

        # Add an empty cell for the user to add their own code
        notebook_cells.append({"format": "code", "content": ""})
        return notebook_cells
