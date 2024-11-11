import os

from typing import Dict, List, Any
from metaflow import Task, namespace, Step
from metaflow.plugins.env_escape import generate_trampolines


class DebugStubGenerator(object):
    def __init__(self, task_pathspec):
        """
        Initializes the DebugStubGenerator.

        Parameters
        ----------
        task_pathspec : str
            The pathspec of the task.
        """
        self.task_pathspec = task_pathspec
        self.task = Task(task_pathspec, _namespace_check=False)
        self.step_name = self.task.parent.id
        self.run_id = self.task.parent.parent.id
        self.flow_name = self.task.parent.parent.parent.id
        self.is_new_conda_step = self.is_new_conda_step()
        self.workflow_dag = self.task["_graph_info"].data
        self.file_name = self.workflow_dag["file"]
        self._dag_structure = self.get_dag_structure(self.workflow_dag["steps"])
        self.step_type = self.get_step_type(self.step_name)
        self._previous_nodes = self.find_previous_nodes(
            self.step_name, self._dag_structure
        )
        self.task_line_num = self._dag_structure[self.step_name]["line"]
        self.node = self._dag_structure[self.step_name]
        self.previous_steps = self.get_previous_steps(self._previous_nodes)
        self.previous_tasks = self.get_previous_tasks(self.previous_steps)
        self.task_namespace = self.get_task_namespace()

    @staticmethod
    def get_dag_structure(dag: Dict[str, Any]) -> Dict[str, Any]:
        """
        Returns the simplified DAG structure of the workflow.

        Parameters:
        ----------
        dag : Dict[str, Any]
            The DAG structure of the workflow.
        Returns:
        ----------
        Dict[str, Any]
            Simplified DAG structure of the workflow.
        """
        dag_structure = {}
        for node, attributes in dag.items():
            dag_structure[node] = {
                "next": attributes["next"],
                "type": attributes["type"],
                "line": attributes["line"],
            }
        return dag_structure

    def is_new_conda_step(self) -> bool:
        """
        Returns True if the step is a new conda step, False otherwise.

        Returns:
        ----------
        bool
            True if the step is a new conda step, False otherwise.
        """
        return "conda_env_id" in self.task.metadata_dict

    def get_step_type(self, step_name: str) -> str:
        """
        Returns the type of the step.

        Parameters
        ----------
        step_name : str
            The name of the step.

        Returns
        -------
        str
            The type of the step.
        """
        return self._dag_structure[step_name]["type"]

    @staticmethod
    def find_previous_nodes(node: str, dag_structure: Dict[str, Any]) -> List[str]:
        """
        Returns the previous nodes for a given node.

        Parameters
        ----------
        node : str
            The name of the node to find the previous nodes for.
        dag_structure : Dict[str, Any]
            The DAG structure of the workflow.

        Returns
        -------
        List[str]
            The list of previous nodes.
        """
        if node == "start":
            return []
        previous_nodes = []
        for node_name, attributes in dag_structure.items():
            if node in attributes["next"]:
                previous_nodes.append(node_name)
        return previous_nodes

    @staticmethod
    def get_join_type(previous_steps: List[Step]) -> str:
        """
        Returns the type of join for the step.

        Parameters
        ----------
        previous_steps : List[Step]
            The list of previous steps.

        Returns
        -------
        str
            The type of join for the step.
        """
        return "foreach" if len(previous_steps) == 1 else "static"

    def get_previous_steps(self, previous_nodes: List[str]) -> List[Step]:
        """
        Returns the previous steps for a given step.

        Parameters
        ----------
        previous_nodes : List[str]
            The list of previous nodes.

        Returns
        -------
        List[Step]
            The list of previous steps.
        """
        return [
            Step(
                "{}/{}/{}".format(self.flow_name, self.run_id, n),
                _namespace_check=False,
            )
            for n in previous_nodes
        ]

    def get_previous_tasks(self, previous_steps: List[Step]) -> List[Task]:
        """
        Returns the previous tasks for a given step.

        Parameters
        ----------
        previous_steps : List[Step]
            The list of previous steps.

        Returns
        -------
        List[Task]
            The list of previous tasks.
        """
        step_type = self.get_step_type(self.step_name)
        if step_type == "join" and self.get_join_type(self.previous_steps) == "foreach":
            return sorted(
                [task for task in previous_steps[0].tasks()], key=lambda x: x.index
            )
        # TODO: This method is incomplete and incorrect in the general case. We actually
        # need more information to return the exact subset of tasks that are runtime
        # parents in some situations with nested foreaches. For now, best effort but
        # we need to revisit/fix these. The primary limitation right now is that the
        # `foreach-stack` field we are referring to is capped at some number of characters
        # which can fail to work for deeply nested foreaches or even shallow ones with
        # long values.
        foreach_list = self.task.metadata_dict.get("foreach-stack", [])
        if not foreach_list:
            # We are not part of a foreach so return all the previous tasks. This will
            # be a list of 1 for most everything except for a join.
            return [step.task for step in previous_steps]

        # We are part of a foreach, we want to list tasks that either have the same
        # foreach_list or match everything but the last element
        def _filter(t):
            t_foreach_list = t.metadata_dict.get("foreach-stack", [])
            return (
                len(t_foreach_list) == len(foreach_list)
                and t_foreach_list == foreach_list
            ) or t_foreach_list == foreach_list[:-1]

        to_return = []
        for step in previous_steps:
            to_return.extend([task for task in step if _filter(task)])
        return to_return

    def get_task_namespace(self) -> str:
        """
        Returns the namespace of the task.

        Returns
        -------
        str
            The namespace of the task.
        """
        task_namespace = None
        for val in self.task.tags:
            if val.startswith("production:") or val.startswith("user:"):
                task_namespace = val
                break
        return task_namespace
