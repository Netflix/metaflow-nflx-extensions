import os

from typing import Dict, List, Any, Optional
from metaflow import Task, namespace, Step, Run
from metaflow.plugins.env_escape import generate_trampolines


class CurrentStubGenerator(object):
    def __init__(self, task_pathspec):
        """
        Initializes the CurrentStubGenerator.

        Parameters
        ----------
        task_pathspec : str
            The pathspec of the task.
        """
        self._task_pathspec = task_pathspec
        self._task = Task(task_pathspec, _namespace_check=False)
        self._step_name = self.task.parent.id
        self._run_id = self.task.parent.parent.id
        self._flow_name = self.task.parent.parent.parent.id

    def get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        """
        Returns the value of the attribute with the given key.

        Parameters
        ----------
        key : str
            The key of the attribute.
        default : Any, optional, default None
            The default value to return if the key is not found.

        Returns
        -------
        Any
            The value of the attribute with the given key, or the default value
            if the key is not found.
        """
        return getattr(self, key, default)

    def __getattr__(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        """
        Called when an attribute lookup has not found the attribute in the usual places.

        Parameters
        ----------
        key : str
            The key of the attribute.
        default : Any, optional, default None
            The default value to return if the key is not found.

        Raises
        ------
        AttributeError
            If the attribute does not exist.
        """
        raise AttributeError(
            "'{}' object has no attribute '{}'. The Current Stub does not support all the "
            "Current singleton attributes yet.".format(type(self).__name__, key)
        )

    def __contains__(self, key: str) -> bool:
        """
        Returns True if the attribute with the given key exists, False otherwise.

        Parameters
        ----------
        key : str
            The key of the attribute.

        Returns
        -------
        bool
            True if the attribute with the given key exists, False otherwise.
        """
        return getattr(self, key, None) is not None

    @property
    def is_running_flow(self) -> bool:
        """
        Always returns True as this is a stub generator for a running flow.

        Returns
        -------
        bool
            True
        """
        return True

    @property
    def flow_name(self) -> Optional[str]:
        """
        The name of the currently executing flow.

        Returns
        -------
        str, optional
            Flow name.
        """
        return self._flow_name

    @property
    def run_id(self) -> Optional[str]:
        """
        The run ID of the currently executing run.

        Returns
        -------
        str, optional
            Run ID.
        """
        return self._run_id

    @property
    def step_name(self) -> Optional[str]:
        """
        The name of the currently executing step.

        Returns
        -------
        str, optional
            Step name.
        """
        return self._step_name

    @property
    def task_id(self) -> Optional[str]:
        """
        The task ID of the currently executing task.

        Returns
        -------
        str, optional
            Task ID.
        """
        return self.task.id

    @property
    def retry_count(self) -> int:
        """
        The index of the task execution attempt.

        Returns
        -------
        int
            The index of the task execution attempt.
        """
        return int(self._task.metadata_dict.get("attempt", 0))

    @property
    def origin_run_id(self) -> Optional[str]:
        """
        The original run ID of the currently executing run.

        Returns
        -------
        str, optional
            Original run ID.
        """
        return str(self._task.metadata_dict.get("origin-run-id", None))

    @property
    def pathspec(self) -> str:
        """
        The pathspec of the currently executing task.

        Returns
        -------
        str
            Pathspec.
        """
        return self._task_pathspec

    @property
    def task(self) -> Task:
        """
        The currently executing task.

        Returns
        -------
        Task
            The currently executing task.
        """
        return self._task

    @property
    def run(self) -> Run:
        """
        The currently executing run.

        Returns
        -------
        Run
            The currently executing run.
        """
        return self.task.parent.parent
