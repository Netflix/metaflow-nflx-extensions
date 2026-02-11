import os
from .jupyter_instructions_markdown import _jupyter_instructions_markdown
from .jupyter_title_markdown import _jupyter_title_markdown


class Constants(object):
    # Default Python version
    DEFAULT_PYTHON_VERSION = "3.10"

    # Name of the main debug file
    DEBUG_SCRIPT_NAME = "00_DEBUG_ME.ipynb"

    # Stubbed class for Metaflow artifacts
    DEBUG_INPUT_ITEM_STUB = """
    class DebugInputItem({FlowSpecClass}):
        def __init__(self, task):
            self._task = task
            if self.has_index():
                self._index = task.index
            self._artifact_data = {{}}
    
        def has_index(self):
            return hasattr(self._task, "index")
        
        def __repr__(self):
            return self._task.__repr__()
    
        @property
        def index(self):
            if self.has_index():
                return self._index
            else:
                raise AttributeError("Task does not have an 'index' attribute")
    
        def __getattr__(self, name):
            if name in self.__dict__:
                return self.__dict__[name]
            
            if name in self._artifact_data:
                return self._artifact_data[name]
            
            try:
                result = self._task[name]
            except KeyError:
                raise AttributeError(f"No such attribute: {{name}}")
            
            result = result.data
            self._artifact_data[name] = result
            return result
    """

    # Stubbed class for inputs in Metaflow
    DEBUG_INPUT_STUB = """
    class DebugInput({FlowSpecClass}):
        def __init__(self, inputs, previous_steps, join_type):
            self._inputs = inputs
            self._previous_steps = previous_steps
            if join_type == "static":
                self._set_input_attributes()
        
        def _set_input_attributes(self):
            for i, step in enumerate(self._previous_steps):
                setattr(self, step.id, self._inputs[i])
        
        def __getitem__(self, idx):
            return self._inputs[idx]
        
        def __iter__(self):
            return iter(self._inputs)        
    """

    # Stubbed class for linear steps in Metaflow
    DEBUG_LINEAR_STEP_STUB = """
    class DebugLinearStep({FlowSpecClass}):
        def __init__(self, task, prev_task, step_name):
            self._cur_task = task
            self._prev_task = prev_task
            self._current_step = step_name
            if self.has_index():
                self._index = self._cur_task.index    
            self._artifact_data = {{}}
    
        def has_index(self):
            return hasattr(self._cur_task, "index")
    
        @property
        def index(self):
            if self.has_index():
                return self._index
            else:
                raise AttributeError("Task does not have an 'index' attribute")
        
        def __getattribute__(self, name):
            # If attribute is of type Parameter in the base class, use __getattr__ instead
            if isinstance({FlowSpecClass}.__dict__.get(name, None), Parameter):
                return object.__getattribute__(self, '__getattr__')(name)
            return super().__getattribute__(name)
    
        @property
        def input(self):
            if self.has_index():
                self._input_name = self._cur_task["_foreach_stack"].data[-1].var
                return self._prev_task[self._input_name].data[self._index]
            else:
                raise AttributeError("Task does not have any inputs")
    
        def __getattr__(self, name):
            if name in self.__dict__:
                return self.__dict__[name]
            
            if name in self._artifact_data:
                return self._artifact_data[name]
            
            self.load_artifact_data(name)
    
            if name in self._artifact_data:
                return self._artifact_data[name]
            
            raise AttributeError(f"No such attribute: {{name}}")
    
        def __delattr__(self, name):
            if name in self._artifact_data:
                del self._artifact_data[name]
                return
            return super().__delattr__(name)

        def load_artifact_data(self, name):
            for artifact in self._prev_task.artifacts:
                if artifact.id == name:
                    self._artifact_data[name] = artifact.data    
    """

    # Stubbed class for join step in Metaflow
    DEBUG_JOIN_STEP_STUB = """
    class DebugJoinStep({FlowSpecClass}):
        def __init__(self, _task, prev_tasks, inputs, step_name, node):
            self._task = _task
            self._prev_tasks = prev_tasks
            self._inputs = inputs
            self._current_step = step_name
            self.node = node
            if self.has_index():
                self._index = _task.index
            self._to_merge = {{}}
    
        def has_index(self):
            return hasattr(self._task, "index")
    
        @property
        def index(self):
            if self.has_index():
                return self._index
            else:
                raise AttributeError("Task does not have an 'index' attribute")    
        
        def __getattribute__(self, name):
            # If attribute is of type Parameter in the base class, use __getattr__ instead
            if isinstance({FlowSpecClass}.__dict__.get(name, None), Parameter):
                return object.__getattribute__(self, '__getattr__')(name)
            return super().__getattribute__(name)
    
        def __getattr__(self, name):
            if name in self.__dict__:
                return self.__dict__[name]
            
            if name in self._to_merge:
                inp, _ = self._to_merge[name]
                setattr(self, name, inp.__getattr__(name))
                return self.__dict__[name]
            
            raise AttributeError(f"No such attribute: {{name}}")

        def merge_artifacts(
            self,
            inputs,
            exclude=None,
            include=None,
        ) -> None:
            # Most of the code has been taken as is from OSS Metaflow
            # NOTE: Keep this function in sync with the OSS code
            include = include or []
            exclude = exclude or []
            if self.node["type"] != "join":
                msg = (
                    "merge_artifacts can only be called in a join and step *{{step}}* "
                    "is not a join".format(step=self._current_step)
                )
                raise MetaflowException(msg)
            if len(exclude) > 0 and len(include) > 0:
                msg = "`exclude` and `include` are mutually exclusive in merge_artifacts"
                raise MetaflowException(msg)
    
            to_merge = {{}}
            unresolved = []
            for inp in inputs:
                # available_vars is the list of variables from inp that should be considered
                if include:
                    available_vars = (
                        (var.id, var.sha)
                        for var in inp._task.artifacts
                        if (var.id in include) and (not hasattr(self, var.id))
                    )
                else:
                    available_vars = (
                        (var.id, var.sha)
                        for var in inp._task.artifacts
                        if (var.id not in exclude) and (not hasattr(self, var.id))
                    )
                for var, sha in available_vars:
                    _, previous_sha = to_merge.setdefault(var, (inp, sha))
                    if previous_sha != sha:
                        # We have a conflict here
                        unresolved.append(var)
            # Check if everything in include is present in to_merge
            missing = []
            for v in include:
                if v not in to_merge and not hasattr(self, v):
                    missing.append(v)
            if unresolved:
                # We have unresolved conflicts, so we do not set anything and error out
                msg = (
                    "Step *{{step}}* cannot merge the following artifacts due to them "
                    "having conflicting values: [{{artifacts}}]. To remedy this issue, "
                    "be sure to explicitly set those artifacts (using "
                    "self.<artifact_name> = ...) prior to calling merge_artifacts.".format(
                        step=self._current_step, artifacts=", ".join(unresolved)
                    )
                )
                raise UnhandledInMergeArtifactsException(msg, unresolved)
            if missing:
                msg = (
                    "Step *{{step}}* specifies that [{{include}}] should be merged but "
                    "[{{missing}}] are not present. To remedy this issue, make sure "
                    "that the values specified in only come from at least one branch".format(
                        step=self._current_step,
                        include=", ".join(include),
                        missing=", ".join(missing),
                    )
                )
                raise MissingInMergeArtifactsException(msg, missing)
            # If things are resolved, we pass down the variables from the input datastore
            self._to_merge = to_merge        

    """

    # Escape hatch stub
    ESCAPE_HATCH_STUB = """
    import os
    import sys
    if os.environ.get("PYTHONPATH") is None:
        sys.path.insert(0, "{MetaflowEnvEscapeDir}")
    """

    # Imports needed to define stubbed classes & Debug steps
    IMPORTS_STUB = """
    from metaflow.exception import (
        MetaflowException,
        UnhandledInMergeArtifactsException,
        MissingInMergeArtifactsException,
    )
    from debug_stub_generator import DebugStubGenerator
    from current_stub_generator import CurrentStubGenerator
    from metaflow import Parameter, Task
    import metaflow.task
    import importlib
    
    module = importlib.import_module("{MetaflowFileName}")
    for name in dir(module):
        if not name.startswith("__"): 
            globals()[name] = getattr(module, name)
    """

    # Template to instantiate the current stub generator
    CURRENT_STUB_INSTANTIATION_TEMPLATE = """
    current = CurrentStubGenerator("{TaskPathspec}")
    """

    # Template to instantiate the debug stub generator
    DEBUG_STUB_GENERATOR_INSTANTIATION_TEMPLATE = """
    debug_stub_generator = DebugStubGenerator("{TaskPathspec}")
    task = debug_stub_generator.task
    step_name = debug_stub_generator.step_name
    node = debug_stub_generator.node
    previous_steps = debug_stub_generator.previous_steps
    previous_tasks = debug_stub_generator.previous_tasks    
    """

    # Template to create instances of stubbed start classes
    START_STUB_INSTANTIATION_TEMPLATE = """
    self = DebugLinearStep(task, Task("{ParameterTask}"), step_name)
    """

    # Template to create instances of stubbed linear classes
    LINEAR_STUB_INSTANTIATION_TEMPLATE = """
    self = DebugLinearStep(task, previous_tasks[0], step_name)
    """

    # Template to create instances of stubbed join classes
    JOIN_STUB_INSTANTIATION_TEMPLATE = """
    join_inputs = [DebugInputItem(task) for task in previous_tasks]
    join_type = debug_stub_generator.get_join_type(previous_steps)
    inputs = DebugInput(join_inputs, previous_steps, join_type)
    self = DebugJoinStep(task, previous_tasks, inputs, step_name, node)
    """

    # Inspect mode means that we return the state of the task after it has finished running
    # The templates remain the same except that the artifact values are fetched from the current task
    # instead of the previous task

    # Template to create instances of stubbed start classes for inspection
    START_STUB_INSPECT_INSTANTIATION_TEMPLATE = """
    prev_task = Task("{ParameterTask}")
    self = DebugLinearStep(task, task, step_name)
    """

    # Template to create instances of stubbed linear classes for inspection
    LINEAR_STUB_INSPECT_INSTANTIATION_TEMPLATE = """
    self = DebugLinearStep(task, task, step_name)
    """

    # Template to create instances of stubbed join classes for inspection
    JOIN_STUB_INSPECT_INSTANTIATION_TEMPLATE = """
    join_inputs = [DebugInputItem(task) for task in previous_tasks]
    join_type = debug_stub_generator.get_join_type(previous_steps)
    inputs = DebugInput(join_inputs, previous_steps, join_type)
    self = DebugJoinStep(task, task, inputs, step_name, node)
    """

    # Template for importing the namespace of the current task
    CURRENT_TASK_NAMESPACE_STUB = """
    from metaflow import namespace
    namespace("{Namespace}")
    """

    # Template for rendering the instructions in markdown
    JUPYTER_INSTRUCTIONS_MARKDOWN = _jupyter_instructions_markdown

    # Template for rendering the notebook title in markdown
    JUPYTER_TITLE_MARKDOWN = _jupyter_title_markdown
