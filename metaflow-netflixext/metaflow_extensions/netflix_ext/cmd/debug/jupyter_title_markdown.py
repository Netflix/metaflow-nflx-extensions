_jupyter_title_markdown = """
# Debugging Notebook for Task: {TaskPathSpec}

### Debug Type: {DebugType}

- A `Pre-Execution State` debug type implies that you can inspect the state of 
the task before it began execution. 
- For instance, if the current Metaflow step is called `stepB` and the 
previous step was `stepA`, then you are essentially debugging the state 
after `stepA` finished execution and before `stepB` began execution.
- In case of a `Post-Execution State` debug type, you can inspect the state 
of the task after it has finished execution. In this case, you can inspect 
the state of the task after `stepB` has finished execution, and before any 
succeeding step has started.
"""
