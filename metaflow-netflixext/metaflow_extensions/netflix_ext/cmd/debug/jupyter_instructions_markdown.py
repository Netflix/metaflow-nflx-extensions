_jupyter_instructions_markdown = """
### Instructions for Debugging 

##### 1. Execute the previous cells before running the code cell below.

##### 2. Select the correct kernel for the notebook.
- By default, the kernel was selected based on the environment we created for 
debugging the task. In case the kernel is not selected correctly, 
follow the steps given below.
- If the task originally ran in a conda environment, select the kernel whose 
display name starts with `Metaflow ...`. Else, select any python kernel 
available.

##### 3. Add the code for the Task in the cells below!
- The code for the task is in the file `{FileName}` and the step begins from 
line no: `{StepStartLine}` and ends at line no: `{StepEndLine}`. 
- You can copy the code inside the function definition and execute it line 
  by line (or however you want) to debug the task as you wish. 
- The function definition has also been provided below for reference:

```python
{FunctionDefinition}
```
"""
