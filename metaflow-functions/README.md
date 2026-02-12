# Metaflow Functions

Metaflow Functions are a way to share computations with other
systems. These computations could be any aspect of an ML pipeline,
e.g. feature engineering, a model, a whole pipeline in your favorite
framework, etc. It could contain your own custom modules and
interfaces (e.g. all your custom code) alongside your favorite data
science packages.

### Background

Functions can be shared without the user of the function needing to
know the details of:
- The environment (e.g. python packages), 
- The code (e.g. your function and supporting files), 
- The parameters (e.g. the weights, or configuration of a model, or 
  details of your custom framework) 

All these are provided from a Metaflow `Step` during training. A Function can be
rehydrated from its reference without worry of these details, only data must be 
provided. This is a property we call **relocatable computation**. Different 
runtimes (e.g. default python, spark, ray) can interpret a Metaflow 
Function metadata and decide the best runtime implementation. 

We provide a 
default one that works in pure python with no dependencies, with all the 
encapsulation features mentioned above. Think of a Metaflow Function's as an 
abstract UX for creating different types of relocatable computations for 
different user experiences. You write your code once, and we target a back-end 
that is suitable for your computation.

### Metaflow Function UX (`DataFrameFunction`)

`DataFrameFunction` is a concrete implementation of a Metaflow
Function. It's a useful abstraction for processing tabular data, and
its meant to work with other tools Metaflow provides like `Table`, and
`MetaflowDataFrame`. You can ship around your tabular data processing
functions to external system. We are actively building bridges to
other paved path MLP provided systems like online serving and offline
serving, more to come on these integrations.

## DataframeFunction Entities

There are three entities to represent a metaflow dataframe function
and the composition of functions:

Note - These are user facing entities, internally these can be
implemented as derived classes from common base classes, and
additionally derived classes can be created with similar
relationships.

1. `dataframe_function`: The raw, callable function. It's decorated
with `@dataframe_function` which enforces the type
structure. `@dataframe_function`'s must take as a first argument a
MetaflowDataFrame (possibly typed) and as a second argument a
`FunctionParameters` (Also possibly typed), which indicates the name
and types parameters expected. `FunctionParameters` is required to be
passed at the call point or have defaults. Calling this function will
run as a normal function in the current environment.

2. `DataFrameFunction`: A class that wraps a `_dataframe_function`. It
represents an atomic execution unit, including its environment and
arguments/parameters (e.g. provided from a `Task`), and packaging of
the code for distribution to external systems. It represents a
relocatable unit of computation where run time parameters are fixed
and only data must be provided. It is a callable that takes only a
single parameter, a `MetaflowDataFrame`, which executes the function
on the dataframe in an isolated environment.

3. `DataFrameFunctionSystem`: `DataFrameFunctionSystem` is a container that represents
the composition of one or more `DataFrameFunction`'s. Implementations
provide additional semantics to aid in the composition of
`DataFrameFunction`'s. The result of encapsulating `DataFrameFunction`'s into a
system is another `DataFrameFunction` The additional information to
express relationships is represented as metadata and can be
implemented by a run time.

### `dataframe_function` decorator

```python
from metaflow import MetaflowDataFrame as MDF
from metaflow import FunctionParameters as FP

@dataframe_function
def f0(data: MDF, params: FP = FP(constant=1)) -> MDF:
    pandas_df = data.to_pandas()
    pandas_df['count'] = pandas_df['count'] + params.constant    
    return MDF.from_pandas(pandas_df)
```

A function decorated with `@dataframe_function` must satisfy the following
conditions:
1. The first argument must be a `MetaflowDataFrame` (possibly typed)
2. The second argument must be a `FunctionParameters` (also possibly typed). 
3. The function must return a `MetaflowDataFrame`
   (possibly typed).

The function is a callable and can be used just like any other function in 
python. The annotated function will have support for static type checking
as well as runtime type checking. More on typing later.

```python
output1 = f0(input_data)
output2 = f0(input_data, params=FP(constant=1))
```

### `DataFrameFunction` class

`DataFrameFunction` constructor takes a function annotated 
with `dataframe_function` and a task that:
1. Provides arguments for the `FunctionParameters`
2. Supplies the environment information that is necessary to operate on the 
   `FunctionParameters` within the function.

This class essentially binds the raw function to a task object and instantiates
the function parameters with artifact information from the task. The 
binding can happen outside a flow as well as within a flow. It is also 
a callable and can be used as shown below:

```python
# Bind within flow
from my_functions import f0
from metaflow import DataFrameFunction

class BindFlow(FlowSpec):
    @step
    def start(self):
        self.constant = 1
        self.next(self.end)

    @step
    def end(self):
        task = ... #use client to get task
        self.function = DataFrameFunction(f0, task=task)

# Bind outside flow
F0 = DataFrameFunction(f0, task=task)
```

The `DataFrameFunction` is also a callable. Unlike the raw function, it
will only take a single argument, a `MetaflowDataFrame`, and during execution,
it will execute the function in an isolated environment defined by the
`task` object. Using it is as simple as:

```python
output = F0(input_data)
```

### `Type` information

The user can encode type information in the function signature using a 
combination of python type hints and dataclasses. An example of a function
with type information is shown below:

```python
from metaflow import MetaflowDataFrame as MDF
from metaflow import FunctionParameters as FP
from typing import Any, Protocol

class MyMdfInp1(Protocol):
    x: Any

class MyMdfInp2(Protocol):
    x: int

class MyMdfInp3(Protocol):
    x: int
    y: int
    
class Inp4:
    x: int
    y: str
    z: MyDataClass3

class MyMdfOut1(Protocol):
    a: Any
    
class MyMdfOut2(Protocol):
    a: Int

# Defining Functions
# The input and output MetaflowDataFrame types are not specified in f0
# We can pass any dataframe to f0, and the output can also be any dataframe
@dataframe_function
def f0(data: MDF, arguments: FP) -> MDF:
    pass

# The input dataframe needs to at least have a column called 'x' of type Any
# The output dataframe will at least have a column called 'a' of type Any
@dataframe_function
def f1(data: MDF[MyMdfInp1], arguments: FP) -> MDF[MyMdfOut1]:
    pass

# The input dataframe needs to at least have a column called 'x' of type int
# The output dataframe will at least have a column called 'a' of type int
@dataframe_function
def f2(data: MDF[MyMdfInp2], arguments: FP) -> MDF[MyMdfOut2]:
    pass

# The input dataframe needs to at least have columns called 'x' and 'y' of type int
# The output dataframe will at least have a column called 'a' of type int
@dataframe_function
def f3(data: MDF[MyMdfInp3], arguments: FP) -> MDF[MyMdfOut2]:
    pass
```

For the purpose of dataframe functions, we will assume that the typing is 
covariant. This is common for read-only structures, like `Sequence[T]`, where a
`Sequence[Dog]` can be treated as a `Sequence[Animal]` because every Dog is an
Animal. Specifically for `MetaflowDataFrame`, this means that if a function takes
a dataframe of type A, it can also take a dataframe of type B where B is a subclass
of A. 

Additionally, we will use structural typing instead of nominal typing for 
type checks. **What this means is that we will check if the dataframe has the 
required columns and types, and it is fine if the dataframe has more columns**. 

Here are some examples of how this would work in practice:

```python
input_data0: Any = MDF.from_pandas(df, ...)
input_data1: MDF[MyMdfInp1] = MDF.from_pandas(df, ...)
input_data2: MDF[MyMdfInp2] = MDF.from_pandas(df, ...)
input_data3: MDF[MyMdfInp3] = MDF.from_pandas(df, ...)

# f0 can take any dataframe
output0 = f0(input_data0) # should work 
output1 = f0(input_data1) # should work
output2 = f0(input_data2) # should work
output3 = f0(input_data3) # should work

# f1 can take any dataframe that at least has a column called 'x' of type Any
output0 = f1(input_data0) # fails, since input_data0 may not have a column called 'x'
output1 = f1(input_data1) # should work
output2 = f1(input_data2) # should work
output3 = f1(input_data3) # should work

# f2 can take any dataframe that at least has a column called 'x' of type int
output0 = f2(input_data0) # fails, since input_data0 may not have a column called 'x'
output1 = f2(input_data1) # fails, since input_data1 will have a column called 'x' but it could be not of type int
output2 = f2(input_data2) # should work
output3 = f2(input_data3) # should work

# f3 can take any dataframe that at least has columns called 'x' and 'y' of type int
output0 = f3(input_data0) # fails, since input_data0 may not have columns called 'x' and 'y'
output1 = f3(input_data1) # fails, since input_data1 may not have column 'y' or column 'x' might not be of type int
output2 = f3(input_data2) # fails, since input_data2 may not have column 'y' 
output3 = f3(input_data3) # should work
```

Metaflow will expose a method/class to help users define these protocols 
encoding type information easily. 
```python
from metaflow import MDFSchemaGen

# Returns a protocol that has columns x and y of type Any
my_schema1 = MDFSchemaGen(['x', 'y'])

# Returns a protocol that has columns a and b of type int
my_schema2 = MDFSchemaGen([('a', int), ('b', int)])

# Complex example
# Returns a protocol that has columns x and y of type int
# and column z consists of a list of structs that implement the my_schema2 protocol
# i.e. z is a 
my_schema3 = MDFSchemaGen([
    ('x', int), 
    ('y', int), 
    ('z', List[my_schema2]),
])
```

The type checks will be done at compile time using `mypy`, and during 
runtime we will raise an error if the input or output dataframes do not respect 
the type information. The underlying types to use to encode the type information
of columns is still open for discussion:

- `arrow` or `avro` is an option since we are using `arrow` for 
  serialization and deserialization of the dataframes.
- `pyiceberg` types since the data is most likely coming from Iceberg tables 
  and there will be scaffolding on the JVM side for it already. 
- Native python types like str , Dict , List and more

### `DataFrameFunctionSystem` UX 

`DataFrameFunctionSystem` is a container that takes one or more 
`DataFrameFunction`'s and has methods to compose these `DataFrameFunction`'s 
into a DAG. The `DataFrameFunctionSystem` also has the same semantics as 
those of `DataFrameFunction`, i.e. it is a callable, and it relocatable.  

```python


schema0 = MDFSchemaGen(['x1', 'x2'])
schema1 = MDFSchemaGen(['y1', 'y2'])
schema2 = MDFSchemaGen(['z1', 'z2'])
schema3 = MDFSchemaGen(['w1', 'w2'])
inp: MDF[schema0] = MDF.from_pandas(df, ...)

# f0(schema0) -> schema1
# f1(schema1) -> schema2
# f2(schema2) -> schema3

# Users can implicitly define the DAG
# The only requirement is that all functions need to output unique column names
# to help with disambiguation
# f0 -> f1 -> f2
# Consolidated input: [x1, x2]
# Consolidated output: [w1, w2]
my_function_system1 = DataFrameFunctionSystem(
   [f0, f1, f2],
)

# Users can explicitly define the DAG and
# Users can remap column names between functions when defining the DAG
# f0 -> f2 -> f1
my_function_system3 = DataFrameFunctionSystem(
   [f0, f1, f2], 
)
my_function_system3.add_edge(
   source="f0", 
   target="f2", 
   column_mapping={
      "z1": "y1", 
      "z2": "y2",
   }
)
my_function_system3.add_edge(
   source="f2", 
   target="f1", 
   column_mapping={
      "y1": "w1",
      "y2": "w2",
    }
)

# Usage
output = my_function_system1(input_data)
```

The output of the `DataFrameFunctionSystem` is a `MetaflowDataFrame` that 
has all the columns from all the leaf nodes in the DAG. The 
`DataFrameFunctionSystem` class will expose methods to view the DAG, view 
the consolidated schema, and more.

## Metaflow Function Implementation details

All Metaflow function decorators and classes will inherit from a base 
decorator and Function class. There will be two entities in the base 
function and decorator implementation:

1. `MetaflowFunctionDecorator`: The raw, callable function that the user can 
   specify using the `@mf_function` annotation. The decorator simply marks 
   that a function is a Metaflow function. Note: User will never use this directly, 
   and will instead use its concrete implementation like `@dataframe_function` or
   `@pytorch_function`.
2. `MetaflowFunction`: A class that wraps a function annotated with 
   `@mf_function`. Note: User will never use this directly, and will instead 
   use its concrete implementation like `DataFrameFunction` or `PytorchFunction`.

### Base Function Decorator Implementation

The base function decorator is a simple callable that takes a function, sets 
the `is_metaflow_function` attribute to True, and does some validation of 
the function signature.

```python
import functools

class MetaflowFunctionDecorator:
    TYPE = "metaflow_function"

    def __init__(self, func):
        # Preserves original function's metadata
        functools.update_wrapper(self, func) 
        self.func = func
        self.func.is_metaflow_function = True
        self._validate_function_signature()

    def __call__(self, *args, **kwargs):
        # Validate arguments before calling the wrapped function
        self.validate(args, kwargs)
        # Execute the wrapped function
        out = self.func(*args, **kwargs)
        self.validate(out)
        return out
    
    def _validate_function_signature(self):
        """
        Placeholder for function signature validation logic.
        """
        pass

    def validate(self, *args, **kwargs):
        """
        Placeholder for validation logic.
        """
        pass
```

Custom decorators like `@dataframe_function` will inherit from this and add 
custom validation logic. For instance,

```python
class DataFrameFunctionDecorator(MetaflowFunctionDecorator):
    TYPE = "dataframe_function"
    pass

# We will only expose dataframe_function to the user, and not the 
# DataFrameFunctionDecorator class
dataframe_function = DataFrameFunctionDecorator
```

### Implementation of `DataFrameFunction`

The `DataFrameFunction` class will be a concrete implementation of the
`MetaflowFunction` base class. The base `MetaflowFunction` class will expose 
the following interface:

```python
class MetaflowFunction(object):
    def __init__(self, func, task):
        self.func = func
        self.task = task
        self._validate_function()
    
    def _validate_function(self):
        pass

    def runtime(self):
        # Each Function will implement its own runtime and override the runtime logic from the parent function's
        pass
    
    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __getstate__(self, *args, **kwargs):
        pass

    def __setstate__(self, *args, **kwargs):
        pass

    @classmethod
    def from_reference(cls: Type[T], path: str) -> T:
        """
        Return a MetaflowFunction object from reference file
        """
        pass
```

Note: An active question is whether the run time validation should happen
in the raw function or in the `MetaflowFunction` class.

### Implementation of Typing

We will simply make `MetaflowDataFrame` and `FunctionParameters` a generic class that takes a type parameter.

```python
from typing import Generic, TypeVar, Type
from dataclasses import fields
import pandas as pd
from typing import Any

T_co = TypeVar('T', covariant=True)
T_FP_co = TypeVar('T_FP_co', covariant=True)

class MetaflowDataFrame(Generic[T_co]):
    def __init__(self, *args, **kwargs):
        pass

class FunctionParameters(Generic[T_FP_co]):
    pass
```

### Implementation of `DataFrameFunctionSystem`

There will be no base class since other function systems may or may not 
support composition.

```python
class DataFrameFunctionSystem:
    def __init__(self, functions: List[DataFrameFunction]):
        pass
    
    def add_edge(self, source: str, target: str, column_mapping: Optional[Dict[str, str]] = None):
        pass
    
    def _construct_dag(self):
        pass
    
    def __call__(self, *args, **kwargs):
        pass
    
    def visualize(self):
        pass
    
    def get_schema(self):
        pass
    
    def __repr__(self):
        pass
```
