import importlib
import importlib.util
import inspect
import os
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from metaflow_extensions.nflx.plugins.functions.core.function_parameters import (
    FunctionParameters,
)
from metaflow_extensions.nflx.plugins.functions.debug import debug
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionArgumentsException,
    MetaflowFunctionException,
    MetaflowFunctionReturnTypeException,
)


def extract_function_signature(func: Callable) -> Tuple[Type, Type, str, str]:
    """
    Extract input/output types and their string representations from function signature.

    Parameters
    ----------
    func : Callable
        Function to extract signature from

    Returns
    -------
    Tuple[Type, Type, str, str]
        (input_type, output_type, input_type_str, output_type_str)
    """
    type_hints = get_type_hints(func)
    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    if len(params) < 1:
        raise ValueError(f"Function {func.__name__} must have at least one parameter")

    # Get input type from first parameter (skip 'self' if present)
    first_param = params[0] if params[0].name != "self" else params[1]
    input_type = type_hints.get(first_param.name, Any)

    # Get output type from return annotation
    output_type = type_hints.get("return", Any)

    return (
        input_type,
        output_type,
        get_type_string(input_type),
        get_type_string(output_type),
    )


def get_function_from_path(function_path: str) -> Tuple[Any, Callable[..., Any]]:
    """
    Return module and function from a path 'module.submodule::function'

    Parameters
    ----------
    function_path : str
        Path to the function in the format 'module.submodule::function'

    Returns
    -------
    Tuple[Any, Callable[..., Any]]
        A tuple containing the module and the function

    Raises
    ------
    MetaflowFunctionException
        If the module or function cannot be loaded
    """
    debug.functions_exec(f"Loading function from path '{function_path}'")
    # Split into module path and function name
    if "::" not in function_path:
        raise MetaflowFunctionException(
            f"Invalid function path format: {function_path}. Expected format: 'module.submodule::function'"
        )
    module_name, func_name = function_path.split("::", 1)

    try:
        module = importlib.import_module(module_name)
    except Exception as e:
        raise MetaflowFunctionException(
            "Cannot load module ('%s'): %s" % (module_name, str(e))
        )

    # Check if the function exists in the module
    if not hasattr(module, func_name):
        raise MetaflowFunctionException(
            "function ('%s') is not found in module ('%s')" % (func_name, module_name)
        )

    func = getattr(module, func_name)
    return module, cast(Callable[..., Any], func)


def load_module_from_string(module_name: str) -> Any:
    """
    Load a module from a string

    Parameters
    ----------
    module_name : str
        Name of the module to load

    Returns
    -------
    Any
        The loaded module

    Raises
    ------
    MetaflowFunctionException
        If the module cannot be loaded
    """
    debug.functions_exec("Loading module '%s'" % module_name)
    try:
        module = importlib.import_module(module_name)
        return module

    except ImportError as e:
        raise MetaflowFunctionException(f"Can't load module '{module_name}': {str(e)}")


def load_class_from_string(module_name: str, class_name: str) -> Optional[Type[Any]]:
    """
    Load a class from a string

    Parameters
    ----------
    module_name : str
        Name of the module containing the class
    class_name : str
        Name of the class to load

    Returns
    -------
    Optional[Type[Any]]
        The loaded class, or None if it cannot be loaded

    Raises
    ------
    MetaflowFunctionException
        If the module or class cannot be loaded
    """
    debug.functions_exec(
        "Loading class '%s' from module '%s'" % (class_name, module_name)
    )
    try:
        module = importlib.import_module(module_name)
        if not hasattr(module, class_name):
            raise MetaflowFunctionException(
                f"Class '{class_name}' not found in module '{module_name}'"
            )
        cls = getattr(module, class_name)
        return cast(Type[Any], cls)

    except ImportError as e:
        raise MetaflowFunctionException(f"Can't load module '{module_name}': {str(e)}")

    except AttributeError as e:
        raise MetaflowFunctionException(
            f"Can't load class '{class_name}' from module '{module_name}': {str(e)}"
        )


def load_type_from_string(type_str: str) -> Optional[Type[Any]]:
    """
    Load a type from a string, handling both module.Class and simple Class formats

    Parameters
    ----------
    type_str : str
        Type string in either 'module.Class' or 'Class' format

    Returns
    -------
    Optional[Type[Any]]
        The loaded type, or None if it cannot be loaded
    """
    if "." in type_str:
        module_name, class_name = type_str.rsplit(".", 1)
        return load_class_from_string(module_name, class_name)
    else:
        # First try built-in types
        builtin_types = {
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "bytes": bytes,
            "dict": dict,
            "list": list,
            "tuple": tuple,
            "set": set,
            "frozenset": frozenset,
            "complex": complex,
            "bytearray": bytearray,
        }
        if type_str in builtin_types:
            return builtin_types[type_str]

        # Fall back to globals if not a built-in
        return globals().get(type_str)


def get_type_string(type_hint: Type) -> str:
    """Convert a type hint to a string representation."""
    if hasattr(type_hint, "__module__") and hasattr(type_hint, "__name__"):
        if type_hint.__module__ == "builtins":
            return type_hint.__name__
        return f"{type_hint.__module__}.{type_hint.__name__}"

    if hasattr(type_hint, "__origin__"):
        origin = type_hint.__origin__
        if hasattr(type_hint, "__args__") and type_hint.__args__:
            args = type_hint.__args__
            arg_strs = [get_type_string(arg) for arg in args]
            return f"{get_type_string(origin)}[{', '.join(arg_strs)}]"
        return get_type_string(origin)

    return str(type_hint)


def is_s3(path: str) -> bool:
    """
    Check if a path is an S3 path

    Parameters
    ----------
    path : str
        The path to check

    Returns
    -------
    bool
        True if the path is an S3 path, False otherwise
    """
    return path.lower().startswith("s3:")


def is_absolute_path(path: str) -> bool:
    """
    Check if a path is an absolute path, either on a local filesystem or on S3.

    Parameters
    ----------
    path : str
        The path to check

    Returns
    -------
    bool
        True if the path is absolute, False otherwise
    """
    return is_s3(path) or os.path.isabs(path)


def types_are_equal(type_str1: str, type_str2: str) -> bool:
    """
    Compare two type strings by importing the actual classes and checking if they're the same.

    Parameters
    ----------
    type_str1 : str
        First type string to compare
    type_str2 : str
        Second type string to compare

    Returns
    -------
    bool
        True if the types represent the same class, False otherwise
    """
    try:
        cls1 = load_type_from_string(type_str1)
        cls2 = load_type_from_string(type_str2)

        if cls1 is not None and cls2 is not None:
            return cls1 == cls2
        else:
            return type_str1 == type_str2
    except Exception:
        return type_str1 == type_str2


def validate_function_signature(
    func: Callable,
    expected_param_count: int,
    param_validators: Optional[List[Callable[[Type], bool]]] = None,
    return_validator: Optional[Callable[[Type], bool]] = None,
    decorator_name: str = "function",
    custom_validator: Optional[Callable[[Callable, List, Dict, str], None]] = None,
) -> Tuple[Type, Type, str, str]:
    """
    Generic function signature validator with configurable type checking.

    Parameters
    ----------
    func : Callable
        Function to validate
    expected_param_count : int
        Expected number of parameters (excluding 'self')
    param_validators : List[Callable[[Type], bool]], optional
        List of validator functions for each parameter type
    return_validator : Callable[[Type], bool], optional
        Validator function for return type
    decorator_name : str
        Name of decorator for error messages
    custom_validator : Callable, optional
        Custom validation function that gets (func, params, type_hints, decorator_name)

    Returns
    -------
    Tuple[Type, Type, str, str]
        (input_type, output_type, input_type_str, output_type_str)

    Raises
    ------
    MetaflowFunctionArgumentsException
        If parameter validation fails
    MetaflowFunctionReturnTypeException
        If return type validation fails
    """
    # Extract basic signature
    input_type, output_type, input_type_str, output_type_str = (
        extract_function_signature(func)
    )

    # Get signature details for validation
    sig = inspect.signature(func)
    type_hints = get_type_hints(func)
    params = [p for p in sig.parameters.values() if p.name != "self"]

    # Validate parameter count (skip if custom validator is provided)
    if custom_validator is None and len(params) != expected_param_count:
        raise MetaflowFunctionArgumentsException(
            f"Function decorated with @{decorator_name} must have exactly "
            f"{expected_param_count} arguments, got {len(params)}"
        )

    # Validate parameter types
    if param_validators:
        for i, (param, validator) in enumerate(zip(params, param_validators)):
            param_type = type_hints.get(param.name)
            if param_type is None:
                raise MetaflowFunctionArgumentsException(
                    f"Parameter '{param.name}' is missing type hint"
                )

            if not validator(param_type):
                raise MetaflowFunctionArgumentsException(
                    f"Parameter {i+1} '{param.name}' of type {param_type} failed type validation for @{decorator_name}"
                )

    # Validate return type
    if return_validator is not None:
        return_type = type_hints.get("return")
        if return_type is None:
            raise MetaflowFunctionReturnTypeException(
                f"Function decorated with @{decorator_name} must have a return type annotation"
            )
        if not return_validator(return_type):
            raise MetaflowFunctionReturnTypeException(
                f"Return type {return_type} failed validation for @{decorator_name}"
            )

    # Apply custom validation if provided
    if custom_validator:
        custom_validator(func, params, type_hints, decorator_name)

    return input_type, output_type, input_type_str, output_type_str


def is_type_or_optional(type_hint: Type, target_type: Type) -> bool:
    """Check if a type is target_type or Optional[target_type]."""

    def _types_match(type1: Type, type2: Type) -> bool:
        try:
            return issubclass(type1, type2)
        except TypeError:
            type1_str = get_type_string(type1)
            type2_str = get_type_string(type2)
            return types_are_equal(type1_str, type2_str)

    try:
        origin = get_origin(type_hint) or type_hint

        # Handle direct type match
        if _types_match(origin, target_type):
            return True

        # Handle Optional[target_type] which is Union[target_type, None]
        if origin is Union:
            args = get_args(type_hint)
            for arg in args:
                if arg is not type(None):  # Skip NoneType
                    arg_origin = get_origin(arg) or arg
                    if _types_match(arg_origin, target_type):
                        return True
        return False
    except (TypeError, ImportError):
        return False


def is_optional_function_parameters_type(type_hint: Type) -> bool:
    """Check if a type is FunctionParameters or Optional[FunctionParameters]."""
    return is_type_or_optional(type_hint, FunctionParameters)


def validate_standard_function_parameters(
    params, type_hints, decorator_name, start_index=1
):
    """
    Validate standard function parameters that are common across all function types.

    Parameters
    ----------
    params : List[inspect.Parameter]
        Function parameters to validate
    type_hints : Dict
        Type hints for the function
    decorator_name : str
        Name of the decorator for error messages
    start_index : int
        Index to start validation from (typically 1, after the data parameter)
    """
    remaining_params = params[start_index:]
    has_params = False

    for param in remaining_params:
        if param.kind == inspect.Parameter.VAR_KEYWORD:  # **kwargs
            continue
        if param.kind == inspect.Parameter.VAR_POSITIONAL:  # *args
            continue

        # Check if this is the 'params' parameter
        if param.name == "params":
            param_type = type_hints.get(param.name, param.annotation)
            # Check if it's the right type (FunctionParameters or Optional[FunctionParameters])
            if not is_optional_function_parameters_type(param_type):
                raise MetaflowFunctionArgumentsException(
                    f"Parameter 'params' must be of type FunctionParameters in @{decorator_name} function"
                )
            # Check if the parameter is actually optional (has default or is Optional type)
            from typing import get_origin, get_args, Union

            is_actually_optional = (
                param.default != inspect.Parameter.empty  # Has default value
                or (
                    get_origin(param_type) is Union
                    and type(None) in get_args(param_type)
                )  # Is Optional[T]
            )
            if not is_actually_optional:
                raise MetaflowFunctionArgumentsException(
                    f"Parameter 'params' must be optional (have a default value or be Optional[FunctionParameters]) in @{decorator_name} function"
                )
            has_params = True

    if not has_params:
        raise MetaflowFunctionArgumentsException(
            f"Function decorated with @{decorator_name} must have a 'params' parameter of type FunctionParameters"
        )


def get_caller_module(frames_back: int = 2) -> Optional[str]:
    """
    Get the module name of a calling function.

    Parameters
    ----------
    frames_back : int, default 2
        How many frames back to look for the caller:
        - 1: immediate caller
        - 2: caller's caller (default, useful for utility functions)
        - 3+: further up the call stack

    Returns
    -------
    Optional[str]
        The module name of the caller, avoiding __main__ when possible.
        Returns None if unable to determine the caller module.
    """
    current_frame = None
    target_frame = None

    try:
        # Get the current frame (this function)
        current_frame = inspect.currentframe()
        if current_frame is None:
            return None

        # Walk back the specified number of frames
        target_frame = current_frame
        for _ in range(frames_back):
            if target_frame is None:
                return None
            target_frame = target_frame.f_back

        if target_frame is None:
            return None

        # Get module name from the frame's globals
        module_name = target_frame.f_globals.get("__name__", "__main__")

        # If it's __main__, try to get a better name from the filename
        if "__main__" in module_name:
            file_path = target_frame.f_globals.get("__file__")
            if file_path:
                # Use the filename without extension as module name
                module_name = os.path.splitext(os.path.basename(file_path))[0]

        return module_name

    except (AttributeError, TypeError):
        # If anything goes wrong, return None to use default behavior
        return None
    finally:
        # Clean up frame references to avoid memory leaks
        if current_frame is not None:
            del current_frame
        if target_frame is not None:
            del target_frame
