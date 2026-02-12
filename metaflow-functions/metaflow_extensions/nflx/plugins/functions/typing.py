from dataclasses import make_dataclass
from typing import Any, Type, Union, List, Dict

from .utils import get_caller_module
import sys


def generate_dataclass(
    fields: Union[List[str], Dict[str, Type]], class_name: str
) -> None:
    """
    Helper function to create a dataclass with specified field names and types.
    If `fields` is a list, all fields are given type `Any`. If it's a dictionary,
    the keys are field names and the values are field types.

    Parameters
    ----------
    fields : Union[List[str], Dict[str, Type]]
        A list of field names or a dictionary mapping field names to types.
    class_name : str
        The name of the class to create.

    Returns
    -------
    Type
        The created dataclass type.
    """
    # Create the dataclass
    if isinstance(fields, list):
        dataclass_type = make_dataclass(
            class_name, [(field_name, Any) for field_name in fields]
        )
    else:
        dataclass_type = make_dataclass(class_name, list(fields.items()))

    # Get the calling function's module and register the class in its namespace
    caller_module_name = get_caller_module(frames_back=2)
    if caller_module_name and caller_module_name in sys.modules:
        caller_module = sys.modules[caller_module_name]

        # Add generated dataclass to caller module's namespace for importability
        setattr(caller_module, class_name, dataclass_type)

        # Set proper module name for the class
        dataclass_type.__module__ = caller_module_name
