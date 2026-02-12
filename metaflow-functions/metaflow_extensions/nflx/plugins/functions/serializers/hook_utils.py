"""
Utility functions for serializer registration hooks.
"""

from typing import get_type_hints, Callable, List
import inspect


def register_serializers_for_function_types(
    func: Callable, type_handlers: List[Callable]
):
    """
    Generic hook to analyze function signature and register serializers for detected types.

    Parameters
    ----------
    func : Callable
        The function to analyze
    type_handlers : List[Callable]
        List of functions that take a type_hint and register serializers if appropriate.
        Each handler should return True if it handled the type, False otherwise.
    """
    # Analyze function signature to get all types
    signature = inspect.signature(func)
    type_hints = get_type_hints(func)

    # Check all parameter types
    for param_name, param in signature.parameters.items():
        param_type = type_hints.get(param_name, param.annotation)
        if param_type and param_type != inspect.Parameter.empty:
            for handler in type_handlers:
                if handler(param_type):
                    break  # Stop once a handler processes the type

    # Check return type
    return_type = signature.return_annotation
    if return_type and return_type != inspect.Parameter.empty:
        return_type_hint = type_hints.get("return", return_type)
        for handler in type_handlers:
            if handler(return_type_hint):
                break  # Stop once a handler processes the type
