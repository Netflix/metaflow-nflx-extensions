from abc import ABC
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(kw_only=True)
class FunctionDecoratorSpec(ABC):
    """
    The base class for defining a function specification.
    """

    name: Optional[str] = None  # function name
    module: Optional[str] = None  # function module name
    file_name: Optional[str] = None  # file name where the function is defined
    doc: Optional[str] = None  # user provided notes
    type: str = (
        "mf_function"  # type of function, subclasses should define their own type
    )

    input_schema: Optional[Dict[str, Any]] = None
    parameter_schema: Optional[Dict[str, Any]] = None
    return_schema: Optional[Dict[str, Any]] = None
