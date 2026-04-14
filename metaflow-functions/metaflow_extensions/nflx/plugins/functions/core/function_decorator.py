import functools
from typing import Callable, Any, Dict, TypeVar, Generic
from metaflow_extensions.nflx.plugins.functions.core.function_decorator_spec import (
    FunctionDecoratorSpec,
)

F_mf = TypeVar("F_mf", bound=Callable[..., Any])


class MetaflowFunctionDecorator(Generic[F_mf]):
    """
    The base class for defining a function decorator.

    All function decorators should inherit from this class.
    """

    TYPE = "mf_function"

    def __init__(self, func: F_mf, **kwargs: Dict[str, Any]):
        """
        Initializes the function decorator with the given function.

        Parameters
        ----------
        func : Callable[..., Any]
            The function to be decorated.

        kwargs : Dict[str, Any]
            Additional keyword arguments to be passed to the decorator.
        """
        # Preserves original function's metadata
        functools.update_wrapper(self, func)
        self.func: F_mf = func
        self.kwargs: Dict[str, Any] = kwargs

        # Specify that this is a Metaflow function
        self._is_function: bool = True
        self._deco_spec: FunctionDecoratorSpec = self._build_deco_spec()

    def _build_deco_spec(self) -> FunctionDecoratorSpec:
        """
        Build the function decorator specification.

        Returns
        -------
        FunctionDecoratorSpec
            The function decorator specification object.
        """
        raise NotImplementedError()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.func(*args, **kwargs)

    @property
    def deco_spec(self) -> FunctionDecoratorSpec:
        """
        Returns the function specification.

        Returns
        -------
        FunctionDecoratorSpec
            The function specification object.
        """
        return self._deco_spec
