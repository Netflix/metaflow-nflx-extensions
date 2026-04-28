from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from typing import Callable, Protocol

    class DebugProtocol(Protocol):
        functions_exec: Callable[[str], None]
        functions_supervisor: Callable[[str], None]


def _noop(*args, **kwargs):
    pass


class _LazyDebug:
    """Lazy proxy for metaflow.debug.debug to avoid importing metaflow at module load."""

    def __init__(self):
        object.__setattr__(self, "_resolved", None)

    def _resolve(self):
        resolved = object.__getattribute__(self, "_resolved")
        if resolved is None:
            from metaflow.debug import debug as _debug

            object.__setattr__(self, "_resolved", _debug)
            return _debug
        return resolved

    def __getattr__(self, name):
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        resolved = self._resolve()
        try:
            val = resolved.__dict__.get(name)
            if val is not None:
                return val
        except AttributeError:
            pass
        return _noop


debug = cast("DebugProtocol", _LazyDebug())
