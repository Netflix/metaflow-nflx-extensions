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
        resolved = self._resolve()
        # Use object.__getattribute__ to avoid recursion if metaflow's debug
        # doesn't have the attribute registered yet (e.g. during init)
        try:
            val = resolved.__dict__.get(name)
            if val is not None:
                return val
        except AttributeError:
            pass
        # Attribute not registered yet - return noop to avoid recursion
        # in metaflow's Debug.__getattr__ which calls getattr(self, name)
        return _noop


debug = cast("DebugProtocol", _LazyDebug())
