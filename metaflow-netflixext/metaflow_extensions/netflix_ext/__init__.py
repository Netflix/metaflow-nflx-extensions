# Backwards compatibility alias: netflix_ext -> nflx
# This allows `import metaflow_extensions.netflix_ext.X.Y` to work
# while the actual package lives under metaflow_extensions.nflx.

import importlib
import sys

from types import ModuleType

_ALIAS_PREFIX = "metaflow_extensions.netflix_ext"
_REAL_PREFIX = "metaflow_extensions.nflx"


class _NetflixExtFinder:
    """Meta path finder that redirects netflix_ext imports to nflx."""

    def find_module(self, fullname, path=None):
        if fullname == _ALIAS_PREFIX or fullname.startswith(_ALIAS_PREFIX + "."):
            return self
        return None

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]
        real_name = _REAL_PREFIX + fullname[len(_ALIAS_PREFIX) :]
        real_mod = importlib.import_module(real_name)
        sys.modules[fullname] = real_mod
        return real_mod


sys.meta_path.insert(0, _NetflixExtFinder())

# Make this module itself point to the real package
_real = importlib.import_module(_REAL_PREFIX)
sys.modules[__name__] = _real
sys.modules[_ALIAS_PREFIX] = _real
