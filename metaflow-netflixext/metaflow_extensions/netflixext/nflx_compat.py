"""
Backward-compat shim: routes metaflow_extensions.nflx.* imports that
previously pointed to modules now living under metaflow_extensions.netflixext.*.

These plugins moved namespaces to escape the pip RECORD wipe bug triggered by
simultaneous installation of old nflx-metaflow and new metaflow-netflixext.

This file lives at netflixext/ (not nflx/) so it is in metaflow-netflixext's
own RECORD and survives nflx-metaflow upgrades without being wiped.

The finder is self-correcting: it only activates for a given nflx.* path if
a matching module actually exists at netflixext.*, so it does not interfere
with other packages (nflx-fastdata, etc.) that still legitimately own their
own metaflow_extensions.nflx.* subpackages.
"""

import importlib
import importlib.util
import sys

_NFLX_PREFIX = "metaflow_extensions.nflx."
_NETFLIXEXT_PREFIX = "metaflow_extensions.netflixext."


class _NflxCompatFinder:
    def find_module(self, fullname, path=None):
        if not fullname.startswith(_NFLX_PREFIX):
            return None
        netflixext_name = _NETFLIXEXT_PREFIX + fullname[len(_NFLX_PREFIX):]
        try:
            if importlib.util.find_spec(netflixext_name) is not None:
                return self
        except (ValueError, ModuleNotFoundError):
            pass
        return None

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]
        netflixext_name = _NETFLIXEXT_PREFIX + fullname[len(_NFLX_PREFIX):]
        module = importlib.import_module(netflixext_name)
        sys.modules[fullname] = module
        return module


def install():
    if not any(isinstance(f, _NflxCompatFinder) for f in sys.meta_path):
        sys.meta_path.insert(0, _NflxCompatFinder())
