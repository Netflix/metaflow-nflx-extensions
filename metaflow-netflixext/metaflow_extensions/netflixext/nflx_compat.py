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
import importlib.machinery
import importlib.util
import sys

_NFLX_PREFIX = "metaflow_extensions.nflx."
_NETFLIXEXT_PREFIX = "metaflow_extensions.netflixext."


class _NflxCompatLoader:
    def __init__(self, real_name):
        self._real_name = real_name

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        real = importlib.import_module(self._real_name)
        module.__dict__.update(real.__dict__)
        if hasattr(real, "__path__"):
            module.__path__ = real.__path__


class _NflxCompatFinder:
    def __init__(self):
        self._checking = False

    def find_spec(self, fullname, path, target=None):
        if self._checking or not fullname.startswith(_NFLX_PREFIX):
            return None

        # Check if the nflx module exists on disk via other finders.
        # Only redirect when the nflx path would genuinely fail — this
        # avoids hijacking intermediate packages (like nflx.plugins) that
        # nflx-metaflow legitimately owns.
        self._checking = True
        try:
            nflx_spec = importlib.util.find_spec(fullname)
        except (ValueError, ModuleNotFoundError):
            nflx_spec = None
        finally:
            self._checking = False

        if nflx_spec is not None:
            return None

        netflixext_name = _NETFLIXEXT_PREFIX + fullname[len(_NFLX_PREFIX) :]
        try:
            real_spec = importlib.util.find_spec(netflixext_name)
            if real_spec is not None:
                return importlib.machinery.ModuleSpec(
                    fullname,
                    _NflxCompatLoader(netflixext_name),
                    origin=real_spec.origin,
                    is_package=real_spec.submodule_search_locations is not None,
                )
        except (ValueError, ModuleNotFoundError):
            pass
        return None


def install():
    if not any(isinstance(f, _NflxCompatFinder) for f in sys.meta_path):
        sys.meta_path.insert(0, _NflxCompatFinder())
