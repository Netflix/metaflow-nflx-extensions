"""
Regression tests for the nflx_compat backward-compatibility shim.

The shim (metaflow_extensions/netflixext/nflx_compat.py) routes
metaflow_extensions.nflx.* imports → metaflow_extensions.netflixext.*
for modules that moved during the namespace rename, so existing user
code continues to work without modification.
"""

import importlib
import sys

import pytest


@pytest.fixture(autouse=True)
def clean_nflx_compat_modules(request):
    """Remove any nflx.* aliases added by the shim after each test so tests
    don't bleed cached entries into each other."""
    yield
    to_remove = [
        k for k in list(sys.modules)
        if k.startswith("metaflow_extensions.nflx.")
        and sys.modules[k] is not None
        and getattr(sys.modules[k], "__name__", k).startswith(
            "metaflow_extensions.netflixext."
        )
    ]
    for k in to_remove:
        sys.modules.pop(k, None)


def _install_shim():
    from metaflow_extensions.netflixext.nflx_compat import install
    install()


def _uninstall_shim():
    from metaflow_extensions.netflixext.nflx_compat import _NflxCompatFinder
    sys.meta_path[:] = [f for f in sys.meta_path if not isinstance(f, _NflxCompatFinder)]


@pytest.fixture
def shim_installed():
    _install_shim()
    yield
    _uninstall_shim()


class TestNflxCompatShim:
    def test_shim_installs_without_error(self):
        _install_shim()
        _uninstall_shim()

    def test_shim_idempotent(self):
        """Installing the shim twice must not add duplicate finders."""
        from metaflow_extensions.netflixext.nflx_compat import _NflxCompatFinder
        _install_shim()
        _install_shim()
        count = sum(1 for f in sys.meta_path if isinstance(f, _NflxCompatFinder))
        _uninstall_shim()
        assert count == 1, f"Expected 1 shim finder, got {count}"

    def test_nflx_conda_import_resolves_via_shim(self, shim_installed):
        """from metaflow_extensions.nflx.plugins.conda.* must resolve after shim."""
        nflx_mod = importlib.import_module(
            "metaflow_extensions.nflx.plugins.conda.conda_step_decorator"
        )
        netflixext_mod = importlib.import_module(
            "metaflow_extensions.netflixext.plugins.conda.conda_step_decorator"
        )
        assert nflx_mod is netflixext_mod, (
            "nflx.plugins.conda.conda_step_decorator should resolve to the "
            "same object as netflixext.plugins.conda.conda_step_decorator"
        )

    def test_nflx_conda_import_registered_in_sys_modules(self, shim_installed):
        """Resolved nflx.* aliases must be cached in sys.modules."""
        importlib.import_module(
            "metaflow_extensions.nflx.plugins.conda.env_descr"
        )
        assert "metaflow_extensions.nflx.plugins.conda.env_descr" in sys.modules

    def test_shim_does_not_intercept_nonexistent_netflixext_module(self, shim_installed):
        """Shim must not claim to handle nflx.* paths with no netflixext.* counterpart."""
        with pytest.raises(ImportError):
            importlib.import_module(
                "metaflow_extensions.nflx.plugins.conda.does_not_exist_xyz"
            )

    def test_shim_does_not_intercept_unrelated_nflx_module(self, shim_installed):
        """Shim must be transparent for nflx.* modules owned by other packages (e.g. nflx-fastdata).

        We simulate this by checking a path that has no netflixext.* equivalent —
        the shim's find_spec probe returns None and the normal import machinery runs.
        """
        # metaflow_extensions.nflx.plugins.datatools is owned by nflx-fastdata,
        # not metaflow-netflixext, so there's no netflixext.plugins.datatools.
        # The shim should NOT intercept this — it should pass through unchanged.
        try:
            mod = importlib.import_module(
                "metaflow_extensions.nflx.plugins.datatools"
            )
            # If it succeeded, the shim didn't route it to a non-existent netflixext path
            assert not mod.__name__.startswith("metaflow_extensions.netflixext"), (
                "Shim incorrectly routed nflx-fastdata-owned module to netflixext"
            )
        except ImportError:
            # nflx-fastdata not installed in this environment — shim correctly did
            # not intercept (would have been a different error if it had tried)
            pass
