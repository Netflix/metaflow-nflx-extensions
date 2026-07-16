"""
Regression tests for the nflx_compat backward-compatibility shim.

The shim (metaflow_extensions/netflixext/nflx_compat.py) routes
metaflow_extensions.nflx.* imports → metaflow_extensions.netflixext.*
for modules that moved during the namespace rename, so existing user
code continues to work without modification.
"""

import importlib
import importlib.util
import sys

import pytest


def _nflx_namespace_available():
    """True only if some installed package provides the metaflow_extensions.nflx
    namespace (e.g. the internal nflx-metaflow / nflx-fastdata). The shim
    redirects nflx.plugins.conda.* children but never the bare `nflx` parent —
    that parent must be importable on its own. In a pure-OSS install (just
    metaflow-netflixext) nothing provides it, so the nflx-import tests below are
    only meaningful when such a package is co-installed."""
    try:
        return importlib.util.find_spec("metaflow_extensions.nflx") is not None
    except (ImportError, ValueError):
        return False


requires_nflx_namespace = pytest.mark.skipif(
    not _nflx_namespace_available(),
    reason="needs a package providing the metaflow_extensions.nflx namespace "
    "(e.g. nflx-metaflow); pure-OSS metaflow-netflixext does not provide it",
)


@pytest.fixture(autouse=True)
def clean_nflx_compat_modules(request):
    """Remove any nflx.* aliases added by the shim after each test so tests
    don't bleed cached entries into each other."""
    yield
    to_remove = [
        k
        for k in list(sys.modules)
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

    sys.meta_path[:] = [
        f for f in sys.meta_path if not isinstance(f, _NflxCompatFinder)
    ]


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

    @requires_nflx_namespace
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

    @requires_nflx_namespace
    def test_nflx_conda_import_registered_in_sys_modules(self, shim_installed):
        """Resolved nflx.* aliases must be cached in sys.modules."""
        importlib.import_module("metaflow_extensions.nflx.plugins.conda.env_descr")
        assert "metaflow_extensions.nflx.plugins.conda.env_descr" in sys.modules

    def test_shim_does_not_intercept_nonexistent_netflixext_module(
        self, shim_installed
    ):
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
            mod = importlib.import_module("metaflow_extensions.nflx.plugins.datatools")
            # If it succeeded, the shim didn't route it to a non-existent netflixext path
            assert not mod.__name__.startswith(
                "metaflow_extensions.netflixext"
            ), "Shim incorrectly routed nflx-fastdata-owned module to netflixext"
        except ImportError:
            # nflx-fastdata not installed in this environment — shim correctly did
            # not intercept (would have been a different error if it had tried)
            pass


class TestShimInstalledEarly:
    """The shim must be active BEFORE Metaflow resolves plugins, not just after
    its toplevel module loads.

    Metaflow loads extension *descriptors* (config/plugins mfextinit) during
    discovery, then imports plugin *classes* in ``resolve_plugins(...)``, then
    merges extension *toplevel* modules last. Another installed extension (e.g.
    the internal nflx-metaflow) contributes step decorators whose classes import
    ``metaflow_extensions.nflx.plugins.conda.*`` at resolve time — so if the shim
    is installed only from the toplevel it is too late and ``import metaflow``
    raises ModuleNotFoundError for that combination. The config descriptor, which
    loads during discovery, must install it.
    """

    def test_config_descriptor_installs_shim_on_import(self):
        # Import metaflow first so metaflow.metaflow_config is fully initialized.
        # Re-executing the config descriptor below does
        # `from metaflow.metaflow_config import ...`; without metaflow already
        # loaded that would re-enter metaflow's bootstrap and hit the conda
        # circular-import, which is unrelated to what we're asserting here.
        import metaflow  # noqa: F401

        from metaflow_extensions.netflixext.nflx_compat import _NflxCompatFinder

        # Remove the finder, then force the config descriptor to re-execute and
        # prove that *it* reinstalls the finder (i.e. the early install lives in
        # the config descriptor, which loads during discovery — before plugin
        # resolution — not only in the toplevel, which loads too late for other
        # extensions' plugins that import metaflow_extensions.nflx.* at resolve time).
        _uninstall_shim()
        sys.modules.pop(
            "metaflow_extensions.netflixext.config.mfextinit_netflixext", None
        )
        try:
            importlib.import_module(
                "metaflow_extensions.netflixext.config.mfextinit_netflixext"
            )
            assert any(isinstance(f, _NflxCompatFinder) for f in sys.meta_path), (
                "importing the netflixext config descriptor must install the "
                "nflx_compat finder (it loads before plugin resolution); a "
                "toplevel-only install is too late for other extensions' plugins"
            )
        finally:
            _uninstall_shim()
