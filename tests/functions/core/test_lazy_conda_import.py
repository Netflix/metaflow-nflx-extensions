"""The local backend must import in a serving environment, which has no conda plugin."""

import importlib
import sys

import metaflow  # noqa: F401  # loads the extensions first, as a real process does

CONDA_PLUGIN = (
    "metaflow_extensions.netflixext.plugins.conda.conda",
    "metaflow_extensions.netflix_ext.plugins.conda.conda",
)
BACKEND_CHAIN = (
    "metaflow_extensions.nflx.plugins.functions.environment",
    "metaflow_extensions.nflx.plugins.functions.backends.abstract_backend",
    "metaflow_extensions.nflx.plugins.functions.backends.local.local_backend",
)


def test_local_backend_imports_without_the_conda_plugin(monkeypatch):
    # None in sys.modules makes importing that module raise ImportError.
    for name in CONDA_PLUGIN:
        monkeypatch.setitem(sys.modules, name, None)
    # Re-import the chain from scratch. monkeypatch puts back the original
    # modules, and the parent packages' attributes, when the test ends.
    for name in BACKEND_CHAIN:
        parent, _, child = name.rpartition(".")
        monkeypatch.delitem(sys.modules, name, raising=False)
        package = importlib.import_module(parent)
        if hasattr(package, child):
            monkeypatch.setattr(package, child, getattr(package, child))
    for name in BACKEND_CHAIN:
        importlib.import_module(name)
