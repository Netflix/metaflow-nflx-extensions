import os
import pytest

print("Initing environments conftest")


def pytest_configure(config):
    """
    Override the global METAFLOW_CONDA_USE_REMOTE_LATEST setting for environments tests.
    This runs before test collection and imports, ensuring the env var is set
    before metaflow modules read it.

    These tests specifically test the resolve code path, so they should use :none:
    to ensure environments are resolved locally rather than fetched remotely.
    """
    os.environ["METAFLOW_CONDA_USE_REMOTE_LATEST"] = ":none:"
    import tempfile

    os.environ.setdefault(
        "METAFLOW_DATASTORE_SYSROOT_LOCAL",
        os.path.join(tempfile.gettempdir(), ".metaflow"),
    )
    os.environ.setdefault("METAFLOW_CONDA_DEPENDENCY_RESOLVER", "micromamba")


def pytest_collection_modifyitems(config, items):
    """Automatically add 'environments' marker to all tests in this directory"""
    for item in items:
        item.add_marker(pytest.mark.environments)
