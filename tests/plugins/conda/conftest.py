import os
import pytest

os.environ["METAFLOW_CONDA_USE_REMOTE_LATEST"] = ":none:"
os.environ.setdefault("METAFLOW_DATASTORE_SYSROOT_LOCAL", ".metaflow")
os.environ.setdefault("METAFLOW_CONDA_DEPENDENCY_RESOLVER", "micromamba")


def pytest_configure(config):
    """
    Override the global METAFLOW_CONDA_USE_REMOTE_LATEST setting for conda tests.
    The env vars are set at module import time, before test collection imports
    any modules that may initialize Metaflow config.

    These tests specifically test the resolve code path, so they should use :none:
    to ensure environments are resolved locally rather than fetched remotely.
    """
