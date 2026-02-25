import os
import pytest


def pytest_configure(config):
    """
    Override the global METAFLOW_CONDA_USE_REMOTE_LATEST setting for conda tests.
    This runs before test collection and imports, ensuring the env var is set
    before metaflow modules read it.

    These tests specifically test the resolve code path, so they should use :none:
    to ensure environments are resolved locally rather than fetched remotely.
    """
    os.environ["METAFLOW_CONDA_USE_REMOTE_LATEST"] = ":none:"
