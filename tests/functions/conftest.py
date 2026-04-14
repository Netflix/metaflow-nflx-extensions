import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "no_backend_parametrization: disable automatic backend parametrization",
    )
    config.addinivalue_line("markers", "memory_only: only run with memory backend")
    config.addinivalue_line("markers", "local_only: only run with local backend")


def pytest_collection_modifyitems(config, items):
    for item in items:
        item.add_marker(pytest.mark.functions)
