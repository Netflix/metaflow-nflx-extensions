"""
Test fixtures for metaflow-fastdata.

Integration tests that need a real Hive Metastore + MinIO use the
`hive_catalog` and `s3_client` fixtures, which are skipped automatically
when the services are unavailable (no docker-compose running).

Unit tests that don't touch real infra use simple mocks and always run.

To run integration tests locally:
    docker-compose up -d
    pytest tests/ -m integration
"""

import os
import pytest


# ---------------------------------------------------------------------------
# Environment markers
# ---------------------------------------------------------------------------


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: marks tests that require docker-compose services"
    )


def _hive_uri():
    return os.environ.get("METAFLOW_HIVE_METASTORE_URI", "thrift://localhost:9083")


def _minio_endpoint():
    return os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")


def _services_available():
    """Return True when docker-compose services appear to be up."""
    import socket

    uri = _hive_uri().replace("thrift://", "")
    host, _, port = uri.partition(":")
    port = int(port) if port else 9083
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def hive_catalog():
    """Return a HiveThriftCatalog connected to the local test metastore.

    Skips the test if the metastore is not reachable.
    """
    if not _services_available():
        pytest.skip("Hive Metastore not available — run docker-compose up -d first")

    from metaflow_extensions.fastdata_ext.plugins.table.hive_thrift_catalog import (
        HiveThriftCatalog,
    )

    return HiveThriftCatalog(metastore_uri=_hive_uri())


@pytest.fixture(scope="session")
def minio_env(monkeypatch_session):
    """Set environment variables to route S3 calls to MinIO."""
    monkeypatch_session.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch_session.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch_session.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch_session.setenv("AWS_ENDPOINT_URL", _minio_endpoint())


@pytest.fixture(scope="session")
def monkeypatch_session(request):
    """Session-scoped monkeypatch (pytest doesn't provide one by default)."""
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    yield mp
    mp.undo()


@pytest.fixture
def mock_catalog():
    """Return a minimal mock MetadataCatalog for unit tests."""
    from unittest.mock import MagicMock
    from collections import OrderedDict
    from metaflow_extensions.fastdata_ext.plugins.table.catalog import (
        MetadataCatalog,
        TableInfo,
        WriterInfo,
    )

    catalog = MagicMock(spec=MetadataCatalog)
    catalog.get_writer_info.return_value = WriterInfo()
    catalog.get_credentials.return_value = {}
    catalog.publish_read_event.return_value = None
    catalog.publish_write_event.return_value = None

    def _make_hive_info(db="testdb", table="testtable", catalog_name="hive"):
        return TableInfo(
            db=db,
            table=table,
            catalog_name=catalog_name,
            type="hive",
            format="parquet",
            schema=OrderedDict([("col1", "int"), ("col2", "string")]),
            partition_schema=OrderedDict([("dateint", "int")]),
        )

    catalog._make_hive_info = _make_hive_info
    return catalog
