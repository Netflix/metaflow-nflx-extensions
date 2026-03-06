"""
Tests for HiveTable and HiveThriftCatalog.

Unit tests (no infra required) mock the MetadataCatalog.
Integration tests (marked `integration`) require docker-compose services.
"""

from collections import OrderedDict
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# HiveTable unit tests
# ---------------------------------------------------------------------------

class TestHiveTableUnit:
    def _make_hive_info(self, **kwargs):
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import TableInfo

        defaults = dict(
            db="testdb",
            table="testtable",
            catalog_name="hive",
            type="hive",
            format="parquet",
            schema=OrderedDict([("col1", "int"), ("col2", "string")]),
            partition_schema=OrderedDict([("dateint", "int")]),
        )
        defaults.update(kwargs)
        return TableInfo(**defaults)

    def _make_catalog(self):
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import (
            MetadataCatalog,
            WriterInfo,
        )

        cat = MagicMock(spec=MetadataCatalog)
        cat.get_partition_uris.return_value = []
        cat.get_writer_info.return_value = WriterInfo()
        cat.get_credentials.return_value = {}
        return cat

    def test_init_rejects_table_without_partition_columns(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_table import HiveTable
        from metaflow_extensions.fastdata_ext.plugins.table.exception import (
            MetaflowTableIncompatible,
        )

        info = self._make_hive_info(partition_schema=OrderedDict())
        cat = self._make_catalog()
        with pytest.raises(MetaflowTableIncompatible, match="no partition columns"):
            HiveTable(info, cat)

    def test_init_rejects_non_parquet_format(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_table import HiveTable
        from metaflow_extensions.fastdata_ext.plugins.table.exception import (
            MetaflowTableIncompatible,
        )

        info = self._make_hive_info(format="orc")
        cat = self._make_catalog()
        with pytest.raises(MetaflowTableIncompatible, match="not using Parquet"):
            HiveTable(info, cat)

    def test_schema_property(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_table import HiveTable

        info = self._make_hive_info()
        cat = self._make_catalog()
        ht = HiveTable(info, cat)
        schema = ht.schema
        assert isinstance(schema, list)
        assert schema[0]["name"] == "col1"
        assert schema[1]["name"] == "col2"

    def test_partition_columns_property(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_table import HiveTable

        info = self._make_hive_info()
        cat = self._make_catalog()
        ht = HiveTable(info, cat)
        assert ht.partition_columns == ["dateint"]

    def test_validate_columns_rejects_unknown_columns(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_table import HiveTable
        from metaflow_extensions.fastdata_ext.plugins.table.exception import (
            MetaflowTableException,
        )

        info = self._make_hive_info()
        cat = self._make_catalog()
        ht = HiveTable(info, cat)
        with pytest.raises(MetaflowTableException, match="aren't data columns"):
            ht.validate_columns(["nonexistent_col"])

    def test_validate_columns_accepts_valid_columns(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_table import HiveTable

        info = self._make_hive_info()
        cat = self._make_catalog()
        ht = HiveTable(info, cat)
        ht.validate_columns(["col1"])  # should not raise

    def test_has_schema_evolution_false(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_table import HiveTable

        info = self._make_hive_info()
        cat = self._make_catalog()
        ht = HiveTable(info, cat)
        assert ht.has_schema_evolution is False
        assert ht.has_partition_evolution is False

    def test_get_grouped_partition_paths_calls_catalog(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_table import HiveTable

        info = self._make_hive_info()
        cat = self._make_catalog()
        cat.get_partition_uris.return_value = ["s3://bucket/prefix/"]

        ht = HiveTable(info, cat)

        # Patch S3.list_recursive so we don't need real AWS
        mock_s3obj = MagicMock()
        mock_s3obj.url = "s3://bucket/prefix/part-0.parquet"
        with patch(
            "metaflow_extensions.fastdata_ext.plugins.table.hive_table.S3"
        ) as MockS3:
            MockS3.return_value.__enter__.return_value.list_recursive.return_value = [
                mock_s3obj
            ]
            result = ht.get_grouped_partition_paths([{"dateint": 20230101}])

        # Verify catalog was called with correct args
        cat.get_partition_uris.assert_called_once_with(
            "testdb", "testtable", "hive", filter_expr="dateint=20230101"
        )
        assert result == [["s3://bucket/prefix/part-0.parquet"]]

    def test_get_grouped_partition_paths_string_filter(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_table import HiveTable

        info = self._make_hive_info(
            schema=OrderedDict([("val", "int")]),
            partition_schema=OrderedDict([("country", "chararray")]),
        )
        cat = self._make_catalog()
        cat.get_partition_uris.return_value = []

        ht = HiveTable(info, cat)

        with patch(
            "metaflow_extensions.fastdata_ext.plugins.table.hive_table.S3"
        ) as MockS3:
            MockS3.return_value.__enter__.return_value.list_recursive.return_value = []
            ht.get_grouped_partition_paths([{"country": "US"}])

        cat.get_partition_uris.assert_called_once_with(
            "testdb", "testtable", "hive", filter_expr="country='US'"
        )

    def test_get_grouped_partition_paths_type_mismatch(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_table import HiveTable
        from metaflow_extensions.fastdata_ext.plugins.table.exception import (
            MetaflowTableTypeMismatch,
        )

        info = self._make_hive_info()  # dateint is int type
        cat = self._make_catalog()
        ht = HiveTable(info, cat)

        # Pass a string for an int partition column
        with pytest.raises(MetaflowTableTypeMismatch, match="String not expected"):
            ht.get_grouped_partition_paths([{"dateint": "20230101"}])

    def test_get_grouped_partition_paths_rejects_grouping_options(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_table import HiveTable
        from metaflow_extensions.fastdata_ext.plugins.table.exception import (
            MetaflowTableException,
        )

        info = self._make_hive_info()
        cat = self._make_catalog()
        ht = HiveTable(info, cat)

        with pytest.raises(MetaflowTableException, match="not applicable"):
            ht.get_grouped_partition_paths([], num_groups=4)

    def test_get_grouped_partition_paths_empty_spec(self):
        """Empty partition_spec (no filter) should call catalog with filter_expr=None."""
        from metaflow_extensions.fastdata_ext.plugins.table.hive_table import HiveTable

        info = self._make_hive_info()
        cat = self._make_catalog()
        cat.get_partition_uris.return_value = []

        ht = HiveTable(info, cat)
        with patch(
            "metaflow_extensions.fastdata_ext.plugins.table.hive_table.S3"
        ) as MockS3:
            MockS3.return_value.__enter__.return_value.list_recursive.return_value = []
            ht.get_grouped_partition_paths([{}])

        cat.get_partition_uris.assert_called_once_with(
            "testdb", "testtable", "hive", filter_expr=None
        )


# ---------------------------------------------------------------------------
# HiveThriftCatalog unit tests (mocked Thrift)
# ---------------------------------------------------------------------------

class TestHiveThriftCatalogUnit:
    def _make_thrift_table(self, table_type="hive"):
        """Build a minimal fake Thrift Table object."""
        params = {}
        if table_type == "iceberg":
            params["table_type"] = "ICEBERG"
            params["metadata_location"] = "s3://bucket/meta/v1.metadata.json"

        col = MagicMock()
        col.name = "col1"
        col.type = "int"

        pk = MagicMock()
        pk.name = "dateint"
        pk.type = "int"

        sd = MagicMock()
        sd.cols = [col]
        sd.outputFormat = (
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
        )
        sd.serdeInfo = MagicMock()
        sd.serdeInfo.outputFormat = sd.outputFormat

        tbl = MagicMock()
        tbl.sd = sd
        tbl.parameters = params
        tbl.partitionKeys = [pk]
        return tbl

    def test_get_table_info_hive(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_thrift_catalog import (
            HiveThriftCatalog,
        )

        cat = HiveThriftCatalog(metastore_uri="thrift://localhost:9083")
        fake_tbl = self._make_thrift_table("hive")

        with patch.object(cat, "_connect") as mock_conn:
            mock_client = MagicMock()
            mock_transport = MagicMock()
            mock_conn.return_value = (mock_client, mock_transport)
            mock_client.get_table.return_value = fake_tbl

            info = cat.get_table_info("mydb", "mytable")

        assert info.db == "mydb"
        assert info.table == "mytable"
        assert info.type == "hive"
        assert "col1" in info.schema
        assert "dateint" in info.partition_schema
        assert info.metadata_location == ""

    def test_get_table_info_iceberg(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_thrift_catalog import (
            HiveThriftCatalog,
        )

        cat = HiveThriftCatalog(metastore_uri="thrift://localhost:9083")
        fake_tbl = self._make_thrift_table("iceberg")

        with patch.object(cat, "_connect") as mock_conn:
            mock_client = MagicMock()
            mock_transport = MagicMock()
            mock_conn.return_value = (mock_client, mock_transport)
            mock_client.get_table.return_value = fake_tbl

            info = cat.get_table_info("mydb", "mytable")

        assert info.type == "iceberg"
        assert info.metadata_location == "s3://bucket/meta/v1.metadata.json"

    def test_get_partition_uris_with_filter(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_thrift_catalog import (
            HiveThriftCatalog,
        )

        cat = HiveThriftCatalog(metastore_uri="thrift://localhost:9083")

        part = MagicMock()
        part.sd = MagicMock()
        part.sd.location = "s3://bucket/mydb/mytable/dateint=20230101"

        with patch.object(cat, "_connect") as mock_conn:
            mock_client = MagicMock()
            mock_transport = MagicMock()
            mock_conn.return_value = (mock_client, mock_transport)
            mock_client.get_partitions_by_filter.return_value = [part]

            uris = cat.get_partition_uris("mydb", "mytable", "hive", filter_expr="dateint=20230101")

        assert uris == ["s3://bucket/mydb/mytable/dateint=20230101"]
        mock_client.get_partitions_by_filter.assert_called_once_with(
            "mydb", "mytable", "dateint=20230101", -1
        )

    def test_get_partition_uris_no_filter(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_thrift_catalog import (
            HiveThriftCatalog,
        )

        cat = HiveThriftCatalog(metastore_uri="thrift://localhost:9083")

        part = MagicMock()
        part.sd = MagicMock()
        part.sd.location = "s3://bucket/mydb/mytable/dateint=20230101"

        with patch.object(cat, "_connect") as mock_conn:
            mock_client = MagicMock()
            mock_transport = MagicMock()
            mock_conn.return_value = (mock_client, mock_transport)
            mock_client.get_partitions.return_value = [part]

            uris = cat.get_partition_uris("mydb", "mytable", "hive", filter_expr=None)

        assert uris == ["s3://bucket/mydb/mytable/dateint=20230101"]
        mock_client.get_partitions.assert_called_once_with("mydb", "mytable", -1)


# ---------------------------------------------------------------------------
# Integration tests (require docker-compose)
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestHiveThriftCatalogIntegration:
    TEST_DB = "default"
    TEST_TABLE = "mf_fastdata_test_hive"

    def test_create_and_read_table(self, hive_catalog):
        info = hive_catalog.create_table(
            db=self.TEST_DB,
            table=self.TEST_TABLE,
            catalog_name="hive",
            column_schema=[("id", "int"), ("name", "string")],
            partition_schema=[("dt", "string")],
        )
        assert info.db == self.TEST_DB
        assert info.table == self.TEST_TABLE
        assert "id" in info.schema
        assert "dt" in info.partition_schema

    def test_get_table_info_roundtrip(self, hive_catalog):
        info = hive_catalog.get_table_info(self.TEST_DB, self.TEST_TABLE)
        assert info.type == "hive"
        assert info.format == "parquet"

    def test_delete_table(self, hive_catalog):
        hive_catalog.delete_table(self.TEST_DB, self.TEST_TABLE, "hive", must_exist=False)
        from metaflow_extensions.fastdata_ext.plugins.table.exception import (
            MetaflowTableNotFound,
        )

        with pytest.raises(MetaflowTableNotFound):
            hive_catalog.get_table_info(self.TEST_DB, self.TEST_TABLE)
