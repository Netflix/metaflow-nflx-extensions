"""
Unit tests for the MetadataCatalog ABC, TableInfo, and WriterInfo dataclasses,
and the _get_default_catalog factory.

These tests do NOT require any external services.
"""

from collections import OrderedDict

import pytest


# ---------------------------------------------------------------------------
# TableInfo
# ---------------------------------------------------------------------------

class TestTableInfo:
    def test_path_property(self):
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import TableInfo

        info = TableInfo(
            db="mydb",
            table="mytable",
            catalog_name="prodhive",
            type="iceberg",
            format="parquet",
        )
        assert info.path == "prodhive/mydb/mytable"

    def test_path_with_empty_catalog_name(self):
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import TableInfo

        info = TableInfo(
            db="db",
            table="tbl",
            catalog_name="",
            type="hive",
            format="parquet",
        )
        # empty catalog_name still produces the separator-joined string
        assert info.path == "/db/tbl"

    def test_schema_defaults_to_empty_ordered_dict(self):
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import TableInfo

        info = TableInfo(db="d", table="t", catalog_name="c", type="hive", format="parquet")
        assert isinstance(info.schema, OrderedDict)
        assert len(info.schema) == 0

    def test_metadata_location_default_is_empty(self):
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import TableInfo

        info = TableInfo(db="d", table="t", catalog_name="c", type="iceberg", format="parquet")
        assert info.metadata_location == ""

    def test_secure_default_is_false(self):
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import TableInfo

        info = TableInfo(db="d", table="t", catalog_name="c", type="hive", format="parquet")
        assert info.secure is False


# ---------------------------------------------------------------------------
# WriterInfo
# ---------------------------------------------------------------------------

class TestWriterInfo:
    def test_defaults(self):
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import WriterInfo

        w = WriterInfo()
        assert w.snapshot_summary is None
        assert w.update_current_snapshot is True
        assert w.set_refs is None

    def test_frozen(self):
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import WriterInfo

        w = WriterInfo()
        with pytest.raises((AttributeError, TypeError)):
            w.update_current_snapshot = False  # frozen dataclass

    def test_custom_values(self):
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import WriterInfo

        w = WriterInfo(
            snapshot_summary={"wap.id": "abc"},
            update_current_snapshot=False,
            set_refs={},
        )
        assert w.snapshot_summary == {"wap.id": "abc"}
        assert w.update_current_snapshot is False
        assert w.set_refs == {}


# ---------------------------------------------------------------------------
# MetadataCatalog ABC
# ---------------------------------------------------------------------------

class TestMetadataCatalogABC:
    def test_cannot_instantiate_directly(self):
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import MetadataCatalog

        with pytest.raises(TypeError):
            MetadataCatalog()

    def test_concrete_subclass_needs_all_abstract_methods(self):
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import MetadataCatalog

        class Incomplete(MetadataCatalog):
            pass  # missing implementations

        with pytest.raises(TypeError):
            Incomplete()

    def test_optional_hooks_have_noop_defaults(self):
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import (
            MetadataCatalog,
            TableInfo,
            WriterInfo,
        )

        class MinimalCatalog(MetadataCatalog):
            def get_table_info(self, db, table):
                pass

            def get_partition_uris(self, db, table, catalog_name, filter_expr=None):
                return []

            def create_table(self, db, table, catalog_name, column_schema, partition_schema, **kwargs):
                pass

            def delete_table(self, db, table, catalog_name, must_exist=True):
                pass

            def update_metadata_location(self, db, table, catalog_name, new_metadata_location, previous_metadata_location=None):
                pass

        cat = MinimalCatalog()
        assert cat.get_credentials("resource") == {}
        assert isinstance(cat.get_writer_info(), WriterInfo)
        assert cat.get_writer_info().update_current_snapshot is True
        assert cat.publish_read_event(["col1"]) is None
        assert cat.publish_write_event() is None


# ---------------------------------------------------------------------------
# _get_default_catalog factory
# ---------------------------------------------------------------------------

class TestGetDefaultCatalog:
    def test_returns_hive_thrift_catalog_by_default(self, monkeypatch):
        """Without any config, should return HiveThriftCatalog."""
        # Ensure the config attr is absent so we fall through to the default
        import metaflow_extensions.fastdata_ext.plugins.table.catalog as catalog_mod

        # Patch out the config import
        monkeypatch.setattr(
            "builtins.__import__",
            _make_import_raiser(
                "metaflow.metaflow_config",
                "DEFAULT_METADATA_CATALOG",
                original_import=__builtins__["__import__"] if isinstance(__builtins__, dict) else __import__,
            ),
            raising=False,
        )

    def test_hive_thrift_catalog_is_importable(self):
        from metaflow_extensions.fastdata_ext.plugins.table.hive_thrift_catalog import (
            HiveThriftCatalog,
        )
        from metaflow_extensions.fastdata_ext.plugins.table.catalog import MetadataCatalog

        assert issubclass(HiveThriftCatalog, MetadataCatalog)

    def test_unknown_backend_raises(self, monkeypatch):
        import sys
        import types

        # Inject a fake config module that sets an unknown backend
        fake_config = types.ModuleType("metaflow.metaflow_config")
        fake_config.DEFAULT_METADATA_CATALOG = "nonexistent_backend"
        monkeypatch.setitem(sys.modules, "metaflow.metaflow_config", fake_config)

        from metaflow_extensions.fastdata_ext.plugins.table.catalog import _get_default_catalog

        with pytest.raises(ValueError, match="Unknown metadata catalog backend"):
            _get_default_catalog()


def _make_import_raiser(module_name, attr_name, original_import):
    """Helper: returns an __import__ that raises ImportError for a specific module."""
    def _import(name, *args, **kwargs):
        if name == module_name:
            raise ImportError("mocked")
        return original_import(name, *args, **kwargs)
    return _import
