from itertools import chain
from typing import Any, Dict, List, Optional

from metaflow.util import is_stringish
from metaflow import S3

from .exception import (
    MetaflowTableIncompatible,
    MetaflowTableException,
    MetaflowTableTypeMismatch,
)
from .utils import format_set
from .catalog import MetadataCatalog, TableInfo


class HiveTable(object):
    """
    Data provider for a Hive table.

    Accepts a TableInfo snapshot (schema/partition metadata) and a
    MetadataCatalog instance (used to fetch partition URIs at read time).
    """

    def __init__(
        self,
        table_info: TableInfo,
        catalog: MetadataCatalog,
        stats=None,
        **kwargs,
    ):
        self._table_info = table_info
        self._catalog = catalog
        self._session_vars = kwargs.get("session_vars", None)

        # Hive tables need partition columns
        if not self.partition_columns:
            raise MetaflowTableIncompatible(
                "Table %s has no partition "
                "columns (required for Hive)" % self._table_info.path
            )

        # Check table file format
        if "parquet" not in self._table_info.format:
            raise MetaflowTableIncompatible(
                "Table %s is not using Parquet" % self._table_info.path
            )

    @property
    def schema(self):
        schema = [
            {"id": i, "name": k, "type": v}
            for i, (k, v) in enumerate(self._table_info.schema.items())
        ]
        return schema

    @property
    def partition_columns(self):
        """
        Return the partition columns
        """
        return list(self._table_info.partition_schema)

    def validate_schema(self):
        pass

    def validate_columns(self, columns=None):
        """
        Validate the specified column names are part of the current
        schema.

        If columns is None skip.
        If columns is an empty list throw an exception
        """
        missing = []
        schema_columns = set(schema["name"] for schema in self.schema)
        for col in columns:
            if col not in schema_columns:
                missing.append(col)

        if missing:
            raise MetaflowTableException(
                "Columns %s from the argument 'columns' aren't "
                "data columns in the Hive tables schema. "
                "Only data columns can be projected for Hive tables."
                % format_set(set(missing))
            )

    @property
    def has_schema_evolution(self):
        return False

    @property
    def has_partition_evolution(self):
        return False

    def get_grouped_partition_paths(
        self,
        partition_specs: List[Dict[str, Any]],
        group_by: str = "record",
        num_groups: Optional[int] = None,
        target_group_size: Optional[int] = None,
    ):
        """
        Get the S3 paths for a list of partition specs and optionally groups
        them in roughly equal groups (either by file size or number of records)

        Parameters
        ----------
        partition_specs : List[Dict[str, Any]]
            List of query predicates. Each query predicate is a dictionary
            where the keys are column names and the value is the value to
            filter by. Multiple keys are interpreted as a conjunction (AND
            query).
            An example of a query predicate:
                {'dateint': 20180811, 'country': 'US'}
        group_by : str, default 'record'
            Not used for Hive tables
        num_groups : int, optional, default None
            Not used for Hive tables
        target_group_size : int, optional, default None
            Not used for Hive tables

        Returns
        -------
        List[List[str]],
            The outer list is the groups and the inner list are paths
            to the parquet files in that group. For Hive tables, the outer
            list will always have a single element.
        """
        if (
            group_by != "record"
            or num_groups is not None
            or target_group_size is not None
        ):
            raise MetaflowTableException(
                "group_by, num_groups and target_group_size are not applicable to hive table %s."
                % self._table_info.path
            )

        def parts(partition_spec):
            for key, val in partition_spec.items():
                coltype = self._table_info.partition_schema[key]
                if is_stringish(val) != (coltype == "chararray"):
                    raise MetaflowTableTypeMismatch(
                        "Invalid column type for '%s'. String %sexpected."
                        % (key, "" if coltype == "chararray" else "not ")
                    )

                if is_stringish(val):
                    yield "%s='%s'" % (key, val)
                else:
                    yield "%s=%s" % (key, val)

        uris = []
        for partition_spec in partition_specs:
            filter_expr = " AND ".join(parts(partition_spec)) if partition_spec else None
            partition_uris = self._catalog.get_partition_uris(
                self._table_info.db,
                self._table_info.table,
                self._table_info.catalog_name,
                filter_expr=filter_expr,
            )
            uris.append(partition_uris)

        uris = list(chain.from_iterable(uris))

        # sorted guarantees a deterministic order of results
        uris = sorted(uri.replace("s3n://", "s3://") for uri in uris)

        # list files
        paths = []
        with S3(session_vars=self._session_vars) as s3:
            s3objs = s3.list_recursive(uris)
            paths = [s3obj.url for s3obj in s3objs]
        return [paths]
