"""
Abstract metadata catalog interface for metaflow-fastdata.

The MetadataCatalog ABC defines the contract between the Table API and
the underlying table metadata service (Hive Metastore, Metacat, etc.).

OSS default: HiveThriftCatalog (connects to a Hive Metastore via Thrift).
Netflix override: MetacatCatalog (registered via mfextinit in nflx-fastdata).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class WriterInfo:
    """Information passed to the Iceberg writer during a commit.

    snapshot_summary:
        Optional extra fields to include in the snapshot summary.
    update_current_snapshot:
        Whether to update the "current snapshot" pointer for the table.
    set_refs:
        If None  → set the main branch to the newly created snapshot.
        If {}    → set no refs at all.
        If {...} → set the specified branch/tag refs.
    """

    snapshot_summary: Optional[Dict[str, str]] = None
    update_current_snapshot: bool = True
    set_refs: Optional[Dict[str, Dict[str, str]]] = None


@dataclass
class TableInfo:
    """Immutable snapshot of table metadata returned by MetadataCatalog.get_table_info().

    This mirrors the properties previously exposed by MetacatTable.
    """

    # Identifying info
    db: str
    table: str
    catalog_name: str  # e.g. "prodhive" — the logical catalog name, not the Python object

    # Table type and format
    type: str  # "hive" or "iceberg"
    format: str  # e.g. "parquet" or full serde output format string

    # Schemas (column name → type string), preserving insertion order
    schema: OrderedDict = field(default_factory=OrderedDict)
    partition_schema: OrderedDict = field(default_factory=OrderedDict)

    # Iceberg-specific
    metadata_location: str = ""
    replaced_prefix: Optional[str] = None  # for s3n→s3 prefix remapping

    # Security
    secure: bool = False

    @property
    def path(self) -> str:
        """Full catalog/db/table path string."""
        return "/".join([self.catalog_name, self.db, self.table])


class MetadataCatalog(ABC):
    """Abstract base for table metadata catalogs.

    Implementations must provide read (get_table_info, get_partition_uris)
    and write (create_table, delete_table, update_metadata_location) operations.

    Optional hook methods have default no-op implementations that subclasses
    may override:
      - get_credentials(resource)   → {} (use ambient AWS credentials)
      - get_writer_info()           → WriterInfo() (no WAP)
      - publish_read_event(...)     → no-op
      - publish_write_event()       → no-op
    """

    # -----------------------------------------------------------------------
    # Abstract methods — must be implemented by every catalog
    # -----------------------------------------------------------------------

    @abstractmethod
    def get_table_info(self, db: str, table: str) -> TableInfo:
        """Return metadata for the given table.

        Parameters
        ----------
        db:    database name
        table: table name

        Returns
        -------
        TableInfo containing type, format, schema, partition_schema, etc.

        Raises
        ------
        MetaflowTableNotFound if the table does not exist.
        """
        ...

    @abstractmethod
    def get_partition_uris(
        self,
        db: str,
        table: str,
        catalog_name: str,
        filter_expr: Optional[str] = None,
    ) -> List[str]:
        """Return S3/storage URIs for partitions matching filter_expr.

        Parameters
        ----------
        db:           database name
        table:        table name
        catalog_name: logical catalog name (e.g. "prodhive")
        filter_expr:  SQL-style partition filter, e.g. "dateint=20230101 AND country='US'"
                      If None or empty, returns URIs for all partitions.

        Returns
        -------
        List of storage URIs (s3:// paths pointing to directories or files).
        """
        ...

    @abstractmethod
    def create_table(
        self,
        db: str,
        table: str,
        catalog_name: str,
        column_schema: List[tuple],
        partition_schema: List[tuple],
        **kwargs,
    ) -> TableInfo:
        """Create a new Hive table in the catalog.

        Parameters
        ----------
        db:               database name
        table:            table name
        catalog_name:     logical catalog name
        column_schema:    list of (name, type) tuples for data columns
        partition_schema: list of (name, type) tuples for partition columns
        **kwargs:         implementation-specific options

        Returns
        -------
        TableInfo for the newly created table.
        """
        ...

    @abstractmethod
    def delete_table(
        self, db: str, table: str, catalog_name: str, must_exist: bool = True
    ) -> None:
        """Drop a table from the catalog (metadata-only operation).

        Parameters
        ----------
        db:           database name
        table:        table name
        catalog_name: logical catalog name
        must_exist:   if True, raise MetaflowTableNotFound when table is absent
        """
        ...

    @abstractmethod
    def update_metadata_location(
        self,
        db: str,
        table: str,
        catalog_name: str,
        new_metadata_location: str,
        previous_metadata_location: Optional[str] = None,
    ) -> None:
        """Update the Iceberg metadata location pointer in the catalog.

        This is called after successfully writing new Iceberg metadata to S3
        in order to make the commit visible to readers.

        Parameters
        ----------
        db, table, catalog_name: table identity
        new_metadata_location:      new metadata.json path (s3://)
        previous_metadata_location: expected current location for optimistic
                                    concurrency; None means unconditional update.

        Raises
        ------
        MetaflowTableConflict  if optimistic concurrency check fails.
        MetaflowTableNotFound  if the table doesn't exist.
        MetaflowTableCommitStateUnknownException if commit outcome is uncertain.
        """
        ...

    # -----------------------------------------------------------------------
    # Optional hooks — default to OSS no-ops; Netflix overrides these
    # -----------------------------------------------------------------------

    def get_credentials(self, resource: str) -> dict:
        """Return S3 session credentials for accessing a secure resource.

        OSS default: empty dict (use ambient credentials / boto3 default chain).
        """
        return {}

    def get_writer_info(self) -> WriterInfo:
        """Return writer configuration for the current Iceberg commit.

        OSS default: plain append, no WAP.
        """
        return WriterInfo()

    def publish_read_event(self, columns=None) -> None:
        """Publish a lineage read event. OSS no-op."""
        pass

    def publish_write_event(self) -> None:
        """Publish a lineage write event. OSS no-op."""
        pass


def _get_default_catalog(catalog_name: str = "") -> "MetadataCatalog":
    """Instantiate the catalog configured by DEFAULT_METADATA_CATALOG.

    Looks up the config value and returns the appropriate MetadataCatalog
    implementation.  The catalog_name string (e.g. "prodhive") is passed
    as a hint to implementations that support named catalogs (Metacat).
    """
    try:
        from metaflow.metaflow_config import DEFAULT_METADATA_CATALOG  # type: ignore

        backend = DEFAULT_METADATA_CATALOG
    except (ImportError, AttributeError):
        backend = "hive_thrift"

    if backend == "hive_thrift":
        from .hive_thrift_catalog import HiveThriftCatalog

        return HiveThriftCatalog()
    elif backend == "metacat":
        # Provided by nflx-fastdata; imported lazily to avoid hard dependency
        from metaflow_extensions.nflx.plugins.table.metacat_catalog import (  # type: ignore
            MetacatCatalog,
        )

        return MetacatCatalog(catalog_name=catalog_name or "prodhive")
    else:
        raise ValueError("Unknown metadata catalog backend: %s" % backend)
