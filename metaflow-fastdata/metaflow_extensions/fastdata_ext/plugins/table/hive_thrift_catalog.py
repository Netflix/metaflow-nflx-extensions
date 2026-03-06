"""
HiveThriftCatalog — OSS metadata catalog backed by the Hive Metastore Thrift API.

Connects to any Hive Metastore (Apache Hive 2+, AWS Glue with HMS-compatible
endpoint, etc.) using the `thrift` protocol.

Requires `PyHive` (with `thrift` extras) or the standalone `thrift` package:
    pip install "PyHive[thrift]"
    # or
    pip install thrift thrift-sasl

Configuration (via metaflow config or environment variables):
    METAFLOW_HIVE_METASTORE_URI  — default: thrift://localhost:9083
"""

from __future__ import annotations

import os
import struct
import time
from collections import OrderedDict
from typing import Dict, List, Optional

from .catalog import MetadataCatalog, TableInfo, WriterInfo
from .exception import (
    MetaflowTableException,
    MetaflowTableNotFound,
    MetaflowTableExists,
    MetaflowTableCommitStateUnknownException,
    MetaflowTableConflict,
    MetaflowTableRateLimitException,
)

# Lazy-loaded thrift/HMS clients
_thrift_client_module = None


def _get_metastore_uri() -> str:
    try:
        from metaflow.metaflow_config import METAFLOW_HIVE_METASTORE_URI  # type: ignore

        if METAFLOW_HIVE_METASTORE_URI:
            return METAFLOW_HIVE_METASTORE_URI
    except (ImportError, AttributeError):
        pass
    return os.environ.get("METAFLOW_HIVE_METASTORE_URI", "thrift://localhost:9083")


def _parse_thrift_uri(uri: str):
    """Parse thrift://host:port into (host, port)."""
    uri = uri.strip()
    if uri.startswith("thrift://"):
        uri = uri[len("thrift://") :]
    host, _, port_str = uri.rpartition(":")
    if not host:
        host = port_str
        port_str = "9083"
    return host, int(port_str)


def _lazy_connect(uri: str):
    """Return an open HiveMetaStoreClient connection."""
    try:
        from thrift.protocol import TBinaryProtocol
        from thrift.transport import TSocket, TTransport

        try:
            from hmsclient.genthrift.hive_metastore import ThriftHiveMetastore
        except ImportError:
            try:
                from hive_metastore import ThriftHiveMetastore  # type: ignore
            except ImportError:
                from thrift_gen.hive_metastore import ThriftHiveMetastore  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "HiveThriftCatalog requires HMS Thrift bindings. "
            "Install with: pip install hmsclient\n"
            "Original error: %s" % exc
        ) from exc

    host, port = _parse_thrift_uri(uri)
    transport = TSocket.TSocket(host, port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = ThriftHiveMetastore.Client(protocol)
    transport.open()
    return client, transport


def _hive_type_to_str(type_obj) -> str:
    """Convert a Thrift FieldSchema type string (already a string in HMS) to str."""
    if hasattr(type_obj, "name"):
        return type_obj.name
    return str(type_obj)


def _serde_to_format(serde_info) -> str:
    """Extract a human-readable format from Hive SerDeInfo."""
    if serde_info is None:
        return "parquet"
    output_fmt = getattr(serde_info, "outputFormat", "") or ""
    if "parquet" in output_fmt.lower():
        return "parquet"
    if "orc" in output_fmt.lower():
        return "orc"
    if "avro" in output_fmt.lower():
        return "avro"
    return output_fmt or "parquet"


def _sanitize_s3(path: str):
    """Normalise non-standard S3 scheme variants to s3://, return (path, replaced_prefix)."""
    if path.startswith("s3://"):
        return path, None
    if path.startswith("s3n://"):
        return "s3://" + path[6:], "s3n"
    # generic: strip scheme, re-attach s3://
    parts = path.split(":", 1)
    if len(parts) == 2:
        return "s3://" + parts[1].lstrip("/"), parts[0]
    return path, None


class HiveThriftCatalog(MetadataCatalog):
    """Metadata catalog that talks to a Hive Metastore via Thrift.

    OSS default implementation — suitable for any standard Hive / Iceberg
    environment that exposes a HMS-compatible Thrift endpoint.
    """

    def __init__(self, metastore_uri: Optional[str] = None):
        self._uri = metastore_uri or _get_metastore_uri()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self):
        """Return (client, transport).  Caller is responsible for closing transport."""
        return _lazy_connect(self._uri)

    def _get_hive_table(self, client, db: str, table: str):
        """Fetch the raw Thrift Table object; raise MetaflowTableNotFound on 404."""
        try:
            return client.get_table(db, table)
        except Exception as exc:
            exc_name = type(exc).__name__
            if (
                "NoSuchObjectException" in exc_name
                or "TApplicationException" in exc_name
            ):
                raise MetaflowTableNotFound(
                    "Table %s/%s not found in Hive Metastore at %s"
                    % (db, table, self._uri)
                ) from exc
            raise MetaflowTableException(
                "Error fetching table %s/%s: %s" % (db, table, exc)
            ) from exc

    # ------------------------------------------------------------------
    # MetadataCatalog interface
    # ------------------------------------------------------------------

    def get_table_info(self, db: str, table: str) -> TableInfo:
        client, transport = self._connect()
        try:
            tbl = self._get_hive_table(client, db, table)

            # Determine table type: Hive or Iceberg
            params = {}
            if tbl.parameters:
                params = dict(tbl.parameters)
            table_type = params.get("table_type", "").lower()
            is_iceberg = table_type == "iceberg"

            # Metadata location for Iceberg tables
            metadata_location = ""
            replaced_prefix = None
            if is_iceberg:
                raw_loc = params.get("metadata_location", "")
                metadata_location, replaced_prefix = _sanitize_s3(raw_loc)

            # Table format (parquet, orc, etc.)
            sd = tbl.sd
            fmt = "parquet"
            if sd and sd.serdeInfo:
                out_fmt = sd.outputFormat or ""
                if "parquet" in out_fmt.lower():
                    fmt = "parquet"
                elif "orc" in out_fmt.lower():
                    fmt = "orc"
                else:
                    fmt = out_fmt

            # Schema: regular columns
            schema = OrderedDict()
            if tbl.sd and tbl.sd.cols:
                for col in tbl.sd.cols:
                    schema[col.name] = col.type

            # Partition columns
            partition_schema = OrderedDict()
            if tbl.partitionKeys:
                for pk in tbl.partitionKeys:
                    partition_schema[pk.name] = pk.type

            return TableInfo(
                db=db,
                table=table,
                catalog_name="hive",  # generic name for Hive Thrift
                type="iceberg" if is_iceberg else "hive",
                format=fmt,
                schema=schema,
                partition_schema=partition_schema,
                metadata_location=metadata_location,
                replaced_prefix=replaced_prefix,
                secure=False,
            )
        finally:
            transport.close()

    def get_partition_uris(
        self,
        db: str,
        table: str,
        catalog_name: str,
        filter_expr: Optional[str] = None,
    ) -> List[str]:
        client, transport = self._connect()
        try:
            if filter_expr:
                # Use get_partitions_by_filter when a filter is specified
                try:
                    partitions = client.get_partitions_by_filter(
                        db, table, filter_expr, -1
                    )
                except Exception:
                    # Fall back to all partitions and return locations
                    partitions = client.get_partitions(db, table, -1)
            else:
                partitions = client.get_partitions(db, table, -1)

            uris = []
            for part in partitions:
                if part.sd and part.sd.location:
                    loc = part.sd.location.replace("s3n://", "s3://")
                    uris.append(loc)
            return uris
        except MetaflowTableNotFound:
            raise
        except Exception as exc:
            raise MetaflowTableException(
                "Error fetching partitions for %s/%s: %s" % (db, table, exc)
            ) from exc
        finally:
            transport.close()

    def create_table(
        self,
        db: str,
        table: str,
        catalog_name: str,
        column_schema: List[tuple],
        partition_schema: List[tuple],
        **kwargs,
    ) -> TableInfo:
        """Register a new empty Iceberg table in the Hive Metastore.

        Parameters
        ----------
        db : str
            Database name (created if it does not exist).
        table : str
            Table name.
        catalog_name : str
            Ignored for HiveThriftCatalog (no named catalogs).
        column_schema : List[tuple]
            List of (column_name, hive_type) pairs, e.g. [("id", "bigint"), ...].
        partition_schema : List[tuple]
            Partition column pairs; Iceberg tables manage partitioning internally so
            this is registered in HMS as regular columns with partition metadata.
        **kwargs :
            location (str, required): S3/object-store path for table data.
                e.g. "s3://my-bucket/db/table"
            table_type (str, optional): "iceberg" (default) or "hive".
        """
        location = kwargs.get("location")
        if not location:
            raise MetaflowTableException(
                "create_table requires a 'location' keyword argument "
                "(e.g. location='s3://bucket/db/table')"
            )
        table_type = kwargs.get("table_type", "iceberg").lower()

        client, transport = self._connect()
        try:
            # Import HMS Thrift types alongside the client
            try:
                from hmsclient.genthrift.hive_metastore.ttypes import (
                    Table as HmsTable,
                    StorageDescriptor,
                    SerDeInfo,
                    FieldSchema,
                    Database,
                )
            except ImportError:
                try:
                    from hive_metastore.ttypes import (  # type: ignore
                        Table as HmsTable,
                        StorageDescriptor,
                        SerDeInfo,
                        FieldSchema,
                        Database,
                    )
                except ImportError:
                    from thrift_gen.hive_metastore.ttypes import (  # type: ignore
                        Table as HmsTable,
                        StorageDescriptor,
                        SerDeInfo,
                        FieldSchema,
                        Database,
                    )

            # Create the database if it does not already exist
            try:
                client.get_database(db)
            except Exception:
                client.create_database(
                    Database(
                        name=db,
                        description="",
                        locationUri="",
                        parameters={},
                    )
                )

            # Drop the table if it already exists (idempotent create)
            try:
                client.drop_table(db, table, True)
            except Exception:
                pass

            # Build column FieldSchema list
            cols = [
                FieldSchema(name=name, type=htype, comment="")
                for name, htype in column_schema
            ]

            # Build partition key FieldSchema list
            part_keys = [
                FieldSchema(name=name, type=htype, comment="")
                for name, htype in partition_schema
            ]

            if table_type == "iceberg":
                in_fmt = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat"
                out_fmt = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat"
                serde_lib = "org.apache.iceberg.mr.hive.HiveIcebergSerDe"
                params = {
                    "table_type": "ICEBERG",
                    "metadata_location": "",
                    "EXTERNAL": "TRUE",
                }
            else:
                in_fmt = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
                out_fmt = (
                    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
                )
                serde_lib = (
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                )
                params = {}

            hms_table = HmsTable(
                dbName=db,
                tableName=table,
                tableType="EXTERNAL_TABLE",
                parameters=params,
                sd=StorageDescriptor(
                    cols=cols,
                    location=location,
                    inputFormat=in_fmt,
                    outputFormat=out_fmt,
                    compressed=False,
                    numBuckets=-1,
                    serdeInfo=SerDeInfo(
                        name=table,
                        serializationLib=serde_lib,
                        parameters={},
                    ),
                    parameters={},
                ),
                partitionKeys=part_keys,
            )
            client.create_table(hms_table)
        except (MetaflowTableException,):
            raise
        except Exception as exc:
            raise MetaflowTableException(
                "Error creating table %s/%s: %s" % (db, table, exc)
            ) from exc
        finally:
            transport.close()

        return self.get_table_info(db, table)

    def delete_table(
        self, db: str, table: str, catalog_name: str, must_exist: bool = True
    ) -> None:
        client, transport = self._connect()
        try:
            client.drop_table(db, table, True)
        except Exception as exc:
            exc_name = type(exc).__name__
            if "NoSuchObjectException" in exc_name:
                if must_exist:
                    raise MetaflowTableNotFound(
                        "Table %s/%s not found" % (db, table)
                    ) from exc
            else:
                raise MetaflowTableException(
                    "Error dropping table %s/%s: %s" % (db, table, exc)
                ) from exc
        finally:
            transport.close()

    def update_metadata_location(
        self,
        db: str,
        table: str,
        catalog_name: str,
        new_metadata_location: str,
        previous_metadata_location: Optional[str] = None,
    ) -> None:
        """Update the Iceberg metadata_location property in Hive Metastore.

        This requires ALTER TABLE privileges on the metastore.
        """
        client, transport = self._connect()
        try:
            tbl = self._get_hive_table(client, db, table)

            if tbl.parameters is None:
                tbl.parameters = {}

            # Optimistic concurrency: check previous location if specified
            if previous_metadata_location is not None:
                current = tbl.parameters.get("metadata_location", "")
                # Normalise for comparison
                current_norm = current.replace("s3n://", "s3://")
                prev_norm = previous_metadata_location.replace("s3n://", "s3://")
                if current_norm != prev_norm:
                    raise MetaflowTableConflict(
                        "Metadata location conflict for table %s/%s: "
                        "expected '%s', found '%s'"
                        % (db, table, prev_norm, current_norm)
                    )

            tbl.parameters["metadata_location"] = new_metadata_location
            tbl.parameters["table_type"] = "ICEBERG"

            client.alter_table(db, table, tbl)
        except (MetaflowTableConflict, MetaflowTableNotFound):
            raise
        except Exception as exc:
            raise MetaflowTableCommitStateUnknownException() from exc
        finally:
            transport.close()
