import concurrent.futures
import os
import time
import tempfile
import uuid
import random
import math

from typing import Any, Dict, List, Optional, cast

from metaflow.debug import debug

from metaflow.plugins.datatools.s3 import S3
from ..datatools.dataframe import MetaflowDataFrame

from metaflow.metaflow_profile import profile

from .catalog import MetadataCatalog, TableInfo, WriterInfo, _get_default_catalog
from .hive_table import HiveTable
from .iceberg_table import IcebergTable
from .utils import format_set

from ..datatools.type_info import (
    ALLOWED_ICEBERG_PARTITION_TRANSFORMS_WRITE,
)

from metaflow.plugins.datatools.iceberg import (
    DataFile,
    DataFileStatus,
    MetaflowIcebergDuplicateDataFiles,
)

from metaflow_extensions.fastdata_ext.plugins.datatools.internal_ops import (
    is_same_value,
    validate_iceberg_mdf,
)

from .exception import (
    MetaflowTableException,
    MetaflowTableNotFound,
    MetaflowTableExists,
    MetaflowTableConflict,
    MetaflowTableEmptyPartition,
    MetaflowTableIncompatible,
    MetaflowTableInvalidData,
    MetaflowTableCommitStateUnknownException,
    MetaflowTableRateLimitException,
)
from itertools import chain

DEFAULT_WRITE_THREADS = 16

# After this many files, we use get_many instead of get
THRESHOLD_GET_MANY = 5


def _is_secure_path(path: str) -> bool:
    """Return True for paths that require special S3 credentials.

    OSS default: always False (no special credential handling).
    Netflix override: checks for "nflx-secure-dataeng" prefix.
    """
    return False


class Table(object):
    """
    Table represents a Hive or Iceberg table but does not contain the data itself.

    Parameters
    ----------
    db : str
        Name of the database.
    table : str
        Name of the table.
    catalog : MetadataCatalog, optional
        The catalog implementation to use.  If None, the catalog configured
        by DEFAULT_METADATA_CATALOG is used (defaults to HiveThriftCatalog).
    catalog_name : str, optional
        Logical catalog name passed as a hint to catalog implementations that
        support named catalogs (e.g. "prodhive" for Metacat). Ignored by the
        OSS HiveThriftCatalog.
    """

    def __init__(
        self,
        db: str,
        table: str,
        catalog: Optional[MetadataCatalog] = None,
        catalog_name: str = "",
    ):
        self._db = db
        self._table_name = table
        self._catalog_name = catalog_name
        self._catalog_obj: MetadataCatalog = catalog or _get_default_catalog(
            catalog_name
        )
        self._stats: Dict[str, Any] = {}
        self._info: Optional[TableInfo] = None
        self._s3_config: Optional[Dict[str, Any]] = None

        def reload():
            self._info = self._catalog_obj.get_table_info(db, table)
            # Fetch credentials for secure Iceberg tables (Netflix-specific hook;
            # OSS default returns {}).
            self._s3_config = None
            if self._info.secure and self._info.type == "iceberg":
                creds = self._catalog_obj.get_credentials(
                    ".".join(filter(None, [self._info.catalog_name, db, table]))
                )
                self._s3_config = creds if creds else None

            # Choose iceberg or hive table implementation
            if self._info.type == "hive":
                self._table = HiveTable(
                    self._info,
                    self._catalog_obj,
                    self._stats,
                    session_vars=self._s3_config,
                )
            elif self._info.type == "iceberg":
                self._table = IcebergTable(
                    self._info.metadata_location,
                    self._stats,
                    allow_empty=True,
                    strict_partition_evolution=False,
                    session_vars=self._s3_config,
                )
            else:
                raise MetaflowTableException(
                    "Unknown table type '%s' for %s/%s" % (self._info.type, db, table)
                )

        self._reload = reload
        self._reload()

        # Full path string for error messages (computed once after first load)
        self._path = self._info.path

    def __getstate__(self):
        raise MetaflowTableException("Persisting Tables is not yet supported.")

    def __str__(self):
        return "<Table: %s>" % self._path

    def __repr__(self):
        return str(self)

    @property
    def db(self) -> str:
        """
        Returns the database name.
        """
        return self._db

    @property
    def table_name(self) -> str:
        """
        Returns the table name.
        """
        return self._table_name

    @property
    def catalog(self) -> str:
        """
        Returns the catalog name string (e.g. "prodhive" or "hive").
        """
        return self._info.catalog_name

    @property
    def path(self) -> str:
        """
        Returns the path of the table in the format "db/table".
        """
        return "/".join([self._db, self._table_name])

    @property
    def full_path(self) -> str:
        """
        Returns the full path of the table in the format "catalog/db/table".
        """
        return self._path

    @property
    def schema(self) -> List[Dict[str, Any]]:
        """
        Returns the schema of the table in its native type (e.g. hive or iceberg)
        """
        return self._table.schema

    @property
    def is_iceberg(self) -> bool:
        """
        Returns True if the table is an Iceberg table
        """
        return self._info.type == "iceberg"

    @property
    def is_hive(self) -> bool:
        """
        Returns True if the table is a Hive table
        """
        return self._info.type == "hive"

    @property
    def stats(self) -> Dict[str, Any]:
        """
        Return a dictionary of statistics about the data loaded.
        """
        return self._stats

    @property
    def partition_columns(self) -> List[str]:
        """
        Return a list of partition columns of this table.
        """
        return self._table.partition_columns

    def get_all_paths(
        self,
        group_by: str = "record",
        num_groups: Optional[int] = None,
        target_group_size: Optional[int] = None,
    ) -> List[List[str]]:
        """
        Retrieves the file locations of all the files for this table.
        """
        return self.get_partition_paths(
            {},
            allow_all=True,
            group_by=group_by,
            num_groups=num_groups,
            target_group_size=target_group_size,
        )

    def get_partition_paths(
        self,
        partition_spec: Optional[Dict[str, Any]] = None,
        allow_all: bool = False,
        specs: Optional[List[Dict[str, Any]]] = None,
        group_by: str = "record",
        num_groups: Optional[int] = None,
        target_group_size: Optional[int] = None,
    ) -> List[List[str]]:
        """
        Get the file location of partitions of this table.
        """
        if partition_spec is None and specs is None:
            raise MetaflowTableException(
                "Either 'partition_spec' or 'specs' must be provided"
            )

        if specs is None:
            assert partition_spec is not None
            specs = [partition_spec]

        for partition_spec in specs:
            non_parts = set(partition_spec) - set(self._table.partition_columns)
            if non_parts:
                raise MetaflowTableException(
                    "'%s' is not a partition column in table '%s'"
                    % (list(non_parts)[0], self._path)
                )

            if not (allow_all or partition_spec):
                raise MetaflowTableException(
                    "Requires a non-empty partition_spec. If you want the whole table, "
                    "please specify allow_all=True"
                )

        # Group parquet files based on record num/byte size is only supported in Iceberg
        if (num_groups is not None or target_group_size is not None) and (
            not self.is_iceberg
        ):
            raise MetaflowTableException(
                "Table '%s' must be an iceberg table to group parquet files."
                % self._path
            )

        if num_groups is not None and target_group_size is not None:
            raise MetaflowTableException(
                "Expect getting num_groups or target_group_size but got both: \n"
                "num_groups: %s, target_group_size: %s"
                % (str(group_by), str(target_group_size))
            )

        if num_groups is not None and not (
            isinstance(num_groups, int) and num_groups >= 1
        ):
            raise MetaflowTableException(
                "Input num_groups must be an integer not smaller than 1; got %s"
                % str(num_groups)
            )

        if target_group_size is not None and not (
            isinstance(target_group_size, int) and target_group_size > 0
        ):
            raise MetaflowTableException(
                "Input target_group_size must be an integer larger than 0; got %s"
                % str(target_group_size)
            )

        if group_by not in ("record", "size"):
            raise MetaflowTableException(
                "Input group_by must be 'record' or 'size'; got %s" % str(group_by)
            )

        with profile("partition_query_time_ms", self._stats):
            s3locs = self._table.get_grouped_partition_paths(
                specs, group_by, num_groups, target_group_size
            )

        return s3locs

    def load_parquet(
        self,
        file_locations: List[str],
        columns: Optional[List[str]] = None,
        num_threads: Optional[int] = None,
    ) -> MetaflowDataFrame:
        """
        Loads the parquet files of the table.
        """
        # Publish lineage read event (no-op in OSS)
        with profile("lineage_publish_time_ms", self._stats):
            self._catalog_obj.publish_read_event(columns=columns)

        if len(file_locations) == 0:
            raise MetaflowTableEmptyPartition(
                "Input file_locations is empty. Nothing to download."
            )

        if self.is_iceberg:
            self._table.validate_schema(columns)

        if columns is not None:
            columns = list(columns)
            if not columns:
                raise MetaflowTableException(
                    "If 'columns' argument is specified, it must be a "
                    "non-empty list of columns."
                )
            self._table.validate_columns(columns)

        # Segregate secure vs non-secure paths
        secure_s3locs = [loc for loc in file_locations if _is_secure_path(loc)]
        nsecure_s3locs = [loc for loc in file_locations if not _is_secure_path(loc)]

        with profile("s3_download_time_ms", self._stats):
            secure_s3 = None
            nsecure_s3 = None
            try:
                res = []
                if secure_s3locs:
                    secure_s3 = S3(session_vars=self._s3_config)
                    if len(secure_s3locs) > THRESHOLD_GET_MANY:
                        res.extend(secure_s3.get_many(secure_s3locs))
                    else:
                        for loc in secure_s3locs:
                            res.append(secure_s3.get(loc))
                if nsecure_s3locs:
                    nsecure_s3 = S3()
                    if len(nsecure_s3locs) > THRESHOLD_GET_MANY:
                        res.extend(nsecure_s3.get_many(nsecure_s3locs))
                    else:
                        for loc in nsecure_s3locs:
                            res.append(nsecure_s3.get(loc))

                downloaded = sum(r.size for r in res)
                self._stats["downloaded_bytes"] = (
                    self._stats.get("downloaded_bytes", 0) + downloaded
                )

                local_paths = [os.path.realpath(r.path) for r in res]
                with profile("from_parquet_time_ms", self._stats):
                    if self._table.has_schema_evolution:
                        schema = self.schema
                        if columns is not None:
                            column_set = set(columns)
                            schema = [s for s in self.schema if s["name"] in column_set]

                        if num_threads:
                            return MetaflowDataFrame.from_parquet_with_schema(
                                local_paths,
                                schema=cast(Any, schema),
                                num_threads=num_threads,
                            )
                        else:
                            return MetaflowDataFrame.from_parquet_with_schema(
                                local_paths, schema=cast(Any, schema)
                            )
                    else:
                        if num_threads:
                            return MetaflowDataFrame.from_parquet(
                                local_paths,
                                columns=columns,
                                num_threads=num_threads,
                            )
                        else:
                            return MetaflowDataFrame.from_parquet(
                                local_paths,
                                columns=columns,
                            )
            finally:
                if secure_s3:
                    secure_s3.close()
                if nsecure_s3:
                    nsecure_s3.close()

    def _validate_and_transform_partition_tuple(
        self, partition_col, partition_transform
    ):
        if partition_transform not in ALLOWED_ICEBERG_PARTITION_TRANSFORMS_WRITE:
            raise MetaflowTableIncompatible(
                "Only %s partition transforms are supported."
                % (format_set(ALLOWED_ICEBERG_PARTITION_TRANSFORMS_WRITE),)
            )

        iterator = iter(partition_col)
        try:
            first_value = next(iterator)
        except:
            return None

        if not is_same_value(partition_col, first_value):
            raise MetaflowTableIncompatible(
                "Partition values for a MetaflowDataframe must be the "
                "same for all records to be written to iceberg "
                "table. Partition column %s has more than one value. "
                % partition_col.name
            )

        return first_value

    def _generate_partition_tuple(self, mdf):
        partition_spec = self._table._partition_spec
        partition_dict = {}
        for partition_field in partition_spec:
            partition_source_id = partition_field["source-id"]
            partition_name = partition_field["name"]
            partition_transform = partition_field["transform"]

            partition_col = mdf[self._table._schema[partition_source_id]["name"]]
            value = self._validate_and_transform_partition_tuple(
                partition_col, partition_transform
            )
            partition_dict[partition_name] = value
        return partition_dict

    def get_all(
        self,
        columns: Optional[List[str]] = None,
        num_threads: Optional[int] = None,
    ) -> MetaflowDataFrame:
        """
        Get all partitions of this table.
        """
        return self.get_partitions(
            {}, columns=columns, allow_all=True, num_threads=num_threads
        )

    def get_partitions(
        self,
        partition_spec: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        allow_all: bool = False,
        specs: Optional[List[Dict[str, Any]]] = None,
        num_threads: Optional[int] = None,
    ) -> MetaflowDataFrame:
        """
        Get partitions of this table.
        """
        s3locs = list(
            chain.from_iterable(
                self.get_partition_paths(
                    partition_spec=partition_spec,
                    allow_all=allow_all,
                    specs=specs,
                )
            )
        )

        if len(s3locs) == 0:
            if partition_spec:
                spec = " AND ".join("%s=%s" % x for x in partition_spec.items())
            else:
                spec = "all partitions"
            raise MetaflowTableEmptyPartition(
                "No data in %s for the requested partition: %s" % (self._path, spec)
            )

        return self.load_parquet(s3locs, columns=columns, num_threads=num_threads)

    def _put_data(
        self,
        mdf: MetaflowDataFrame,
        max_file_size=128 * 1024**2,
        num_threads: Optional[int] = None,
    ) -> List["DataFile"]:
        """
        Write the input MetaflowDataFrame to parquet file(s) in S3 and return a
        list of iceberg DataFile objects.
        """
        PARQUET_COMPRESSION_RATIO = 0.2
        GROUP_SIZE = math.ceil(max_file_size / PARQUET_COMPRESSION_RATIO)

        data_files = []

        if not self.is_iceberg:
            raise MetaflowTableException(
                "Table must be an iceberg table to write data."
            )

        with profile("partition_spec_validation_ms", self._stats):
            self._table._validate_partition_spec_write(self._table._partition_spec)

        with profile("metaflowdataframe_validation_ms", self._stats):
            iceberg_schema = list(self._table._schema.values())
            validate_iceberg_mdf(mdf, iceberg_schema)

        data_path = self._table.data_path

        mdfs = mdf.split()

        with profile("generate_partition_tuple_ms", self._stats):
            try:
                partition_tuples = [self._generate_partition_tuple(m) for m in mdfs]
            except MetaflowTableIncompatible as e:
                raise MetaflowTableIncompatible(
                    "Cannot generate partition information for this dataframe "
                    "because it has inconsistent partition "
                    "values. MetaflowDataframes are made of "
                    "multiple parts (e.g. row groups). When putting data in "
                    "the warehouse, the MetaflowDataFrame must have the same "
                    "partition values in each part. Please create a separate "
                    "dataframe for each partition value. You can stack "
                    "multiple dataframes with "
                    "`MetaflowDataFrame.stack(list of mdf)` and call `.put`, or "
                    "call `.put` on each MetaflowDataframe separately"
                )

        with tempfile.TemporaryDirectory(dir="./", prefix="table-parquet-") as td:
            n_mdfs = len(mdfs)

            def _write_parquet(index):
                uid = str(uuid.uuid4())
                template = "%s-part-{index}-metaflow.parquet" % uid
                local_path = os.path.join(td, template)
                result = mdfs[index].to_parquet_set(
                    local_path,
                    iceberg_schema=iceberg_schema,
                    max_file_size=max_file_size,
                    return_num_rows=True,
                )
                return result

            max_parallel = DEFAULT_WRITE_THREADS
            if num_threads:
                max_parallel = num_threads

            with profile("write_parquet_ms", self._stats):
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_parallel
                ) as exc:
                    result = exc.map(_write_parquet, range(n_mdfs))
            parquet_fnames, row_counts = zip(*result)

            local_fnames = [[os.path.basename(p) for p in pq] for pq in parquet_fnames]
            remote_paths = [
                [os.path.join(data_path, l) for l in lf] for lf in local_fnames
            ]
            parquet_sizes = [[os.stat(l).st_size for l in lp] for lp in parquet_fnames]
            with profile("move_parquet_to_s3_ms", self._stats):
                with S3(session_vars=self._s3_config) as s3:
                    s3.put_files(
                        list(
                            zip(
                                chain.from_iterable(remote_paths),
                                chain.from_iterable(parquet_fnames),
                            )
                        )
                    )

            for remote_path, row_count, parquet_size, partition_tuple in zip(
                remote_paths, row_counts, parquet_sizes, partition_tuples
            ):
                for rp, rc, s in zip(remote_path, row_count, parquet_size):
                    data_files.append(
                        DataFile(
                            status=DataFileStatus.ADDED,
                            file_path=rp,
                            partition=partition_tuple,
                            record_count=rc,
                            file_size_in_bytes=s,
                        )
                    )

        return data_files

    def _try_commit(self, data_files, overwrite=False):
        """
        Try to commit the table. May fail if concurrent writes conflict.
        """
        # Get writer configuration (WAP info in Netflix; default WriterInfo in OSS)
        writer_info = self._catalog_obj.get_writer_info()

        # Create iceberg metadata files and put them in S3
        metadata_files = self._table.put_partition_paths(
            data_files, overwrite_partitions=overwrite, writer_info=writer_info
        )

        # Update the catalog metadata pointer
        self._catalog_obj.update_metadata_location(
            self._db,
            self._table_name,
            self._info.catalog_name,
            metadata_files["metadata"],
            previous_metadata_location=self._info.metadata_location or None,
        )

        # Reload cached metadata_location from the updated TableInfo
        self._info = self._catalog_obj.get_table_info(self._db, self._table_name)

        # Publish lineage write event (no-op in OSS)
        with profile("lineage_publish_time_ms", self._stats):
            self._catalog_obj.publish_write_event()

    def _commit_confirmed_successful(self, metadata_file_path):
        """
        Check if the commit was successful by checking if our metadata file is the
        current one or if it is in the metadata log.
        """
        reloaded_table = Table(
            self._db,
            self._table_name,
            catalog=self._catalog_obj,
            catalog_name=self._catalog_name,
        )
        assert reloaded_table.is_iceberg

        if reloaded_table._table.metadata_path == metadata_file_path:
            return True

        return any(
            entry.file == metadata_file_path
            for entry in reloaded_table._table.metadata_log
        )

    def _exponential_backoff_commit(self, data_files, overwrite=False):
        """
        Try to commit with exponential backoff on conflict/rate-limit errors.
        """

        def debug_(string, attempt, conflict, rate_limit):
            debug.table_exec(
                string
                + " (attempt=%d, conflict=%d, rate_limit=%d)"
                % (attempt, conflict, rate_limit)
            )

        MAX_ATTEMPTS = 7
        MAX_CONFLICTS = 100
        MAX_RATE_LIMIT_ATTEMPTS = 100
        attempt = 0
        conflict = 0
        rate_limit = 0
        for _ in range(MAX_ATTEMPTS + MAX_CONFLICTS + MAX_RATE_LIMIT_ATTEMPTS):
            reload_ = False
            has_conflict = False
            has_rate_limit = False

            try:
                debug_("Trying commit", attempt, conflict, rate_limit)
                with profile("try_commit_metadata_ms", self._stats):
                    self._try_commit(data_files, overwrite=overwrite)

                debug_("Commit succeeded", attempt, conflict, rate_limit)
                break

            except MetaflowTableNotFound:
                debug_("Table not found", attempt, conflict, rate_limit)
                raise

            except MetaflowTableExists:
                debug_("Table exists", attempt, conflict, rate_limit)
                if attempt == MAX_ATTEMPTS - 1:
                    raise
                attempt += 1
                reload_ = True

            except MetaflowTableConflict:
                debug_("Table metadata conflict", attempt, conflict, rate_limit)
                if conflict == MAX_CONFLICTS - 1:
                    raise
                conflict += 1
                has_conflict = True
                reload_ = True

            except MetaflowTableRateLimitException:
                debug_("Rate limit exceeded", attempt, conflict, rate_limit)
                if rate_limit == MAX_RATE_LIMIT_ATTEMPTS - 1:
                    raise
                rate_limit += 1
                has_rate_limit = True
                reload_ = True

            except MetaflowTableException:
                if attempt == MAX_ATTEMPTS - 1:
                    raise
                attempt += 1

            except MetaflowIcebergDuplicateDataFiles:
                raise

            except MetaflowTableCommitStateUnknownException as ex:
                debug_("Got unknown commit state", attempt, conflict, rate_limit)
                if self._commit_confirmed_successful(
                    # pull metadata path from the last IcebergTable state
                    getattr(self._table, "metadata_path", "")
                ):
                    debug_(
                        "Confirmed the previous commit was successful.",
                        attempt,
                        conflict,
                        rate_limit,
                    )
                    break
                raise MetaflowTableException(
                    "We tried to commit the Iceberg table change to the catalog, "
                    "but couldn't determine whether the commit was successful. "
                    "We didn't retry the operation because it may not be safe to retry. "
                    "Please check the table manually to see if your commit is successful."
                ) from ex

            except:
                if attempt == MAX_ATTEMPTS - 1:
                    raise
                attempt += 1

            if has_rate_limit:
                sleep_time = min(600, max(60, 2.0**rate_limit))
                sleep_time += random.uniform(0, sleep_time * 0.50)
            else:
                sleep_time = min(60, 1.5**conflict if has_conflict else attempt) - 0.5
                sleep_time += random.uniform(-sleep_time / 2.0, sleep_time / 2.0)
            debug_("sleep on retry: %s" % sleep_time, attempt, conflict, rate_limit)
            time.sleep(sleep_time)
            if reload_:
                debug_("Reload table metadata", attempt, conflict, rate_limit)
                self._reload()

        # Reload with newly committed metadata
        with profile("reload_iceberg_metadata_ms", self._stats):
            self._reload()

    def put_data(
        self,
        mdf: MetaflowDataFrame,
        num_threads: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Write the input MetaflowDataFrame to parquet file(s) in S3 and return a list
        of objects that can be passed to the `commit` call.

        This method is only supported on Iceberg tables.
        """
        if not len(mdf):
            raise MetaflowTableInvalidData(
                "Cannot put an empty MetaflowDataframe into a table"
            )

        data_files_list_of_namedtuple = self._put_data(mdf, num_threads=num_threads)
        data_files = [
            self._table._iceberg._ntuple_to_dict(x)
            for x in data_files_list_of_namedtuple
        ]
        return data_files

    def commit(self, data_files: List[Dict[str, Any]], overwrite: bool = False):
        """
        Commit data to iceberg table using the list previously returned by
        `put_data`.

        This method is only supported on Iceberg tables.
        """
        data_files_list_of_namedtuple = [
            self._table._iceberg.data_file_from_kwargs(**x) for x in data_files
        ]
        self._exponential_backoff_commit(
            data_files_list_of_namedtuple, overwrite=overwrite
        )

    def put(
        self,
        mdf: MetaflowDataFrame,
        overwrite: bool = False,
        num_threads: Optional[int] = None,
    ):
        """
        Convenience method equivalent to put_data() with commit().

        This method is only supported on Iceberg tables.
        """
        data_files = self.put_data(mdf, num_threads=num_threads)
        self._exponential_backoff_commit(
            [self._table._iceberg.data_file_from_kwargs(**x) for x in data_files],
            overwrite=overwrite,
        )
