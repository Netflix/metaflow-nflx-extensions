# Only supporting python 3
import sys

if sys.version_info[0] < 3:
    raise ImportError("Iceberg tables are not supported in python < 3")

from collections import defaultdict, OrderedDict
import os
import json
import struct
import uuid
import tempfile
import shutil
from itertools import chain
from datetime import date, datetime

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from metaflow.util import is_stringish
from metaflow.plugins.datatools.s3 import S3
from metaflow.metaflow_profile import profile
from ..datatools.type_info import (
    ALLOWED_ICEBERG_COLUMN_PRIMITIVE_TYPES,
    ALLOWED_ICEBERG_COLUMN_NESTED_TYPES,
    ALLOWED_ICEBERG_PARTITION_TYPES,
    ALLOWED_ICEBERG_PARTITION_TRANSFORMS,
    ALLOWED_ICEBERG_PARTITION_TRANSFORMS_WRITE,
    is_valid_iceberg_type,
    get_iceberg_field_type_prefix,
)

from .utils import format_set

from .exception import (
    MetaflowTableException,
    MetaflowTableIncompatible,
    MetaflowTableEmptyPartition,
    MetaflowTableEmptyTable,
    MetaflowTableInvalidData,
)
import math

from metaflow.plugins.datatools.iceberg import (
    FieldSummary,
    DataFile,
    DataFileStatus,
    update_data_file_status,
)


def pack_bool(val):
    return b"\x00" if not val else b"\x01"


def pack_int(val):
    """
    '<i' for 4-byte int in little endian
    """
    return struct.pack("<i", val)


def pack_long(val):
    """
    '<q' for 8-byte long in little endian
    """
    return struct.pack("<q", val)


def pack_string(val):
    """
    Convert string to UTF-8
    """
    return val.encode("utf-8")


def pack_binary(val):
    """
    No-op
    """
    return val


def unpack_bool(b):
    """
    Return False if hex is hexadecimal 00 (Null) else True. Iceberg spec is that non
    zero is true.
    """
    return b != b"\x00"


def unpack_int(b):
    """
    Unpack bytes representing an int into a python integer
    """
    return struct.unpack("<l", b)[0]


def unpack_long(b):
    """
    Unpack bytes representing a long into a python integer
    """
    return struct.unpack("<q", b)[0]


def unpack_string(b):
    """
    Decode bytes into a python string
    """
    return b.decode("utf-8")


def unpack_binary(b):
    """
    No-op
    """
    return b


def _is_s3_path(path):
    return path.startswith("s3:") or path.startswith("s3n:")


def _sanitize_s3(path):
    return path.replace("s3n:", "s3:")


@dataclass
class MetadataLogEntry:
    """
    A data class to represent the Iceberg metadata log entry.

    Attributes:
        file (str): The metadata file location.
        timestampMillis (int): The timestamp.
    """

    file: str
    timestampMillis: int


class IcebergTable(object):
    """
    Interface to iceberg table metadata and partition grain search planning
    """

    # A map of functions for decoding partition stats.
    STATS_DECODERS = {
        "boolean": unpack_bool,
        "int": unpack_int,
        "long": unpack_long,
        "string": unpack_string,
        "binary": unpack_binary,
    }

    # A map of functions for encoding partition stats.
    STATS_ENCODERS = {
        "boolean": pack_bool,
        "int": pack_int,
        "long": pack_long,
        "string": pack_string,
        "binary": pack_binary,
    }

    def validate_schema(self, columns=None):
        """
        Validates the schema fields to ensure the columns have supported types
        and raises an exception with the list of unsupported columns.

        Restricts the checks to `columns` if specified.
        """
        col_set = None
        if columns:
            col_set = set(columns)

        unsupported = []
        for field in self._schema.values():
            name = field["name"]
            if col_set is None or name in col_set:
                valid = is_valid_iceberg_type(field)
                if not valid:
                    unsupported.append(name)

        if unsupported:
            raise MetaflowTableIncompatible(
                "Columns %s have unsupported types. Only %s column types "
                "are supported."
                % (
                    format_set(unsupported),
                    format_set(
                        ALLOWED_ICEBERG_COLUMN_PRIMITIVE_TYPES,
                        ALLOWED_ICEBERG_COLUMN_NESTED_TYPES,
                    ),
                )
            )

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
                "columns in the tables schema." % format_set(set(missing))
            )

    def _validate_partition_spec_write(self, partition_spec):
        # Validate partition transforms for write
        mismatched_parts = []
        mismatched_transforms = []
        for field in partition_spec:
            transform = field["transform"]
            if transform not in ALLOWED_ICEBERG_PARTITION_TRANSFORMS_WRITE:
                mismatched_parts.append(field["name"])
                mismatched_transforms.append(field["transform"])
        if mismatched_parts:
            raise MetaflowTableIncompatible(
                "Partitions %s have transforms %s that are unsupported in iceberg write. "
                "Only %s partition transforms are supported."
                % (
                    format_set(mismatched_parts),
                    format_set(mismatched_transforms),
                    format_set(ALLOWED_ICEBERG_PARTITION_TRANSFORMS_WRITE),
                )
            )

    def _validate_partition_spec(self, partition_spec):
        # Validate partition transforms.
        mismatched_parts = []
        for field in partition_spec:
            transform = field["transform"]
            # Skip dropped partitions (marked with void transform)
            if transform == "void":
                continue
            # `int` is the most common result type across all transforms.
            field["type"] = "int"
            if transform.startswith("truncate") or transform == "identity":
                # These preserve source type(s).
                field["type"] = self._schema[field["source-id"]]["type"].lower()
            elif (
                transform.startswith("bucket")
                or transform == "day"
                or transform == "year"
                or transform == "month"
                or transform == "hour"
            ):
                # We choose to store `day` as int instead of `date` (as per
                # spec) since its more consistent with the other fields - year,
                # month etc. - all of which have delta encoded (int) stats.
                pass
            else:
                mismatched_parts.append(field["name"])
        if mismatched_parts:
            raise MetaflowTableIncompatible(
                "Partitions %s have transforms that are unsupported. "
                "Only %s partition transforms are supported."
                % (
                    format_set(mismatched_parts),
                    format_set(ALLOWED_ICEBERG_PARTITION_TRANSFORMS),
                )
            )

        # Validate partition (result) types.
        mismatched_parts = [
            field["name"]
            for field in partition_spec
            if field["type"].lower() not in ALLOWED_ICEBERG_PARTITION_TYPES
        ]
        if mismatched_parts:
            raise MetaflowTableIncompatible(
                "Partitions %s have partition types that are unsupported. "
                "Only %s partition types are supported."
                % (
                    format_set(mismatched_parts),
                    format_set(ALLOWED_ICEBERG_PARTITION_TYPES),
                )
            )

    def _validate_partition_value_types(self, partition_spec):
        for part in self._partition_spec:
            name = part["name"]
            if name not in partition_spec:
                # Partial spec is specified.
                continue
            val = partition_spec[name]
            typ = part["type"]

            mismatch = False
            # Code below is expected to be in sync with
            # `_validate_partition_spec`.
            # Allow val to be `None` for null value partitions.
            if val is not None:
                # NOTE: This must be updated whenever there are newer entries in
                # `ALLOWED_ICEBERG_PARTITION_TYPES`.
                if typ == "boolean" and not isinstance(val, bool):
                    mismatch = True
                elif typ == "int" and not isinstance(val, int):
                    mismatch = True
                # note - py2 supports long so this code is py3 only
                elif typ == "long" and not isinstance(val, int):
                    mismatch = True
                elif typ == "string" and not isinstance(val, str):
                    mismatch = True
                elif typ == "binary" and not isinstance(val, bytes):
                    mismatch = True

            if mismatch:
                raise MetaflowTableIncompatible(
                    "Expected value of type '%s' for partition column '%s' but "
                    "found type '%s'." % (typ, name, type(val))
                )

    def validate_iceberg_schema_rename_compatibility(self, old_schema, new_schema):
        # First find new schema items that are also in the old schema, its
        # ok to add and drop high level items, and transform types but we
        # can't change struct elements.
        for fid, n_schema in new_schema.items():
            o_schema = old_schema.get(fid, None)
            if o_schema is None:
                continue

            if o_schema["name"] != n_schema["name"]:
                raise MetaflowTableIncompatible(
                    "This table has undergone an incompatible schema "
                    "evolution for column '%s'. Renaming of columns "
                    "is currently not supported." % n_schema["name"]
                )

    def validate_iceberg_schema_evolution_compatibility(self, old_schema, new_schema):
        def _build_path_string(iceberg_type):
            # must be a primitive type at this point
            if is_stringish(iceberg_type):
                typ = get_iceberg_field_type_prefix(iceberg_type).lower()
                return ""

            # must be a nested type
            typ = iceberg_type["type"]
            if isinstance(typ, dict):
                return _build_path_string(typ)

            if typ == "list":
                elem_typ = iceberg_type["element"]
                subpath = _build_path_string(elem_typ)
                return "list." + subpath if subpath else "list"

            elif typ == "struct":
                fields = []
                for field in iceberg_type["fields"]:
                    fields.append(_build_path_string(field))
                return "struct." + ".".join(fields)

            elif typ == "map":
                k = _build_path_string(iceberg_type["key"])
                v = _build_path_string(iceberg_type["value"])
                ret = "map"
                if k:
                    ret += "." + k
                if v:
                    ret += "." + v
                return ret

            return _build_path_string(typ)

        # First find new schema items that are also in the old schema, its
        # ok to add and drop high level items, and transform types but we
        # can't change struct elements.
        for fid, n_schema in new_schema.items():
            o_schema = old_schema.get(fid, None)
            if o_schema is None:
                continue

            # ok we have a match, lets check recursively if it matches the
            # struct types order and name, since those can't change. Yet
            # types can be cast.
            n_path_str = _build_path_string(n_schema)
            o_path_str = _build_path_string(o_schema)

            if n_path_str != o_path_str:
                raise MetaflowTableIncompatible(
                    "This table has undergone an incompatible schema evolution "
                    "for column '%s'. Manipulation of netsed types is not "
                    "allowed except for type casting." % n_schema["name"]
                )

    def _find_manifest(self, partition_spec):
        """
        Linearly scan stats to find manifest files that might have data for
        the partition spec.

        TODO can we end as soon as we find a file and return a string instead
        of a list? We can check the guarantee of the writer.
        """
        manifest_files = set()
        for manifest, stats, spec in zip(
            self._manifests, self._manifest_stats, self._partition_specs
        ):
            # Make sure the provided partition spec is valid for this
            # manifests partition spec
            for name in partition_spec:
                if not any(s["name"] == name for s in spec):
                    raise MetaflowTableIncompatible(
                        "Cannot satisfy the partition specification "
                        "'%s' across all data files because partition "
                        "'%s' cannot be found in all partition specifications. "
                        "The reason is that the table has undergone partition "
                        "evolution. Please use a partition specification that "
                        "is common to all data files or perform a CTAS to a "
                        "temporary table." % (partition_spec, name)
                    )

            # Consider missing columns non-constraining dimension during bounds
            # checking
            vb_pairs = (
                (partition_spec[col], bounds)
                for col, bounds in zip(self.partition_columns, stats)
                if col in partition_spec
            )

            for value, bounds in vb_pairs:
                if value is None:
                    # For null partition values ignore the bounds, the first
                    # field is a boolean indicating whether the manifest
                    # contains any null for this partition field.
                    if bounds[0] == False:
                        break
                else:
                    # For non-null partition values, skip it if we only contain
                    # nulls otherwise check the bounds.
                    if bounds[1] is None and bounds[2] is None:
                        break

                    # For booleans check the two states
                    elif isinstance(value, bool):
                        if not ((value == bounds[1]) or (value == bounds[2])):
                            break

                    # Otherwise consider lexographic ordering
                    elif (value < bounds[1]) or (value > bounds[2]):
                        break
            else:
                manifest_files.add(manifest)

        return manifest_files

    def _get_datafiles_from_manifests(self, manifest_files):
        """
        Return DataFile objects for each row of all manifests

        Returns
        -------
        List[DataFile]
            A list of list DataFile objects
        """

        def _parse_manifest(manifest_path):
            data_files = []
            reader = self._iceberg.manifest_file_reader(manifest_path)
            metadata, manifest_reader = reader
            for row in manifest_reader:
                # Skip deleted files
                if row["status"] == DataFileStatus.DELETED.value:
                    continue
                data_files.append(
                    DataFile(
                        status=DataFileStatus(row["status"]),
                        file_path=row["file_path"],
                        partition=row["partition"],
                        record_count=row["record_count"],
                        file_size_in_bytes=row["file_size_in_bytes"],
                    )
                )
            return data_files

        if len(manifest_files) == 0:
            return list()

        data_files = []
        if _is_s3_path(next(iter(manifest_files))):
            files = []
            with S3(session_vars=self._session_vars) as s3:
                paths = []
                # Download manifest contents.
                with profile("manifest_download_ms", self._stats):
                    paths = [s3obj.path for s3obj in s3.get_many(manifest_files)]

                # Parse manifest files and return data files
                with profile("manifest_read_data_files_ms", self._stats):
                    for path in paths:
                        data_files.extend(_parse_manifest(path))
        else:  # local files
            # Parse manifest files and return data files
            for path in manifest_files:
                data_files.extend(_parse_manifest(path))

        return data_files

    def _group_parquet_files(self, files, group_by, num_groups, target_group_size):
        """
        Group parquet files based on group_by criteria.

        Returns
        -------
        List[List[str]]
            A list of list of parquet file paths
        """
        if group_by == "record":
            size_idx = 1
        elif group_by == "size":
            size_idx = 2
            if target_group_size is not None:
                # Convert MB to byte
                target_group_size = target_group_size * 1048576
                print(
                    "Target group size after converting to bytes: ", target_group_size
                )
        else:
            raise MetaflowTableException("Unknown group_by %s" % str(group_by))

        total_size = sum([f[size_idx] for f in files])

        # Fast path for no grouping
        # Note: returns 1 group of len(files)
        if num_groups is None and target_group_size is None:
            return [[f[0] for f in sorted(files, key=lambda x: x[0])]]

        # Calculate num_group using target size
        if num_groups is None and target_group_size > 0:
            # Use ceil to make sure the num_groups is at least 1
            num_groups = math.ceil(total_size / target_group_size)

        # Fast path for only one group
        # Note: returns 1 group of len(files)
        if num_groups == 1:
            return [[f[0] for f in sorted(files, key=lambda x: x[0])]]

        # Fast path for too few files
        # Note: returns len(files) groups of 1
        if len(files) <= num_groups:
            print(
                "The number of files to be read is smaller than the number of groups. "
                "Please check your input to see if num_group is too big or "
                "target_group_size is too small. If group_by is size, "
                "the unit of target_group_size is MB."
            )
            sort_files = sorted(files, key=lambda x: x[0])
            return [[f[0]] for f in sort_files]

        # Please refer to
        # [Multiway number partitioning](https://en.wikipedia.org/wiki/Multiway_number_partitioning)
        # problem for mathematical analysis. The greedy implementation here should reach
        # a max partition size <= 4/3 of optimal. We could consider switch to a dynamic
        # programming implementation or use existed oss packages for the problem.

        # Initialize groups

        groups = [[] for _ in range(num_groups)]
        group_sizes = [0] * num_groups

        files.sort(key=lambda x: x[size_idx], reverse=True)
        # Assign files to shards greedily

        for file in files:
            smallest_shard_index = min(range(num_groups), key=lambda i: group_sizes[i])
            groups[smallest_shard_index].append(file[0])
            group_sizes[smallest_shard_index] += file[size_idx]

        return groups

    def _get_partition_files_from_manifest(
        self, manifest_path, partition_values, partial_partitions
    ):
        """
        Parse the manifest and return files belonging to a partition that
        is either present in `partition_values` or match the spec in
        `partial_partitions`.

        Returns:
        -------
            A list of tuples includes parquet file path, record count and file size
        """
        # Read the manifest file
        reader = self._iceberg.manifest_file_reader(manifest_path)
        metadata, manifest_reader = reader

        # Check if the manifest file has a different schema and if is
        # a valid evolution to the current schema.
        file_schema = json.loads(metadata["schema"])
        file_schema = OrderedDict(
            (field["id"], field) for field in file_schema["fields"]
        )
        file_partition_spec = json.loads(metadata["partition-spec"])
        self.validate_iceberg_schema_evolution_compatibility(file_schema, self._schema)

        # Specifically check for top level renaming, which isn't
        # allowed in these tables. Note - we use a separate function
        # that above because we may relax this in the future
        self.validate_iceberg_schema_rename_compatibility(file_schema, self._schema)

        files = []
        for row in manifest_reader:
            # Skip deleted files
            if row["status"] == DataFileStatus.DELETED.value:
                continue

            # Check for files that match
            partition_info = row["partition"]
            partition_value = tuple(
                [partition_info[part["name"]] for part in file_partition_spec]
            )
            parquet_file = _sanitize_s3(row["file_path"])
            record_count = row["record_count"]
            file_size = row["file_size_in_bytes"]
            if partition_value in partition_values:
                files.append((parquet_file, record_count, file_size))
            else:
                # Check for partial match.
                for partition_spec in partial_partitions:
                    # Iterate over all columns in canonical order.
                    match = [
                        partition_spec[name] == partition_info[name]
                        for name in self.partition_columns
                        if name in partition_spec
                    ]
                    if all(match):
                        # Matches this partial spec.
                        files.append((parquet_file, record_count, file_size))
                        break
        return files

    def _get_partition_tuples_from_manifest(self, manifest_path):
        """
        Parse the manifest and return a list of partitions
        """
        # Read the manifest file
        reader = self._iceberg.manifest_file_reader(manifest_path)
        metadata, manifest_reader = reader

        for row in manifest_reader:
            # Skip deleted files
            if row["status"] == DataFileStatus.DELETED.value:
                continue

            partition_info = row["partition"]
            partition_value = {
                part["name"]: partition_info[part["name"]]
                for part in self._partition_spec
            }
            yield partition_value

    def _current_summary(self):
        snapshots = self._metadata["snapshots"]
        for snapshot in snapshots:
            if self._current_snapshot == snapshot["snapshot-id"]:
                return snapshot["summary"]
        return None

    def _get_manifest_list_contents(self):
        if self._manifest_list_file is None:
            # Empty table
            return []

        if _is_s3_path(self._manifest_list_file):
            with S3(session_vars=self._session_vars) as s3:
                path = s3.get(self._manifest_list_file).path
                return self._iceberg.read_manifest_list(path)
        else:
            return self._iceberg.read_manifest_list(self._manifest_list_file)

    def __init__(
        self,
        metadata_path: str,
        stats: Optional[Dict[str, Any]] = None,
        allow_empty: bool = False,
        strict_partition_evolution: bool = False,
        **kwargs,
    ):
        """
        Initializes an IcebergTable from an S3 path to a iceberg metadata file

        Parameters
        ----------
        metadata_path : str
            S3 location of a metadata json file
        stats : Dict[str, Any], optional, default None
            A dictionary to collect performance stats
        allow_empty : bool, default False
            If True do not raise exceptions for empty tables
        strict_partition_evolution : bool, default False
            If True fail early on all partition evolution
        """
        # This will lazy import metaflow-data for iceberg manifest parsing
        from metaflow.plugins.datatools import iceberg

        self._iceberg = iceberg

        self._session_vars = kwargs.get("session_vars", None)

        # Profiling stats
        self._stats = {} if stats is None else stats

        # If true, we don't allow tables with partition evolution to
        # be loaded. If False, we check only on the result of the scan
        # planning.
        self._strict_partition_evolution = strict_partition_evolution

        # Get metadata
        self._metadata_path = metadata_path
        if _is_s3_path(self._metadata_path):
            with S3(session_vars=self._session_vars) as s3:
                blob = s3.get(self._metadata_path).text
                self._metadata = json.loads(blob)
        else:
            with open(self._metadata_path) as fh:
                self._metadata = json.load(fh)

        # Iceberg v1 supported until iceberg v2 is approved. There shouldn't be
        # many differences for this limited client.
        if self._metadata["format-version"] != 1:
            raise MetaflowTableIncompatible("Only iceberg format 'v1' is supported.")

        self._metadata_log = [
            MetadataLogEntry(entry["metadata-file"], entry["timestamp-ms"])
            for entry in self._metadata.get("metadata-log", [])
        ]

        # Check if current_snapshot_id is the same as main branch
        if "refs" in self._metadata and self._metadata["refs"]:
            try:
                if (
                    self._metadata["refs"]["main"]["snapshot-id"]
                    != self._metadata["current-snapshot-id"]
                ):
                    raise MetaflowTableIncompatible(
                        "The snapshot id of main branch does not match current snapshot id.\n"
                        "Expected %s, got %s"
                        % (
                            self._metadata["current-snapshot-id"],
                            self._metadata["refs"]["main"]["snapshot-id"],
                        )
                    )
            except KeyError:
                raise MetaflowTableIncompatible(
                    "Could not find main branch information in the "
                    "'ref' field of the metadata. The writer of this "
                    "table has created an invalid metadata file."
                )

        # Grab the table base location so writers know where to put files
        if _is_s3_path(self._metadata_path):
            self._location = _sanitize_s3(self._metadata["location"])
        else:
            self._location = self._metadata["location"]

        # If location is '<LOCAL>' is a special mode for testing, set it to the
        # path of the metadata file.
        if self._location == "<LOCAL>":
            self._location = os.path.split(os.path.split(self._metadata_path)[0])[0]

        self._metadata_location = os.path.join(self._location, "metadata")
        self._data_location = os.path.join(self._location, "data")

        try:
            # The spec has these as optional but we require them so no scanning
            # is performed. In practice they are always present and required
            # in V2.
            self._current_snapshot = self._metadata["current-snapshot-id"]
            snapshots = self._metadata["snapshots"]
        except KeyError as e:
            raise MetaflowTableIncompatible(
                "Snapshot information is required in the tables metadata."
            )

        try:
            # Store the partition spec information. An optional field but in
            # practice its always present.
            self._all_partition_specs = self._metadata["partition-specs"]

            # Store the partition spec id information. An optional field but in
            # practice it's always present.
            self._default_spec_id = self._metadata["default-spec-id"]
        except KeyError as e:
            try:
                # its possible very old metadata tables could use this
                # deprecated field
                self._all_partition_specs = [self._metadata["partition-spec"]]

                self._default_spec_id = 0
            except KeyError as k:
                raise MetaflowTableIncompatible(
                    "A partition specification is required in the tables metadata."
                )

        # Set the default partition spec as that from the metadata default
        self._partition_spec = self._all_partition_specs[self._default_spec_id][
            "fields"
        ]

        try:
            # We should have some top level fields for the schema, very old tables
            # may have them in a deprecated place.

            # The spec has these as optional but in practice they are always
            # present and required in V2
            self._schemas = self._metadata["schemas"]
            self._default_schema_id = self._metadata["current-schema-id"]
        except KeyError as e:
            try:
                # it's possible very old metadata tables could use this
                # deprecated field
                self._schemas = [self._metadata["schema"]]
                self._default_schema_id = 0
            except KeyError as k:
                raise MetaflowTableIncompatible(
                    "A default partition specification is required in the table metadata."
                )

        # See if we have schema evolution
        self._has_schema_evolution = len(self._schemas) > 1

        # Index by the field id, but keep ordered.
        self._schema = OrderedDict(
            (field["id"], field)
            for field in self._schemas[self._default_schema_id]["fields"]
        )

        # Inject source-id type into all specs, and if the source-id
        # isn't in the current schema just throw an exception since
        # its not clear how to handle that case.
        for spec in self._all_partition_specs:
            fields = spec["fields"]
            for field in fields:
                if "source-id" in field and not "type" in field:
                    source_field = self._schema.get(field["source-id"], None)
                    if source_field is not None:
                        field["type"] = self._schema[field["source-id"]]["type"]
                    else:
                        raise MetaflowTableIncompatible(
                            "The partition field '%s' is not present "
                            "in the current schema. This table has undergone "
                            "unsupported evolution. The partition columns must "
                            "be present in the current schema." % field["name"]
                        )

        # We need to store manifest file names and stats about them
        self._manifests = []
        self._manifest_stats: List[Any] = []

        # Find and process the current snapshot, we don't allow for historical
        # snapshot selection as of today.
        manifest_list_file = None
        for snapshot in snapshots:
            if self._current_snapshot == snapshot["snapshot-id"]:
                try:
                    # It's possible that this key is missing, and
                    # 'manifest' contains the manifest list locations directly
                    # in the metadata. This has however been deprecated and
                    # isn't seen in practice in the examples.
                    manifest_list_file = snapshot["manifest-list"]
                except KeyError as e:
                    raise MetaflowTableIncompatible(
                        "Iceberg tables without a manifest list file "
                        "are not supported."
                    )
                break
        else:
            if not allow_empty:
                # This can occur if you run a SQL query to create a table; but not
                # insert any data into it.
                raise MetaflowTableEmptyTable(
                    f"The metadata file '{metadata_path}' contains no data "
                    "snapshots. The table is empty."
                )

        # Parse the manifest list which contains the locations of the manifest
        # files
        manifest_stats = []
        partition_spec_ids = []
        self._manifest_list_file = None
        if manifest_list_file is not None:
            manifest_list_file = _sanitize_s3(manifest_list_file)
            self._manifest_list_file = manifest_list_file
            if _is_s3_path(manifest_list_file):
                with S3(session_vars=self._session_vars) as s3:
                    path = s3.get(manifest_list_file).path
                    with profile("manifest_list_parse_ms", self._stats):
                        reader = self._iceberg.manifest_list_reader(path)
            else:
                with profile("manifest_list_parse_ms", self._stats):
                    reader = self._iceberg.manifest_list_reader(manifest_list_file)

            metadata, manifest_list_reader = reader
            for row in manifest_list_reader:
                path = _sanitize_s3(row["manifest_path"])
                self._manifests.append(path)
                manifest_stats.append(row["partitions"])
                partition_spec_ids.append(row["partition_spec_id"])

        # Raise an exception if we have no data
        if not self._manifests and not allow_empty:
            raise MetaflowTableEmptyTable(
                f"The manifest list file ('{manifest_list_file}') contains no entries. The table is empty."
            )

        self._has_partition_evolution = not all(
            [self._default_spec_id == spec_id for spec_id in partition_spec_ids]
        )
        if self._strict_partition_evolution:
            # Check that all the partition specs ids are the same and match the
            # current id. If not, the table, for at least some partitions, has
            # evolved. Alternatively this logic could be applied after partition
            # filtering, but that means that some partition requests may succeed
            # while others fail.
            if self._has_partition_evolution:
                raise MetaflowTableIncompatible(
                    "The table has undergone partition evolution and is not compatible"
                )

        # Decode partition stats
        self._manifest_stats = []
        self._partition_specs = []
        with profile("manifest_stats_ms", self._stats):
            for manifest_stat, partition_spec_id in zip(
                manifest_stats, partition_spec_ids
            ):
                # make sure we support this specs transform
                partition_spec = self._all_partition_specs[partition_spec_id]["fields"]
                self._validate_partition_spec(partition_spec)

                # Store in same order
                self._partition_specs.append(partition_spec)

                # Compute partition specific stats
                bounds = []
                for stat, field in zip(manifest_stat, partition_spec):
                    decoder = self.STATS_DECODERS[field["type"]]
                    # These bounds are delta encoded (e.g. days since 1970-1-1
                    # for `day` transform) and stored as ints.
                    lb = None if stat[1] is None else decoder(bytes.fromhex(stat[1]))
                    ub = None if stat[2] is None else decoder(bytes.fromhex(stat[2]))
                    bounds.append((stat[0], lb, ub))
                self._manifest_stats.append(tuple(bounds))

    @property
    def metadata_log(self):
        """
        Returns the metadata log for the table.
        """
        return self._metadata_log

    @property
    def stats(self):
        """
        Returns profiling statistics
        """
        return self._stats

    @property
    def metadata_path(self):
        """
        Returns path to the metadata file for the table.
        """
        return self._metadata_path

    @property
    def data_path(self):
        return self._data_location

    @property
    def has_schema_evolution(self):
        return self._has_schema_evolution

    @property
    def has_partition_evolution(self):
        return self._has_partition_evolution

    @property
    def partition_columns(self):
        """
        Return the partition columns.
        """
        return [p["name"] for p in self._partition_spec]

    @property
    def metadata(self):
        """
        Return the table metadata

        Returns:
            A Dict that represent the table metadata JSON

        """
        return self._metadata

    @property
    def schema(self):
        """
        Return the schema

        Returns:
            A list of dicts of column schemas
        """
        return list(self._schema.values())

    @property
    def current_snapshot(self):
        """
        Return the current snapshot metadata
        """
        return self._current_snapshot

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
            The criteria to group parquet files by. The supported modes are:
              - 'record': group parquet files based on the number of records
              - 'size': group parquet files based on their size in bytes. Note that the
                size used is the *compressed* size of the parquet files which may be
                significantly smaller than the uncompressed size once loaded into memory.
        num_groups : int, optional, default None
            Number of approximately equal-sized groups to form.
            Note that if num_groups is greater than the number of files (let's say N)
            available for this table, this function will return N groups with one file
            in each group. This cannot be used in combination with `target_group_size`.
            If neither `num_groups` or `target_group_size` is specified, this function
            assumes `num_groups=1`.
        target_group_size: int, optional, default None
            Target size of each approximately equal-size group to form.
            - If group_by=record, target_group_size is the number of rows per group.
            - If group_by=size, target_group_size is the total size of all files in
              each group in MB.
            - If target_group_size is greater than the total number of rows available
              for this table (if group_by=record) or the total size of the files
              available of this table (if group_by=size), this function will return
              1 group.
            This cannot be used in combination with `num_groups`. If neither `num_groups`
            or `target_group_size` is specified, this function assumes `num_groups=1`.

        Returns
        -------
        List[List[str]]
            A list of list of parquet file paths
        """
        # If partition_specs is empty list, it probably wasn't intended
        if not partition_specs:
            raise MetaflowTableEmptyPartition(
                "'partition_specs' is an empty "
                "list, but its expected to be a list of partition tuples, "
                "e.g. [{'country':'US'}, {'country':'CAN'}]"
            )

        if num_groups is not None and target_group_size is not None:
            raise MetaflowTableException(
                "Expect getting num_groups or target_group_size but got both: "
                "\n num_groups: %s, target_group_size: %s"
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

        files: List[Any] = []
        manifest_files = set()
        partition_values = set()
        partial_partitions = []
        partition_columns_set = set(self.partition_columns)
        with profile("manifest_discovery_ms", self._stats):
            for partition_spec in partition_specs:
                invalid_names = [
                    k for k in partition_spec.keys() if k not in partition_columns_set
                ]
                if invalid_names:
                    raise MetaflowTableException(
                        "Columns %s from the partition spec aren't "
                        "valid partition columns (%s)."
                        % (
                            format_set(invalid_names),
                            format_set(self.partition_columns),
                        )
                    )

                # Validate against the current spec
                self._validate_partition_value_types(partition_spec)

                # Now partition_spec is fully describing if it contains values
                # for each partition column.
                if len(partition_spec) == len(self._partition_spec):
                    partition_value = tuple(
                        partition_spec[col] for col in self.partition_columns
                    )
                    partition_values.add(partition_value)
                else:
                    partial_partitions.append(partition_spec)

                # Find manifest files to cover this partition
                # spec. This code will fail of the partition spec or
                # partial spec doesn't match across partition schemas
                manifest_files.update(self._find_manifest(partition_spec))

        if not manifest_files:
            raise MetaflowTableEmptyPartition(
                "No data for the requested partitions '%s'" % str(partition_specs)
            )

        # Download manifest contents.
        if _is_s3_path(list(manifest_files)[0]):
            with profile("manifest_download_ms", self._stats):
                files = []
                with S3(session_vars=self._session_vars) as s3:
                    paths = [s3obj.path for s3obj in s3.get_many(manifest_files)]
                    # Parse manifest files and return (parquet) files that match
                    # the spec.
                    with profile("manifest_parse_ms", self._stats):
                        for path in paths:
                            files.extend(
                                self._get_partition_files_from_manifest(
                                    path, partition_values, partial_partitions
                                )
                            )
        else:
            # Parse manifest files and return (parquet) files that match
            # the spec.
            with profile("manifest_parse_ms", self._stats):
                for path in manifest_files:
                    files.extend(
                        self._get_partition_files_from_manifest(
                            path, partition_values, partial_partitions
                        )
                    )

        if files:
            return self._group_parquet_files(
                files, group_by, num_groups, target_group_size
            )
        else:
            raise MetaflowTableEmptyPartition(
                "No data for the requested partitions '%s'" % str(partition_specs)
            )

    def _generate_field_summary(self, partitions):
        if not partitions:
            return []
        # Combine the partition values
        combined_partitions = {
            key: {d[key] for d in partitions} for key in partitions[0]
        }

        field_summaries = []

        for partition, field in zip(combined_partitions.items(), self._partition_spec):
            contains_null = any(x is None for x in partition[1])
            typ = field["type"]
            encoder = self.STATS_ENCODERS[typ]

            # Compute upper and lower bound
            lower_bound = min(
                [x for x in partition[1] if x is not None],
                default=None,
            )
            upper_bound = max(
                [x for x in partition[1] if x is not None],
                default=None,
            )
            field_summaries.append(
                FieldSummary(
                    contains_null=contains_null,
                    lower_bound=(
                        encoder(lower_bound) if lower_bound is not None else None
                    ),
                    upper_bound=(
                        encoder(upper_bound) if upper_bound is not None else None
                    ),
                )
            )
        return field_summaries

    def _validate_summary(self, summary):
        """
        Validate the summary dictionary.
        """
        # Define the keys to check
        INT_FIELD_IN_SUMMARY = [
            "added-data-files",
            "added-records",
            "added-files-size",
            "deleted-data-files",
            "deleted-records",
            "removed-files-size",
            "changed-partition-count",
        ]
        # Check if all integer values are non-negative
        invalid_summary_field = {}
        all_positive = True

        for key in INT_FIELD_IN_SUMMARY:
            if not (isinstance(summary[key], int) and summary[key] >= 0):
                invalid_summary_field[key] = summary[key]
                all_positive = False

        if not all_positive:
            raise MetaflowTableInvalidData(
                "All int values in the snapshot summary must be positive integers, found the following invalid values: %s"
                % (
                    ", ".join(
                        "%s: %s" % (k, str(v)) for k, v in invalid_summary_field.items()
                    )
                )
            )

    def put_partition_paths(
        self, data_files, overwrite_partitions=False, writer_info=None
    ):
        """
        Args:
            data_files(list): list of DataFile
            overwrite_partitions(bool): If true overwrite existing
            partition info
            writer_info(WriterInfo): Optional information that is used to update the snapshot
            info and whether to "promote" the snapshot

        Returns a set of new s3 files

        Note - Callers must verify the parquet files exist and match the
        current table schema.
        """
        # If the data_files is empty, throw out an exception
        if not data_files:
            raise MetaflowTableInvalidData("Writing empty DataFiles is not supported.")

        unique_partitions = set(frozenset(d.partition.items()) for d in data_files)
        removed_manifest_files = set()
        deleted_data_files = []
        existing_data_files = []
        all_data_files = data_files

        if overwrite_partitions:
            # Overwriting partitions requires:
            # 1. find partition tuples to be updated
            # 2. scan matching manifests for data file references with matching tuples
            # 3. store data file references for non-matching tuples
            # 4. update summary to included overwrite operation and deleted files
            # 5. write a new manifest with old non-matching tuples plus latest partition data
            # 6. write a new manifest list that references existing and new manifests but not old ones

            # 1. Find manifest files that have entries for the
            # partitions being updated and get data_file references
            for p in unique_partitions:
                removed_manifest_files.update(self._find_manifest(dict(p)))
            dfs = self._get_datafiles_from_manifests(removed_manifest_files)

            # 2. Group data files by whether they match the partitions
            # being written
            deleted_data_files = []
            existing_data_files = []
            for df in dfs:
                if frozenset(df.partition.items()) in unique_partitions:
                    deleted_data_files.append(df)
                else:
                    existing_data_files.append(df)

            # 3. organize deleted, existing and updated files and update their status
            deleted_data_files = [
                update_data_file_status(df, DataFileStatus.DELETED)
                for df in deleted_data_files
            ]
            existing_data_files = [
                update_data_file_status(df, DataFileStatus.EXISTING)
                for df in existing_data_files
            ]
            all_data_files = data_files + existing_data_files + deleted_data_files

        # 4. Compute summary values
        existing_files = len(existing_data_files)
        existing_records = sum(d.record_count for d in existing_data_files)
        added_files = len(data_files)
        added_records = sum(d.record_count for d in data_files)
        added_size = sum(d.file_size_in_bytes for d in data_files)
        deleted_files = len(deleted_data_files)
        deleted_records = sum(d.record_count for d in deleted_data_files)
        deleted_size = sum(d.file_size_in_bytes for d in deleted_data_files)
        changed_partition_count = len(unique_partitions)

        # Current summary or default
        current_summary = self._current_summary()
        if current_summary is None:
            # Table is empty
            current_summary = defaultdict(lambda: 0)

        # Generate a summary
        summary = {
            # note - operation determination will require more complex
            # logic if we support other modes.
            "operation": "overwrite" if deleted_files > 0 else "append",
            "added-data-files": added_files,
            "added-records": added_records,
            "added-files-size": added_size,
            "deleted-data-files": deleted_files,
            "deleted-records": deleted_records,
            "removed-files-size": deleted_size,
            "replace-partitions": "true" if deleted_files > 0 else "false",
            "changed-partition-count": changed_partition_count,
            "partition-summaries-included": "false",  # we shouldn't need this
        }  # not supported

        if "total-records" in current_summary:
            summary["total-records"] = (
                added_records + int(current_summary["total-records"])
            ) - deleted_records

        if "total-data-files" in current_summary:
            summary["total-data-files"] = (
                added_files + int(current_summary["total-data-files"])
            ) - deleted_files

        if "total-delete-files" in current_summary:
            summary["total-delete-files"] = current_summary["total-delete-files"]

        if "total-position-deletes" in current_summary:
            summary["total-position-deletes"] = current_summary[
                "total-position-deletes"
            ]

        if "total-equality-deletes" in current_summary:
            summary["total-equality-deletes"] = current_summary[
                "total-equality-deletes"
            ]

        # Add additional information if present
        if writer_info and writer_info.snapshot_summary:
            summary.update(writer_info.snapshot_summary)

        # Validate the summary
        self._validate_summary(summary)

        # Convert all values to string
        summary = {k: str(v) for k, v in summary.items()}

        # Generate a snapshot id as a unique 64-bit int
        snapshot_id = self._iceberg.generate_snaphot_id()

        # Use the exsting schema to write
        schema = list(self._schema.values())

        # Generate field summaries for the new manfest
        partitions = [d.partition for d in all_data_files]
        field_summaries = self._generate_field_summary(partitions)

        # Generate file names
        manifest_file = "-".join([str(uuid.uuid4()), "m0.avro"])
        manifest_list_file = "-".join(
            ["snap", "%d" % snapshot_id, str(uuid.uuid4()) + ".avro"]
        )
        metadata_file = self._iceberg.generate_metadata_filename(self._metadata_path)
        _, metadata_file = os.path.split(metadata_file)

        f_manifest_file = os.path.join(self._metadata_location, manifest_file)
        f_manifest_list_file = os.path.join(self._metadata_location, manifest_list_file)
        f_metadata_file = os.path.join(self._metadata_location, metadata_file)

        # 5. Write manifest and manifest list.
        # Write files to a local temporary file, then we will move them if
        # everything succeeds
        with tempfile.TemporaryDirectory(dir="./", prefix="metaflow-iceberg-") as td:
            temp_manifest = os.path.join(td, manifest_file)
            temp_list = os.path.join(td, manifest_list_file)
            temp_metadata = os.path.join(td, metadata_file)

            # write a manifest file for all the data files that were added
            self._iceberg.manifest_file_writer(
                temp_manifest,
                snapshot_id,
                schema,
                self._default_schema_id,
                self._partition_spec,
                self._default_spec_id,
                all_data_files,
            )

            # write a manifest list file with manifest file entries,
            # making sure to remove manifests that were overwritten
            manifest_file = self._iceberg.ManifestFile(
                manifest_path=f_manifest_file,
                manifest_length=os.stat(temp_manifest).st_size,
                partition_spec_id=self._default_spec_id,
                added_snapshot_id=snapshot_id,
                added_files_count=added_files,
                # "existing_*" and "deleted_*" fields refer to existing
                # and deleted data files referenced in this manifest and not
                # to existing or deleted files referenced in other manifests.
                existing_files_count=existing_files,
                deleted_files_count=deleted_files,
                added_rows_count=added_records,
                existing_rows_count=existing_records,
                deleted_rows_count=deleted_records,
                partitions=field_summaries,
            )

            # 6. Remove manifests that are in the removed set, and add
            # the new ManifestFile entry
            existing_manifests = self._get_manifest_list_contents()
            existing_manifests = [
                m
                for m in existing_manifests
                if _sanitize_s3(m.manifest_path) not in removed_manifest_files
            ]
            all_manifests = existing_manifests + [manifest_file]

            # write the manifest entries to the file
            self._iceberg.manifest_list_writer(temp_list, all_manifests)

            # write the metadata json blob
            self._iceberg.table_metadata_writer(
                temp_metadata,
                self._metadata,
                self._metadata_path,
                snapshot_id,
                summary,
                f_manifest_list_file,
                writer_info,
            )

            # Copy files
            paths = [
                (f_manifest_file, temp_manifest),
                (f_manifest_list_file, temp_list),
                (f_metadata_file, temp_metadata),
            ]

            if _is_s3_path(self._location):
                with S3(session_vars=self._session_vars) as s3:
                    s3.put_files(paths)
            else:
                for dst, src in paths:
                    shutil.move(src, dst)

        return {
            "manifest": f_manifest_file,
            "manifest-list": f_manifest_list_file,
            "metadata": f_metadata_file,
        }

    def list_partitions(self):
        """
        Returns a list of partitions

        """
        if _is_s3_path(self._manifests[0]):
            # Download manifest contents.
            with profile("manifest_download_ms", self._stats):
                s3 = S3(session_vars=self._session_vars)
                paths = [s3obj.path for s3obj in s3.get_many(self._manifests)]

            # Get an iterator that parses rows
            ii = chain.from_iterable(
                self._get_partition_tuples_from_manifest(path) for path in paths
            )
            for i in ii:
                yield i

            s3.close()
        else:
            # Get an iterator that parses rows
            ii = chain.from_iterable(
                self._get_partition_tuples_from_manifest(path)
                for path in self._manifests
            )
            for i in ii:
                yield i


def delta_days(dt):
    return (dt - date(1970, 1, 1)).days


def delta_years(dt):
    return dt.year - 1970


def delta_months(dt):
    return (dt.year - 1970) * 12 + (dt.month - 1)


# NOTE: Iceberg encodes all timestamps as UTC. So we expect users to call this
# function with UTC time only (and do timezone conversions prior).
def delta_hours(dt):
    base_dt = datetime(1970, 1, 1, 0, 0, 0)
    delta = dt - base_dt
    return int(delta.total_seconds() / 3600)
