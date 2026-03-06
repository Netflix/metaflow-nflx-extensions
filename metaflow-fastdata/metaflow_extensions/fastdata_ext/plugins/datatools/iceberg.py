import copy
import json
import os
import time
import uuid
import tempfile
import shutil
from collections import namedtuple
import uuid
from enum import Enum
from typing import (
    NamedTuple,
    Optional,
    Any,
    TYPE_CHECKING,
    Dict,
    List,
    Union,
)

from .exceptions import (
    MetaflowIcebergManifestException,
    MetaflowIcebergMetadataException,
    MetaflowIcebergDuplicateDataFiles,
)

if TYPE_CHECKING:
    MD: Any = None
    ffi: Any = None
else:
    MD = None
    ffi = None
_is_md_import_complete = False

EXCEPTION_CAPACITY = 2048

# Definitions for different files used in iceberg writing
# ManifestEntry
ManifestEntryFields = ["status", "snapshot_id", "data_file"]

ManifestEntryDefaults = [None, None, None]


class ManifestEntry(NamedTuple):
    status: Optional[int] = None
    snapshot_id: Optional[Union[int, str]] = None
    data_file: Optional["DataFile"] = None


# define some constants for DataFile status field, the integer values
# chosen are part of the iceberg specification V1.
class DataFileStatus(Enum):
    EXISTING = 0
    ADDED = 1
    DELETED = 2


# DataFile
DataFileFields = [
    "status",
    "file_path",
    "file_format",
    "partition",
    "record_count",
    "file_size_in_bytes",
    "block_size_in_bytes",
]

# block_size_in_bytes is deprecated and must be set to a default
# in iceberg v1 (removed in v2).  Other fields must have non-default
# values.
DataFileDefaults = [None, None, "PARQUET", None, None, None, 0]


class DataFile(NamedTuple):
    status: Optional[DataFileStatus] = None
    file_path: Optional[str] = None
    file_format: str = "PARQUET"
    partition: Optional[Any] = None
    record_count: Optional[int] = None
    file_size_in_bytes: Optional[int] = None
    block_size_in_bytes: int = 0


# FieldSummary
# To align with Netflix spark, we remove contains_nan in manifest_list_schema as an optional field
FieldSummaryFields = ["contains_null", "lower_bound", "upper_bound"]

FieldSummaryDefaults = [False, None, None]


class FieldSummary(NamedTuple):
    contains_null: bool = False
    lower_bound: Optional[Any] = None
    upper_bound: Optional[Any] = None


# ManifestFile
ManifestFileFields = [
    "manifest_path",
    "manifest_length",
    "partition_spec_id",
    "added_snapshot_id",
    # See https://github.com/apache/iceberg/issues/8684 for more details.
    # TODO: Investigate if we can support parsing Avro schema by field id
    "added_files_count",  # used to be added_data_files_count in older version of Iceberg
    "existing_files_count",  # used to be existing_data_files_count in older version of Iceberg
    "deleted_files_count",  # used to be deleted_data_files_count in older version of Iceberg
    # --
    "added_rows_count",
    "existing_rows_count",
    "deleted_rows_count",
    "partitions",
]

ManifestFileDefaults = [None, None, None, None, 0, 0, 0, 0, 0, 0, None]


class ManifestFile(NamedTuple):
    manifest_path: Optional[str] = None
    manifest_length: Optional[int] = None
    partition_spec_id: Optional[int] = None
    added_snapshot_id: Optional[Union[int, str]] = None
    added_files_count: int = 0
    existing_files_count: int = 0
    deleted_files_count: int = 0
    added_rows_count: int = 0
    existing_rows_count: int = 0
    deleted_rows_count: int = 0
    partitions: Optional[List[Dict[str, Union[int, str, None, List[str]]]]] = None


def update_data_file_status(data_file, status):
    kvals = {dff: getattr(data_file, dff) for dff in DataFileFields}
    kvals["status"] = status
    return DataFile(**kvals)


def data_file_from_kwargs(**kwargs):
    status = kwargs.pop("status")
    status = DataFileStatus(status) if status is not None else None
    return DataFile(status=status, **kwargs)


def lazy_import_MD():
    global _is_md_import_complete, MD, ffi
    if not _is_md_import_complete:
        from .md import ffi, MD

        _is_md_import_complete = True


def _ntuple_to_dict(t):
    res = {}
    for k, v in dict(t._asdict()).items():
        if isinstance(v, (ManifestFile, FieldSummary, ManifestEntry, DataFile)):
            v = _ntuple_to_dict(v)
        res[k] = v
        if isinstance(v, list):
            res[k] = []
            for item in v:
                if isinstance(
                    item, (ManifestFile, FieldSummary, ManifestEntry, DataFile)
                ):
                    item = _ntuple_to_dict(item)
                res[k].append(item)
        if isinstance(v, Enum):
            res[k] = v.value
    return res


def _parse_iceberg(filename, type):
    """
    Parse an iceberg file given its type. Type should be 'MANIFEST_FILE', or
    'MANIFEST_LIST'.
    """
    lazy_import_MD()
    assert MD is not None and ffi is not None

    exception = ffi.new("char[]", EXCEPTION_CAPACITY)
    json_str = ffi.new("char***")
    metadata_str = ffi.new("char**")
    size = ffi.new("int64_t*")

    MD.md_iceberg_to_json(
        filename.encode("utf8"),
        type.encode("utf8"),
        metadata_str,
        json_str,
        size,
        exception,
    )

    exception_str = ffi.string(exception).decode("utf8")
    if exception_str:
        raise MetaflowIcebergManifestException(
            "Error with file '%s': %s" % (filename, exception_str)
        )

    metadata = json.loads(ffi.string(metadata_str[0]))
    MD.md_free_memory(metadata_str[0])

    def _gen():
        for i in range(0, size[0]):
            yield json.loads(ffi.string(json_str[0][i]))
            MD.md_free_memory(json_str[0][i])
        MD.md_free_memory(json_str[0])

    IcebergReader = namedtuple("IcebergReader", ["metadata", "rows"])
    return IcebergReader(metadata, _gen())


def manifest_list_reader(filename):
    """
    Parse a manifest list

    Args:
    filename (str): The filename of the manifest list file

    Returns:
    A named typle with .metadata, and .rows
    """
    return _parse_iceberg(filename, "MANIFEST_LIST")


def manifest_file_reader(filename):
    """
    Parse a manifest file

    Args:
    filename (str): The filename of the manifest file

    Returns:
    A named typle with .metadata, and .rows
    """
    return _parse_iceberg(filename, "MANIFEST_FILE")


def read_manifest_list(filename):
    """
    Parse a manifest list file using fastavro

    Args:
    filename: Filename of manifest list

    Returns:
    List of parsed ManifestFile objects
    """
    import fastavro

    if filename is None:
        # Empty table
        return []

    manifest_list_schema_file = os.path.join(
        os.path.dirname(__file__), "manifest_list_schema.json"
    )
    with open(manifest_list_schema_file, "r") as fh:
        manifest_list_schema = json.load(fh)
    with open(filename, "rb") as fh:
        avro_reader = fastavro.reader(fh, reader_schema=manifest_list_schema)
        records = list(iter(avro_reader))
    return [ManifestFile(**record) for record in records]


def _inject_partition_spec(parsed_schema, partition_spec):
    """
    The expected schema structure of partition spec is as follow
    In Icberg V1, 'field-id' always start at 1000
    'name' should be the name of partition column
    {
        'field-id': 102,
        'name': 'partition',
        'type': {
            'type': 'record',
            'name': 'r102',
            'fields': [
                {
                    'field-id': 1000,
                    'default': None,
                    'name': 'partition_1',
                    'type': ['null', 'int']
                },
                {
                    'field-id': 1001,
                    'default': None,
                    'name': 'partition_2',
                    'type': ['null', 'string']
                }
            ]
        }
    }
    """
    partitions = []
    default_field_id = 1000
    for part in partition_spec:
        p = {}
        if "field-id" in part:
            p["field-id"] = part["field-id"]
        else:
            p["field-id"] = default_field_id
            default_field_id += 1
        # TODO May need to change this to support partition evolution
        p["default"] = None
        p["name"] = part["name"]
        p["type"] = ["null"]
        p["type"].append(part["type"])
        partitions.append(p)
    try:
        parsed_schema["fields"][2]["type"]["fields"][2]["type"]["fields"] = partitions
    except:
        raise MetaflowIcebergManifestException(
            "Unable to update the manifest file with partition information because the required fields are not present. Please reach out to #ask-metaflow for "
        )
    return parsed_schema


def manifest_file_writer(
    filename,
    snapshot_id,
    schema,
    schema_id,
    partition_spec,
    partition_spec_id,
    data_files,
):
    """
    Write a manifest file for an unpartitioned table

    filename (str): path where manifest file will be written
    snapshot_id: long int for the id of the snapshot
    schema (dict): list of iceberg fields
    schema_id (int): iceberg schema id, to handle schema evolution
    partition_spec (list): list of iceberg field partition specs
    partition_spec_id (int): iceberg partition id, to handle partition evolution
    data_files(list): list of DataFiles

    Returns:
    None

    """
    from fastavro import writer, parse_schema
    from collections import defaultdict

    # detect the data_file duplicates
    file_status = defaultdict(list)
    for df in data_files:
        file_status[df.file_path].append(df.status)
    if any(len(v) > 1 for v in file_status.values()):
        raise MetaflowIcebergDuplicateDataFiles(
            f"Data files have duplicates: {file_status}. Please reach out to #ask-metaflow for help. If you are overwriting an Iceberg table, it may be safe to retry."
        )

    # get the schema from file
    sdir = os.path.abspath(os.path.dirname(__file__))
    # Source for manifest schema: https://iceberg.apache.org/spec/#manifests
    with open(os.path.join(sdir, "manifest_schema.json"), "r") as fh:
        manifest_schema = json.load(fh)
    manifest_schema = parse_schema(manifest_schema)
    # Inject partition spec into the schema
    manifest_schema_final = _inject_partition_spec(manifest_schema, partition_spec)

    # set metadata
    metadata = {
        "schema": json.dumps({"type": "struct", "fields": schema}),
        "schema-id": str(schema_id),
        "partition-spec": json.dumps(partition_spec),
        "partition-spec-id": str(partition_spec_id),
        "format-version": "1",
    }
    # prepare records
    records = []
    for data_file in data_files:
        manifest_entry = ManifestEntry(
            status=data_file.status.value, snapshot_id=snapshot_id, data_file=data_file
        )
        record = _ntuple_to_dict(manifest_entry)
        del record["data_file"]["status"]
        records.append(record)

    # write records
    with tempfile.NamedTemporaryFile(dir="./", prefix="metaflow-iceberg") as tf:
        writer(fo=tf, schema=manifest_schema_final, records=records, metadata=metadata)
        tf.flush()
        shutil.copy(tf.name, filename)


def manifest_list_writer(filename, manifest_files):
    """
    Write a manifest list file

    filename (str): path where manifest list will be written

    """
    from fastavro import writer, parse_schema

    sdir = os.path.abspath(os.path.dirname(__file__))
    # Source for manifest list schema: https://iceberg.apache.org/spec/#manifests
    with open(os.path.join(sdir, "manifest_list_schema.json"), "r") as fh:
        json_str = fh.read()
        manifest_schema = json.loads(json_str)
    manifest_schema = parse_schema(manifest_schema)

    # prepare records
    records = []
    for manifest in manifest_files:
        record = _ntuple_to_dict(manifest)
        records.append(record)

    # write the records
    with tempfile.NamedTemporaryFile(dir="./", prefix="metaflow-iceberg") as tf:
        writer(fo=tf, schema=manifest_schema, records=records)
        tf.flush()
        shutil.copy(tf.name, filename)


def generate_metadata_filename(metadata_file):
    # metadata files always have a <V>-<random-uuid>.metadata.json
    # structure. Where V is an int string.
    path, fname = os.path.split(metadata_file)
    parts = fname.split("-")
    version = int(parts[0]) + 1
    uid = str(uuid.uuid4())
    fname = f"{str(version).zfill(5)}-{uid}.metadata.json"
    fname = os.path.join(path, fname)
    return fname


def generate_snaphot_id():
    return uuid.uuid4().int % (2**63)


def table_metadata_writer(
    new_metadata_file,
    meta,
    metadata_path,
    snapshot_id,
    summary,
    manifest_list_file,
    writer_info=None,
):
    """
    Write a new metadata file adding a new snapshot.

    """
    # Here we list all fields for V1 and V2, including required and optional ones.
    # The required field configuration is meant to match spark 2.4 at Netflix.
    # We write all required fields for V1 and some optional fields.
    FIELDS = {
        "format-version": True,
        "table-uuid": False,
        "location": True,
        "last-column-id": True,
        "last-sequence-number": False,
        "last-updated-ms": True,
        "schema": False,  # schema or (schemas, current-schema-id) are both acceptable
        "schemas": False,
        "current-schema-id": False,
        "partition-spec": False,
        "partition-specs": False,
        "default-spec-id": False,
        "last-partition-id": False,  # This is what the spec says
        "last-assigned-partition-id": False,  # This is what spark 2.4 produces at NFLX
        "properties": False,
        "current-snapshot-id": False,
        "snapshots": False,
        "snapshot-log": False,
        "metadata-log": False,
        "sort-orders": False,
        "default-sort-order-id": False,
        "refs": False,  # This is an essential field in spark 3.3
        "statistics": False,
    }

    # Make a deepcopy of the current metadata dict, to avoid modifying the original.
    # A few things are passed by reference, so we need to be careful.
    # List of keys modified below (if not deepcopied):
    # - snapshots
    # - snapshot-log
    # - metadata-log
    # - refs
    meta = copy.deepcopy(meta)

    if str(meta["format-version"]) != "1":
        raise MetaflowIcebergMetadataException(
            f" The table metadata file is a version '{meta['format-version']}' iceberg metadata file. "
            "Only version '1' is supported."
        )

    timestamp = int(time.time() * 1000)

    snapshot = {}
    snapshot["snapshot-id"] = snapshot_id
    snapshot["timestamp-ms"] = timestamp
    snapshot["manifest-list"] = manifest_list_file
    snapshot["summary"] = summary
    snapshot["parent-snapshot-id"] = meta["current-snapshot-id"]
    snapshots = meta["snapshots"]
    snapshots.append(snapshot)

    metadata_log = {}
    metadata_log["timestamp-ms"] = timestamp
    metadata_log["metadata-file"] = metadata_path
    metadata_logs = meta.get("metadata-log", [])

    metadata_logs.append(metadata_log)

    snapshot_logs = meta["snapshot-log"]

    newmeta = {k: v for k, v in meta.items() if k in FIELDS}
    if writer_info is None or writer_info.update_current_snapshot:
        newmeta["current-snapshot-id"] = snapshot_id
        # Only update snapshot-log when the snapshot is going to be promoted as current-snapshot-id
        snapshot_log = {}
        snapshot_log["timestamp-ms"] = timestamp
        snapshot_log["snapshot-id"] = snapshot_id
        snapshot_logs.append(snapshot_log)

    newmeta["snapshots"] = snapshots
    newmeta["snapshot-log"] = snapshot_logs
    newmeta["metadata-log"] = metadata_logs
    newmeta["last-updated-ms"] = timestamp

    if writer_info is None or writer_info.set_refs is None:
        # Default "update" which is to point main to the snapshot
        # Initialize main branch in ref if it does not exist
        if "refs" not in newmeta or not newmeta["refs"]:
            newmeta["refs"] = {"main": {"snapshot-id": snapshot_id, "type": "branch"}}
        else:
            newmeta["refs"]["main"]["snapshot-id"] = snapshot_id
    else:
        # We update the refs based on user direction

        # First convert all "current" to the proper value
        new_set_refs = {
            k: (
                v
                if v["snapshot-id"] != "current"
                else {"snapshot-id": snapshot_id, "type": v["type"]}
            )
            for k, v in writer_info.set_refs.items()
        }

        # If this is a table with no refs initially, we at least need to set main
        # if we set anything
        if new_set_refs and ("refs" not in newmeta or not newmeta["refs"]):
            if "main" not in writer_info.set_refs:
                raise MetaflowIcebergMetadataException(
                    "Cannot update refs without setting main branch"
                )

        for branch, snap_info in new_set_refs.items():
            newmeta["refs"][branch] = snap_info

    # Check that required fields are present
    missing_fields = [k for k, v in FIELDS.items() if v and (k not in newmeta)]
    if missing_fields:
        raise MetaflowIcebergMetadataException(
            f"Table metadata is missing required fields: {','.join(missing_fields)}"
        )

    with open(new_metadata_file, "w") as fh:
        json.dump(newmeta, fh)
