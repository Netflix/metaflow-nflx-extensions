"""
Custom operations on Metaflow Dataframes or Columns for internal use
"""

from metaflow.util import to_bytes

from .exceptions import MetaflowDataFrameException


np = None

MD = None
ffi = None
MetaflowDataFrame = None
type_info = None
_is_md_import_complete = False


def lazy_import_MD():
    global _is_md_import_complete, ffi, MD, MetaflowDataFrame, type_info
    if not _is_md_import_complete:
        from .md import ffi, MD
        from .dataframe import MetaflowDataFrame
        from . import type_info

        _is_md_import_complete = True


def lazy_import_np():
    global np
    try:
        import numpy as np
    except ImportError:
        raise MetaflowDataFrameException(
            "Numpy required for column operations.  Please install numpy"
        )


def add_null_column(df, name, dtype=None, like=None):
    """
    Returns a new MetaflowDataFrame with a new column with all null values.

    Args:
        name: str, name of new column.  Cannot match any existing columns.

        dtype: (optional) dtype of new column, either numpy dtype or arrow
            format string.  Nested types are not supported.

        like: (optional) MetaflowColumn with type of new column.  May be
            a nested type.

    Either {dtype} or {like} must be specified.
    """
    lazy_import_MD()
    if name in df.schema:
        raise MetaflowDataFrameException(f"Column: {name} already exists in dataframe")

    if dtype is None and like is None:
        raise MetaflowDataFrameException(
            f"Must specify exactly one of 'dtype' or 'like'"
        )
    if dtype is not None and like is not None:
        raise MetaflowDataFrameException(
            f"Must specify exactly one of 'dtype' or 'like'"
        )

    if dtype is None:
        fmt = None
    elif isinstance(dtype, (str, bytes)):
        fmt = to_bytes(dtype)
    else:
        lazy_import_np()
        # numpy dtype
        fmt = type_info.arrow_type(np.dtype(dtype))
    if dtype is not None and fmt.startswith(b"+"):
        raise MetaflowDataFrameException(
            f"Nested dtype not supported when adding null column, got: {dtype}"
        )

    cmdf = ffi.gc(
        MD.md_add_null_column(
            df._df,
            ffi.new("char []", to_bytes(name)),
            ffi.new("char []", fmt) if fmt is not None else ffi.NULL,
            like._mf_chunked_array if like is not None else ffi.NULL,
            df._exception,
        ),
        MD.md_free_dataframe,
    )
    if cmdf == ffi.NULL:
        exception_str = ffi.string(df._exception).decode("utf-8")
        raise MetaflowDataFrameException(exception_str)

    return MetaflowDataFrame([cmdf])


def is_same_value(column, value):
    """
    Returns True if every element of the columns equals the specified
    value. Values must be int, byte or string, or None.

    Args:
        column: column from an MDF
        value: the value to compare

    Returns:
        bool. True if every element of column equals value

    """
    lazy_import_MD()

    if value is None:
        return column.null_count == len(column)

    if not isinstance(value, (bytes, str, int)):
        raise MetaflowDataFrameException(
            "'value' must be an int, string or bytes not a %s" % type(value)
        )

    exception = ffi.new("char[]", 1024)

    if isinstance(value, int):
        value_int = ffi.new("int64_t*", value)
        value_bytes = ffi.NULL
    else:
        value_int = ffi.NULL
        value_bytes = ffi.new("char[]", to_bytes(value))  # this is null terminated

    result = MD.md_is_same_value(
        column._mf_chunked_array, value_int, value_bytes, exception
    )
    if result < 0:
        exception_str = ffi.string(exception).decode("utf-8")
        raise MetaflowDataFrameException(exception_str)

    return bool(result)


def validate_iceberg_mdf(mdf, iceberg_schema):
    """
    Check that a dataframe matches a given iceberg schema.
    The iceberg schema is provided as a json

    Args:
        mdf: MetaflowDataFrame
        iceberg_schema: List of iceberg JSON field schemas.  For examples
            of iceberg JSON field schema formats see:
            https://iceberg.apache.org/spec/#appendix-c-json-serialization

    Returns:
        None
    """
    lazy_import_MD()

    mdf_c_schema = ffi.gc(MD.md_c_schema(mdf._df), MD.md_release_schema)

    SUPPORTED_NESTED_TYPES = ["map", "list", "struct"]
    SUPPORTED_NON_NESTED_TYPES = [
        type_meta.iceberg_type
        for type_meta in type_info.TYPE_META.values()
        if (type_meta.iceberg_type is not None and not type_meta.is_nested)
    ]

    extra_cols = []
    missing_cols = []
    mistyped_cols = []

    def _validate_recursive(c_schema, col_schema, col_path):
        def _path_str():
            return ".".join(col_path)

        col_schema_is_scalar = isinstance(col_schema["type"], str)

        # for time types, include their timezone information
        if type_info.is_arrow_timezone_type(c_schema):
            c_schema_type = type_info.arrow_full_type_str(c_schema)
        else:
            c_schema_type = type_info.arrow_type_str(c_schema)

        def _dbg_type():
            return type_info.type_debug_name(c_schema_type)

        # Note: type_info does not consider string and binary types to be scalar,
        # but here scalar means not nested and includes string and binary types.
        c_schema_is_scalar = not type_info.is_nested(c_schema_type)
        if c_schema_is_scalar != col_schema_is_scalar:
            mistyped_cols.append(
                f"Expected scalar type for column: {_path_str()}, "
                f"found: {_dbg_type()}"
            )
            return
        if c_schema_is_scalar:
            col_schema_type = col_schema["type"]
            if col_schema_type not in SUPPORTED_NON_NESTED_TYPES:
                # Malformed iceberg type.  This should only happen during testing or if the iceberg schema
                # is hand-written.
                maybe_type_meta = type_info.get_type_meta_by_debug_name(col_schema_type)
                if (
                    maybe_type_meta is not None
                    and maybe_type_meta.iceberg_type is not None
                ):
                    # Check if the iceberg schema uses the debug name instead of the iceberg name
                    mistyped_cols.append(
                        f"Column: {_path_str()} has unsupported iceberg type: {col_schema_type}, "
                        f"suggested iceberg type: {maybe_type_meta.iceberg_type}"
                    )
                else:
                    mistyped_cols.append(
                        f"Column: {_path_str()} has unsupported iceberg type: {col_schema_type}."
                    )
            c_schema_iceberg_type = type_info.iceberg_type(
                c_schema_type, error_if_missing=False
            )
            if c_schema_iceberg_type is None:
                mistyped_cols.append(
                    f"Column: {_path_str()} with has type: {_dbg_type()}"
                    " which is not a valid iceberg type."
                )
            if c_schema_iceberg_type != col_schema_type:
                mistyped_cols.append(
                    f"Type mismatch for column: {_path_str()}, expected: {col_schema['type']}"
                    f", found: {_dbg_type()}."
                )
            return

        nested_type = col_schema["type"]["type"]
        if nested_type not in SUPPORTED_NESTED_TYPES:
            mistyped_cols.append(
                f"Column: {_path_str()} has unsupported iceberg type: {nested_type}."
            )
            return

        if nested_type == "map":
            if not type_info.is_map(c_schema_type) and not type_info.is_list(
                c_schema_type
            ):
                mistyped_cols(
                    f"Expected map or list type for column: {_path_str()}, found: {_dbg_type()}"
                )

            # Map type can be either "+m" or "+l"/"+L".  In both cases it is logically
            # List<Struct<key, value>>.  Recurse to underlying Struct child.
            kv_child_schema = c_schema.children[0]  # Type: Struct
            _validate_recursive(
                kv_child_schema,
                {
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"name": "key", "type": col_schema["type"]["key"]},
                            {"name": "value", "type": col_schema["type"]["value"]},
                        ],
                    }
                },
                col_path,
            )
        elif nested_type == "list":
            if not type_info.is_list(c_schema_type):
                mistyped_cols.append(
                    f"Expected list type for column: {_path_str()}, found: {_dbg_type()}"
                )
                return
            # Convert list schema format to the format used for struct and scalars
            value_schema = {
                "type": col_schema["type"]["element"],
            }
            _validate_recursive(
                c_schema.children[0], value_schema, col_path + ["element"]
            )
        else:
            # Recurse for struct members.  Field order does not need to
            # match between C schema and iceberg schema.
            if not type_info.is_struct(c_schema_type):
                # This check is redundant with the NESTED_TYPES check above, but will
                # trigger if other nested types are supported in the future.
                mistyped_cols.append(
                    f"Expected struct type for column: {_path_str()}, found: {_dbg_type()}"
                )
            # Check for missing or extra struct fields
            c_child_names = {
                ffi.string(c_schema.children[i].name).decode("utf-8")
                for i in range(c_schema.n_children)
            }
            iceberg_child_names = {
                field["name"] for field in col_schema["type"]["fields"]
            }
            extra_fields = sorted(c_child_names - iceberg_child_names)
            missing_fields = sorted(iceberg_child_names - c_child_names)
            common_fields = sorted(c_child_names & iceberg_child_names)
            for name in extra_fields:
                extra_cols.append(col_path + [name])
            for name in missing_fields:
                missing_cols.append(col_path + [name])
            # Check subfields
            for col in common_fields:
                c_field = next(
                    c_schema.children[i]
                    for i in range(c_schema.n_children)
                    if ffi.string(c_schema.children[i].name).decode("utf-8") == col
                )
                iceberg_field = next(
                    field
                    for field in col_schema["type"]["fields"]
                    if field["name"] == col
                )
                _validate_recursive(c_field, iceberg_field, col_path + [col])

    # Restructure iceberg schema as struct
    iceberg_struct_schema = {"type": {"type": "struct", "fields": iceberg_schema}}
    _validate_recursive(mdf_c_schema, iceberg_struct_schema, [])
    if extra_cols or missing_cols or mistyped_cols:
        error_msg = "Invalid dataframe schema for iceberg table: \n"
        if extra_cols:
            extra_cols_string = ",".join(".".join(path) for path in extra_cols)
            error_msg += f"Extra columns: ({extra_cols_string})\n"
        if missing_cols:
            missing_cols_string = ",".join(".".join(path) for path in missing_cols)
            error_msg += f"Missing columns: ({missing_cols_string})\n"
        if mistyped_cols:
            mistyped_cols_string = "\n".join(msg for msg in mistyped_cols)
            error_msg += f"Type errors: ({mistyped_cols_string})\n"
        raise MetaflowDataFrameException(error_msg)


def extract_field_ids(iceberg_schema):
    """
    Extract iceberg field ids for all fields, including nested fields.

    This method uses the following conventions to identify nested fields:
    - For lists, the item field path is ${path} + ["element"]
    - For structs, the child field paths are given by ${path} + [${child_field}]
    - For maps, the key and value field paths are given by ${path} + [("key"|"value")]

    Args:
        iceberg_schema: List of iceberg JSON field schemas.  For examples
            of iceberg JSON field schema formats see:
            https://iceberg.apache.org/spec/#appendix-c-json-serialization

    Returns:
        dict mapping field path to field id, with type Dict[List[str], int]
    """
    path_and_ids = []

    def _extract_recursive(schema, cur_path):
        if "id" in schema:
            path_and_ids.append((cur_path, schema["id"]))
        if isinstance(schema["type"], str):
            # Scalar
            return
        nested_schema_type = schema["type"]["type"]
        if nested_schema_type == "list":
            # Convert list schema format to the format used for struct and scalars
            value_schema = {
                "id": schema["type"]["element-id"],
                "type": schema["type"]["element"],
            }

            _extract_recursive(value_schema, cur_path + ["element"])
        elif nested_schema_type == "struct":
            for field in schema["type"]["fields"]:
                _extract_recursive(field, cur_path + [field["name"]])
        elif nested_schema_type == "map":
            # map is like List<Struct<key, value>> but the implicit Struct has
            # no id (i.e. the List has no "element-id" field).
            _extract_recursive(
                {
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "id": schema["type"]["key-id"],
                                "name": "key",
                                "type": schema["type"]["key"],
                            },
                            {
                                "id": schema["type"]["value-id"],
                                "name": "value",
                                "type": schema["type"]["value"],
                            },
                        ],
                    }
                },
                cur_path,
            )

    for field in iceberg_schema:
        _extract_recursive(field, [field["name"]])

    return path_and_ids
