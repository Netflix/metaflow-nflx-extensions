"""
Custom operations on Metaflow Dataframes or Columns
"""

from metaflow.util import to_bytes

from .exceptions import MetaflowDataFrameException
from . import type_info


np = None

MD = None
ffi = None
_metaflow_column = None
_is_md_import_complete = False


def lazy_import_MD():
    global _is_md_import_complete, ffi, MD, _metaflow_column
    if not _is_md_import_complete:
        from .md import ffi, MD
        from .column import _metaflow_column

        _is_md_import_complete = True


def lazy_import_np():
    global np
    try:
        import numpy as np
    except ImportError:
        raise MetaflowDataFrameException(
            "Numpy required for sparse_to_dense().  Please install numpy"
        )


def _check_project_struct_fields_schema(column, child_columns, op_name):
    column_type = ffi.string(column._schema.format)
    if column_type not in (b"+L", b"+l", b"+m"):
        raise MetaflowDataFrameException(
            f"{op_name} called on non-list column {column.name}"
        )

    struct_schema = column._schema.children[0]
    inner_type = ffi.string(struct_schema.format)
    if inner_type != b"+s":
        raise MetaflowDataFrameException(
            f"{op_name} called on column {column.name} which is not a list of structs"
        )

    struct_fields = [
        ffi.string(struct_schema.children[i].name)
        for i in range(struct_schema.n_children)
    ]
    missing_fields = [
        col for col in child_columns if to_bytes(col) not in struct_fields
    ]
    if missing_fields:
        raise MetaflowDataFrameException(
            f"{op_name} called with missing field(s): {missing_fields}.  Available struct fields: {struct_fields}"
        )


def project_struct_fields(column, child_columns):
    """
    For a MetaflowColumn of type List<Struct<...>> and a list of child fields
    of the inner Struct column, return a projected column with schema
    List<Struct<{child_columns}>>.

    Args:
        column: MetaflowColumn of type List<Struct<...>>
        child_columns: List of child column names
    """
    lazy_import_MD()
    _check_project_struct_fields_schema(column, child_columns, "project_struct_field")
    c_child_columns = [ffi.new("char []", to_bytes(col)) for col in child_columns]
    mf_chunked_array = ffi.gc(
        MD.md_project_struct_list_array(
            column._mf_chunked_array,
            c_child_columns,
            len(c_child_columns),
            False,  # =promote single_field
        ),
        MD.md_free_chunked_array,
    )
    return _metaflow_column(mf_chunked_array, column._mdf_cls)


def promote_struct_field(column, child):
    """
    For a MetaflowColumn of type List<Struct<...>> and a child field of the
    Struct, return a column with schema List<{child}>.

    Args:
        column: MetaflowColumn of type List<Struct<...>>
        child: name of child column
    """
    lazy_import_MD()
    _check_project_struct_fields_schema(column, [child], "promote_struct_field")
    c_child_columns = [ffi.new("char []", to_bytes(child))]
    mf_chunked_array = ffi.gc(
        MD.md_project_struct_list_array(
            column._mf_chunked_array,
            c_child_columns,
            1,
            True,  # =promote single field
        ),
        MD.md_free_chunked_array,
    )
    return _metaflow_column(mf_chunked_array, column._mdf_cls)


def sparse_to_dense(
    indices, values, shape, default_value, flat_indices=True, null_value=None
):
    """
    Convert a sparse array representation to a dense numpy array.

    Args:
        indices: MetaflowColumn of type List<integral>
        values: MetaflowColumn of type List<primitive>
        shape: tuple of output dense array
        default_value: Default value when not specified by sparse array
        flat_indices: bool, indicating whether {indices} are flat or multi-dimensional
                      indices (see below).
        null_value: Value for null entries in {value} column.  Not required if {value} has
                    no nulls.

    Returns:
        Dense numpy array with dimensions (num rows, *shape).

    Flat indices:
        If flat_indices is True, {indices} is interpreted as 1-D indices into a flattened
        view of the output tensor.  If flat_indices is False, {indices} is determined
        as a list of concatenated n-tuples of indices into the n-dimensional shape.
        For example, the dense tensor:
        [[2, 0],
         [0, 3]]
        has flattened representation {indices: [0, 3], values: [2, 3]},
        and concatenated representation {indices: [0, 0, 1, 1], values: [2, 3]}
    """
    lazy_import_MD()
    lazy_import_np()
    # Check types
    indices_schema = indices._schema
    values_schema = values._schema
    n_chunks = values._c_chunked_array.n_chunks

    if ffi.string(indices_schema.format).upper() != b"+L":
        raise MetaflowDataFrameException("sparse_to_dense called with non-list indices")

    if ffi.string(values_schema.format).upper() != b"+L":
        raise MetaflowDataFrameException("sparse_to_dense called with non-list values")

    indices_type = ffi.string(indices_schema.children[0].format)
    values_type = ffi.string(values_schema.children[0].format)

    if not type_info.is_integral(indices_type):
        raise MetaflowDataFrameException(
            "sparse_to_dense called with non-integral indices"
        )

    if not type_info.is_primitive(values_type):
        raise MetaflowDataFrameException(
            "sparse_to_dense called with non-primitive values"
        )

    if values_type == b"b":
        raise MetaflowDataFrameException(
            "sparse_to_dense not supported for boolean values"
        )

    values_has_nulls = any(
        values._c_chunked_array.chunks[i].null_count > 0
        for i in range(values._c_chunked_array.n_chunks)
    )
    if values_has_nulls and null_value is None:
        raise MetaflowDataFrameException(
            "sparse_to_dense called with values array containing nulls but not null_val"
        )

    # Check sizes
    if indices._c_chunked_array.n_chunks != n_chunks:
        raise MetaflowDataFrameException(
            "sparse_to_dense called with unaligned indices and values columns"
        )

    for c_idx in range(n_chunks):
        indices_chunk = indices._c_chunked_array.chunks[c_idx]
        values_chunk = values._c_chunked_array.chunks[c_idx]
        if indices_chunk.null_count > 0 or values_chunk.null_count > 0:
            # Null list entries can have arbitrary child array data
            # so lengths might not match.
            continue
        indices_child_chunk = indices_chunk.children[0]
        values_child_chunk = values_chunk.children[0]
        indices_len_factor = 1 if flat_indices else len(shape)
        if indices_child_chunk.length != values_child_chunk.length * indices_len_factor:
            raise MetaflowDataFrameException(
                (
                    f"indices length does not match expected values length: expected "
                    + f"{indices_len_factor} * {values_child_chunk.length}, found "
                    + f"{indices_child_chunk.length}"
                )
            )

    itemsize = int(np.prod(shape))
    type_meta = type_info.get_type_meta(values_type)
    out_np = np.full(
        shape=(len(indices), itemsize),
        fill_value=default_value,
        dtype=type_info.numpy_dtype(type_meta.array_type),
    )
    out = ffi.cast("uint8_t *", out_np.ctypes.data)
    offset = 0
    md_shape = [itemsize] if flat_indices else shape
    for c_idx in range(n_chunks):
        MD.md_sparse_to_dense(
            indices._c_chunked_array.chunks[c_idx],
            indices_schema,
            values._c_chunked_array.chunks[c_idx],
            values_schema,
            md_shape,
            len(md_shape),
            null_value,
            out + offset,
        )
        offset += (
            indices._c_chunked_array.chunks[c_idx].length
            * itemsize
            * type_meta.byte_width
        )

    return out_np.reshape((-1, *shape))
