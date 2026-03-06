"""
A module to get size information
"""

from . import type_info
from .exceptions import MetaflowDataFrameException
import math

ffi = None
as_array = None
_is_md_import_complete = False


def lazy_import_MD():
    global _is_md_import_complete, ffi, as_array
    if not _is_md_import_complete:
        from .md import ffi
        from .util import as_array

        _is_md_import_complete = True


def _carray_nbytes(arr, schema, rng=None):
    """
    Compute number of bytes of data in a potentially nested array
    """
    rng = (0, arr.length) if rng is None else rng
    length = rng[1] - rng[0]
    type_ = type_info.arrow_type_str(schema)
    nulls_bytes = 0 if arr.buffers[0] == ffi.NULL else math.ceil(length / 8)
    total_bytes = 0
    if type_ in (b"+l", b"+L", b"+m"):
        # list, large list and map type
        # map is an alias for list<struct<key_type, value_type>>
        type_meta = type_info.TYPE_META[type_]
        offset_bytes = (length + 1) * type_meta.index_width
        index = as_array(
            arr.buffers[1],
            arr.length + 1,
            arr.offset,
            array_type=type_meta.index_type,
            itemsize=type_meta.index_width,
        )
        rng = (index[rng[0]], index[rng[1]])
        data_bytes = _carray_nbytes(arr.children[0], schema.children[0], rng)
        total_bytes += nulls_bytes + offset_bytes + data_bytes
    elif type_ == b"+s":
        # struct type
        child_bytes = 0
        for c_idx in range(schema.n_children):
            child_bytes += _carray_nbytes(
                arr.children[c_idx], schema.children[c_idx], rng
            )
        total_bytes += nulls_bytes + child_bytes
    elif type_info.is_scalar(type_):
        # any scalar which includes primitive and time types
        type_meta = type_info.TYPE_META[type_]
        data_bytes = length * type_meta.byte_width
        if type_ == b"b":
            data_bytes = math.ceil(data_bytes / 8)
        total_bytes += nulls_bytes + data_bytes
    elif type_info.is_binary(type_):
        # any binary type which includes strings and binary
        type_meta = type_info.TYPE_META[type_]
        offset_bytes = (length + 1) * type_meta.index_width
        index = as_array(
            arr.buffers[1],
            arr.length + 1,
            arr.offset,
            array_type=type_meta.index_type,
            itemsize=type_meta.index_width,
        )
        data_bytes = index[rng[1]] - index[rng[0]]
        total_bytes += nulls_bytes + offset_bytes + data_bytes
    else:
        raise MetaflowDataFrameException(
            "The array has an unsupported type format string ('%s')" % type_
        )

    if arr.dictionary != ffi.NULL:
        total_bytes += _carray_nbytes(arr.dictionary, schema.dictionary)

    return total_bytes


def nbytes(chunked_array, schema):
    """
    Compute number of bytes in a chunked array
    """
    lazy_import_MD()

    nbytes = 0
    for i in range(chunked_array.n_chunks):
        nbytes += _carray_nbytes(chunked_array.chunks[i], schema)
    return nbytes
