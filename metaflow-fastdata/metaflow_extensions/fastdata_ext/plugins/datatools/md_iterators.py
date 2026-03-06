"""
A module for dataframe iterators
"""

from .md import ffi, MD
from .util import as_array
from . import type_info
from datetime import datetime, date
from pytz import timezone


Timestamp = None


def lazy_import_timestamp():
    global Timestamp
    if not Timestamp:
        from pandas import Timestamp


def unpack_boolean(arr):
    # offsets are not needed since 'md_unpack_boolean' is offset aware
    bools = ffi.new("uint8_t[]", arr.length)
    MD.md_unpack_boolean(arr, 0, True, bools)
    return bools


def _boolean_type_iterator(arr, _itemsize, _array_type, row_offset=0):
    cnulls = unpack_boolean(arr)
    cvals = ffi.new("uint8_t[]", arr.length)
    MD.md_unpack_boolean(arr, 1, False, cvals)  # this function is offset aware
    length = arr.length
    nulls = as_array(cnulls, length - row_offset, row_offset)
    vals = as_array(cvals, length - row_offset, row_offset)
    for j in range(length - row_offset):
        if nulls[j]:
            yield None
        else:
            yield vals[j] == 1


def _primitive_type_iterator(arr, itemsize, array_type, row_offset=0):
    cnulls = unpack_boolean(arr)
    length = arr.length
    offset = arr.offset
    nulls = as_array(cnulls, length - row_offset, row_offset)
    vals = as_array(
        arr.buffers[1], length - row_offset, offset + row_offset, array_type, itemsize
    )
    for j in range(length - row_offset):
        if nulls[j]:
            yield None
        else:
            yield vals[j]


def _binary_type_iterator(arr, large_index, is_string, row_offset=0):
    length = arr.length
    offset = arr.offset
    cnulls = unpack_boolean(arr)
    nulls = as_array(cnulls, length - row_offset, row_offset)
    (offsets_type, offsets_size) = ("q", 8) if large_index else ("i", 4)
    offsets = as_array(
        arr.buffers[1],
        length + 1 - row_offset,
        offset + row_offset,
        offsets_type,
        offsets_size,
    )
    data = as_array(arr.buffers[2], offsets[length - row_offset])
    for j in range(length - row_offset):
        if nulls[j]:
            yield None
        else:
            string = data[offsets[j] : offsets[j + 1]].tobytes()
            if is_string:
                yield string.decode("utf-8")
            else:
                yield string


def _list_type_iterator(
    arr, child_iter, child_length, index_item_size, index_type, row_offset=0
):
    length = arr.length
    offset = arr.offset
    cnulls = unpack_boolean(arr)
    nulls = as_array(cnulls, length - row_offset, row_offset)
    offsets = as_array(
        arr.buffers[1],
        length + 1 - row_offset,
        offset + row_offset,
        index_type,
        index_item_size,
    )
    offsets_iter = iter(offsets)
    child_idx = 0

    if child_length > 0:
        offset = next(offsets_iter)  # apply buffer offset
        while child_idx < offset and child_idx < child_length:
            _ = next(child_iter)
            child_idx += 1

    for is_invalid, offset in zip(nulls, offsets_iter):
        if is_invalid:
            # Iterate without building list
            while child_idx < offset and child_idx < child_length:
                _ = next(child_iter)
                child_idx += 1
            yield None
        else:
            entry = []
            while child_idx < offset and child_idx < child_length:
                value = next(child_iter)
                child_idx += 1
                entry.append(value)
            yield entry


def _struct_type_iterator(arr, child_iter_dict, row_offset=0):
    length = arr.length
    offset = arr.offset
    cnulls = unpack_boolean(arr)
    nulls = as_array(cnulls, length - row_offset, row_offset)

    for _ in range(offset + row_offset):
        for child_iter in child_iter_dict.values():
            _ = next(child_iter)

    for is_invalid in nulls:
        if is_invalid:
            for child_iter in child_iter_dict.values():
                _ = next(child_iter)
            yield None
        else:
            yield {k: next(child_iter) for k, child_iter in child_iter_dict.items()}


def _map_type_iterator(arr, entries_iterator, entries_length, row_offset=0):
    # Map type is equivalent to List<entries: Struct<key: K, value: V>>
    entries_list_iterator = _list_type_iterator(
        arr, entries_iterator, entries_length, 4, "i", row_offset
    )
    for entries in entries_list_iterator:
        yield (
            None
            if entries is None
            else {entry["key"]: entry["value"] for entry in entries}
        )


def _time_type_iterator(arr, type_, row_offset=0):
    values = _primitive_type_iterator(arr, 8, "q", row_offset)
    tz = type_[4:]
    tz_str = tz.decode("utf8")
    if any(type_.startswith(s) for s in (b"tss:", b"tsm:", b"tsu:")):
        if tz_str:
            tzinfo_ = timezone(tz_str)
        else:
            tzinfo_ = None
        if type_.startswith(b"tss:"):
            unit = 1.0
        elif type_.startswith(b"tsm:"):
            unit = 1e-3
        else:
            unit = 1e-6
        return map(
            lambda x: (
                datetime.fromtimestamp(x * unit, tz=tzinfo_) if x is not None else None
            ),
            values,
        )
    elif type_.startswith(b"tsn:"):
        try:
            lazy_import_timestamp()
        except ImportError:
            raise ImportError("'pandas' must be installed to use nanosecond timestamps")
        if not tz_str:
            tz_str = None
        return map(
            lambda x: Timestamp(ts_input=x, tz=tz_str) if x is not None else None,
            values,
        )


def _date64_type_iterator(arr, row_offset=0):
    values = _primitive_type_iterator(arr, 8, "q", row_offset)
    return map(
        lambda x: datetime.fromtimestamp(x * 1e-3) if x is not None else None, values
    )


def _date_type_iterator(arr, row_offset=0):
    # 32-bit uint representing days since epoch
    values = _primitive_type_iterator(arr, 4, "I", row_offset)

    # Need this function instead of a simple date.fromtimestamp(x * 86400) because:
    # 1. fromtimestamp() uses local timezone which can cause dates to shift by a day
    # 2. We need proper error handling for invalid/sentinel date values
    def _safe_date_convert(x):
        if x is None:
            return None
        try:
            # Convert days since Unix epoch (1970-01-01) to date
            from datetime import timedelta

            return date(1970, 1, 1) + timedelta(days=int(x))
        except (ValueError, OverflowError, OSError, TypeError):
            # If it fails, this is likely a sentinel value for null dates
            # Return None to match pandas null date handling
            return None

    return map(_safe_date_convert, values)


ITERATOR_TYPES = {
    b"b": (_boolean_type_iterator,),  # bool
    b"c": (_primitive_type_iterator,),  # int8
    b"C": (_primitive_type_iterator,),  # uint8
    b"s": (_primitive_type_iterator,),  # int16
    b"S": (_primitive_type_iterator,),  # uint16
    b"i": (_primitive_type_iterator,),  # int32
    b"I": (_primitive_type_iterator,),  # uint32
    b"l": (_primitive_type_iterator,),  # int64
    b"L": (_primitive_type_iterator,),  # uint64
    b"f": (_primitive_type_iterator,),  # float32
    b"g": (_primitive_type_iterator,),  # float64
    b"z": (_binary_type_iterator, False, False),  # binary
    b"Z": (_binary_type_iterator, True, False),  # large binary
    b"u": (_binary_type_iterator, False, True),  # utf-8 string
    b"U": (_binary_type_iterator, True, True),  # large utf-8 string
    b"+l": (_list_type_iterator, 4, "i"),  # small list type
    b"+L": (_list_type_iterator, 8, "q"),  # large list type
    b"+s": (_struct_type_iterator,),  # struct type
    b"+m": (_map_type_iterator,),  # map type
    b"tss": (_time_type_iterator,),  # timestamp second iterator type
    b"tsm": (_time_type_iterator,),  # timestamp milli iterator type
    b"tsu": (_time_type_iterator,),  # timestamp micro iterator type
    b"tsn": (_time_type_iterator,),  # timestamp nano iterator type
    b"tdD": (_date_type_iterator,),  # days since UNIX epoch 1970-01-01
    b"tdm": (_date64_type_iterator,),  # millis since UNIX epoch 1970-01-01
}


def is_supported_type(schema):
    """
    Check whether an ArrowSchema has a type which can be parsed by md_iterators.
    This includes checking for types of sub-schemas.
    """
    type_ = type_info.arrow_type_str(schema)

    if type_ in (b"+l", b"+L"):
        return is_supported_type(schema.children[0])
    elif type_ == b"+s":
        return all(
            is_supported_type(schema.children[i]) for i in range(schema.n_children)
        )
    elif type_ == b"+m":
        # Map is equivalent to List<item: Struct<key: K, value: V>>
        # Validate by treating it as a list.
        return is_supported_type(schema.children[0])
    else:
        return type_ in ITERATOR_TYPES


def carray_iterator(arr, schema, offset=0):
    type_ = type_info.arrow_type_str(schema)
    iter_spec = ITERATOR_TYPES[type_]
    if type_ in (b"+l", b"+L"):
        child_iter = carray_iterator(arr.children[0], schema.children[0])
        child_length = arr.children[0].length
        return iter_spec[0](
            arr, child_iter, child_length, *iter_spec[1:], row_offset=offset
        )
    elif type_ == b"+s":
        child_iter_dict = {}
        for c_idx in range(schema.n_children):
            child_name = ffi.string(schema.children[c_idx].name).decode("utf-8")
            child_iter = carray_iterator(arr.children[c_idx], schema.children[c_idx])
            child_iter_dict[child_name] = child_iter
        return iter_spec[0](arr, child_iter_dict, *iter_spec[1:], row_offset=offset)
    elif type_ == b"+m":
        entries_schema = schema.children[0]
        entries_arr = arr.children[0]
        entries_iter = carray_iterator(entries_arr, entries_schema)
        return iter_spec[0](
            arr, entries_iter, entries_arr.length, *iter_spec[1:], row_offset=offset
        )
    elif type_.startswith(b"ts"):
        return iter_spec[0](
            arr, ffi.string(schema.format), *iter_spec[1:], row_offset=offset
        )
    elif type_.startswith(b"td"):
        return iter_spec[0](arr, *iter_spec[1:], row_offset=offset)
    elif type_info.is_scalar(type_):
        type_meta = type_info.TYPE_META[type_]
        return iter_spec[0](
            arr,
            *iter_spec[1:],
            type_meta.byte_width,
            type_meta.array_type,
            row_offset=offset
        )
    else:
        return iter_spec[0](arr, *iter_spec[1:], row_offset=offset)


def iterator(chunked_array, schema, offset=0):
    chunk_offset = 0

    # skip chunks before offset
    if offset > 0:
        total_rows = 0
        for chunk_offset in range(chunked_array.n_chunks):
            n_rows = chunked_array.chunks[chunk_offset].length
            if offset < total_rows + n_rows:
                offset -= total_rows
                break
            total_rows += n_rows

    for i in range(chunk_offset, chunked_array.n_chunks):
        chunk_iter = carray_iterator(chunked_array.chunks[i], schema, offset)
        for value in chunk_iter:
            yield value
        offset = 0
