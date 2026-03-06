from typing import TYPE_CHECKING, Any, List, Optional, Iterator, Tuple

from .exceptions import MetaflowDataFrameException
from . import type_info
from . import size_info

"""
Column API for reading data from MetaflowDataFrame with accelerated
conversion functions for common operations.

All columns types expose an iterator interface which converts
individual elements to python objects.  Some column types additionally
expose optimized conversions to numpy arrays, which may be useful
when passing data to another framework without row-by-row processing.
"""

# Type stubs for dynamically imported modules
MD: Optional[Any] = None
ffi: Optional[Any] = None
as_array: Optional[Any] = None
iterator: Optional[Any] = None
unpack_boolean: Optional[Any] = None
promote_struct_field: Optional[Any] = None
_is_md_import_complete = False

np: Optional[Any] = None

if TYPE_CHECKING:
    import numpy

    # Forward reference for MetaflowDataFrame
    class MetaflowDataFrame: ...


def lazy_import_MD():
    global _is_md_import_complete, MD, ffi, as_array, iterator, unpack_boolean, promote_struct_field
    if not _is_md_import_complete:
        from .md import ffi, MD
        from .md_iterators import as_array, iterator, unpack_boolean
        from .ops import promote_struct_field

        _is_md_import_complete = True


def lazy_import_np():
    global np
    try:
        import numpy as np
    except ImportError:
        raise MetaflowDataFrameException(
            "Numpy required for to_numpy() methods.  Please install numpy"
        )


def _metaflow_column(mf_chunked_array, mdf_cls):
    lazy_import_MD()
    assert ffi is not None and MD is not None  # Ensure lazy imports worked (for mypy)
    schema = ffi.gc(MD.md_c_schema_from_array(mf_chunked_array), MD.md_release_schema)
    type_ = ffi.string(schema.format)[:3]
    args = (mf_chunked_array, schema, mdf_cls)
    if type_ in (b"+l", b"+L"):
        return ListMetaflowColumn(*args)
    elif type_ == b"+s":
        return StructMetaflowColumn(*args)
    elif type_ == b"+m":
        return MapMetaflowColumn(*args)
    elif type_ in (b"u", b"U", b"z", b"Z"):
        return BinaryMetaflowColumn(*args)
    else:
        return PrimitiveMetaflowColumn(*args)


def _check_for_column_type(column, column_type) -> bool:
    """
    Check if a column type is present in a column, recursively

    Parameters
    ----------
    column : MetaflowColumn
        The column to check
    column_type : str
        The type to check for

    Returns
    -------
    bool
        True if the column contains the type, False otherwise
    """
    if isinstance(column, column_type):
        return True

    if isinstance(column, StructMetaflowColumn):
        return all(
            [
                _check_for_column_type(column[struct_col_name], column_type)
                for struct_col_name in column.fields
            ]
        )
    elif isinstance(column, ListMetaflowColumn):
        return _check_for_column_type(column.flatten(), column_type)
    elif isinstance(column, MapMetaflowColumn):
        return _check_for_column_type(
            column.map_keys(), column_type
        ) or _check_for_column_type(column.map_values(), column_type)

    return False


class MetaflowColumn(object):
    """
    Base class representing one column in a dataframe.
    """

    def __init__(self, mf_chunked_array, schema, mdf_cls):
        """
        Constructor

        @param mf_chunked_array: CFFI CMetaflowChunkedArray object
        @param schema: CFFI ArrowSchema pointing to this column's schema
        @param mdf_cls: MetaflowDataFrame class
        """
        self._mf_chunked_array = mf_chunked_array
        assert (
            ffi is not None and MD is not None
        )  # Ensure lazy imports worked (for mypy)
        self._c_chunked_array = ffi.gc(
            MD.md_c_chunked_array(mf_chunked_array), MD.md_release_chunked_array
        )
        self._schema = schema
        self._name = ffi.string(schema.name).decode("utf-8")
        self._mdf_cls = mdf_cls

        typeclass = ffi.string(schema.format)
        typeclass = typeclass[0:3]
        self._is_scalar = True
        self._is_array = False
        if typeclass.startswith(b"+"):
            self._is_scalar = False
        if typeclass.upper() == b"+L":
            self._is_array = True

        self._value_type = ffi.string(schema.format)

        self._field_id = MD.md_get_field_id(self._mf_chunked_array)

    def __len__(self):
        return sum(
            self._c_chunked_array.chunks[i].length
            for i in range(self._c_chunked_array.n_chunks)
        )

    def __iter__(self) -> Iterator[Any]:
        """
        Iterate over values of the column.

        Yields
        ------
        Any
            One value in this column
        """
        return self.values()

    @property
    def name(self) -> str:
        """
        Returns the name of the column.

        Returns
        -------
        str
            Name of the column
        """
        return self._name

    @property
    def field_id(self) -> int:
        """
        Returns the field id of the column or -1 if one is not set

        Returns
        -------
        int
            Field id of the column
        """
        return self._field_id

    @property
    def is_binary(self) -> bool:
        """
        Returns a boolean indicating whether this column is of type BINARY,
        i.e. it includes arbitrary binary data.

        Returns
        -------
        bool
            True if the column contains binary data (ie: arbitrary binary data)
        """
        return self._value_type.upper() == b"Z"

    @property
    def is_array_type(self) -> bool:
        """
        Returns a boolean indicating whether this column is of type ARRAY

        Returns
        -------
        bool
            True if the column is of type ARRAY
        """
        return self._is_array

    @property
    def is_scalar_type(self) -> bool:
        """
        Returns a boolean indicating whether this column is of scalar type

        Returns
        -------
        bool
            True if the column is of scalar type
        """
        return self._is_scalar

    def values(self, offset: int = 0) -> Iterator[Any]:
        """
        Iterate over values of the column.

        Parameters
        ----------
        offset : int, default 0
            Offset at which to start returning values.

        Yields
        ------
        Any
            One value in this column
        """
        if offset > len(self):
            raise MetaflowDataFrameException(
                "Iterator offset value must be less than or "
                "equal the length of the array"
            )
        assert iterator is not None  # Ensure lazy imports worked
        return iterator(self._c_chunked_array, self._schema, offset)

    @property
    def size(self):
        """
        Size in bytes of the column

        Parameters
        ----------

        Returns
        ------
        int
            Size in bytes of the column
        """
        return size_info.nbytes(self._c_chunked_array, self._schema)

    @property
    def null_count(self) -> int:
        """
        Returns the number of null entries in this column

        Returns
        -------
        int
            The number of null entries in this column.
        """
        return sum(
            self._c_chunked_array.chunks[i].null_count
            for i in range(self._c_chunked_array.n_chunks)
        )

    def split(self) -> List["MetaflowColumn"]:
        """
        Returns a list of MetaflowColumn objects with one for each memory
        chunk in this column. Each chunk returned will therefore be contained in
        contiguous memory.

        Returns
        -------
        List[MetaflowColumn]
            Contiguous memory chunks forming this column
        """
        assert (
            ffi is not None and MD is not None
        )  # Ensure lazy imports worked (for mypy)
        n_chunks = self._c_chunked_array.n_chunks
        chunks = []
        for i in range(n_chunks):
            chunk = ffi.gc(
                MD.md_get_chunk(self._mf_chunked_array, i), MD.md_free_chunked_array
            )
            chunks.append(_metaflow_column(chunk, self._mdf_cls))
        return chunks

    def is_same_chunking(self, other: "MetaflowColumn") -> bool:
        """
        Check if two columns have the same chunking, meaning that they
        have the same number of memory chunks and the chunks at each index
        are the same size.
        Parameters
        ----------
        other : MetaflowColumn
            The other column to compare to
        Returns
        -------
        bool
            True if the columns have the same chunking, False otherwise
        """
        if self._c_chunked_array.n_chunks != other._c_chunked_array.n_chunks:
            return False
        for i in range(self._c_chunked_array.n_chunks):
            if (
                self._c_chunked_array.chunks[i].length
                != other._c_chunked_array.chunks[i].length
            ):
                return False
        return True


class PrimitiveMetaflowColumn(MetaflowColumn):
    """
    Column with primitive entries, e.g. ints, floats, or bools.
    """

    def to_numpy(
        self,
        null_value: Optional[Any] = None,
        as_type: Optional["numpy.dtype"] = None,
        as_view: bool = False,
    ) -> "numpy.ndarray":
        """
        Convert this column to a single 1-d numpy array.

        Parameters
        ----------
        null_value : Any, optional, default None
            Value to insert for null entries.  If this
            column has null values and no null_value is provided this
            method will raise an exception.
        as_type : numpy.dtype, optional, default None
            Cast array to this type instead
            of the underlying arrow type.  If as_type and null_value are
            both set then null_value must be a valid value in the target
            type.
        as_view : bool, default False
            If True, return a read-only view of the data instead of
            making a copy. This method will fail if the
            underlying data cannot be returned as a view.  This can
            occur if:
                1) This column contains null values
                2) This column is backed by more than 1 chunk of memory
                3) as_type is provided and does not match the underlying
                    arrow type.
                4) This column has type bool

        Returns
        -------
        numpy.ndarray
            The 1-d numpy array representation of this column
        """
        lazy_import_np()
        if null_value is None and self.null_count > 0:
            raise MetaflowDataFrameException(
                f"Converting column: {self.name} containing null entries "
                f"to numpy with no default null value"
            )

        n_chunks = self._c_chunked_array.n_chunks
        if as_view:
            if self.null_count > 0:
                raise MetaflowDataFrameException(
                    f"Converting column: {self.name} to numpy view but "
                    f"column contains null entries"
                )
            if n_chunks > 1:
                raise MetaflowDataFrameException(
                    f"Converting column: {self.name} to numpy as view but "
                    f"column has multiple chunks.  Try col.split()"
                )
            assert np is not None  # Ensure numpy imported
            if self._numpy_dtype() == np.bool_:
                raise MetaflowDataFrameException(
                    f"Cannot convert column: {self.name} with type bool to "
                    f"numpy view"
                )
            if as_type is not None and (self._numpy_dtype() != np.dtype(as_type)):
                raise MetaflowDataFrameException(
                    f"Cannot cast column: {self.name} from type: "
                    f"{self._numpy_dtype()} to {as_type} when taking numpy view"
                )

        if n_chunks == 0:
            assert np is not None  # Ensure numpy imported
            empty = np.empty(shape=(0,), dtype=self._numpy_dtype())
            if as_view:
                empty.flags.writeable = False
            return empty
        elif n_chunks == 1:
            # Avoid extra copies for a single chunk
            return self._numpy_chunk(null_value, 0, as_type, as_view)

        merged: Optional["numpy.ndarray"] = None
        offset = 0
        for chunk_idx in range(n_chunks):
            np_chunk = self._numpy_chunk(null_value, chunk_idx, as_type, as_view)
            if merged is None:
                assert np is not None  # Ensure numpy imported
                merged = np.zeros_like(np_chunk, shape=len(self))
            chunk_len = len(np_chunk)
            merged[offset : (offset + chunk_len)] = np_chunk
            offset += chunk_len

        assert merged is not None
        return merged

    def _numpy_chunk(self, null_value, chunk_idx, as_type, as_view):
        """
        Convert chunk to a numpy array
        """
        type_ = ffi.string(self._schema.format)[:3]
        is_bool = type_ == b"b"

        is_view = True

        c_chunk = self._c_chunked_array.chunks[chunk_idx]
        assert unpack_boolean is not None  # Ensure lazy imports worked
        c_nulls = unpack_boolean(c_chunk) if c_chunk.null_count > 0 else None
        if is_bool:
            is_view = False
            np_array = np.zeros(c_chunk.length, dtype=type_info.numpy_dtype("B"))
            cvals = ffi.cast("uint8_t *", np_array.ctypes.data)
            MD.md_unpack_boolean(c_chunk, 1, False, cvals)
        else:
            type_meta = type_info.get_type_meta(type_)
            assert as_array is not None  # Ensure lazy imports worked
            cvals = as_array(
                c_chunk.buffers[1],
                c_chunk.length,
                c_chunk.offset,
                type_meta.array_type,
                type_meta.byte_width,
            )
            np_array = np.asarray(cvals)
            # Ensure Arrow data is not overwritten
            np_array.flags.writeable = False

        if as_type is not None and as_type != self._numpy_dtype():
            np_array = np_array.astype(as_type)
            is_view = False

        if c_nulls is not None:
            if is_view:
                np_array = np_array.copy()  # Copy before overwriting
                is_view = False
            null_indices = np.nonzero(as_array(c_nulls, len(c_nulls)))[0]
            np_array[null_indices] = null_value

        if as_view:
            np_array.flags.writeable = False
        elif is_view:
            np_array = np_array.copy()

        return np_array

    def _numpy_dtype(self):
        return type_info.numpy_dtype(
            type_info.get_type_meta(self._value_type).array_type
        )


class BinaryMetaflowColumn(MetaflowColumn):
    """
    Column with either binary or utf-8 string entries.
    """


class ListMetaflowColumn(MetaflowColumn):
    def flatten(self) -> "MetaflowColumn":
        """
        Returns a flattened view of this column.

        May copy data if column has null entries.

        Returns
        -------
        MetaflowColumn
            A flattened view of this column; this may incur a copy
        """
        assert (
            ffi is not None and MD is not None
        )  # Ensure lazy imports worked (for mypy)
        flattened_mf_array = ffi.gc(
            MD.md_flatten_list_array(self._mf_chunked_array), MD.md_free_chunked_array
        )
        return _metaflow_column(flattened_mf_array, self._mdf_cls)

    def to_numpy(
        self,
        shape: Tuple[int, ...],
        pad_value: Optional[Any] = None,
        null_value: Optional[Any] = None,
        as_type: Optional["numpy.dtype"] = None,
        as_view: bool = False,
    ) -> "numpy.ndarray":
        """
        Convert this column to a read-only numpy array, with each list entry being
        converted to a tensor with given shape, in row-major order.  Lists
        larger than total shape size are truncated, while lists shorter than
        total shape size are suffixed with copies of pad_value.

        This method is only valid for lists of primitive types.

        Parameters
        ----------
        shape : Tuple[int, ...]
            Tuple of dimensions specifying the shape of returned data.
        pad_value : Any, optional, default None
            Value to add at the end of arrays if the
            underlying list is smaller than output shape. This method
            will raise an exception if there are rows which require padding
            and no pad_value is provided.
        null_value : Any, optional, default None
            Value to insert for null entries. This
            method will raise an exception if there are null entries and
            no null_value is provided.
        as_type : numpy.dtype, optional, default None
            Cast array to this type instead
            of the underlying arrow type. If as_type and null_value or
            pad_value are set then null_value/pad_value must be a valid
            value in the target type.
        as_view : bool, default False
            Return a read-only view of the data instead of
            making a copy (default). This method will fail if the
            underlying data cannot be returned as a view. This can
            occur if:
                1) This column contains null values
                2) This column is backed by more than 1 chunk of memory
                3) as_type is provided and does not match the underlying
                    arrow type.
                4) This column has type bool
                5) Any row needs to be padded or truncated

        Returns
        -------
        numpy.ndarray
            The numpy array representation of this column
        """
        lazy_import_np()
        dtype = self._child_dtype()
        if dtype is None:
            assert ffi is not None  # Ensure lazy imports worked
            child_type = ffi.string(self._schema.children[0].format)
            raise MetaflowDataFrameException(
                f"Cannot convert list of non-primitive arrow type: {child_type} to numpy"
            )

        n_chunks = self._c_chunked_array.n_chunks
        if as_view:
            if n_chunks > 1:
                raise MetaflowDataFrameException(
                    f"Converting column: {self.name} to numpy as view but "
                    f"column has multiple chunks.  Try col.split()"
                )
            assert np is not None  # Ensure numpy imported
            if dtype == np.bool_:
                raise MetaflowDataFrameException(
                    f"Cannot convert column: {self.name} with type bool to "
                    f"numpy view"
                )
            # TODO: see issue with np_array.dtype vs. self._numpy_dtype()
            if as_type is not None and (dtype != np.dtype(as_type)):
                raise MetaflowDataFrameException(
                    f"Cannot cast column: {self.name} from type: "
                    f"{dtype} to {as_type} when taking numpy view"
                )

        if n_chunks == 0:
            assert np is not None  # Ensure numpy imported
            empty = np.empty(shape=(0, *shape), dtype=dtype)
            if as_view:
                empty.flags.writeable = False
            return empty
        elif n_chunks == 1:
            return self._numpy_chunk(shape, pad_value, null_value, 0, as_type, as_view)

        merged: Optional["numpy.ndarray"] = None
        offset = 0
        for chunk_idx in range(n_chunks):
            np_chunk = self._numpy_chunk(
                shape, pad_value, null_value, chunk_idx, as_type, as_view
            )
            if merged is None:
                assert np is not None  # Ensure numpy imported
                merged = np.zeros_like(np_chunk, shape=(len(self), *shape))
            chunk_len = np_chunk.shape[0]
            merged[offset : (offset + chunk_len)] = np_chunk
            offset += chunk_len

        assert merged is not None
        return merged

    def _numpy_chunk(self, shape, pad_value, null_value, chunk_idx, as_type, as_view):
        """
        Convert chunk to numpy array
        """
        child_type = ffi.string(self._schema.children[0].format)
        is_bool = child_type == b"b"
        type_meta = type_info.get_type_meta(child_type)
        byte_width = type_meta.byte_width
        value_type = type_meta.array_type
        dtype = self._child_dtype()
        itemsize = int(np.prod(shape))

        chunk = self._c_chunked_array.chunks[chunk_idx]
        # Check for nulls and padding
        if chunk.children[0].null_count > 0:
            if null_value is None:
                raise MetaflowDataFrameException(
                    f"Converting column: {self.name} containing null entries "
                    f"to numpy with no default null value"
                )
            if as_view:
                raise MetaflowDataFrameException(
                    f"Converting column: {self.name} to numpy view but "
                    f"column contains null entries"
                )

        needs_padding = ffi.new("bool *")
        needs_truncating = ffi.new("bool *")
        MD.md_check_list_lengths(
            chunk, self._schema, itemsize, needs_padding, needs_truncating
        )

        if needs_padding[0]:
            if pad_value is None:
                raise MetaflowDataFrameException(
                    f"Converting column: {self.name} with padding required but "
                    f"no default pad value"
                )
            if as_view:
                raise MetaflowDataFrameException(
                    f"Converting column: {self.name} to numpy view but "
                    f"column requires padding"
                )

        dtype = np.dtype(as_type) if as_type is not None else self._child_dtype()

        # Taking a view is unsafe safe if:
        # - Values are bit-packed (bool)
        # - Any lists are the wrong length
        # - Any lists are null (empty)
        # - Storage and view types don't match
        can_use_view = not (
            is_bool
            or needs_padding[0]
            or needs_truncating[0]
            or chunk.null_count > 0
            or dtype != type_info.numpy_dtype(value_type)
        )
        if can_use_view:
            is_view = True
            # Use view if possible even if not requested to avoid
            # slower list_to_tensor copy.
            out_array = as_array(
                chunk.children[0].buffers[1],
                chunk.length * itemsize,
                chunk.children[0].offset,
                value_type,
                byte_width,
            )
            np_array = np.asarray(out_array)
            # Ensure arrow data is not overwritten
            np_array.flags.writeable = False

            if dtype != np_array.dtype:
                np_array = np_array.astype(dtype)
                is_view = False

            if chunk.children[0].null_count > 0:
                if is_view:
                    np_array = np_array.copy()  # Copy before overwriting
                    is_view = False
                c_nulls = unpack_boolean(chunk.children[0])
                null_indices = np.nonzero(as_array(c_nulls, chunk.length * itemsize))[0]
                np_array[null_indices] = null_value

            np_array = np_array.reshape((-1, *shape))

            if as_view:
                np_array.flags.writeable = False
            elif is_view:
                np_array = np_array.copy()

            return np_array
        else:
            # Check that null_value and pad_value match target type
            if null_value is not None and not type_info.value_matches_dtype(
                null_value, dtype
            ):
                raise MetaflowDataFrameException(
                    f"Converting column: {self.name} to numpy, null value: "
                    f"{null_value} incompatible with target dtype: {dtype}"
                )
            if pad_value is not None and not type_info.value_matches_dtype(
                pad_value, dtype
            ):
                raise MetaflowDataFrameException(
                    f"Converting column: {self.name} to numpy, pad value: "
                    f"{pad_value} incompatible with target dtype: {dtype}"
                )

            out_np = np.zeros(chunk.length * itemsize, dtype=dtype)
            out = ffi.cast("uint8_t *", out_np.ctypes.data)
            target_arrow_type = type_info.arrow_type(dtype)
            if isinstance(target_arrow_type, str):
                target_arrow_type = target_arrow_type.encode("utf-8")

            MD.md_list_to_tensor(
                chunk,
                self._schema,
                itemsize,
                target_arrow_type,
                # CFFI requires a double value for null_value and pad_value.
                # null_value and pad_value are checked for None-correctness
                # above.
                null_value if null_value is not None else 0.0,
                pad_value if pad_value is not None else 0.0,
                out,
            )
            return out_np.reshape((-1, *shape))

    def _child_dtype(self):
        """
        Returns the numpy dtype of the child array if the child array is
        primitive, otherwise returns None.
        """
        child_type = ffi.string(self._schema.children[0].format)
        child_meta = type_info.TYPE_META.get(child_type)
        if child_meta is None:
            return None
        else:
            return type_info.numpy_dtype(child_meta.array_type)

    def is_tensor(self) -> bool:
        """
        Check if the list is a well-formed tensor, i.e. this
        is a list type (possibly nested) where the base type is a primitive
        and that the tensor is not ragged.
        Example:
        [[1, 2], [3, 4]] is a well-formed tensor
        [1, 2, 3, 4] is a well-formed tensor
        [[1, 2], [3]] is not a well-formed tensor

        Returns
        -------
        bool
            True if the list is a well-formed tensor, False otherwise
        """
        # TODO: consider moving this loop into C++ layer
        n_chunks = self._c_chunked_array.n_chunks
        for chunk_idx in range(n_chunks):
            if not self._is_well_formed_tensor_chunk(chunk_idx):
                return False
        return True

    def _is_well_formed_tensor_chunk(self, chunk_idx) -> bool:
        assert (
            ffi is not None and MD is not None
        )  # Ensure lazy imports worked (for mypy)
        chunk = self._c_chunked_array.chunks[chunk_idx]
        is_well_formed = ffi.new("bool *")
        MD.md_check_tensor_format(chunk, self._schema, is_well_formed)
        return is_well_formed[0]

    def tensor_shape(self) -> List[int]:
        """
        Extract the shape of the tensor.
        Assumes this is a well-formed tensor which you can call is_tensor() to check.
        Example:
        [[1, 2, 3], [4, 5, 6]] has shape [2, 3]
        [1, 2, 3, 4] has shape [4]

        Returns
        -------
        List[int]
            The shape of the tensor
        """
        # calculate the number of dimensions in this tensor
        assert (
            ffi is not None and MD is not None
        )  # Ensure lazy imports worked (for mypy)
        num_dims = 1
        curr_schema = self._schema
        curr_type = ffi.string(self._schema.format)
        while curr_type == b"+l" or curr_type == b"+L":
            num_dims += 1
            curr_schema = curr_schema.children[0]
            curr_type = ffi.string(curr_schema.format)

        if self._c_chunked_array.n_chunks == 0:
            return [0] * num_dims

        if num_dims == 1:
            return [len(self)]

        # calculate the shape for each chunk and
        # ensure the inner tensor shape is consistent across all chunks
        c_shape = ffi.new("int64_t[]", num_dims)
        first_chunk = self._c_chunked_array.chunks[0]
        MD.md_get_tensor_shape(first_chunk, num_dims, c_shape)
        first_shape = ffi.unpack(c_shape, num_dims)

        for chunk_idx in range(1, self._c_chunked_array.n_chunks):
            chunk = self._c_chunked_array.chunks[chunk_idx]
            # c_shape is re-initialized within c function
            MD.md_get_tensor_shape(chunk, num_dims, c_shape)
            shape_chunk = ffi.unpack(c_shape, num_dims)
            if shape_chunk[1:] != first_shape[1:]:
                raise MetaflowDataFrameException(
                    "Tensor shape is not consistent across all rows in "
                    f"column: {self.name}"
                )

        return [len(self)] + first_shape[1:]


class StructMetaflowColumn(MetaflowColumn):
    """
    Nested column with named child columns.
    """

    def __init__(self, *args):
        super().__init__(*args)
        schema = self._schema
        self._child_name_to_idx = {}
        for i in range(schema.n_children):
            name = ffi.string(schema.children[i].name).decode("utf-8")
            self._child_name_to_idx[name] = i

    def __getitem__(self, name: str) -> MetaflowColumn:
        """
        Returns a child column by name

        Parameters
        ----------
        name : str
            Name of the child column to return

        Returns
        -------
        MetaflowColumn
            Child column
        """
        if name not in self._child_name_to_idx:
            raise MetaflowDataFrameException(
                "Invalid column name: {}, expecting one of: {}".format(
                    name, ",".join(self._child_name_to_idx.keys())
                )
            )
        return self._child_column(self._child_name_to_idx[name])

    @property
    def fields(self) -> List[str]:
        """
        Returns a list of all child field names

        Returns
        -------
        List[str]
            Names of the children columns
        """
        sorted_items = sorted(self._child_name_to_idx.items(), key=lambda p: p[1])
        return [name for name, _ in sorted_items]

    def as_dataframe(self) -> "MetaflowDataFrame":
        """
        Returns a MetaflowDataFrame whose columns are the children of this column

        Returns
        -------
        MetaflowDataFrame
            Dataframe containing the children columns
        """
        assert (
            ffi is not None and MD is not None
        )  # Ensure lazy imports worked (for mypy)
        cmdf = ffi.gc(
            MD.md_df_from_struct_array(self._mf_chunked_array), MD.md_free_dataframe
        )
        return self._mdf_cls([cmdf])

    def _child_column(self, idx):
        assert (
            ffi is not None and MD is not None
        )  # Ensure lazy imports worked (for mypy)
        child_mf_chunked_array = ffi.gc(
            MD.md_get_child(self._mf_chunked_array, idx), MD.md_free_chunked_array
        )
        return _metaflow_column(child_mf_chunked_array, self._mdf_cls)


class MapMetaflowColumn(ListMetaflowColumn):
    """
    Column whose entries are maps with string keys and all values
    of the same type.
    """

    def map_keys(self):
        """
        Return a List column of the keys for each element
        """
        assert promote_struct_field is not None  # Ensure lazy imports worked
        return promote_struct_field(self, "key")

    def map_values(self):
        """
        Return a List column of the values for each element
        """
        return promote_struct_field(self, "value")
