import os
import math
import threading
import csv
import shutil
import sys

from tempfile import NamedTemporaryFile, TemporaryFile, TemporaryDirectory
from itertools import groupby
from collections import OrderedDict
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    TYPE_CHECKING,
    Type,
    Union,
    Tuple,
    BinaryIO,
    TypeVar,
    Generic,
)
import math

from metaflow.util import to_bytes
from metaflow.plugins.datatools.s3 import S3
from metaflow.multicore_utils import parallel_map
from ..datatools.type_info import (
    check_supported_iceberg_schema,
    iceberg_to_pyarrow_schema,
    iceberg_to_arrow_c_schema,
    pyarrow_ensure_all_fields_nullable,
    pyarrow_ensure_no_map_fields,
    proto_to_pyarrow_schema,
    avro_to_pyarrow_schema,
)

from .exceptions import (
    ParquetDecodingException,
    ParquetEncodingException,
    MetaflowDataFrameException,
    MetaflowDataFrameUnknownType,
    MetaflowDataFrameBufferException,
    MetaflowDataFrameBufferNotLargeEnough,
    MetaflowDataFrameBufferEmptyDataFrame,  # noqa: F401
    MetaflowDataFrameInvalidBuffer,
)

# Python 3 only - no need for Python 2 compatibility
import io

BytesIO = io.BytesIO
izip = zip
ifilter = filter
imap = map
# Python3 handles unicode->utf-8 conversion internally
# in the file object, so no explicit conversion is needed
CSV_MODE = {"mode": "w+", "newline": "", "encoding": "utf-8"}
rows_to_unicode = lambda x: x

DEFAULT_READ_THREADS = 16

if TYPE_CHECKING:
    import pandas
    import pyarrow
    import polars  # type: ignore[import-not-found]  # Optional dependency
    import torch  # type: ignore[import-not-found]  # Optional dependency
    import numpy as np
    import numpy
    import fastavro
    import metaflow_extensions.fastdata_ext.plugins.datatools.column
    from .frameworks.polars_utils import check_max_index
    from google.protobuf.message import Message
else:
    # Runtime globals - will be populated by lazy imports
    pyarrow = None  # type: Any
    pandas = None  # type: Any
    polars = None  # type: Any
    torch = None  # type: Any
    np = None  # type: Any
    check_max_index = None  # type: Any
    protobuf = None  # type: Any
    fastavro = None  # type: Any

# These symbols will be populated by lazy_import_MD() at runtime
# For mypy, we declare them as Any so it knows they exist
MD: Any = None
ffi: Any = None
is_supported_type: Any = None
has_dictionary: Any = None
_metaflow_column: Any = None
MetaflowColumn: Any = None
MapMetaflowColumn: Any = None
StructMetaflowColumn: Any = None
BinaryMetaflowColumn: Any = None
_check_for_column_type: Any = None
validate_iceberg_mdf: Any = None
extract_field_ids: Any = None

# Populated by lazy_import_proto
_proto_wrapper_types: Any = None

_is_md_import_complete = False


def lazy_import_MD():  # type: ignore[name-defined]
    """Load MD-related modules and make globals available for mypy."""
    global _is_md_import_complete, MD, ffi, is_supported_type, has_dictionary, _metaflow_column, MetaflowColumn, MapMetaflowColumn, StructMetaflowColumn, BinaryMetaflowColumn, _check_for_column_type, validate_iceberg_mdf, extract_field_ids
    if not _is_md_import_complete:
        from .md import ffi, MD  # type: ignore[name-defined]
        from .md_iterators import is_supported_type  # type: ignore[name-defined]
        from .type_info import has_dictionary  # type: ignore[name-defined]
        from .column import (  # type: ignore[name-defined]
            _metaflow_column,
            MetaflowColumn,
            MapMetaflowColumn,
            StructMetaflowColumn,
            BinaryMetaflowColumn,
            _check_for_column_type,
        )
        from .internal_ops import validate_iceberg_mdf, extract_field_ids  # type: ignore[name-defined]

        _is_md_import_complete = True


def lazy_import_pyarrow():
    global pyarrow
    if pyarrow is None:
        error = False
        error_str = (
            "Conversion to/from a 'pyarrow.Table' requires "
            "pyarrow>=0.17.0 to be installed."
        )

        try:
            import pyarrow
        except ModuleNotFoundError:
            error = True
        except Exception as e:
            error = True
            error_str += (
                "\nAn error occurred importing the systems "
                "version of 'pyarrow':\n{error_str}".format(error_str=str(e))
            )

        if error:
            raise MetaflowDataFrameException(error_str)

        if not hasattr(pyarrow.Schema, "_import_from_c"):
            raise MetaflowDataFrameException(error_str)


def lazy_import_proto():
    global protobuf
    global _proto_wrapper_types
    if protobuf is None:
        error = False
        error_str = (
            "Conversion from protobuf messages requires "
            "google.protobuf to be installed."
        )

        try:
            import google.protobuf as protobuf
            from google.protobuf.wrappers_pb2 import (
                FloatValue,
                DoubleValue,
                Int32Value,
                Int64Value,
                UInt32Value,
                UInt64Value,
                BoolValue,
                StringValue,
                BytesValue,
            )

            _proto_wrapper_types = (
                FloatValue,
                DoubleValue,
                Int32Value,
                Int64Value,
                UInt32Value,
                UInt64Value,
                BoolValue,
                StringValue,
                BytesValue,
            )

        except ModuleNotFoundError:
            error = True
        except Exception as e:
            error = True
            error_str += (
                "\nAn error occurred importing the systems "
                "version of 'google.protobuf':\n{error_str}".format(error_str=str(e))
            )

        if error:
            raise MetaflowDataFrameException(error_str)


def lazy_import_pandas():
    global pandas
    if pandas is None:
        import pandas


def lazy_import_polars():
    assert sys.version_info >= (3, 10), "Polars methods require Python 3.10+"
    global polars, check_max_index
    if polars is None:
        import polars
    from .frameworks.polars_utils import check_max_index


def lazy_import_torch():
    global torch
    if torch is None:
        import torch


def lazy_import_np():
    global np
    try:
        import numpy as np
    except ImportError:
        raise MetaflowDataFrameException(
            "Numpy required for to_numpy() methods.  Please install numpy"
        )


def lazy_import_fastavro():
    global fastavro
    if fastavro is None:
        error = False
        error_str = "Conversion from Avro requires " "fastavro to be installed."

        try:
            import fastavro
        except ModuleNotFoundError:
            error = True
        except Exception as e:
            error = True
            error_str += (
                "\nAn error occurred importing the systems "
                "version of 'fastavro':\n{error_str}".format(error_str=str(e))
            )

        if error:
            raise MetaflowDataFrameException(error_str)


def _has_embedded_avro_schema(binary_data: bytes) -> bool:
    """
    Check if binary data is an Avro Object Container File with embedded schema.

    Avro Object Container Files start with a 4-byte magic header: b'Obj\x01'

    Parameters
    ----------
    binary_data : bytes
        The binary data to check.

    Returns
    -------
    bool
        True if the data appears to be an Avro container file with embedded schema,
        False otherwise.
    """
    return len(binary_data) >= 4 and binary_data[:4] == b"Obj\x01"


def _is_wrapper_type(message):
    """
    Check if a protobuf message is a wrapper type (e.g., google.protobuf.FloatValue).

    Wrapper types are used to make primitive types nullable in proto3.

    Parameters
    ----------
    message : google.protobuf.message.Message
        The protobuf message instance to check.

    Returns
    -------
    bool
        True if the message is a wrapper type, False otherwise.
    """
    return isinstance(message, _proto_wrapper_types)


def _proto_message_to_dict(message):
    """
    Convert a protobuf message to a dictionary by directly extracting field values.

    This preserves native Python types (e.g., int64 stays as int instead of string).

    Parameters
    ----------
    message : google.protobuf.message.Message
        The protobuf message to convert.

    Returns
    -------
    dict
        A dictionary with field names as keys and field values as values.
    """
    lazy_import_proto()

    FieldDescriptor = protobuf.descriptor.FieldDescriptor

    result = {}
    descriptor = message.DESCRIPTOR

    for field in descriptor.fields:
        field_name = field.name
        value = getattr(message, field_name)

        # Handle repeated (maps too)
        if field.label == FieldDescriptor.LABEL_REPEATED:
            # Handle message types
            if field.type == FieldDescriptor.TYPE_MESSAGE:
                # Handle map type
                if field.message_type.GetOptions().map_entry:
                    result[field_name] = dict(value.items())

                # Handle len zero separately
                elif len(value) == 0:
                    result[field_name] = []

                # Handle wrapper type
                elif _is_wrapper_type(value[0]):
                    result[field_name] = [v.value for v in value]

                # Repeated message type
                else:
                    result[field_name] = [_proto_message_to_dict(v) for v in value]

            # Handle repeated primitive types (list of values)
            else:
                result[field_name] = list(
                    value
                )  # Note this doesn't handle repeated wrappers

        # Single message
        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            # wrapper type
            if _is_wrapper_type(value):
                result[field_name] = value.value
            else:
                result[field_name] = _proto_message_to_dict(value)

        # Primitive
        else:
            result[field_name] = value

    return result


def _from_arrow(table):
    lazy_import_MD()
    lazy_import_pyarrow()

    # Get batches
    batches = table.to_batches()
    nbatches = len(batches)

    # Export the schema
    schema = ffi.new("struct ArrowSchema*")
    schema_adr = int(ffi.cast("uintptr_t", schema))
    table.schema._export_to_c(schema_adr)

    batch_adrs = []
    # Export the batches
    for batch in batches:
        array = ffi.new("struct ArrowArray*")
        adr = int(ffi.cast("uintptr_t", array))
        batch._export_to_c(adr)
        batch_adrs.append(array)

    df = MD.md_dataframe_from_record_batches(schema, batch_adrs, nbatches)
    df = ffi.gc(df, MD.md_free_dataframe)
    return df


def _from_parquet_generic(
    urls: List[str],
    columns: Optional[List[str]] = None,
    field_ids: Optional[List[int]] = None,
    schema: Optional[List[int]] = None,
    allow_missing: bool = False,
    num_threads: Optional[int] = None,
    **kwargs,
) -> List["MetaflowDataFrame"]:
    """
    Internal from_parquet function use "from_parquet" and or
    "from_parquet_with_schema"
    """
    lazy_import_MD()

    if num_threads is None:
        num_threads = DEFAULT_READ_THREADS

    if sum(0 if i is None else 1 for i in (columns, field_ids, schema)) > 1:
        raise MetaflowDataFrameException(
            "Only set one of 'columns', 'field_ids' or schemas"
        )

    if columns is not None:
        columns = list(columns)
        if not columns:
            # Metaflow-data will interpret an empty list of columns as
            # all-columns, which is probably not what the user
            # intended. Let's make sure this won't cause silent errors.
            raise MetaflowDataFrameException(
                "if columns is specified, it must be a non-empty list of columns."
            )

        # build a c array of column names
        cols_str = [ffi.new("char[]", c.encode("utf-8")) for c in columns]
        cols_str_array = ffi.new("char*[]", cols_str)

    if field_ids is not None:
        field_ids = list(field_ids)
        if not field_ids:
            # Same as above for field ids, let's avoid silent errors
            raise MetaflowDataFrameException(
                "if 'field_ids' is specified, it must be a "
                "non-empty list of field_ids."
            )

        # build a c array of field ids
        field_ids_int = ffi.new("int32_t[]", field_ids)
        field_ids_int_array = ffi.cast("int32_t*", field_ids_int)

    results = []

    def load_one(paths):
        exception = ffi.new("char[]", EXCEPTION_BUF_LEN)
        for idx, path in paths:
            try:
                if columns:
                    p = MD.md_decode_parquet(
                        path.encode("utf-8"),
                        cols_str_array,
                        ffi.NULL,
                        len(columns),
                        allow_missing,
                        exception,
                    )
                elif field_ids:
                    p = MD.md_decode_parquet(
                        path.encode("utf-8"),
                        ffi.NULL,
                        field_ids_int_array,
                        len(field_ids),
                        allow_missing,
                        exception,
                    )
                elif schema:
                    # Concert iceberg schema to a c-schema, and keep the pointer to the underlyiing objects
                    c_schema, c_objs = iceberg_to_arrow_c_schema(schema)
                    p = MD.md_decode_parquet_with_schema(
                        path.encode("utf-8"), c_schema, exception
                    )
                else:
                    p = MD.md_decode_parquet(
                        path.encode("utf-8"),
                        ffi.NULL,
                        ffi.NULL,
                        0,
                        allow_missing,
                        exception,
                    )

                p = ffi.gc(p, MD.md_free_dataframe)
                exception_str = ffi.string(exception).decode("utf-8")
                if exception_str:
                    raise ParquetDecodingException(exception_str)

                results.append((True, (idx, p)))
            except Exception as ex:
                results.append((False, (ex, path)))

    s3urls = [url for url in urls if url.startswith("s3://")]

    with S3(**kwargs) as s3:
        if s3urls:
            s3objs = {s3obj.url: s3obj.path for s3obj in s3.get_many(s3urls)}
            # we allow a mixture of local and S3 URLs. Their relative order
            # must stay intact
            local_paths = [
                s3objs[url] if url.startswith("s3://") else url for url in urls
            ]
        else:
            local_paths = urls

        # we must retain the original order of urls although loading
        # is performed in an indeterministic order, hence enumerate()
        # and sorted() below
        threads = []
        for paths in iter_batches(list(enumerate(local_paths)), num_threads):
            t = threading.Thread(target=load_one, args=(paths,))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

    failed = [res for success, res in results if not success]
    if failed:
        exc, path = failed[0]
        raise MetaflowDataFrameException(
            "Loading Parquet URL '%s' failed: %s" % (path, exc)
        )
    else:
        return [parq for _, parq in sorted(t for _, t in results)]


EXCEPTION_BUF_LEN = 1024

T_MDF_co = TypeVar("T_MDF_co", covariant=True)


class MetaflowDataFrame(Generic[T_MDF_co]):
    """
    MetaflowDataFrame is a simple data frame object. Consider it a
    bare bones, immutable version of Pandas.
    """

    def __init__(self, cmdfs=None, exception_buffer=None):
        lazy_import_MD()

        if exception_buffer is None:
            self._exception = ffi.new("char[]", EXCEPTION_BUF_LEN)
        else:
            self._exception = exception_buffer

        if cmdfs is None:
            cmdfs = list()

        cmdfs = list(cmdfs)
        if cmdfs:
            if not isinstance(cmdfs[0], ffi.CData):
                cmdfs = [_from_arrow(table) for table in cmdfs]

            cmdfs_array = ffi.new("struct CMetaflowDataFrame*[]", cmdfs)
            self._df = ffi.gc(
                MD.md_stack_dataframes(cmdfs_array, len(cmdfs), self._exception),
                MD.md_free_dataframe,
            )
            exception_str = ffi.string(self._exception).decode("utf-8")
            self._exception[0] = b"\0"
            if exception_str:
                raise MetaflowDataFrameException(exception_str)

            top_schema = ffi.gc(MD.md_c_schema(self._df), MD.md_release_schema)

            column_data = []
            for i in range(top_schema.n_children):
                schema = top_schema.children[i]
                name = ffi.string(schema.name).decode("utf-8")
                col = ffi.gc(MD.md_get_column(self._df, i), MD.md_free_chunked_array)
                column_data.append((name, col))

            self._columns = OrderedDict(
                (name, _metaflow_column(col, MetaflowDataFrame))
                for name, col in column_data
            )

            self._check_columns()
        else:
            self._df = None
            self._columns = {}

    def _check_columns(self):
        invalid_columns = []
        dict_columns = []
        for colname, column in self._columns.items():
            schema = column._schema
            if not is_supported_type(schema):
                invalid_columns.append(colname)
            if has_dictionary(schema):
                dict_columns.append(colname)

        if invalid_columns:
            raise MetaflowDataFrameUnknownType(
                "Dataframe has columns with unsupported types: %s. schema: %s"
                % (invalid_columns, self.to_arrow().to_string())
            )
        if dict_columns:
            raise MetaflowDataFrameUnknownType(
                "Dataframe has columns with dictionary type: %s.  schema: %s"
                % (dict_columns, self.to_arrow().to_string())
            )

    @property
    def size(self):
        """
        Size in bytes of the dataframe. This is an estimate of the data only
        (data buffers, index buffers, null buffers), and does not include
        internal book keeping objects.

        Returns
        -------
        int
            Size in bytes of the data in the dataframe
        """
        return sum(column.size for column in self.columns)

    def to_buffer(self, buffer: Union[BinaryIO, memoryview]) -> int:
        """
        Writes the dataframe to a binary buffer.

        Parameters
        ----------
        buffer : BinaryIO
            The buffer to which to write the record batches. This can
            be a file-like object or a python memoryview object.

        Returns
        -------
        int
            Number of bytes written
        """
        if self._df is None:
            return 0

        num_bytes_written = ffi.new("uint64_t *")

        if not isinstance(buffer, memoryview):
            raise MetaflowDataFrameBufferException(
                "Only memoryview objects are supported for `to_buffer`"
            )

        buffer_size = buffer.nbytes
        c_buffer = ffi.from_buffer(buffer, require_writable=True)
        MD.md_write_dataframe_to_buffer(
            self._df, c_buffer, buffer_size, num_bytes_written, self._exception
        )

        if num_bytes_written[0] == 0:
            raise MetaflowDataFrameBufferNotLargeEnough(
                "Failed to write dataframe to buffer. Buffer may be too small. "
            )

        exception_str = ffi.string(self._exception).decode("utf-8")
        self._exception[0] = b"\0"
        if exception_str:
            raise MetaflowDataFrameBufferException(exception_str)

        return num_bytes_written[0]

    def to_pandas(self) -> "pandas.DataFrame":
        """
        Returns a Pandas dataframe corresponding to this object.
        This should be a relatively cheap call.

        You must have pandas and arrow installed locally in order to use this call.
        This conversion also loses schema information and therefore is considered lossy.
        In order to guarantee consistency, a schema is required in the from_pandas function

        Returns
        -------
        DataFrame
            A pandas dataframe representation of this Metaflow DataFrame.
        """
        lazy_import_MD()

        try:
            lazy_import_pandas()
        except ImportError:
            raise MetaflowDataFrameException(
                "'to_pandas' requires pandas to be installed"
            )

        if self._df is None:
            return pandas.DataFrame()
        else:
            return self.to_arrow().to_pandas()

    def to_polars(
        self, strict_map_type: bool = True, assemble: bool = False
    ) -> "polars.DataFrame":
        """
        Returns a Polars DataFrame corresponding to this object.

        Parameters
        ----------
        strict_map_type : bool, default True
            If True, the function will raise an exception if the dataframe
            contains columns with map types. If False, the function
            will allow the map type columns to be converted to a Polars list of structs.
            This is because Polars does not support map types natively.
        assemble : bool, default False
            If True, instructs Polars to rechunk all data into contiguous memory.
            This causes a copy of the data but may improve performance for subsequent
            operations by ensuring all record batches are in the same memory block.

        Returns
        -------
        polars_dataframe: polars.DataFrame
            A polars DataFrame representation of this Metaflow DataFrame.
        """
        lazy_import_MD()

        if strict_map_type:
            if any(
                [_check_for_column_type(col, MapMetaflowColumn) for col in self.columns]
            ):
                raise MetaflowDataFrameException(
                    "to_polars does not support map types by default "
                    "to support lossless round-trip conversion. "
                    "If you want to convert the map type to a list of structs "
                    "in polars, set strict_map_type=False."
                )

        try:
            lazy_import_polars()
        except ImportError as e:
            raise MetaflowDataFrameException(
                "'to_polars' requires polars to be installed"
            )

        if self._df is None:
            return polars.DataFrame()
        else:
            return polars.from_arrow(self.to_arrow(), rechunk=assemble)

    def to_arrow(self) -> "pyarrow.Table":
        """
        Returns a PyArrow table corresponding to this object.
        This is a zero-copy call.

        Returns
        -------
        Table
            A PyArrow table representation of this Metaflow DataFrame.
        """
        lazy_import_pyarrow()

        if self._df is None:
            return pyarrow.Table.from_arrays([], [])

        c_schema = ffi.gc(MD.md_c_schema(self._df), MD.md_release_schema)
        adr = int(ffi.cast("uintptr_t", c_schema))
        schema = pyarrow.Schema._import_from_c(adr)

        c_batches = ffi.gc(
            MD.md_split_dataframe(self._df, 0), MD.md_release_chunked_array
        )
        batches = []
        for i in range(c_batches.n_chunks):
            adr = int(ffi.cast("uintptr_t", c_batches.chunks[i]))
            batches.append(pyarrow.RecordBatch._import_from_c(adr, schema))

        return pyarrow.Table.from_batches(batches, schema)

    def to_torch_tensors(self, device: str = "cpu") -> Dict[str, "torch.Tensor"]:
        """
        Returns a dictionary of PyTorch tensors corresponding to this MDF.
        All tensors will be on the specified device.
        Torch conversion does not support Binary, Map, or Struct types.
        Nested lists should be well-formed (no ragged arrays)
        Example:
        - An int column with 2 rows will be represented as a tensor with shape (2,).
        - A List[int] column with 2 rows (3 int elements in each row) will
        be represented as a tensor with shape (2, 3).
        - A List[int] column with 2 rows (3 int elements in Row 1 and 4 int
        elements in Row 2) is ragged and will not be supported.

        Parameters
        ----------
        device: str, default 'cpu'
            The device to place the tensors on, following PyTorch conventions.
            Most commonly 'cpu or 'cuda'.

        Returns
        -------
        dict: Dict[str, torch.Tensor]
            A dictionary of PyTorch tensors representation of this Metaflow DataFrame.
            The keys will be the column names and the values will be the corresponding
            PyTorch tensors for each column.
        """
        try:
            lazy_import_torch()
        except ImportError as e:
            raise MetaflowDataFrameException(
                "'to_torch_tensors' requires torch to be installed"
            )

        try:
            lazy_import_np()
        except ImportError as e:
            raise MetaflowDataFrameException(
                "'to_torch_tensors' requires numpy to be installed"
            )

        try:
            lazy_import_pyarrow()
        except ImportError as e:
            raise MetaflowDataFrameException(
                "'to_torch_tensors' requires pyarrow to be installed"
            )

        if self._df is None or len(self) == 0:
            return {}

        self._to_torch_schema_validation()

        result_dict = {}
        # arrow_table = self.to_arrow()
        num_rows = len(self)

        for col_name in self.schema:
            mdf_col = self[col_name]
            if mdf_col.is_array_type:
                # 2+d tensor case
                if not mdf_col.is_tensor():
                    raise MetaflowDataFrameException(
                        f"Column `{col_name}` is not a valid tensor type since it is not "
                        "well-formed with square dimensions. You can check this with "
                        "`mdf[col_name].is_tensor()`"
                    )
                shape = mdf_col.tensor_shape()
                flattened_mf_array = ffi.gc(
                    MD.md_flatten_list_array_recursively(mdf_col._mf_chunked_array),
                    MD.md_free_chunked_array,
                )
                col = _metaflow_column(flattened_mf_array, MetaflowDataFrame)
                try:
                    np_array = col.to_numpy()
                except MetaflowDataFrameException as e:
                    raise MetaflowDataFrameException(
                        "Null values are not allowed in `to_torch_tensors` for column "
                        f"{col_name}"
                    )
            else:
                # 1d tensor case
                try:
                    np_array = mdf_col.to_numpy()
                except MetaflowDataFrameException as e:
                    raise MetaflowDataFrameException(
                        "Null values are not allowed in `to_torch_tensors` for column "
                        f"{col_name}"
                    )
                shape = [num_rows]

            # create torch tensor and reshape into the original shape
            result_dict[col_name] = torch.from_numpy(np_array).reshape(shape).to(device)
        return result_dict

    def _to_torch_schema_validation(self) -> None:
        """
        Throws an exception if the dataframe does not meet the requirements for torch conversion.
        Requirement: dataframe doesn't contain any map, struct, or binary types
        at any level of nesting.
        """
        if any(
            [_check_for_column_type(col, MapMetaflowColumn) for col in self.columns]
        ):
            raise MetaflowDataFrameException(
                "`to_torch_tensors` does not support map types."
            )
        if any(
            [_check_for_column_type(col, StructMetaflowColumn) for col in self.columns]
        ):
            raise MetaflowDataFrameException(
                "`to_torch_tensors` does not support struct types."
            )
        if any(
            [_check_for_column_type(col, BinaryMetaflowColumn) for col in self.columns]
        ):
            raise MetaflowDataFrameException(
                "`to_torch_tensors` does not support binary types."
            )

    def _to_parquet(
        self,
        file_path,
        compression_type="SNAPPY",
        rng=None,
        iceberg_schema=None,
        iceberg_schema_id=0,
        **kwargs,
    ):
        lazy_import_MD()

        # set slice range
        if rng is None:
            rng = (-1, -1)

        # set iceberg schema info
        num_fields = -1
        field_ids = ffi.NULL
        field_paths = ffi.NULL
        if iceberg_schema is not None:
            validate_iceberg_mdf(self, iceberg_schema)
            paths_with_ids = extract_field_ids(iceberg_schema)
            num_fields = len(paths_with_ids)
            field_ids = []
            field_paths = []
            for path, field_id in paths_with_ids:
                field_ids.append(field_id)
                for part in path:
                    field_paths.append(ffi.new("char []", to_bytes(part)))
                field_paths.append(ffi.NULL)

        # Check for legacy int96 in the kwargs
        use_int96_timestamp = kwargs.get("use_int96_timestamp", False)

        # write the parquet file
        MD.md_encode_parquet(
            file_path.encode("utf-8"),
            self._df,
            compression_type.encode("utf-8"),
            rng[0],
            rng[1],
            num_fields,
            field_ids,
            field_paths,
            use_int96_timestamp,
            self._exception,
        )
        exception_str = ffi.string(self._exception).decode("utf-8")
        self._exception[0] = b"\0"
        if exception_str:
            raise ParquetEncodingException(exception_str)

    def to_parquet(
        self,
        url: Optional[str],
        compression_type: str = "SNAPPY",
        iceberg_schema: Optional[List[Dict[str, Any]]] = None,
        iceberg_schema_id: int = 0,
        **kwargs,
    ):
        """
        Writes the entire Metaflow DataFrame as a single parquet file.

        Parameters
        ----------
        url : str
            The location of where to write the parquet file to.
            It can be a local path or a S3 path.
        compression_type : str, default "SNAPPY"
            Compression type to use for the parquet file. It must match one of the types
            listed at
            https://arrow.apache.org/docs/cpp/api/utilities.html#_CPPv4N5arrow11Compression4typeE
        iceberg_schema : List[Dict[str, Any]], optional, default None
            List of iceberg column schemas.
        iceberg_schema_id : int, default 0
            Iceberg schema id, in case of schema evolution.

        Returns
        -------
            None
        """
        if url and url.startswith("s3://"):
            with NamedTemporaryFile(suffix=".parquet") as tmp:
                self._to_parquet(
                    tmp.name,
                    compression_type,
                    None,
                    iceberg_schema,
                    iceberg_schema_id,
                    **kwargs,
                )

                with S3(**kwargs) as s3:
                    s3.put_files([(url, tmp.name)])

        else:
            self._to_parquet(
                url, compression_type, None, iceberg_schema, iceberg_schema_id, **kwargs
            )

    def to_parquet_set(
        self,
        url_template: Optional[str],
        compression_type: str = "SNAPPY",
        max_file_size: int = 128 * 1024**2,
        iceberg_schema: Optional[List[Dict[str, Any]]] = None,
        iceberg_schema_id: int = 0,
        return_num_rows: bool = False,
        **kwargs,
    ) -> Union[List[str], Tuple[List[str], List[int]]]:
        """
        Writes a Metaflow DataFrame to multiple parquet files based on
        a filename template

        Parameters
        ----------
        url_template : str
            The location to write the parquet files. It can be a local path
            or a S3 path. Provide a template that includes the placeholder '{index}'.
            This function will insert the parquet file part number in that
            placeholder
        compression_type : str, default "SNAPPY"
            Compression type to use for the parquet file. It must match one of the types
            listed at
            https://arrow.apache.org/docs/cpp/api/utilities.html#_CPPv4N5arrow11Compression4typeE
        max_file_size: int, default 128MB
            The max file size in compressed bytes when writing parquet
            files. The number of files will be determined by this value.
        iceberg_schema : List[Dict[str, Any]], optional, default None
            List of iceberg column schemas.
        iceberg_schema_id : int, default 0
            Iceberg schema id, in case of schema evolution.
        return_num_rows : bool default False
            Return the number of rows per parquet file as a second return argument

        Returns
        -------
        Union[List[str], Tuple[List[str], List[int]]]
            A list of file paths that were created during parquet writing.
            Note - if return_size=True a List[str], List[int] is returned. The
            first list is the parquet filenames and the second list is the
            number of rows.
        """
        PARQUET_COMPRESSION_RATIO = 0.2
        FILL_WIDTH = 8

        if url_template and "{index}" not in url_template:
            raise MetaflowDataFrameException(
                "Function 'to_parquet_set' requires a template with a "
                "placeholder instead of a filename since it may create "
                "multiple files. Put '{index}' in your file name and this "
                "function will insert the file number at the placeholder "
                "position."
            )

        # return if the dataframe is empty
        if len(self) == 0:
            return []

        # estimate the bytes per row and batch size
        bpr_estimate = self.size / len(self)
        batch_size = math.ceil(
            max_file_size / (bpr_estimate * PARQUET_COMPRESSION_RATIO)
        )
        rows_written = 0
        bytes_written = 0

        # Check path exists
        path, template = os.path.split(url_template or "")
        if not path:
            path = "./"
        if not path.startswith("s3://") and not os.path.isdir(path):
            raise IOError(
                "The base path '%s' must exist before writing a parquet set." % path
            )

        # Write files to a temporary directory and then move them
        with TemporaryDirectory(dir="./", prefix="metaflow-parquet") as tmpdir:
            temp_paths = []
            rows = []
            file_idx = 0
            offset = 0
            while offset < len(self):
                if offset + batch_size >= len(self):
                    batch_size = len(self) - offset
                rng = (offset, batch_size)

                part_num = str(file_idx).zfill(FILL_WIDTH)
                local_path = os.path.join(
                    tmpdir, (template or "").format(index=part_num)
                )
                self._to_parquet(
                    local_path,
                    compression_type,
                    rng,
                    iceberg_schema,
                    iceberg_schema_id,
                    **kwargs,
                )

                temp_paths.append(local_path)
                rows.append(batch_size)

                # update offset and file index
                offset += batch_size
                file_idx += 1

                # update our batch size estimate
                bytes_written += os.stat(local_path).st_size
                rows_written += batch_size
                bpr_estimate = bytes_written / rows_written
                batch_size = math.ceil(max_file_size / bpr_estimate)

            url_paths = []
            for f in temp_paths:
                p, h = os.path.split(f)
                url_paths.append(os.path.join(path, h))

            if path.startswith("s3://"):
                with S3(**kwargs) as s3:
                    s3.put_files(list(zip(url_paths, temp_paths)))
            else:
                for tmpf, f in zip(temp_paths, url_paths):
                    shutil.move(tmpf, f)

        if return_num_rows:
            return url_paths, rows
        else:
            return url_paths

    def take(
        self,
        indicator: Union[
            List[Union[bool, int]],
            "metaflow_extensions.fastdata_ext.plugins.datatools.column.MetaflowColumn",
            "numpy.ndarray",
        ],
        treat_null_as: Optional[bool] = None,
        max_parallel: int = 16,
        min_run_threshold: int = 65536,
    ) -> "MetaflowDataFrame":
        """
        Take rows from this Metaflow DataFrame as determined by `indicator` and
        return a new Metaflow DataFrame.

        Parameters
        ----------
        indicator : Union[List[Union[bool, int]], MetaflowColumn, numpy.ndarray]
            An array of boolean
            or integer values indicating which rows are to be selected
            (True for booleans, and non-zero for integers).
        treat_null_as : bool, optional, default None
            Option to override meaning of null/None in the array to True or False.
            By default, the function will raise an error if null/None values are present
            in the array.
        max_parallel : int, default 16
            Maximum number of threads to use while executing this operation.
        min_run_threshold: int, default 64K
            Minimum number of rows for internal heuristic to take a slice instead of copy.

        Returns
        -------
        MetaflowDataFrame
            A new Metaflow DataFrame extracted from the original one.
        """
        min_run_threshold = min_run_threshold or 64 * 1024
        max_parallel = max_parallel or 16

        if max_parallel < 0:
            raise MetaflowDataFrameException(
                "max_parallel must be positive.  Found: {}".format(max_parallel)
            )
        if min_run_threshold < 0:
            raise MetaflowDataFrameException(
                "min_run_threshold must be positive.  Found: {}".format(
                    min_run_threshold
                )
            )

        if self._df is None:
            raise MetaflowDataFrameException(
                "Please invoke `take` on a non-empty dataframe."
            )

        if len(indicator) != self.num_rows:
            raise MetaflowDataFrameException(
                "The indicator array must have the same number of elements "
                "as the table does rows."
            )

        def _apply_null_raise(ind):
            ret = []
            for i in ind:
                if i is None:
                    raise MetaflowDataFrameException(
                        "Indicator arrays may not contain null values with "
                        "'treat_null_as=None'. Modify the null handling "
                        "argument if you expect your indicator array to "
                        "contain nulls."
                    )
                ret.append(i)
            return ret

        def _apply_null_select(ind):
            return [1 if i is None else i for i in ind]

        def _apply_null_deselect(ind):
            return [0 if i is None else i for i in ind]

        if treat_null_as == None:
            null_policy = -1
            apply_null = _apply_null_raise
        elif treat_null_as == True:
            null_policy = 1
            apply_null = _apply_null_select
        elif treat_null_as == False:
            null_policy = 0
            apply_null = _apply_null_deselect
        else:
            raise ValueError(
                "'treat_null_as' argument is expected to be None, True, or False"
            )

        # Create the indicator
        if isinstance(indicator, list):
            # Apply null conversion logic to the list and create a carray that
            # is used to populate a dataframe
            indicator = apply_null(indicator)
            try:
                indicator_ptr = ffi.new("int64_t[]", indicator)
            except TypeError:
                raise MetaflowDataFrameException(
                    "The indicator array must be a list, a "
                    "numpy.ndarray, or a metaflow column containing "
                    "boolean or integer values"
                )

            indicator_array = MD.md_array_from_data(
                b"indicator", b"l", indicator_ptr, ffi.NULL, len(indicator), False
            )
            indicator_array = ffi.gc(indicator_array, MD.md_free_chunked_array)

        elif isinstance(indicator, MetaflowColumn):
            indicator_array = indicator._mf_chunked_array
            format = ffi.string(indicator._schema.format)
            if format not in (b"b", b"s", b"i", b"l"):
                raise MetaflowDataFrameException(
                    "The indicator array must be a list, a "
                    "numpy.ndarray, or a metaflow column containing "
                    "boolean or integer values"
                )

        else:
            # Could this be a numpy array
            if hasattr(indicator, "ctypes"):
                try:
                    indicator = indicator.astype(  # type: ignore[union-attr]
                        dtype="int64", copy=False, casting="safe"
                    )
                except TypeError:
                    raise MetaflowDataFrameException(
                        "The indicator array must be a list, a "
                        "numpy.ndarray, or a metaflow column containing "
                        "boolean or integer values"
                    )

                # numpy maintains lifetime
                indicator_ptr = ffi.cast("int64_t*", indicator.ctypes.data)
                indicator_array = MD.md_array_from_data(
                    b"indicator", b"l", indicator_ptr, ffi.NULL, len(indicator), False
                )
                indicator_array = ffi.gc(indicator_array, MD.md_free_chunked_array)

            else:
                raise MetaflowDataFrameException(
                    "The indicator array must be a list, a "
                    "numpy.ndarray, or a metaflow column containing "
                    "boolean or integer values"
                )

        out = MD.md_select_rows(
            self._df,
            indicator_array,
            null_policy,
            max_parallel,
            min_run_threshold,
            self._exception,
        )
        out = ffi.gc(out, MD.md_free_dataframe)

        exception_str = ffi.string(self._exception).decode("utf-8")
        self._exception[0] = b"\0"
        if exception_str:
            raise MetaflowDataFrameException(exception_str)

        return MetaflowDataFrame([out])

    def rename(self, column_names: List[str]) -> "MetaflowDataFrame":
        """
        Returns a new MetaflowDataFrame by renaming the columns of this one.

        Throws an exception if the length of the new columns names
        does not match the number of columns in the dataframe

        Parameters
        ----------
        column_names: List[str]
            New names of the columns


        Returns
        -------
        MetaflowDataFrame
            A new MetaflowDataFrame with column names replaced
        """
        if len(column_names) != len(self.columns):
            raise MetaflowDataFrameException(
                "The list of new column names supplied to 'rename()' must be "
                "the same length as the number of columns in the dataframe."
            )

        column_strs = []
        for col in column_names:
            name = col if isinstance(col, bytes) else col.encode("utf-8")
            column_strs.append(ffi.new("char []", name))

        cmdf = ffi.gc(
            MD.md_rename_dataframe(self._df, column_strs, self._exception),
            MD.md_free_dataframe,
        )
        return MetaflowDataFrame([cmdf])

    def drop(self, columns: Union[str, List[str]]):
        """
        Returns a new MetaflowDataFrame by dropping columns from this one.

        Throws an exception if any columns don't exist in this dataframe.

        Parameters
        ----------
        columns: Union[str, List[str]]
            Name of column or columns to drop.


        Returns
        -------
        MetaflowDataFrame
            A new MetaflowDataFrame with the columns dropped from the original one.
        """
        if isinstance(columns, (str, bytes)):
            columns = [columns]

        missing_cols = set(columns) - set(self.schema)
        if missing_cols:
            raise MetaflowDataFrameException(
                (
                    f"drop() called with missing columns: {missing_cols}, "
                    + f"available columns: {self.schema}"
                )
            )

        column_strs = []
        for col in columns:
            name = col if isinstance(col, bytes) else col.encode("utf-8")
            column_strs.append(ffi.new("char []", name))

        cmdf = ffi.gc(
            MD.md_drop_columns(
                self._df, column_strs, len(column_strs), self._exception
            ),
            MD.md_free_dataframe,
        )

        exception_str = ffi.string(self._exception).decode("utf-8")
        self._exception[0] = b"\0"
        if exception_str:
            raise MetaflowDataFrameException(exception_str)

        return MetaflowDataFrame([cmdf])

    def __iter__(self):
        """
        Iterates over rows of the dataframe.
        """
        return self.rows()

    def rows(self, offset=0):
        """
        Iterates over rows of the dataframe.
        """
        return izip(*[col.values(offset=offset) for col in self._columns.values()])

    def __getitem__(self, name):
        """
        Returns a single column of the dataframe.
        """
        return self._columns[name]

    def __len__(self):
        """
        Returns the number of rows in this dataframe.
        """
        return self.num_rows

    def __getstate__(self):
        raise MetaflowDataFrameException(
            "Persisting MetaflowDataFrames is not yet supported."
        )

    @property
    def schema(self) -> List[str]:
        """
        Returns a list of column names for this dataframe.

        Returns
        -------
        List[str]
            Column names for this dataframe.
        """
        return list(self._columns)

    def validate_schema(
        self,
        expected: Type[Any],
        strict: bool = False,
        check_types: bool = False,
    ) -> "MetaflowDataFrame[T_MDF_co]":
        """
        Validate DataFrame columns against an expected dataclass schema.

        This method provides runtime validation that the DataFrame contains
        the expected columns (and optionally types) defined in a dataclass.
        Returns self to enable method chaining.

        Parameters
        ----------
        expected : Type
            A dataclass type defining the expected schema. Each field in the
            dataclass represents an expected column.
        strict : bool, default False
            If True, the DataFrame must have exactly the columns defined in
            the schema (no extra columns allowed). If False (default), extra
            columns are permitted.
        check_types : bool, default False
            If True, validate that column types are compatible with the
            dataclass field type annotations. If False (default), only
            column names are validated.

        Returns
        -------
        MetaflowDataFrame
            Returns self to enable method chaining, e.g.:
            ``mdf.validate_schema(MySchema).to_pandas()``

        Raises
        ------
        MetaflowDataFrameException
            If validation fails (missing columns, extra columns in strict mode,
            or type mismatches when check_types=True).

        Examples
        --------
        Basic usage with a dataclass schema:

        >>> from dataclasses import dataclass
        >>> @dataclass
        ... class UserSchema:
        ...     user_id: int
        ...     name: str
        ...     score: float
        ...
        >>> mdf.validate_schema(UserSchema)  # Validates column names exist

        Strict validation (no extra columns allowed):

        >>> mdf.validate_schema(UserSchema, strict=True)

        Type checking:

        >>> mdf.validate_schema(UserSchema, check_types=True)

        Method chaining:

        >>> df = mdf.validate_schema(UserSchema).to_pandas()
        """
        from .schema_validation import validate_schema, SchemaValidationError

        try:
            validate_schema(
                self._columns,
                set(self.schema),
                expected,
                strict=strict,
                check_types=check_types,
            )
        except SchemaValidationError as e:
            raise MetaflowDataFrameException(str(e)) from e

        return self

    @property
    def columns(
        self,
    ) -> List[
        "metaflow_extensions.fastdata_ext.plugins.datatools.column.MetaflowColumn"
    ]:
        """
        Returns a list of MetaflowColumn objects corresponding to this dataframe.

        Returns
        -------
        List[MetaflowColumn]
            MetaflowColumn objects for this dataframe.
        """
        return list(self._columns.values())

    @property
    def num_rows(self) -> int:
        """
        Returns the number of rows in this dataframe.

        Returns
        -------
        int
            Number of rows in this dataframe.
        """
        if self._df is None:
            return 0
        else:
            for col in self._columns.values():
                return len(col)
            return 0

    @property
    def _table(self):
        """
        Returns the dataframe as a PyArrow Table. This function is provided for
        backward compatibility.
        """
        return self.to_arrow()

    @property
    def _tables(self):
        """
        Returns the dataframe as list of PyArrow Table. This function is
        provided for backward compatibility.
        """
        return [batch.to_arrow() for batch in self.split()]

    def split(self, max_rows: int = 0) -> List["MetaflowDataFrame"]:
        """
        Split a dataframe by its row groups

        Parameters
        ----------
        max_rows : int, default 0
            The maximum number of rows in each split dataframe. If 0, the
            splits are determined by the underlying data storage.

        Returns
        -------
        List[MetaflowDataFrame]
            A list of MetaflowDataFrames.

        """
        return [MetaflowDataFrame([df]) for df in self._split(max_rows)]

    def _split(self, max_rows=0):
        if max_rows < 0:
            raise MetaflowDataFrameException(
                "`max_rows` must be >= 0 when splitting MetaflowDataFrame"
            )

        if self._df is None:
            return []
        else:
            c_chunked_array = ffi.gc(
                MD.md_split_dataframe(self._df, max_rows), MD.md_release_chunked_array
            )
            dfs = []
            for i in range(c_chunked_array.n_chunks):
                # c_schema is consumed during dataframe construction - export a new copy
                # for each chunk.
                c_schema = ffi.gc(MD.md_c_schema(self._df), MD.md_release_schema)
                df = ffi.gc(
                    MD.md_dataframe_from_record_batches(
                        c_schema, [c_chunked_array.chunks[i]], 1
                    ),
                    MD.md_free_dataframe,
                )
                dfs.append(df)

            return dfs

    def _table_batches(self, max_parallel: int):
        if self._df is None:
            return []
        else:
            return map(MetaflowDataFrame, iter_batches(self._split(), max_parallel))

    def parallel_map(
        self, fun: Callable[["MetaflowDataFrame"], Any], max_parallel: int = 16
    ) -> List[Any]:
        """
        Evaluates the given function with shards of this dataframe over
        multiple cores. Use this to evaluate expressions over all rows
        of the dataframe in parallel.

        Parameters
        ----------
        fun : Callable[[MetaflowDataFrame], Any]
            A function that takes one argument, a MetaflowDataFrame representing a
            subset of this dataframe. It may return anything.
        max_parallel : int, default 16
            The maximum number of functions evaluated in parallel

        Returns
        -------
        List[Any]
            A list of results returned by the given function.
        """
        if not max_parallel:
            raise MetaflowDataFrameException("parallel_map requires max_parallel > 0")

        return parallel_map(
            fun, self._table_batches(max_parallel), max_parallel=max_parallel
        )

    def parallel_map_args(
        self,
        fun: Callable[["MetaflowDataFrame", Any], Any],
        args: Iterator[Any],
        max_parallel: int = 16,
    ) -> List[Any]:
        """
        Evaluates the given function with shards of this dataframe over
        multiple cores. Use this to evaluate expressions over all rows
        of the dataframe in parallel.

        Parameters
        ----------
        fun : Callable[[MetaflowDataFrame, Any], Any]
            A function that takes two arguments, a MetaflowDataFrame representing a
            subset of this dataframe, and a value from the args iterator. The function
            may return anything.
        args : Iterator[Any]
            An iterator that provides an additional argument for the function. For
            instance, use `itertools.count()` to give a unique identity for each shard
            evaluated.
        max_parallel : int, default 16
            The maximum number of functions evaluated in parallel

        Returns
        -------
        List[Any]
            A list of results returned by the given function.
        """
        it = izip(self._table_batches(max_parallel), args)
        return parallel_map(fun, it, max_parallel=max_parallel)  # type: ignore[arg-type]

    def to_csv(
        self,
        path: Optional[str] = None,
        fileobj: Optional[io.TextIOBase] = None,
        header: bool = True,
        delimiter: str = ",",
        row_filter: Optional[Callable[[Any], bool]] = None,
        max_parallel: int = 16,
    ):
        """
        Serializes this dataframe as a CSV file.

        Parameters
        ----------
        path : str, optional, default None
            A path for the CSV file written.
        fileobj : io.TextIOBase, optional, default None
            A file object where the CSV file should be written.
        header : bool, default True
            A flag for indicating whether to add a header row or not.
        delimiter : str, default ","
            Delimiter character.
        row_filter : Callable[[Any], bool], optional, default None
            A function that gets a single argument, a row tuple. It should return True
            if the row should be included in the CSV file.
        max_parallel : int, default 16
            The maximum number of cores for parallel processing
        """
        if path:
            fileobj = open(path, **CSV_MODE)  # type: ignore[call-overload]

        tmpfiles = [TemporaryFile(**CSV_MODE) for _ in range(max_parallel)]  # type: ignore[call-overload]
        csvargs = {
            "delimiter": delimiter,
            "quoting": csv.QUOTE_MINIMAL,
            "lineterminator": "\n",
        }

        def write_rows(writer, df):
            if row_filter:
                for row in rows_to_unicode(ifilter(row_filter, df.rows())):
                    writer.writerow(row)
            else:
                for row in rows_to_unicode(df.rows()):
                    writer.writerow(row)

        def shard_to_csv(arg):
            df, tmpfile = arg
            writer = csv.writer(tmpfile, **csvargs)
            write_rows(writer, df)
            tmpfile.flush()

        writer = csv.writer(fileobj, **csvargs)  # type: ignore[arg-type]
        if header:
            writer.writerow(self.schema)
        if max_parallel == 1:
            write_rows(writer, self)
            fileobj.flush()  # type: ignore[union-attr]
        else:
            list(self.parallel_map_args(shard_to_csv, tmpfiles, max_parallel))  # type: ignore[arg-type]
            fast_concat(fileobj, tmpfiles)

        if path:
            fileobj.close()  # type: ignore[union-attr]
        else:
            fileobj.flush()  # type: ignore[union-attr]

    @classmethod
    def _from_polars_schema_validation(
        cls,
        polars_df: "polars.DataFrame",
        pyarrow_schema: Optional["pyarrow.Schema"] = None,
        iceberg_schema: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional["pyarrow.Schema"]:
        """
        Validate the schema for converting a polars DataFrame to MetaflowDataFrame

        Parameters
        ----------
        polars_df : polars.DataFrame
            A polars DataFrame.
        pyarrow_schema : pyarrow.Schema, optional, default None
            A pyarrow schema for the resulting Metaflow DataFrame
        iceberg_schema : List[Dict[str,Any]], optional, default None
            An iceberg schema for the resulting Metaflow DataFrame.
            Example:
               [
               {'type': 'int', 'name':'myintcol'},
               {'type': {'type': 'list', 'element':'int'}, 'name':'mylistcol'}
               ]

            See the iceberg documentation
            (https://iceberg.apache.org/spec/#avro) for a complete description
            of the type specification.

        Returns
        -------
        pyarrow.Schema
            A validated pyarrow.Schema
        """
        if not pyarrow_schema and not iceberg_schema:
            # allow not passing in a schema
            return None

        if iceberg_schema:
            # check if the schema is supported, throw if it's not proper format
            unsupported = check_supported_iceberg_schema(iceberg_schema)  # type: ignore[arg-type]
            if unsupported:
                raise MetaflowDataFrameException(
                    "The requested iceberg schema has unsupported types for "
                    "columns %s." % dict(unsupported)
                )
            # convert from iceberg to pyarrow
            pyarrow_schema = iceberg_to_pyarrow_schema(iceberg_schema)  # type: ignore[arg-type]

        # check we have all the columns
        col_names = pyarrow_schema.names  # type: ignore[union-attr]
        df_col_names = polars_df.columns
        if not (col_names == df_col_names):
            raise MetaflowDataFrameException(
                "Columns in the provided schema do not match the columns in the DataFrame: "
                f"{col_names}, {df_col_names}"
            )
        # check that there is no map type in the schema
        pyarrow_ensure_no_map_fields(pyarrow_schema)
        # check that all fields are nullable, throw if not
        pyarrow_ensure_all_fields_nullable(pyarrow_schema)
        return pyarrow_schema

    @classmethod
    def from_polars(
        cls,
        polars_df: "polars.DataFrame",
        pyarrow_schema: Optional["pyarrow.Schema"] = None,
        iceberg_schema: Optional[List[Dict[str, Any]]] = None,
        max_chunk_size: int = 2 * 10**9,
    ) -> "MetaflowDataFrame":
        """
        Convert a polars DataFrame to a MetaflowDataFrame. Either a pyarrow schema
        or iceberg schema can be provided. If both are provided, the iceberg schema
        will be used. If neither is provided, the function will attempt to infer the
        schema from the Polars DataFrame. This is not recommended when writing tables,
        as it may result in a schema that is not compatible with the data warehouse.

        Parameters
        ----------
        polars_df : polars.DataFrame
            A polars.DataFrame.
        pyarrow_schema : pyarrow.Schema, optional, default None
            A pyarrow schema for the resulting Metaflow DataFrame
        iceberg_schema : List[Dict[str,Any]], optional, default None
            An iceberg schema for the resulting Metaflow DataFrame.
            Either this or pyarrow_schema must be provided.
            Example:
               [
               {'type': 'int', 'name':'myintcol'},
               {'type': {'type': 'list', 'element':'int'}, 'name':'mylistcol'}
               ]

            See the iceberg documentation
            (https://iceberg.apache.org/spec/#avro) for a complete description
            of the type specification.
        max_chunk_size : int, default 2 * 10**9
            Maximum amount of memory to be loaded in a data chunk, in
            bytes. This parameter is needed to avoid issues with
            larger than 2GB record batches in Apache Arrow. Due to
            indexing incompatibilities between polars and arrow, for
            non-fixed size types (e.g. List, string, binary), this
            value provides a safe bound on record batch size. The
            default is the maximum safe size, callers may lower the
            value.

        Returns
        -------
        MetaflowDataFrame
            A MetaflowDataFrame

        """
        lazy_import_MD()

        try:
            lazy_import_pyarrow()
        except ImportError as e:
            raise MetaflowDataFrameException(
                "'from_polars' requires pyarrow to be installed"
            )
        try:
            lazy_import_polars()
        except ImportError as e:
            raise MetaflowDataFrameException(
                "'from_polars' requires polars to be installed"
            )

        # handle zero rows case
        num_rows = polars_df.height
        if num_rows == 0:
            return cls([])

        # Get a schema
        pyarrow_schema = cls._from_polars_schema_validation(
            polars_df, pyarrow_schema, iceberg_schema
        )

        # Early exit if we are within chunk size
        if polars_df.estimated_size() < max_chunk_size:
            arrow_df = polars_df.to_arrow()
            if pyarrow_schema:
                arrow_df = arrow_df.cast(pyarrow_schema)
            return cls.from_arrow(arrow_df)

        # Chunk the polars df
        # establish an initial batch size based on the estimated size per row
        # then reduce if the max index check fails
        col_names = polars_df.columns
        est_size_per_row = polars_df.estimated_size() / num_rows
        initial_batch_size = max(int((max_chunk_size / est_size_per_row) // 2), 1)
        arrow_chunks = []
        curr_idx = 0

        while curr_idx < num_rows:
            batch_size = initial_batch_size
            candidate_batch = polars_df.slice(curr_idx, batch_size)
            passing_max_index_check = all(
                [check_max_index(candidate_batch[col]) for col in col_names]
            )

            while not passing_max_index_check:
                batch_size = batch_size // 2
                if batch_size < 1:
                    raise MetaflowDataFrameException(
                        f"'from_polars' error determining chunk size at polars DataFrame index \
                            {curr_idx}. Consider passing in a larger 'max_chunk_size'"
                    )
                candidate_batch = polars_df.slice(curr_idx, batch_size)
                passing_max_index_check = all(
                    [check_max_index(candidate_batch[col]) for col in col_names]
                )
            arrow_chunk = candidate_batch.to_arrow()
            if pyarrow_schema:
                arrow_chunk = arrow_chunk.cast(pyarrow_schema)

            arrow_chunks.append(arrow_chunk)
            curr_idx += batch_size

        return cls.stack([cls.from_arrow(chunk) for chunk in arrow_chunks])

    @classmethod
    def from_arrow(cls, table: "pyarrow.Table") -> "MetaflowDataFrame":
        """
        Convert a pyarrow.Table to a CMetaflowDataFrame

        Parameters
        ----------
        table : pyarrow.Table
            A pyarrow.Table.

        Returns
        -------
        MetaflowDataFrame
            A MetaflowDataFrame

        """
        return MetaflowDataFrame([_from_arrow(table)])

    @classmethod
    def from_buffer(cls, buffer: Union[BinaryIO, memoryview]) -> "MetaflowDataFrame":
        """
        Read a dataframe from a binary buffer.

        Parameters
        ----------
        buffer : BinaryIO
            The buffer from which to read the data. This can be a file-like object
            or a memoryview.

        Returns
        -------
        MetaflowDataFrame
            A MetaflowDataFrame
        """
        lazy_import_MD()

        if not isinstance(buffer, memoryview):
            raise MetaflowDataFrameBufferException(
                "Only memoryview is supported for reading from buffer."
            )

        # If the buffer is empty, return an empty dataframe
        if len(buffer) == 0:
            return cls()

        buffer_size = buffer.nbytes
        c_buffer = ffi.from_buffer(buffer)

        exception = ffi.new("char[]", EXCEPTION_BUF_LEN)
        df = MD.md_read_dataframe_from_buffer(c_buffer, buffer_size, exception)
        df = ffi.gc(df, MD.md_free_dataframe)

        exception_str = ffi.string(exception).decode("utf-8")
        if exception_str:
            raise MetaflowDataFrameInvalidBuffer(exception_str)

        if df == ffi.NULL:
            raise MetaflowDataFrameInvalidBuffer(
                "Failed to read DataFrame from buffer. "
                "The buffer may be invalid or corrupted."
            )

        return MetaflowDataFrame([df])

    # TODO: implement this later after adding a native from_numpy method
    # @classmethod
    # def from_torch_tensors(
    #     cls, tensor_dict: Dict[str, "torch.Tensor"]
    # ) -> "MetaflowDataFrame":
    #     """
    #     Convert a dictionary of torch tensors to a MetaflowDataFrame.

    #     Parameters
    #     ----------
    #     tensor_dict : Dict[str, torch.Tensor]
    #         A dictionary of torch tensors.

    #     Returns
    #     -------
    #     MetaflowDataFrame
    #         A MetaflowDataFrame
    #     """
    #     lazy_import_MD()

    #     try:
    #         lazy_import_torch()
    #     except ImportError as e:
    #         raise MetaflowDataFrameException(
    #             "'from_torch_tensors' requires torch to be installed"
    #         )

    #     try:
    #         lazy_import_pyarrow()
    #     except ImportError as e:
    #         raise MetaflowDataFrameException(
    #             "'from_torch_tensors' requires pyarrow to be installed"
    #         )

    #     if not isinstance(tensor_dict, dict) or not all(
    #         isinstance(v, torch.Tensor) for v in tensor_dict.values()
    #     ):
    #         raise MetaflowDataFrameException(
    #             "'from_torch_tensors' requires a dictionary of torch tensors"
    #         )

    #     arrays = []
    #     for tensor in tensor_dict.values():
    #         np_arr = tensor.cpu().numpy()
    #         if isinstance(np_arr, np.ndarray) and np_arr.ndim > 1:
    #             # 2+d tensor
    #             # have to make the pyarrow type by wrapping a list type for each dimension
    #             pyarr_type = pyarrow.from_numpy_dtype(np_arr.dtype)
    #             for _ in range(np_arr.ndim - 1):
    #                 pyarr_type = pyarrow.list_(pyarr_type)
    #             # convert np array to list for input to pyarrow array without having to flatten
    #             # todo: examine tolist() call and see if it can be more efficient
    #             arrays.append(pyarrow.array(np_arr.tolist(), type=pyarr_type))
    #         else:
    #             # 1d tensor
    #             pyarr_type = pyarrow.from_numpy_dtype(np_arr.dtype)
    #             arrays.append(pyarrow.array(np_arr, type=pyarr_type))

    #     return cls.from_arrow(
    #         pyarrow.Table.from_arrays(arrays, list(tensor_dict.keys()))
    #     )

    @classmethod
    def from_pandas(
        cls,
        pandas_df: "pandas.DataFrame",
        pyarrow_schema: Optional["pyarrow.Schema"] = None,
        iceberg_schema: Optional[List[Dict[str, Any]]] = None,
    ) -> "MetaflowDataFrame":
        """
        Convert a pandas.DataFrame to a MetaflowDataFrame.

        This conversion requires both pandas and pyarrow to be installed by the
        user.

        The easiest and most common way to provide type information is with an
        iceberg schema. This is often used when the data will be written to the
        data warehouse. The 'metaflow.Table' object can provide the schema of
        an existing table.

        It is also possible to provide a PyArrow schema directly instead of
        inferring it from an iceberg schema.

        If you do not provide a schema, the function will attempt to infer the
        schema from the pandas DataFrame. This is not recommended when writing tables,
        as it may result in a schema that is not compatible with the data warehouse.

        Parameters
        ----------
        pandas_df : pandas.DataFrame
            A Pandas DataFrame
        pyarrow_schema : pyarrow.Schema, optional, default None
            A pyarrow schema for the resulting Metaflow DataFrame
        iceberg_schema : List[Dict[str,Any]], optional, default None
            An iceberg schema for the resulting Metaflow DataFrame.
            Example:
               [
               {'type': 'int', 'name':'myintcol'},
               {'type': {'type': 'list', 'element':'int'}, 'name':'mylistcol'}
               ]

            See the iceberg documentation
            (https://iceberg.apache.org/spec/#avro) for a complete description
            of the type specification.


        Returns
        -------
        MetaflowDataFrame
            A MetaflowDataFrame
        """
        lazy_import_MD()

        try:
            lazy_import_pyarrow()
        except ImportError as e:
            raise MetaflowDataFrameException(
                "'from_pandas' requires pyarrow to be installed"
            )

        try:
            lazy_import_pandas()
        except ImportError as e:
            raise MetaflowDataFrameException(
                "'from_pandas' requires pandas to be installed"
            )

        if pyarrow_schema:
            pa_table = pyarrow.Table.from_pandas(
                pandas_df, schema=pyarrow_schema, safe=True
            )
        elif iceberg_schema:
            # check if the schema is supported, throw if its not proper format
            unsupported = check_supported_iceberg_schema(iceberg_schema)  # type: ignore[arg-type]
            if unsupported:
                raise MetaflowDataFrameException(
                    "The requested iceberg schema has unsupported types for "
                    "columns %s." % dict(unsupported)
                )

            # check we have all the columns
            col_names = set(col["name"] for col in iceberg_schema)
            df_col_names = set(pandas_df.columns)
            if not col_names.issubset(df_col_names):
                raise MetaflowDataFrameException(
                    "Columns %s from the iceberg schema are "
                    "not columns in the provided dataframe."
                    % col_names.difference(df_col_names)
                )

            # convert from iceberg to pyarrow and create table
            schema = iceberg_to_pyarrow_schema(iceberg_schema)  # type: ignore[arg-type]
            pa_table = pyarrow.Table.from_pandas(pandas_df, schema=schema, safe=True)
        else:
            # there's no schema provided at all
            # this may fail with complex types, like map types
            # wrap any error and suggest using a schema
            try:
                pa_table = pyarrow.Table.from_pandas(
                    pandas_df, preserve_index=False, safe=True
                )
            except Exception as e:
                raise MetaflowDataFrameException(
                    "Failed to infer schema from pandas DataFrame. "
                    "Please provide a schema or use a different dataframe "
                    "format. The original error was: %s" % str(e)
                ) from e

        return cls.from_arrow(pa_table)

    @classmethod
    def from_protobuf(cls, messages: List["Message"]) -> "MetaflowDataFrame":
        """
        Convert a list of Protocol Buffer messages to a
        MetaflowDataFrame. This conversion requires google.protobuf
        and pyarrow to be installed.

        Parameters
        ----------
        messages : List[google.protobuf.message.Message]
            A list of Protocol Buffer message instances to convert. All messages
            should be of the same type. All fields in each message (including
            default values) will be included in the resulting dataframe.

        Returns
        -------
        MetaflowDataFrame
            A MetaflowDataFrame where each row corresponds to one protobuf
            message from the input list. Column names will match the protobuf
            field names. If an empty list is provided, an empty MetaflowDataFrame
            is returned.
        """
        lazy_import_proto()
        lazy_import_pyarrow()

        # Handle empty list case
        if not messages:
            return cls([])

        # Convert each protobuf message to dictionary, directly extracting field values
        message_dicts = [_proto_message_to_dict(msg) for msg in messages]

        # Generate a schema
        schema = proto_to_pyarrow_schema(messages[0])

        # Convert list of dictionaries to PyArrow table and then to MetaflowDataFrame
        return MetaflowDataFrame.from_arrow(
            pyarrow.Table.from_pylist(message_dicts, schema=schema)
        )

    @classmethod
    def from_avro(
        cls, data: bytes, avro_schema: Optional[Dict] = None
    ) -> "MetaflowDataFrame":
        """
        Convert binary Avro data to a MetaflowDataFrame.
        This conversion requires fastavro and pyarrow to be installed.

        Supports two formats:
        1. Avro Object Container File (with embedded schema) - can contain multiple records
        2. Schemaless Avro record (requires avro_schema parameter) - single record

        Parameters
        ----------
        data : bytes
            Binary Avro data to convert.
            - For container file: A complete Avro file with embedded schema
              containing one or more records
            - For schemaless record: A single Avro record, requires avro_schema
        avro_schema : Dict, optional
            The Avro schema as a dictionary (required for schemaless records).
            If not provided, the function will attempt to read the schema from
            the Avro container file format.

        Returns
        -------
        MetaflowDataFrame
            A MetaflowDataFrame where each row corresponds to one Avro
            record from the input. Column names will match the Avro
            field names.

        Raises
        ------
        MetaflowDataFrameException
            If no schema is provided and the data is not in Avro container
            file format with embedded schema.
        """
        lazy_import_fastavro()
        lazy_import_pyarrow()

        # Check if data is an Avro container file with embedded schema
        has_schema = _has_embedded_avro_schema(data)

        if has_schema:
            # Read as Avro container file (may contain multiple
            # records). It has a schema so use that one.
            fo = BytesIO(data)
            reader = fastavro.reader(fo)
            schema = avro_to_pyarrow_schema(reader.writer_schema)  # type: ignore
            record_dicts = list(reader)
        else:
            if avro_schema is None:
                raise MetaflowDataFrameException(
                    "No Avro schema provided and data does not contain embedded schema. "
                    "Please provide the avro_schema parameter or use Avro container file format."
                )

            fo = BytesIO(data)
            record_dict = fastavro.schemaless_reader(fo, avro_schema)  # type: ignore
            record_dicts = [record_dict]  # We can only expect single records

            # Use the provided schema
            schema = avro_to_pyarrow_schema(avro_schema)

        # Convert list of dictionaries to PyArrow table and then to MetaflowDataFrame
        return MetaflowDataFrame.from_arrow(
            pyarrow.Table.from_pylist(record_dicts, schema=schema)
        )

    @classmethod
    def from_parquet_with_schema(
        cls,
        urls: List[str],
        schema: Optional[List[str]] = None,
        num_threads: Optional[int] = None,
        **kwargs,
    ) -> "MetaflowDataFrame":
        """
        Instantiate a MetaflowDataFrame given a list of local paths or
        S3 URLs pointing to Parquet-encoded files, and a schema to
        decode them. This will apply basic schema evolution to the
        dataframe upon read.

        Parameters
        ----------
        urls : List[str]
            A list of S3 URLs or local paths.
        schema : [List[str]], optional, default None
            Schema used for decoding parquet, data will be cast to
            the schema. See Apache Iceberg format
            (https://iceberg.apache.org/spec/)
        num_threads : int, default 16
            Use this many threads to decode Parquet files in parallel.

        Returns
        -------
        MetaflowDataFrame
            A MetaflowDataFrame corresponding to the given urls and
            evolved by the given schema
        """
        return cls(
            _from_parquet_generic(
                urls, None, None, schema, False, num_threads, **kwargs  # type: ignore[arg-type]
            )
        )

    @classmethod
    def from_parquet(
        cls,
        urls: List[str],
        columns: Optional[List[str]] = None,
        num_threads: Optional[int] = None,
        **kwargs,
    ) -> "MetaflowDataFrame":
        """
        Instantiate a MetaflowDataFrame given a list of local paths or S3 URLs
        pointing to Parquet-encoded files.

        Parameters
        ----------
        urls : List[str]
            A list of S3 URLs or local paths.
        columns:  List[str], optional, default None
            A list of column names to include in MetaflowDataFrame.
        num_threads : int, default 16
            Use this many threads to decode Parquet files in parallel.

        Returns
        -------
        MetaflowDataFrame
            A MetaflowDataFrame corresponding to the given urls.
        """
        return cls(
            _from_parquet_generic(
                urls, columns, None, None, False, num_threads, **kwargs
            )
        )

    @classmethod
    def stack(cls, mdfs: List["MetaflowDataFrame"]) -> "MetaflowDataFrame":
        """
        Stack a list of MetaflowDataFrame's into a single MetaflowDataFrame

        Parameters
        ----------
        mdfs: List[MetaflowDataFrame]
            List of MetaflowDataFrame's to be stacked together


        Returns
        -------
        MetaflowDataFrame
            A new MetaflowDataFrame

        """
        return cls([mdf._df for mdf in mdfs])

    @classmethod
    def from_columns(
        cls, columns: List["MetaflowColumn"], names: Optional[List[str]] = None  # type: ignore[name-defined]
    ) -> "MetaflowDataFrame":
        """
        Create a MetaflowDataFrame from a list of MetaflowColumns.

        Parameters
        ----------
        columns: List[MetaflowColumn]
            List of MetaflowColumns
            They must have the same length and underlying memory chunking. This is guaranteed
            if your input columns are all from the same dataframe. You can check this with the
            `MetaflowColumn.is_same_chunking()` method.
        names: List[str], optional, default None
            List of unique column names. If not provided, the original names of the columns
            will be used. This parameter is required if the input columns have duplicate names.
            Note that if you will be saving this MDF to an Iceberg table, the column names
            will be treated as case-insensitive.

        Returns
        -------
        MetaflowDataFrame
            A new MetaflowDataFrame from the given columns

        """
        if not columns:
            raise MetaflowDataFrameException(
                "`columns` argument must be a non-empty list"
            )

        if not names and len(set([col.name for col in columns])) != len(columns):
            raise MetaflowDataFrameException(
                "`names` argument must be provided if `columns` argument contains duplicate names"
            )

        if names:
            if len(names) != len(columns):
                raise MetaflowDataFrameException(
                    "`names` argument must have the same length as `columns` argument"
                )
            if len(set(names)) != len(names):
                raise MetaflowDataFrameException(
                    "`names` list argument must not contain duplicate values"
                )

        fixed_size = len(columns[0])
        if any(len(col) != fixed_size for col in columns[1:]):
            raise MetaflowDataFrameException(
                "Columns in `columns` argument must all be the same length"
            )

        for col in columns[1:]:
            if not col.is_same_chunking(columns[0]):
                raise MetaflowDataFrameException(
                    "Columns in `columns` argument must have same memory chunking"
                )

        if not names:
            names = [col.name for col in columns]

        # create the new CMetaflowDataFrame object
        exception = ffi.new("char[]", EXCEPTION_BUF_LEN)
        c_names = [ffi.new("char[]", name.encode("utf-8")) for name in names]
        c_names = ffi.new("char*[]", c_names)
        cmdf = ffi.gc(
            MD.md_dataframe_from_chunked_arrays(
                [col._mf_chunked_array for col in columns],
                len(columns),
                c_names,
                exception,
            ),
            MD.md_free_dataframe,
        )
        exception_str = ffi.string(exception).decode("utf-8")
        if exception_str:
            raise MetaflowDataFrameException(exception_str)

        # create MetaflowDataFrame object from cmdf
        new_mdf = cls([cmdf])
        return new_mdf


def fast_concat(dst_file, src_files):
    total = sum(src.tell() for src in src_files)
    dst_file.flush()
    # resize the destination file to header + shard sizes
    dst_file.truncate(dst_file.tell() + total)
    BUFFER_SIZE = 64 * 1024 * 1024
    for src in src_files:
        src.seek(0)
        shutil.copyfileobj(src, dst_file, BUFFER_SIZE)
    dst_file.flush()


def iter_batches(lst, num_batches):
    batch_size = int(math.ceil(len(lst) / float(num_batches)))
    for _, items in groupby(enumerate(lst), lambda x: x[0] // batch_size):
        yield [x for _, x in items]


def parquet_files(root="."):
    for dirpath, dirnames, filenames in os.walk(root):
        for fname in filenames:
            if fname.endswith(".parquet"):
                yield os.path.join(dirpath, fname)


if __name__ == "__main__":
    import sys
    import time

    start = time.time()
    df = MetaflowDataFrame.from_parquet(parquet_files())
    print(df.schema, df.num_rows)
    start = time.time()
    with open("foo.csv", "w") as f:
        df.to_csv(fileobj=f)
    print("to_csv took %dms" % ((time.time() - start) * 1000))
