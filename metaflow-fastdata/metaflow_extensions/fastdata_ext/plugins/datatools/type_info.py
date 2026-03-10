"""
Utilities for handling types and schema in arrow, numpy, memoryview, etc.
"""

import struct
import datetime
import dataclasses

from typing import (
    Dict,
    Optional,
    List,
    Union,
    Tuple,
    Any,
    TYPE_CHECKING,
    get_type_hints,
    get_origin,
    get_args,
    cast,
)
from types import SimpleNamespace
from metaflow.util import is_stringish
from .schema import make_c_schema
from .exceptions import MetaflowDataFrameException

if TYPE_CHECKING:
    import numpy
    import pyarrow as pa
    from google.protobuf.descriptor import FieldDescriptor as FieldDescriptorType

    CData = Any  # CFFI types are dynamically generated, use Any for type hints

    # Define the ArrowSchema type for type checking
    class ArrowSchema:
        format: bytes
        name: bytes
        flags: int
        n_children: int
        children: List["ArrowSchema"]
        dictionary: Optional["ArrowSchema"]


np: Optional[Any] = None
ffi: Optional[Any] = None
pyarrow: Optional[Any] = None
FieldDescriptor: Optional[Any] = None
Descriptor: Optional[Any] = None
_has_proto = False
_is_md_import_complete = False


def lazy_import_np():
    global np
    try:
        import numpy as np
    except ImportError:
        raise MetaflowDataFrameException(
            "Numpy required for operation.  Please install numpy"
        )


def lazy_import_pyarrow():
    global pyarrow
    try:
        import pyarrow
    except ImportError:
        raise MetaflowDataFrameException(
            "Pyarrow required for operation.  Please install pyarrow"
        )


def lazy_import_MD():
    global _is_md_import_complete, ffi
    if not _is_md_import_complete:
        from .md import ffi

        _is_md_import_complete = True


def lazy_import_protobuf():
    global FieldDescriptor
    global Descriptor
    global _has_proto
    try:
        from google.protobuf.descriptor import FieldDescriptor
        from google.protobuf.descriptor import Descriptor

        _has_proto = True
    except ImportError:
        raise MetaflowDataFrameException(
            "Protobuf library is required for operation. Please install protobuf"
        )


# Structure to hold mappings between different types
class TypeMeta:
    def __init__(
        self,
        arrow_type,
        array_type,
        byte_width,
        index_type,
        index_width,
        iceberg_type,
        debug_name,
        suggested_iceberg_type=None,
    ):
        self.arrow_type = arrow_type
        self.array_type = array_type
        self.byte_width = byte_width
        self.index_type = index_type
        self.index_width = index_width
        self.is_scalar = byte_width is not None
        self.iceberg_type = iceberg_type
        self.debug_name = debug_name
        self.suggested_iceberg_type = suggested_iceberg_type
        if self.iceberg_type is None and self.suggested_iceberg_type is None:
            raise MetaflowDataFrameException(
                f"No suggested iceberg type provided for type: {self.debug_name}"
            )

    @property
    def is_primitive(self) -> bool:
        """
        Check if type is scalar and not a timestamp or date type.
        """
        return self.is_scalar and len(self.arrow_type) == 1

    @property
    def is_stringish(self) -> bool:
        """
        Check if type is stringish (binary or string types)
        """
        return self.index_width is not None and len(self.arrow_type) == 1

    @property
    def is_nested(self) -> bool:
        """
        Check if type is a nested type (list, struct, map)
        """
        return self.arrow_type.startswith(b"+")

    def __repr__(self):
        return f"Type({self.debug_name}, arrow_type: {self.arrow_type}, array_type: {self.array_type}, iceberg_type: {self.iceberg_type})"


def _build_type_meta():
    type_metas = [
        TypeMeta(
            arrow_type=b"b",
            # use uint8 for memoryview
            array_type="B",
            byte_width=1,
            index_type=None,
            index_width=None,
            iceberg_type="boolean",
            debug_name="bool",
        ),
        TypeMeta(
            arrow_type=b"c",
            array_type="b",
            byte_width=1,
            index_type=None,
            index_width=None,
            iceberg_type=None,
            debug_name="int8",
            suggested_iceberg_type="int",
        ),
        TypeMeta(
            arrow_type=b"C",
            array_type="B",
            byte_width=1,
            index_type=None,
            index_width=None,
            iceberg_type=None,
            debug_name="uint8",
            suggested_iceberg_type="int",
        ),
        TypeMeta(
            arrow_type=b"s",
            array_type="h",
            byte_width=2,
            index_type=None,
            index_width=None,
            iceberg_type=None,
            debug_name="int16",
            suggested_iceberg_type="int",
        ),
        TypeMeta(
            arrow_type=b"S",
            array_type="H",
            byte_width=2,
            index_type=None,
            index_width=None,
            iceberg_type=None,
            debug_name="uint16",
            suggested_iceberg_type="int",
        ),
        TypeMeta(
            arrow_type=b"i",
            array_type="i",
            byte_width=4,
            index_type=None,
            index_width=None,
            iceberg_type="int",
            debug_name="int32",
        ),
        TypeMeta(
            arrow_type=b"I",
            array_type="I",
            byte_width=4,
            index_type=None,
            index_width=None,
            iceberg_type=None,
            debug_name="uint32",
            suggested_iceberg_type="int",
        ),
        TypeMeta(
            arrow_type=b"l",
            array_type="q",
            byte_width=8,
            index_type=None,
            index_width=None,
            iceberg_type="long",
            debug_name="int64",
        ),
        TypeMeta(
            arrow_type=b"L",
            array_type="Q",
            byte_width=8,
            index_type=None,
            index_width=None,
            iceberg_type=None,
            debug_name="uint64",
            suggested_iceberg_type="long",
        ),
        TypeMeta(
            arrow_type=b"f",
            array_type="f",
            byte_width=4,
            index_type=None,
            index_width=None,
            iceberg_type="float",
            debug_name="float32",
        ),
        TypeMeta(
            arrow_type=b"g",
            array_type="d",
            byte_width=8,
            index_type=None,
            index_width=None,
            iceberg_type="double",
            debug_name="float64",
        ),
        TypeMeta(
            arrow_type=b"tss",
            array_type="q",
            byte_width=8,
            index_type=None,
            index_width=None,
            iceberg_type=None,
            debug_name="timestamp(sec)",
            suggested_iceberg_type="timestamp",
        ),
        TypeMeta(
            arrow_type=b"tsm",
            array_type="q",
            byte_width=8,
            index_type=None,
            index_width=None,
            iceberg_type=None,
            debug_name="timestamp(milli)",
            suggested_iceberg_type="timestamp",
        ),
        TypeMeta(
            arrow_type=b"tsu",
            array_type="q",
            byte_width=8,
            index_type=None,
            index_width=None,
            iceberg_type="timestamp",
            debug_name="timestamp(micro)",
        ),
        # NOTE: We only support UTC timezone for now, as iceberg timestamptz type does not support other timezones
        # As defined in https://iceberg.apache.org/spec/#primitive-types footnote 2
        # Timestamp values with time zone represent a point in time: values are stored as UTC and do not retain
        #  a source time zone (2017-11-16 17:10:34 PST is stored/retrieved as 2017-11-17 01:10:34 UTC and
        # these values are considered identical).
        TypeMeta(
            arrow_type=b"tsu:utc",
            array_type="q",
            byte_width=8,
            index_type=None,
            index_width=None,
            iceberg_type="timestamptz",
            debug_name="timestamptz(micro)",
        ),
        TypeMeta(
            arrow_type=b"tsn",
            array_type="q",
            byte_width=8,
            index_type=None,
            index_width=None,
            iceberg_type=None,
            debug_name="timestamp(nano)",
            suggested_iceberg_type="timestamp",
        ),
        TypeMeta(
            arrow_type=b"tdD",
            array_type="I",
            byte_width=4,
            index_type=None,
            index_width=None,
            iceberg_type="date",
            debug_name="date",
        ),
        TypeMeta(
            arrow_type=b"tdm",
            array_type="q",
            byte_width=8,
            index_type=None,
            index_width=None,
            iceberg_type=None,
            debug_name="date64",
            suggested_iceberg_type="date",
        ),
        TypeMeta(
            arrow_type=b"u",
            array_type=None,
            byte_width=None,
            index_type="i",
            index_width=4,
            iceberg_type="string",
            debug_name="string",
        ),
        TypeMeta(
            arrow_type=b"U",
            array_type=None,
            byte_width=None,
            index_type="q",
            index_width=8,
            iceberg_type=None,
            debug_name="large-string",
            suggested_iceberg_type="string",
        ),
        TypeMeta(
            arrow_type=b"z",
            array_type=None,
            byte_width=None,
            index_type="i",
            index_width=4,
            iceberg_type="binary",
            debug_name="binary",
        ),
        TypeMeta(
            arrow_type=b"Z",
            array_type=None,
            byte_width=None,
            index_type="q",
            index_width=8,
            iceberg_type=None,
            debug_name="large-binary",
            suggested_iceberg_type="binary",
        ),
        TypeMeta(
            arrow_type=b"+l",
            array_type=None,
            byte_width=None,
            index_type="i",
            index_width=4,
            iceberg_type="list",
            debug_name="list",
        ),
        TypeMeta(
            arrow_type=b"+L",
            array_type=None,
            byte_width=None,
            index_type="q",
            index_width=8,
            iceberg_type=None,
            debug_name="large-list",
            suggested_iceberg_type="list",
        ),
        TypeMeta(
            arrow_type=b"+s",
            array_type=None,
            byte_width=None,
            index_type=None,
            index_width=None,
            iceberg_type="struct",
            debug_name="struct",
        ),
        # map type is an alias for list<struct<key_type, value_type>> and uses
        # a small list index type (int32)
        TypeMeta(
            arrow_type=b"+m",
            array_type=None,
            byte_width=None,
            index_type="i",
            index_width=4,
            iceberg_type="map",
            debug_name="map",
        ),
    ]

    return {meta.arrow_type: meta for meta in type_metas}


TYPE_META = _build_type_meta()


def get_type_meta(arrow_type: bytes) -> TypeMeta:
    """
    Get the TypeMeta object for a given arrow type.

    Parameters
    ----------
    arrow_type : bytes
        The arrow type as a byte string (e.g., b"i" for int32).

    Returns
    -------
    TypeMeta
        The TypeMeta object corresponding to the arrow type.
    """
    # NOTE: We only support UTC timezone in microseconds precision for now, as iceberg timestamptz type does not support other timezones
    if arrow_type.startswith(b"tsu:"):
        # Check if the arrow type is a timestamp type with timezone
        if arrow_type[4:].lower() == b"utc":
            arrow_type = arrow_type.lower()
        else:
            raise MetaflowDataFrameException(
                f"Only microseconds precision timestamp with UTC timezone is supported: got unsupported timestamp type {arrow_type!r}"
            )

    type_meta = TYPE_META.get(arrow_type)
    if type_meta is None:
        raise MetaflowDataFrameException(f"Unknown arrow type: {arrow_type!r}")
    return type_meta


def get_type_meta_by_debug_name(debug_name: str) -> Optional[TypeMeta]:
    """
    Get the TypeMeta object for a given debug name.

    Parameters
    ----------
    debug_name : str
        The debug name of the type (e.g., "int32", "string").

    Returns
    -------
    Optional[TypeMeta]
        The TypeMeta object corresponding to the debug name, or None if not found.
    """
    return next(
        (
            type_meta
            for type_meta in TYPE_META.values()
            if type_meta.debug_name == debug_name
        ),
        None,
    )


def type_debug_name(arrow_type: bytes) -> str:
    """
    Return the debug name of the arrow type.

    Parameters
    ----------
    arrow_type : bytes
        The arrow type as a byte string (e.g., b"i" for int32).

    Returns
    -------
    str
        The debug name of the arrow type (e.g., "int32", "string").
    """
    return get_type_meta(arrow_type).debug_name


# Arrow functions
#
def arrow_type_str(schema: "ArrowSchema") -> str:
    """
    Return the arrow type name from an ArrowSchema object

    Parameters
    ----------
    schema : ArrowSchema
        The ArrowSchema object containing the format string.

    Returns
    -------
    str
        The first three characters of the format string, which represent the arrow type.
    """
    lazy_import_MD()
    assert ffi is not None

    return ffi.string(schema.format)[:3]


def arrow_full_type_str(schema: "ArrowSchema") -> str:
    """
    Return the arrow type name from an ArrowSchema object with extra
    information (e.g. timezone)

    Parameters
    ----------
    schema : ArrowSchema
        The ArrowSchema object containing the format string.

    Returns
    -------
    str
        The full arrow type format string
    """
    lazy_import_MD()
    assert ffi is not None

    return ffi.string(schema.format)


def is_arrow_time_type(schema: "ArrowSchema") -> bool:
    """
    Return true of the schema is an arrow time type

    Parameters
    ----------
    schema : ArrowSchema
        The ArrowSchema object containing the format string.

    Returns
    -------
    bool
        True if the schema is an arrow time type
    """
    lazy_import_MD()
    assert ffi is not None

    type_ = ffi.string(schema.format)[:3]
    return type_ in {b"tss", b"tsm", b"tsu", b"tsn"}


def is_arrow_timezone_type(schema: "ArrowSchema") -> bool:
    """
    Return true of the schema is an arrow time type with timezone

    Parameters
    ----------
    schema : ArrowSchema
        The ArrowSchema object containing the format string.

    Returns
    -------
    bool
        True if the schema is an arrow time type
    """
    lazy_import_MD()
    assert ffi is not None

    type_ = ffi.string(schema.format)
    return type_[:4] in {b"tss:", b"tsm:", b"tsu:", b"tsn:"}


def c_schema_metadata(c_schema: "CData") -> Dict[str, str]:
    """
    Read the metadata from a c schema as a dict

    Parameters
    ----------
    c_schema : ffi.CData
        The c schema object from which to read the metadata.

    Returns
    -------
    Dict[str, str]
        A dictionary containing the metadata key-value pairs.
    """
    lazy_import_MD()

    INT = 4

    def _read_int(metadata):
        b = ffi.buffer(metadata, INT)
        val = struct.unpack("@i", b)[0]
        metadata += INT
        return val, metadata

    def _read_string(metadata, length):
        b = ffi.buffer(metadata, length)
        val = bytes(b).decode()
        metadata += length
        return val, metadata

    meta = {}
    metadata = c_schema.metadata
    assert ffi is not None
    if metadata == ffi.NULL:
        return {}

    n_keys, metadata = _read_int(metadata)
    for i in range(n_keys):
        n_bytes, metadata = _read_int(metadata)
        key, metadata = _read_string(metadata, n_bytes)

        n_bytes, metadata = _read_int(metadata)
        value, metadata = _read_string(metadata, n_bytes)

        meta[key] = value
    return meta


def is_scalar(arrow_type: bytes) -> bool:
    """
    Check whether an arrow format string is scalar.

    Parameters
    ----------
    arrow_type : bytes
        The arrow type as a byte string (e.g., b"i" for int32).

    Returns
    -------
    bool
        True if the arrow type is scalar, False otherwise.
    """
    type_meta = get_type_meta(arrow_type)
    return type_meta.is_scalar


def is_primitive(arrow_type: bytes) -> bool:
    """
    Check whether an arrow format string is primitive non-timestamp.

    Parameters
    ----------
    arrow_type : bytes
        The arrow type as a byte string (e.g., b"i" for int32).

    Returns
    -------
    bool
        True if the arrow type is primitive (non-timestamp), False otherwise.
    """
    type_meta = get_type_meta(arrow_type)
    return type_meta.is_primitive


def is_integral(arrow_type: bytes) -> bool:
    """
    Check whether an arrow format string is primitive and integral

    Parameters
    ----------
    arrow_type : bytes
        The arrow type as a byte string (e.g., b"i" for int32).

    Returns
    -------
    bool
        True if the arrow type is primitive and integral, False otherwise.
    """
    return is_primitive(arrow_type) and arrow_type not in (b"f", b"g")


def is_binary(arrow_type: bytes) -> bool:
    """
    Check whether an arrow format string is a string or binary type

    Parameters
    ----------
    arrow_type : bytes
        The arrow type as a byte string (e.g., b"i" for int32).

    Returns
    -------
    bool
        True if the arrow type is a string or binary type, False otherwise.
    """
    type_meta = get_type_meta(arrow_type)
    return type_meta.is_stringish


def is_nested(arrow_type: bytes) -> bool:
    """
    Check whether an arrow format string is a nested type

    Parameters
    ----------
    arrow_type : bytes
        The arrow type as a byte string (e.g., b"i" for int32).

    Returns
    -------
    bool
        True if the arrow type is a nested type (e.g., list, struct, map), False otherwise.
    """
    type_meta = get_type_meta(arrow_type)
    return type_meta.is_nested


def is_map(arrow_type: bytes) -> bool:
    """
    Check whether an arrow format string is a map type

    Parameters
    ----------
    arrow_type : bytes
        The arrow type as a byte string (e.g., b"i" for int32).

    Returns
    -------
    bool
        True if the arrow type is a map type, False otherwise.
    """
    return arrow_type == b"+m"


def is_struct(arrow_type: bytes) -> bool:
    """
    Check whether an arrow format string is a struct type

    Parameters
    ----------
    arrow_type : bytes
        The arrow type as a byte string (e.g., b"i" for int32).

    Returns
    -------
    bool
        True if the arrow type is a struct type, False otherwise.
    """
    return arrow_type == b"+s"


def is_list(arrow_type: bytes) -> bool:
    """
    Check whether an arrow format string is a list type

    Parameters
    ----------
    arrow_type : bytes
        The arrow type as a byte string (e.g., b"i" for int32).

    Returns
    -------
    bool
        True if the arrow type is a list type, False otherwise.
    """
    return arrow_type in [b"+l", b"+L"]


def has_dictionary(schema: "ArrowSchema") -> bool:
    """
    Check whether an ArrowSchema or any of its children has a non-null
    dictionary.

    Parameters
    ----------
    schema : ArrowSchema
        The ArrowSchema object to check for dictionaries.

    Returns
    -------
    bool
        True if the schema or any of its children has a non-null dictionary,
        False otherwise.
    """
    lazy_import_MD()
    type_ = arrow_type_str(schema)

    assert ffi is not None
    if schema.dictionary != ffi.NULL:
        return True

    if type_ in (b"+l", b"+L"):
        return has_dictionary(schema.children[0])
    elif type_ == b"+s":
        return any(has_dictionary(schema.children[i]) for i in range(schema.n_children))
    elif type_ == b"+m":
        # Map is equivalent to List<item: Struct<key: K, value: V>>
        # Validate by treating it as a list.
        return has_dictionary(schema.children[0])
    else:
        return False


def print_c_schema(schema: "CData", indent: int = 0):
    """
    Recursively print the structure of a C Arrow schema.

    Parameters:
    -----------
    schema : ffi.CData
        The C Arrow schema to print
    indent : int
        The indentation level for pretty printing
    """
    lazy_import_MD()

    assert ffi is not None
    format_str = (
        ffi.string(schema.format).decode() if schema.format != ffi.NULL else "NULL"
    )
    name_str = ffi.string(schema.name).decode() if schema.name != ffi.NULL else ""

    # Print basic schema info
    print(" " * indent + f"Name: '{name_str}'")
    print(" " * indent + f"Format: '{format_str}'")
    print(" " * indent + f"Flags: {schema.flags}")
    print(" " * indent + f"n_children: {schema.n_children}")

    metadata = c_schema_metadata(schema)
    if metadata:
        print(" " * indent + "Metadata:")
        for key, value in metadata.items():
            print(" " * (indent + 2) + f"{key}: {value}")

    # Recursively print children
    if schema.n_children > 0:
        print(" " * indent + "Children:")
        for i in range(schema.n_children):
            print(" " * (indent + 2) + f"Child {i}:")
            print_c_schema(schema.children[i], indent + 4)

    if schema.dictionary != ffi.NULL:
        print(" " * indent + "Dictionary:")
        print_c_schema(schema.dictionary, indent + 2)


# Numpy functions
#
def _np_dtype_to_arrow_type() -> Dict["numpy.dtype[Any]", bytes]:
    """
    Create a mapping from numpy dtypes to arrow format strings.

    Returns
    -------
    Dict[np.dtype, bytes]
        A dictionary mapping numpy dtypes to arrow format strings.
    """
    res = {}
    for arrow_type, scalar_meta in TYPE_META.items():
        if len(arrow_type) > 1 or not scalar_meta.is_scalar:
            # Ignore complex/timestamp types
            continue
        array_type = scalar_meta.array_type
        np_dtype = numpy_dtype(array_type)
        res[np_dtype] = arrow_type
    # arrow bool maps to numpy uint8_t in SCALAR_TYPE_META
    res[np.dtype(bool)] = b"b"  # type: ignore[union-attr]
    return res


# Manually cache in case functools.lru_cache is not available
_NP_ARROW_MAP = None


def arrow_type(np_dtype: "numpy.dtype[Any]") -> bytes:
    """
    Convert numpy dtype to arrow format string

    Parameters
    ----------
    np_dtype : np.dtype
        The numpy dtype to convert (e.g., np.int32, np.float64).

    Returns
    -------
    bytes
        The corresponding arrow format string (e.g., b"i" for int32, b"g" for float64).

    Raises
    -------
    MetaflowDataFrameException
        If no arrow type is defined for the given numpy dtype.
    """
    global _NP_ARROW_MAP
    if _NP_ARROW_MAP is None:
        _NP_ARROW_MAP = _np_dtype_to_arrow_type()
    res = _NP_ARROW_MAP.get(np_dtype)
    if res is None:
        raise MetaflowDataFrameException(
            f"No arrow type defined for numpy dtype: {np_dtype}"
        )
    return res


def numpy_dtype(array_type: str) -> "numpy.dtype[Any]":
    """
    Convert memoryview array type string to numpy dtype

    Parameters
    ----------
    array_type : str
        The memoryview array type string (e.g., "B" for uint8, "f" for float32).

    Returns
    -------
    np.dtype
        The corresponding numpy dtype (e.g., np.uint8, np.float32).
    """
    lazy_import_np()
    assert np is not None
    return np.asarray(memoryview(bytearray()).cast(array_type)).dtype  # type: ignore[attr-defined,call-overload]


# Iceberg types and functions
#
def iceberg_type(arrow_type: bytes, error_if_missing: bool = True) -> Optional[str]:
    """
    Get the Iceberg type for a given arrow type.

    Parameters
    ----------
    arrow_type : bytes
        The arrow type as a byte string (e.g., b"i" for int32).

    error_if_missing : bool, optional
        If True, raise an exception if no Iceberg type is found for the arrow type.
        If False, return None instead of raising an exception. Default is True.

    Returns
    -------
    str or None
        The corresponding Iceberg type as a string (e.g., "int", "string").
        If no Iceberg type is found and error_if_missing is False, returns None.
    """
    type_meta = get_type_meta(arrow_type)
    if type_meta.iceberg_type is not None:
        return type_meta.iceberg_type
    elif error_if_missing:
        raise MetaflowDataFrameException(
            f"No iceberg type found for column type: {type_meta}"
        )
    else:
        return None


def suggested_iceberg_type_meta(arrow_type: bytes) -> TypeMeta:
    """
    Get the suggested Iceberg type meta for a given arrow type.

    Parameters
    ----------
    arrow_type : bytes
        The arrow type as a byte string (e.g., b"i" for int32).

    Returns
    -------
    TypeMeta
        The TypeMeta object corresponding to the suggested Iceberg type.

    Raises
    -------
    MetaflowDataFrameException
        If no suggested Iceberg type is found for the arrow type.
    """
    type_meta = get_type_meta(arrow_type)
    if type_meta.iceberg_type is not None:
        return type_meta
    else:
        suggested_type_metas = [
            tm
            for tm in TYPE_META.values()
            if tm.iceberg_type == type_meta.suggested_iceberg_type
        ]
        if len(suggested_type_metas) != 1:
            # Internal error
            raise MetaflowDataFrameException(
                f"Invalid type meta for type: {type_meta}, found the following "
                f"suggested iceberg types: {suggested_type_metas}"
            )
        else:
            return suggested_type_metas[0]


def iceberg_to_pyarrow_schema(
    iceberg_schema: List[Union[str, bytes, Dict]],
) -> "pa.Schema":
    """
    Convert an already verified Iceberg schema to a pyarrow schema

    Parameters
    ----------
    iceberg_schema : List[Union[str, bytes, Dict]]
        The Iceberg schema as a list of dictionaries or string-like objects
        representing the types of the columns.

    Returns
    -------
    pyarrow.Schema
        The corresponding pyarrow Schema object for the Iceberg schema.
    """
    lazy_import_pyarrow()
    assert pyarrow is not None

    schema = []
    for col in iceberg_schema:
        pa_schema = iceberg_to_pyarrow_type(col)
        schema.append((col["name"], pa_schema))  # type: ignore[call-overload,index]

    return pyarrow.schema(schema)


def iceberg_to_arrow_c_type(
    iceberg_type: Union[str, bytes, Dict],
) -> Tuple["CData", List]:
    """
    Convert an already verified iceberg type to a pyarrow schema

    The return type is a tuple with the first element a cffi c_schema
    object and the second all the supporting objects we need to keep
    in memory until the downstream user completes its call.

    Parameters
    ----------
    iceberg_type : Union[str, bytes, Dict]
        The iceberg type specification as a dictionary or a string-like object
        (e.g., "int", "string", {"type": "struct", "fields": [...]})

    Returns
    -------
    Tuple[ffi.CData, List]
        A tuple containing the C schema object and a list of supporting objects.
    """
    lazy_import_MD()

    def get_mapping(iceberg_type, name=None, objects=None):
        if name is None:
            name = ""

        if objects is None:
            objects = []

        if is_stringish(iceberg_type):
            arrow_type = ICEBERG_C_ARROW_MAPPING[
                get_iceberg_field_type_prefix(iceberg_type).lower()
            ].decode()
            schema, objs = make_c_schema(name, arrow_type)
            objects += objs
            return schema, objects

        typ = iceberg_type["type"]
        if isinstance(typ, dict):
            return get_mapping(typ, name, objects)

        if typ == "list":
            elem_typ = iceberg_type["element"]
            child, child_objs = get_mapping(elem_typ, "element")
            schema, objs = make_c_schema(name, "+l", [child], child_objs)
            objects += objs
            return schema, objects

        elif typ == "struct":
            children = []
            children_objects = []
            for field in iceberg_type["fields"]:
                child_name = field["name"]
                child, child_objs = get_mapping(field, child_name)
                children.append(child)
                children_objects.append(child_objs)
            schema, objs = make_c_schema(name, "+s", children, children_objects)
            objects += objs
            return schema, objects

        elif typ == "map":
            child_k, obj_k = get_mapping(iceberg_type["key"], "key")
            child_v, obj_v = get_mapping(iceberg_type["value"], "value")
            e_schema, e_objs = make_c_schema(
                "entires", "+s", [child_k, child_v], [obj_k, obj_v]
            )
            schema, objs = make_c_schema(name, "+m", [e_schema], e_objs)
            objects += objs
            return schema, objects

        return get_mapping(typ, name, objects)

    return get_mapping(iceberg_type, iceberg_type["name"])  # type: ignore[call-overload,index]


def iceberg_to_arrow_c_schema(
    iceberg_schema: List[Union[str, bytes, Dict]],
) -> Tuple["CData", List]:
    """
    Convert an already verified iceberg schema to an arrow c schema

    The return type is a tuple with the first element a cffi c_schema
    object and the second all the supporting objects we need to keep
    in memory until the downstream user completes its call.

    Parameters
    ----------
    iceberg_schema : List[Union[str, bytes, Dict]]
        The Iceberg schema as a list of dictionaries or string-like objects
        representing the types of the columns.

    Returns
    -------
    Tuple[ffi.CData, List]
        A tuple containing the C schema object and a list of supporting objects.
    """
    children = []
    children_objects = []
    for col in iceberg_schema:
        child, child_objects = iceberg_to_arrow_c_type(col)
        children += [child]
        children_objects += child_objects

    schema, objects = make_c_schema("", "+s", children, children_objects)
    return schema, objects


# Note: fixed binary and uuid aren't added intentionally. Spark SQL doesn't
# support DDL for these types so we can't create them in our tests.
# Context:
# https://netflix.slack.com/archives/CD7C5951R/p1596559387323200?thread_ts=1596491392.322600&cid=CD7C5951R
#
# Note: We don't include Decimal in the supported types even though this
# implementation supports it. Decimal is not officially supported by
# MetaflowDataFrame. If we add support their we can add 'decimal' to the
# list below.

ALLOWED_ICEBERG_COLUMN_PRIMITIVE_TYPES = {
    "boolean",
    "int",
    "long",
    "float",
    "double",
    "date",
    "time",
    "timestamp",
    "timestamptz",
    "string",
    "binary",
}

ALLOWED_ICEBERG_COLUMN_NESTED_TYPES = {"list", "struct", "map"}

ALLOWED_ICEBERG_PARTITION_TRANSFORMS = {
    "identity",
    "bucket",
    "truncate",
    "year",
    "month",
    "day",
    "hour",
}

ALLOWED_ICEBERG_PARTITION_TRANSFORMS_WRITE = {
    "identity",
}

# Let's also restrict the partition columns (result) types to be primitive
# types we have example tables and we know its lossless without
# dependencies.
# Note: We explicitly choose to handle `date` as `int` since
# our users will never use a `date` object as a partition value.
ALLOWED_ICEBERG_PARTITION_TYPES = {"boolean", "int", "long", "string", "binary"}

C_ARROW_TIME_TYPES = {b"tss", b"tsm", b"tsu", b"tsn"}

ICEBERG_TYPES_TO_PYTHON = {
    # Primitive types
    "boolean": bool,
    "int": int,
    "long": int,
    "float": float,
    "double": float,
    "date": datetime.date,
    "time": datetime.time,
    "timestamp": datetime.datetime,
    "timestamptz": datetime.datetime,
    "string": str,
    "binary": bytes,
    # Nested types
    "list": List,
    "map": Dict,
    "struct": Any,
}

PYTHON_TYPES_TO_ICEBERG = {
    # Primitive types
    bool: "boolean",
    int: "int",
    float: "float",
    str: "string",
    bytes: "binary",
    datetime.date: "date",
    datetime.time: "time",
    datetime.datetime: "timestamp",
    # Nested types
    List: "list",
    Dict: "map",
}


def _sanitize_time(arrow_type: bytes) -> bytes:
    """
    Sanitize the arrow type for time types by appending a colon if it is a
    time type without a colon.

    Parameters
    ----------
    arrow_type : bytes
        The arrow type as a byte string (e.g., b"tsu" for timestamp with microseconds).

    Returns
    -------
    bytes
        The sanitized arrow type, with a colon appended if it is a time type
        without a colon (e.g., b"tsu:").
    """
    if arrow_type in C_ARROW_TIME_TYPES and b":" not in arrow_type:
        return arrow_type + b":"
    return arrow_type


# Mapping from Iceberg types to C Arrow types
ICEBERG_C_ARROW_MAPPING = {
    t.iceberg_type: _sanitize_time(t.arrow_type)
    for t in TYPE_META.values()
    if t.iceberg_type is not None and not t.is_nested
}


def get_iceberg_field_type_prefix(typ: str) -> str:
    """
    Some field types have additional specification like `decimal(S, P)` so
    this function extracts the relevant part so the lookup can be efficient.

    Parameters
    ----------
    typ : str
        The Iceberg type as a string (e.g., "decimal(10, 2)", "string").

    Returns
    -------
    str
        The prefix of the Iceberg type, which is the part before any parentheses
        or additional specifications (e.g., "decimal" for "decimal(10, 2)").
    """
    if typ.startswith("decimal"):
        return "decimal"
    else:
        return typ


def is_valid_iceberg_type(iceberg_type: Union[Dict, str, bytes]) -> bool:
    """
    Check if an iceberg type is valid and throw a MetaflowDataFrameException
    if it is not.

    Parameters
    ----------
    iceberg_type : Union[Dict, str, bytes]
        The iceberg type specification as a dictionary or a string-like object.

    Returns
    -------
    bool
        True if the iceberg type is valid, False otherwise.
    """

    def is_valid(iceberg_type):
        # if we are a string we must be a primitive type at this point, and we
        # can end the recursion
        if is_stringish(iceberg_type):
            typ = get_iceberg_field_type_prefix(iceberg_type).lower()
            return typ in ALLOWED_ICEBERG_COLUMN_PRIMITIVE_TYPES

        try:
            typ = iceberg_type["type"]
        except KeyError as e:
            raise MetaflowDataFrameException(
                "The iceberg specification doesn't contain the required 'type' "
                "field. Make sure you are using a valid iceberg specification."
            )

        # 'typ' will be either a string or dict. If its a dict 'iceberg_type'
        # is a nested type and we need to process recursively. If 'typ' a
        # string its either a primitive or the details of a nested type. Nested
        # types need to be handled specially and otherwise we can recurse on
        # the primitive (handled above)
        if isinstance(typ, dict):
            return is_valid(typ)

        if typ == "list":
            try:
                elem_typ = iceberg_type["element"]
            except KeyError as e:
                raise MetaflowDataFrameException(
                    "The iceberg specification doesn't contain the required "
                    "'element' field for the 'list' type. Make sure you are "
                    "using a valid iceberg specification."
                )

            return is_valid(elem_typ)

        elif typ == "struct":
            if "fields" not in iceberg_type:
                raise MetaflowDataFrameException(
                    "The iceberg specification doesn't contain the required "
                    "'fields' field for the 'struct' type. Make sure you are "
                    "using a valid iceberg specification."
                )

            fields = []
            for field in iceberg_type["fields"]:
                fields.append(is_valid(field))

            return all(fields)

        elif typ == "map":
            if "key" not in iceberg_type:
                raise MetaflowDataFrameException(
                    "The iceberg specification doesn't contain the required "
                    "field 'key' for the 'map' type. Make sure you are "
                    "using a valid iceberg specification."
                )

            if "value" not in iceberg_type:
                raise MetaflowDataFrameException(
                    "The iceberg specification doesn't contain the required "
                    "field 'value' for the 'map' type. Make sure you are "
                    "using a valid iceberg specification."
                )

            k = is_valid(iceberg_type["key"])
            v = is_valid(iceberg_type["value"])
            return k and v

        return is_valid(typ)

    try:
        valid = is_valid(iceberg_type)
    except MetaflowDataFrameException as e:
        raise MetaflowDataFrameException(
            "'%r' is not a proper iceberg specification "
            "for a type. %s" % (iceberg_type, str(e))
        )

    return valid


def check_supported_iceberg_schema(
    iceberg_schema: List[Union[str, bytes, Dict]],
) -> Optional[List[Tuple[str, str]]]:
    """
    Check if there are unsupported types and return them. Throw if the schema
    is not valid

    Parameters
    ----------
    iceberg_schema : List[Union[str, bytes, Dict]]
        The iceberg schema as a list of dictionaries or string like objects
        representing the types of the columns.

    Returns
    -------
    List[Tuple[str, str]] or None
        A list of tuples containing the names and types of unsupported columns,
        or None if all columns are supported.
    """
    unsupported = []
    for col in iceberg_schema:
        try:
            typ = col["type"]  # type: ignore[call-overload,index]
            name = col["name"]  # type: ignore[call-overload,index]
        except KeyError:
            raise MetaflowDataFrameException(
                "The iceberg schema for each column must contain "
                "the keys 'type' and 'name'"
            )

        res = is_valid_iceberg_type(col)
        if not res:
            unsupported.append((name, typ))

    return unsupported if unsupported else None


def iceberg_to_pyarrow_type(
    iceberg_type: Union[str, bytes, Dict],
) -> "pa.DataType":
    """
    Convert an already verified iceberg type to a pyarrow schema

    Parameters
    ----------
    iceberg_type : Union[str, bytes, Dict]
        The iceberg type specification as a dictionary or a string-like object
        (e.g., "int", "string", {"type": "struct", "fields": [...]})

    Returns
    -------
    pyarrow.DataType
        The corresponding pyarrow DataType object for the iceberg type.
    """
    lazy_import_pyarrow()
    assert pyarrow is not None

    MAPPING = {
        "boolean": pyarrow.bool_(),
        "int": pyarrow.int32(),
        "long": pyarrow.int64(),
        "float": pyarrow.float32(),
        "double": pyarrow.float64(),
        "date": pyarrow.date32(),
        "time": pyarrow.time64("us"),
        "timestamp": pyarrow.timestamp("us"),
        "timestamptz": pyarrow.timestamp("us", tz="UTC"),
        "string": pyarrow.string(),
        "binary": pyarrow.binary(),
    }

    def get_mapping(iceberg_type):
        if is_stringish(iceberg_type):
            return MAPPING[get_iceberg_field_type_prefix(iceberg_type).lower()]

        typ = iceberg_type["type"]
        if isinstance(typ, dict):
            return get_mapping(typ)

        if typ == "list":
            elem_typ = iceberg_type["element"]
            return pyarrow.list_(get_mapping(elem_typ))

        elif typ == "struct":
            fields = []
            for field in iceberg_type["fields"]:
                name = field["name"]
                fields.append((name, get_mapping(field)))
            return pyarrow.struct(fields)

        elif typ == "map":
            k = get_mapping(iceberg_type["key"])
            v = get_mapping(iceberg_type["value"])
            return pyarrow.map_(k, v)

        return get_mapping(typ)

    return get_mapping(iceberg_type)


# PyArrow functions
#
def pyarrow_ensure_no_map_fields(schema: "pa.Schema") -> None:
    """
    Check that there is no map type in a pyarrow schema, recursively.

    Parameters
    ----------
    schema : pyarrow.Schema
        The pyarrow schema to check for map types.

    Raises
    -------
    MetaflowDataFrameException
        If a map type is found in the schema or any nested field.
    """
    try:
        lazy_import_pyarrow()
    except ImportError:
        raise MetaflowDataFrameException(
            "'ensure_no_map_fields' requires pyarrow to be installed"
        )

    assert pyarrow is not None

    def check_field_no_map(field) -> None:
        if pyarrow.types.is_map(field.type):
            raise MetaflowDataFrameException(
                (
                    f"Field {field} in schema is of type map. "
                    "Map fields are not supported for conversion.\n"
                )
            )
        if pyarrow.types.is_struct(field.type):
            pyarrow_ensure_no_map_fields(field.type)
        elif pyarrow.types.is_list(field.type):
            check_field_no_map(field.type.value_field)

    for field in schema:
        check_field_no_map(field)


def pyarrow_ensure_all_fields_nullable(schema: "pa.Schema") -> None:
    """
    Check if all fields in a pyarrow schema are nullable

    Parameters
    ----------
    schema : pyarrow.Schema
        The pyarrow schema to check for nullable fields.

    Raises
    -------
    MetaflowDataFrameException
        If any field in the schema is not nullable or if a nested field is not nullable.
    """
    try:
        lazy_import_pyarrow()
    except ImportError as e:
        raise MetaflowDataFrameException(
            "'ensure_all_fields_nullable' requires pyarrow to be installed"
        )

    assert pyarrow is not None

    def check_field_nullable(field) -> None:
        # Check if the field itself is nullable
        if not field.nullable:
            raise MetaflowDataFrameException(
                (
                    f"Field {field} in schema is not nullable. "
                    "All fields must be nullable for conversion.\n"
                    f"Schema: {schema}"
                )
            )
        if pyarrow.types.is_struct(field.type):
            pyarrow_ensure_all_fields_nullable(field.type)
        elif pyarrow.types.is_list(field.type):
            check_field_nullable(field.type.value_field)
        elif pyarrow.types.is_map(field.type):
            check_field_nullable(field.type.item_field)

    for field in schema:
        check_field_nullable(field)


def arrow_c_schema_to_iceberg(
    arrow_c_schema: Union["CData", List["CData"]],
) -> List[Dict]:
    """
    Make an iceberg schema from an arrow c schema. Returned is a list
    of dicts in the iceberg schema format

    Parameters
    ----------
    arrow_c_schema : ffi.CData or List[ffi.CData]
        The arrow c schema object or a list of arrow c schema objects
        to convert to an iceberg schema.

    Returns
    -------
    List[Dict]
        A list of dictionaries representing the iceberg schema, where each
        dictionary contains the column name, id, and type.
    """
    lazy_import_MD()

    def _column_type(schema, field_id=0):
        assert ffi is not None
        typ = ffi.string(schema.format)
        type_meta = get_type_meta(typ)
        iceberg_type = type_meta.iceberg_type
        if iceberg_type is None:
            iceberg_type = type_meta.suggested_iceberg_type

        if iceberg_type is None:
            raise MetaflowDataFrameException(
                "Cannot convert dataframe column type '{debug_name}' to an "
                "iceberg type.".format(debug_name=type_meta.debug_name)
            )

        # handle nested cases
        if iceberg_type == "list":
            child = schema.children[0]
            metadata = c_schema_metadata(child)
            field_id = metadata.get("PARQUET:field_id", field_id)
            itype, last_field_id = _column_type(child, field_id + 1)
            return {
                "type": iceberg_type,
                "element": itype,
                "element-id": field_id,
            }, last_field_id

        elif iceberg_type == "map":
            # map are implemented as list<struct<key_type, value_type>>
            last_field_id = field_id
            child = schema.children[0]
            metadata = c_schema_metadata(child.children[0])
            key_id = metadata.get("PARQUET:field_id", last_field_id)
            last_field_id += 1
            metadata = c_schema_metadata(child.children[1])
            val_id = metadata.get("PARQUET:field_id", last_field_id)
            ki_type, last_field_id = _column_type(child.children[0], last_field_id)
            vi_type, last_field_id = _column_type(child.children[1], last_field_id)
            return {
                "type": iceberg_type,
                "key": ki_type,
                "key-id": key_id,
                "value": vi_type,
                "value-id": val_id,
            }, last_field_id

        elif iceberg_type == "struct":
            # struct has a 'fields' key with a list of column types
            fields = []
            last_field_id = field_id
            for i in range(schema.n_children):
                child = schema.children[i]
                assert ffi is not None
                name = ffi.string(child.name).decode()
                metadata = c_schema_metadata(child)
                field_id = metadata.get("PARQUET:field_id", last_field_id)
                last_field_id += 1
                itype, last_field_id = _column_type(child, last_field_id)
                fields.append({"name": name, "id": field_id, "type": itype})
            return {"type": iceberg_type, "fields": fields}, last_field_id

        return iceberg_type, field_id

    schema = []
    iceberg_schema = []
    if isinstance(arrow_c_schema, list):
        schema = arrow_c_schema
    else:
        assert ffi is not None
        typ = ffi.string(arrow_c_schema.format)
        if typ == "+s":
            for i in range(arrow_c_schema.n_children):
                schema.append(arrow_c_schema.children[i])

    last_field_id = len(schema)
    for idx, c in enumerate(schema):
        assert ffi is not None
        name = ffi.string(c.name)
        metadata = c_schema_metadata(c)
        field_id = metadata.get("PARQUET:field_id", idx)
        typ, last_field_id = _column_type(c, last_field_id)
        iceberg_schema.append({"name": name.decode(), "id": field_id, "type": typ})

    return iceberg_schema


# def value_matches_dtype(value, np_dtype):
def value_matches_dtype(value: Any, np_dtype: "numpy.dtype[Any]") -> bool:
    """
    Checks if a value can be assigned to a numpy array of given dtype.

    Parameters
    ----------
    value : Any
        The value to check.

    np_dtype : np.dtype
        The numpy dtype to check against (e.g., np.int32, np.float64).

    Returns
    -------
    bool
        True if the value can be assigned to a numpy array of the given dtype,
        False otherwise.

    Raises
    -------
    ValueError
        If the value cannot be assigned to a numpy array of the given dtype.
    """
    lazy_import_np()
    assert np is not None
    try:
        np.empty([1], dtype=np_dtype)[:] = value
        return True
    except ValueError:
        return False


# Python type functions for conversions
#
def _parse_optional_type(field_type: type) -> Tuple[type, bool]:
    """
    Parse a field type to determine if it is optional and return the non-None type.

    Parameters
    ----------
    field_type : type
        The type to be parsed.

    Returns
    -------
    Tuple[type, bool]
        A tuple containing the non-None type and a boolean indicating if it is optional.
    """
    is_optional = False
    field_args = get_args(field_type)

    if get_origin(field_type) is Union and type(None) in field_args:
        is_optional = True
        non_none_args = [arg for arg in field_args if arg is not type(None)]
        field_type = non_none_args[0]

    return field_type, is_optional


def _iceberg_field(
    field: Any, field_type: type, next_id: int
) -> Tuple[Dict[str, Any], int]:
    """
    Convert a field to an Iceberg field representation.

    Parameters
    ----------
    field : Any
        The field object to convert, typically a dataclass field.
    field_type : type
        The Python type of the field.
    next_id : int
        The next available ID to use for this field.

    Returns
    -------
    Tuple[Dict[str, Any], int]
        A tuple containing the Iceberg field representation and the next available ID.

    Raises
    ------
    ValueError
        If the field type is not supported.
    """
    # Check if the field type is optional
    field_type, is_optional = _parse_optional_type(field_type)
    current_id = next_id
    next_id += 1

    iceberg_field = {
        "id": current_id,
        "name": field.name if hasattr(field, "name") else "element",
        "required": not is_optional,
    }

    iceberg_type: Union[str, Dict[str, Any]]
    if field_type in PYTHON_TYPES_TO_ICEBERG:
        # It is a primitive type or a known Iceberg type
        iceberg_type = PYTHON_TYPES_TO_ICEBERG.get(field_type, "")
    elif dataclasses.is_dataclass(field_type):
        # If it is a dataclass, we need to convert it to an Iceberg struct
        iceberg_type, next_id = _dataclass_to_iceberg_schema(
            field_type, next_id=next_id
        )
    elif get_origin(field_type) is list:
        # If it is a list, we need to convert the inner type
        iceberg_type, next_id = _iceberg_list_type(field_type, next_id)
    elif get_origin(field_type) is dict:
        # If it is a dict, we need to convert the key and value types
        iceberg_type, next_id = _iceberg_map_type(field_type, next_id)
    else:
        raise ValueError(
            f"Unsupported type {field_type} for field {iceberg_field['name']}"
        )

    iceberg_field["type"] = iceberg_type
    return iceberg_field, next_id


def _iceberg_list_type(field_type: type, next_id: int) -> Tuple[Dict[str, Any], int]:
    """
    Convert a Python list type to an Iceberg list type.

    Parameters
    ----------
    field_type : type
        The Python list type to convert.
    next_id : int
        The next available ID to use.

    Returns
    -------
    Tuple[Dict[str, Any], int]
        A Tuple containing the Iceberg list type representation and the next available ID.
    """
    inner_type = get_args(field_type)[0]

    element_field = SimpleNamespace(name="element")
    element_id = next_id

    element_iceberg_field, next_id = _iceberg_field(element_field, inner_type, next_id)
    element_iceberg_field["id"] = element_id

    return {
        "type": "list",
        "element-id": element_id,
        "element": element_iceberg_field,
    }, next_id


def _iceberg_map_type(field_type: type, next_id: int) -> Tuple[Dict[str, Any], int]:
    """
    Convert a Python dict type to an Iceberg map type.

    Parameters
    ----------
    field_type : type
        The Python dict type to convert.
    next_id : int
        The next available ID to use.

    Returns
    -------
    Tuple[Dict[str, Any], int]
        A Tuple containing the Iceberg map type representation and the next available ID.
    """
    key_type, value_type = get_args(field_type)

    key_field = SimpleNamespace(name="key")
    value_field = SimpleNamespace(name="value")

    key_id = next_id
    key_iceberg_field, next_id = _iceberg_field(key_field, key_type, next_id)

    value_id = next_id
    value_iceberg_field, next_id = _iceberg_field(value_field, value_type, next_id)

    key_iceberg_field["id"] = key_id
    value_iceberg_field["id"] = value_id

    return {
        "type": "map",
        "key-id": key_id,
        "key": key_iceberg_field,
        "value-id": value_id,
        "value": value_iceberg_field,
    }, next_id


def _dataclass_to_iceberg_schema(
    dataclass: Any, next_id: int = 1
) -> Tuple[Dict[str, Any], int]:
    """
    Convert a dataclass to an Iceberg struct schema.

    Parameters
    ----------
    dataclass : Any
        The dataclass to convert.
    next_id : int, optional
        The next available ID to use, defaults to 1.

    Returns
    -------
    Tuple[Dict[str, Any], int]
        A Tuple containing the Iceberg struct schema and the next available ID.

    Raises
    ------
    ValueError
        If the input is not a dataclass.
    """
    if not dataclasses.is_dataclass(dataclass):
        raise ValueError(f"{dataclass.__name__} is not a dataclass")

    fields = []
    type_hints = get_type_hints(dataclass)
    for field in dataclasses.fields(dataclass):
        field_type = type_hints[field.name]
        iceberg_field, next_id = _iceberg_field(field, field_type, next_id)
        fields.append(iceberg_field)

    return {
        "type": "struct",
        "fields": fields,
    }, next_id


def convert_dataclass_to_iceberg(dataclass: Any, schema_id: int = 0) -> Dict[str, Any]:
    """
    Convert a dataclass to an Iceberg schema.

    This is the main entry point for converting Python dataclasses to Iceberg schemas.

    Parameters
    ----------
    dataclass : Any
        The dataclass to be converted.
    schema_id : int, optional
        The schema ID to use, defaults to 0.

    Returns
    -------
    Dict[str, Any]
        The converted Iceberg schema as a dictionary.

    Raises
    ------
    TypeError
        If the input `dataclass` is not a valid dataclass type.

    Examples
    --------
    ```python
    @dataclass
    class Address:
        street: str
        city: str

    @dataclass
    class Person:
        name: str
        age: Optional[int]
        email_ids: List[str]
        addresses: List[Address]
        employment: Dict[str, str]  # e.g., {"company": "Netflix", "role": "Engineer"}

    schema = convert_dataclass_to_iceberg(Person)
    ```
    """
    if not dataclasses.is_dataclass(dataclass):
        raise TypeError("Input class must be a dataclass type.")

    iceberg_schema, _ = _dataclass_to_iceberg_schema(dataclass)
    iceberg_schema["schema-id"] = schema_id
    return iceberg_schema


def _pivot_schema_helper(dataclass: Any) -> List[Dict[str, Any]]:
    """
    Helper function to convert a dataclass to an Iceberg schema and check for unsupported types.

    Parameters
    ----------
    dataclass : Any
        The dataclass to be converted.

    Returns
    -------
    List[Dict[str, Any]]
        The converted Iceberg schema fields.

    Raises
    ------
    ValueError
        If the dataclass contains unsupported Iceberg types.
    """
    iceberg_schema, _ = _dataclass_to_iceberg_schema(dataclass)
    fields = iceberg_schema["fields"]
    unsupported_types = check_supported_iceberg_schema(fields)
    if unsupported_types:
        raise ValueError(
            f"Unsupported Iceberg types found: {unsupported_types}. "
            "Please ensure the dataclass uses supported types."
        )
    return fields


def convert_dataclass_to_pyarrow(
    dataclass: Any,
) -> "pa.Schema":
    """
    Convert a dataclass to a PyArrow schema.

    Parameters
    ----------
    dataclass : Any
        The dataclass to be converted.

    Returns
    -------
    pa.Schema
        The converted PyArrow schema.
    """
    fields = _pivot_schema_helper(dataclass)
    pyarrow_schema = iceberg_to_pyarrow_schema(cast(Any, fields))
    return pyarrow_schema


def convert_dataclass_to_c_arrow(
    dataclass: Any,
) -> Any:
    """
    Convert a dataclass to a C Arrow schema.

    Parameters
    ----------
    dataclass : Any
        The dataclass to be converted.

    Returns
    -------
    "ffi.CData"
        The converted C Arrow schema.
    """
    fields = _pivot_schema_helper(dataclass)

    c_schema, supporting_objects = iceberg_to_arrow_c_schema(cast(Any, fields))
    # TODO: Do we really need to compute metadata here?
    _ = c_schema_metadata(c_schema)
    return c_schema


def convert_dataclasses_to_json_schema(dataclass: Any) -> List[Dict[str, Any]]:
    """
    Convert a dataclass hierarchy into a structured JSON representation,
    including type and module information for each field.

    Parameters
    ----------
    dataclass : Any
        The dataclass to be converted.

    Returns
    -------
    List[Dict[str, Any]]
        A list of dictionaries, each representing a dataclass in the hierarchy.
    """
    if not dataclasses.is_dataclass(dataclass):
        raise TypeError(f"{dataclass.__name__} is not a dataclass")

    supported_primitive_types = {*PYTHON_TYPES_TO_ICEBERG.keys()}
    supported_primitive_types.add(type(Any))

    processed = {}

    def _format_type(type_obj: type) -> Dict[str, Any]:
        """
        Formats a Python type into a structured dictionary.

        This function inspects a type and represents it as a dictionary
        containing its name, module, and any generic parameters.

        Parameters
        ----------
        type_obj : type
            The type to format (e.g., int, List[str], Optional[MyDataClass]).

        Returns
        -------
        Dict[str, Any]
            A dictionary with keys 'type', 'module', and 'parameters'.
        """
        origin = get_origin(type_obj)
        args = get_args(type_obj)

        type_name_str: str
        module_name_str: str = "unknown"

        if origin is Union and type(None) in args:  # Optional[X] or Union[X, Y, None]
            non_none_args = [arg for arg in args if arg is not type(None)]
            if len(non_none_args) == 1:  # Optional[X]
                inner_type_formatted = _format_type(non_none_args[0])
                return {
                    "type": "Optional",
                    "module": "typing",
                    "parameters": [inner_type_formatted],
                }
            else:  # Union[X, Y, ..., None]
                params = [_format_type(arg) for arg in non_none_args]
                return {"type": "Union", "module": "typing", "parameters": params}

        if origin is not None:  # Generic types (List, Dict, Tuple, etc.)
            name_candidate = getattr(origin, "_name", None) or (
                origin.__name__ if hasattr(origin, "__name__") else str(origin)
            )
            module_name_str = origin.__module__
            return {
                "type": name_candidate,
                "module": module_name_str,
                "parameters": [_format_type(arg) for arg in args],
            }

        # Non-generic: dataclasses, primitives, Any, or instances like pa.int64()
        if dataclasses.is_dataclass(type_obj):
            type_name_str = type_obj.__name__
            module_name_str = type_obj.__module__
        elif hasattr(type_obj, "__name__"):  # int, str, typing.Any, pa.int64 (type)
            type_name_str = type_obj.__name__
            module_name_str = type_obj.__module__
        else:  # Fallback: instance used as type hint (e.g. pa.int64())
            type_name_str = str(type_obj)
            actual_type = type(type_obj)
            if hasattr(actual_type, "__module__"):
                module_name_str = actual_type.__module__
                return {
                    "type": type_name_str,
                    "module": module_name_str,
                    "parameters": [],
                    "instance_hint_details": {  # Provide details if it was an instance
                        "instance_str": str(type_obj),
                        "actual_type_name": actual_type.__name__,
                        "actual_type_module": actual_type.__module__,
                    },
                }
        return {"type": type_name_str, "module": module_name_str, "parameters": []}

    def _check_supported_type(type_obj: type) -> bool:
        """
        Checks if a type is generally supported for schema representation.

        Parameters
        ----------
        type_obj : type
            The type to validate.

        Returns
        -------
        bool
            True if the type is supported.

        Raises
        ------
        TypeError
            If a type is not supported (e.g., a dict with complex keys).
        """
        origin = get_origin(type_obj)
        args = get_args(type_obj)

        if dataclasses.is_dataclass(type_obj) or type_obj in supported_primitive_types:
            return True
        if origin is Union and type(None) in args:
            return _check_supported_type(
                next(arg for arg in args if arg is not type(None))
            )
        if origin is list:
            return _check_supported_type(args[0])
        if origin is dict:
            key_type, value_type = args[0], args[1]
            if dataclasses.is_dataclass(key_type) or get_origin(key_type) is not None:
                raise TypeError(f"Dict keys must be primitive, not {key_type}")
            if key_type not in supported_primitive_types and key_type is not Any:
                raise TypeError(f"Unsupported key type for Dict: {key_type}")
            return _check_supported_type(value_type)
        return True

    def _collect_nested_dataclasses(type_obj: type):
        """
        Finds and triggers the processing of nested dataclasses within a type.

        Parameters
        ----------
        type_obj : type
            The type to inspect for nested dataclasses.

        Returns
        -------
        None
        """
        origin = get_origin(type_obj)
        args = get_args(type_obj)

        if dataclasses.is_dataclass(type_obj):
            _process_dataclass(type_obj)
            return
        if origin is Union and type(None) in args:
            _collect_nested_dataclasses(
                next(arg for arg in args if arg is not type(None))
            )
            return
        if origin is list:
            _collect_nested_dataclasses(args[0])
        elif origin is dict:
            _collect_nested_dataclasses(
                args[1]
            )  # Check value type for nested dataclasses
        elif origin is not None and args:  # Other generics like Tuple
            for arg_type in args:
                _collect_nested_dataclasses(arg_type)

    def _process_dataclass(dc: Any):
        """
        Processes a single dataclass and adds its schema to the 'processed' dict.

        This function handles cycle detection by marking a dataclass as
        'processing' before iterating through its fields.

        Parameters
        ----------
        dc : Any
            The dataclass type to process.
        """
        if dc.__name__ in processed:
            return
        processed[dc.__name__] = {
            "name": dc.__name__,
            "fields": "processing...",
        }  # cycle detection

        fields_dict = {}
        type_hints = get_type_hints(dc)

        for field in dataclasses.fields(dc):
            field_type = type_hints[field.name]
            try:
                _check_supported_type(field_type)
            except TypeError as e:
                raise TypeError(f"In {dc.__name__}.{field.name}: {str(e)}")
            fields_dict[field.name] = _format_type(field_type)
            _collect_nested_dataclasses(field_type)

        processed[dc.__name__] = {"name": dc.__name__, "fields": fields_dict}

    try:
        _process_dataclass(dataclass)
    except TypeError as e:
        raise TypeError(
            f"Error converting dataclass '{getattr(dataclass, '__name__', str(dataclass))}': {str(e)}"
        )

    return list(processed.values())


# Convert Avro to PyArrow
#
_AVRO_ARROW_MAP = None
_CACHED_AVRO_ARROW_SCHEMA: Optional[Dict[str, "pa.Schema"]] = None


def _build_avro_to_pyarrow_map():
    """
    Build a mapping from Avro type strings to PyArrow types.

    Returns
    -------
    Dict[str, Any]
        A dictionary mapping Avro type strings to PyArrow data types.
    """
    return {
        "null": pyarrow.null(),
        "boolean": pyarrow.bool_(),
        "int": pyarrow.int32(),
        "long": pyarrow.int64(),
        "float": pyarrow.float32(),
        "double": pyarrow.float64(),
        "bytes": pyarrow.binary(),
        "string": pyarrow.string(),
    }


def _avro_get_field_type(avro_type: Union[str, Dict, List]) -> Any:  # type: ignore[misc]
    """
    Get the PyArrow type for an Avro type specification.

    Parameters
    ----------
    avro_type : Union[str, dict, list]
        The Avro type specification. Can be:
        - A string for primitive types ("int", "string", etc.)
        - A dict for complex types ({"type": "record", "fields": [...]})
        - A list for union types. Only ["null", <type>] or [<type>, "null"]
          unions are supported (nullable types).

    Returns
    -------
    pa.DataType
        The corresponding PyArrow data type.

    Raises
    ------
    MetaflowDataFrameException
        If the Avro type is not supported, or if union types other than
        nullable types are encountered.
    """
    global _AVRO_ARROW_MAP

    # Handle union types (lists) - only support nullable types
    if isinstance(avro_type, list):
        # Check if this is a nullable type (union with null and one other type)
        if len(avro_type) == 2:
            # Extract null and non-null types
            non_null_types = [t for t in avro_type if t != "null"]
            has_null = len(non_null_types) < len(avro_type)

            # If it's exactly ["null", <type>] or [<type>, "null"]
            if has_null and len(non_null_types) == 1:
                # PyArrow handles nullability by default, so just use the non-null type
                return _avro_get_field_type(non_null_types[0])

        # Reject all other union types
        raise MetaflowDataFrameException(
            f"Union types other than nullable types are not supported: {avro_type}. "
            "Only unions of the form ['null', <type>] or [<type>, 'null'] are allowed."
        )

    # Handle primitive types
    elif isinstance(avro_type, str):
        if avro_type in _AVRO_ARROW_MAP:  # type: ignore[operator]
            return _AVRO_ARROW_MAP[avro_type]  # type: ignore[index]
        raise MetaflowDataFrameException(
            f"Unsupported Avro primitive type: {avro_type}"
        )

    # Handle complex types (dicts)
    elif isinstance(avro_type, dict):
        type_name = avro_type["type"]
        if type_name == "record":
            # Struct type
            fields = []
            for field in avro_type.get("fields", []):
                fields.append((field["name"], _avro_get_field_type(field["type"])))
            return pyarrow.struct(fields)  # type: ignore[union-attr]

        elif type_name == "array":
            # List type
            items_type = _avro_get_field_type(avro_type["items"])
            return pyarrow.list_(items_type)  # type: ignore[union-attr]

        elif type_name == "map":
            # Map type - Avro maps have string keys and specified value types
            values_type = _avro_get_field_type(avro_type["values"])
            return pyarrow.map_(pyarrow.string(), values_type)  # type: ignore[union-attr]

        elif type_name == "enum":
            raise MetaflowDataFrameException(
                f"Enum type ({avro_type}) is not supported."
            )

        raise MetaflowDataFrameException(f"Unsupported Avro type: {type_name}")

    raise MetaflowDataFrameException(f"Unsupported Avro type: {avro_type}")


def avro_to_pyarrow_schema(avro_schema: Dict[Any, Any]) -> "pa.Schema":
    """
    Convert an Avro schema to a PyArrow schema.

    Parameters
    ----------
    avro_schema : Dict[Any, Any]
        The Avro schema as a dict representing a record type with "type": "record" and "fields".

    Returns
    -------
    pa.Schema
        The converted PyArrow schema.

    Raises
    ------
    MetaflowDataFrameException
        If the Avro schema contains unsupported field types or is malformed.

    Examples
    --------
    >>> schema_dict = {
    ...     "type": "record",
    ...     "name": "User",
    ...     "fields": [
    ...         {"name": "name", "type": "string"},
    ...         {"name": "age", "type": ["null", "int"]},
    ...         {"name": "emails", "type": {"type": "array", "items": "string"}}
    ...     ]
    ... }
    >>> pa_schema = avro_to_pyarrow_schema(schema_dict)
    """
    lazy_import_pyarrow()

    global _AVRO_ARROW_MAP
    if _AVRO_ARROW_MAP is None:
        _AVRO_ARROW_MAP = _build_avro_to_pyarrow_map()

    # Check cache
    global _CACHED_AVRO_ARROW_SCHEMA
    if _CACHED_AVRO_ARROW_SCHEMA is None:
        _CACHED_AVRO_ARROW_SCHEMA = {}

    schema_name = avro_schema["name"]
    if schema_name in _CACHED_AVRO_ARROW_SCHEMA:
        return _CACHED_AVRO_ARROW_SCHEMA[schema_name]

    # We expect a record
    if avro_schema["type"] != "record":
        raise MetaflowDataFrameException(
            "Avro schema must be a record type with fields. "
            f"Got type: {avro_schema.get('type')}"
        )

    # Process fields recursively
    fields = []
    for field in avro_schema.get("fields", []):
        fields.append((field["name"], _avro_get_field_type(field["type"])))
    schema = pyarrow.schema(fields)  # type: ignore[union-attr]

    # Cache the schema
    if schema_name:
        _CACHED_AVRO_ARROW_SCHEMA[schema_name] = schema

    return schema


# Convert proto to PyArrow
#
_PROTO_ARROW_MAP = None
_PROTO_WRAPPER_ARROW_MAP = None
_CACHED_PROTO_ARROW_SCHEMA: Optional[Dict[str, "pa.Schema"]] = None


def _build_proto_to_pyarrow_map():
    """
    Build a mapping from protobuf FieldDescriptor types to PyArrow types.

    Returns
    -------
    Dict[int, Any]
        A dictionary mapping FieldDescriptor type constants to PyArrow data types.
    """
    return {
        FieldDescriptor.TYPE_DOUBLE: pyarrow.float64(),
        FieldDescriptor.TYPE_FLOAT: pyarrow.float32(),
        FieldDescriptor.TYPE_INT64: pyarrow.int64(),
        FieldDescriptor.TYPE_UINT64: pyarrow.uint64(),
        FieldDescriptor.TYPE_INT32: pyarrow.int32(),
        FieldDescriptor.TYPE_UINT32: pyarrow.uint32(),
        FieldDescriptor.TYPE_BOOL: pyarrow.bool_(),
        FieldDescriptor.TYPE_STRING: pyarrow.string(),
        FieldDescriptor.TYPE_BYTES: pyarrow.binary(),
    }


def _build_proto_wrapper_arrow_map():
    """
    Build a mapping from protobuf wrapper type full names to PyArrow types.

    Wrapper types (google.protobuf.*Value) should map to their corresponding
    primitive PyArrow types rather than being treated as structs.

    Returns
    -------
    Dict[str, Any]
        A dictionary mapping wrapper type full names to PyArrow data types.
    """
    return {
        "google.protobuf.StringValue": pyarrow.string(),
        "google.protobuf.Int32Value": pyarrow.int32(),
        "google.protobuf.Int64Value": pyarrow.int64(),
        "google.protobuf.UInt32Value": pyarrow.uint32(),
        "google.protobuf.UInt64Value": pyarrow.uint64(),
        "google.protobuf.FloatValue": pyarrow.float32(),
        "google.protobuf.DoubleValue": pyarrow.float64(),
        "google.protobuf.BoolValue": pyarrow.bool_(),
        "google.protobuf.BytesValue": pyarrow.binary(),
    }


def _proto_get_field_type(field: "FieldDescriptorType") -> Any:  # type: ignore[misc]
    """
    Get the PyArrow type for a protobuf field.

    Parameters
    ----------
    field : google.protobuf.descriptor.FieldDescriptor
        The field descriptor.

    Returns
    -------
    pa.DataType
        The corresponding PyArrow data type.

    Raises
    ------
    MetaflowDataFrameException
        If the protobuf field type is not supported or if the protobuf
        library is not installed.
    """
    global _PROTO_ARROW_MAP
    global _PROTO_WRAPPER_ARROW_MAP

    # Handle nested messages (structs)
    if field.type == FieldDescriptor.TYPE_MESSAGE:  # type: ignore[union-attr]
        nested_descriptor = field.message_type  # type: ignore[union-attr]

        # Check if this is a wrapper type first
        if _PROTO_WRAPPER_ARROW_MAP and nested_descriptor.full_name in _PROTO_WRAPPER_ARROW_MAP:  # type: ignore[union-attr]
            return _PROTO_WRAPPER_ARROW_MAP[nested_descriptor.full_name]  # type: ignore[index]

        # Regular nested message - treat as struct
        nested_fields = [_proto_convert_field(f) for f in nested_descriptor.fields]  # type: ignore[union-attr]
        return pyarrow.struct(nested_fields)  # type: ignore[union-attr]

    # Handle enums as int32
    if field.type == FieldDescriptor.TYPE_ENUM:  # type: ignore[union-attr]
        return pyarrow.int32()  # type: ignore[union-attr]

    # Handle primitive types
    if field.type in _PROTO_ARROW_MAP:  # type: ignore[union-attr,operator]
        return _PROTO_ARROW_MAP[field.type]  # type: ignore[index]

    raise MetaflowDataFrameException(
        f"Unsupported protobuf field type: {field.type} for field {field.name}"  # type: ignore[union-attr]
    )


def _proto_convert_field(field: "FieldDescriptorType") -> Tuple[str, Any]:  # type: ignore[misc]
    """
    Convert a single protobuf field descriptor to a PyArrow field.

    Parameters
    ----------
    field : google.protobuf.descriptor.FieldDescriptor
        The field descriptor to convert.

    Returns
    -------
    Tuple[str, pa.DataType]
        A tuple of (field_name, pyarrow_type).

    Raises
    ------
    MetaflowDataFrameException
        If the protobuf library is not installed.
    """
    field_name = field.name  # type: ignore[union-attr]

    # Handle map fields (repeated message with map_entry option)
    if field.label == FieldDescriptor.LABEL_REPEATED:  # type: ignore[union-attr]
        # Check if this is a map type
        if (
            field.type == FieldDescriptor.TYPE_MESSAGE  # type: ignore[union-attr]
            and field.message_type.GetOptions().map_entry
        ):  # type: ignore[union-attr]
            # This is a map field - extract key and value types
            # Map entry messages have exactly 2 fields: 'key' and 'value'
            key_field = field.message_type.fields_by_name["key"]  # type: ignore[union-attr]
            value_field = field.message_type.fields_by_name["value"]  # type: ignore[union-attr]

            key_type = _proto_get_field_type(key_field)
            value_type = _proto_get_field_type(value_field)

            return (field_name, pyarrow.map_(key_type, value_type))  # type: ignore[union-attr]
        else:
            # Regular repeated field (list/array)
            element_type = _proto_get_field_type(field)
            return (field_name, pyarrow.list_(element_type))  # type: ignore[union-attr]

    # Handle regular fields
    field_type = _proto_get_field_type(field)
    return (field_name, field_type)


def proto_to_pyarrow_schema(proto_message: Any) -> "pa.Schema":
    """
    Convert a protobuf message to a PyArrow schema.

    Parameters
    ----------
    proto_message : google.protobuf.message.Message
        The protobuf message instance or message class to convert.

    Returns
    -------
    pa.Schema
        The converted PyArrow schema.

    Raises
    ------
    MetaflowDataFrameException
        If the protobuf message contains unsupported field types.
    """
    lazy_import_protobuf()
    lazy_import_pyarrow()

    global _PROTO_ARROW_MAP
    if _PROTO_ARROW_MAP is None:
        _PROTO_ARROW_MAP = _build_proto_to_pyarrow_map()

    global _PROTO_WRAPPER_ARROW_MAP
    if _PROTO_WRAPPER_ARROW_MAP is None:
        _PROTO_WRAPPER_ARROW_MAP = _build_proto_wrapper_arrow_map()

    # Get the descriptor from the message
    if hasattr(proto_message, "DESCRIPTOR"):
        descriptor = proto_message.DESCRIPTOR
    else:
        raise MetaflowDataFrameException(
            "proto_message must be a protobuf message class or instance"
        )

    # Check if the schema has already been computed for this object
    global _CACHED_PROTO_ARROW_SCHEMA
    if _CACHED_PROTO_ARROW_SCHEMA is None:
        _CACHED_PROTO_ARROW_SCHEMA = {}

    if descriptor.name in _CACHED_PROTO_ARROW_SCHEMA:
        return _CACHED_PROTO_ARROW_SCHEMA[descriptor.name]

    # Convert all fields in the message
    fields = [_proto_convert_field(field) for field in descriptor.fields]
    schema = pyarrow.schema(fields)  # type: ignore[union-attr]
    _CACHED_PROTO_ARROW_SCHEMA[descriptor.name] = schema
    return schema
