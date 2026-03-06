"""
Schema validation utilities for MetaflowDataFrame.

Provides runtime validation of DataFrame columns against dataclass schemas.
This is the practical approach to schema enforcement since Python's type system
cannot provide static type checking for mdf["column_name"] operations.
"""

from dataclasses import fields, is_dataclass
from typing import Any, Dict, List, Set, Type, get_type_hints, get_origin, get_args
import datetime

from .exceptions import MetaflowDataFrameException


class SchemaValidationError(MetaflowDataFrameException):
    """Raised when DataFrame schema validation fails."""

    headline = "Schema validation failed"


# Cached FFI instance for CFFI cdata string conversion (lazy-initialized)
_ffi_instance = None


def _get_ffi():
    """Get or create cached FFI instance."""
    global _ffi_instance
    if _ffi_instance is None:
        from cffi import FFI

        _ffi_instance = FFI()
    return _ffi_instance


# Arrow format codes to Python types
# Based on Arrow C Data Interface format strings
# https://arrow.apache.org/docs/format/CDataInterface.html#format-strings
ARROW_TO_PYTHON_COMPATIBLE: Dict[str, List[Type]] = {
    # Boolean
    "b": [bool, int],
    # Signed integers
    "c": [int],  # int8
    "s": [int],  # int16
    "i": [int],  # int32
    "l": [int],  # int64
    # Unsigned integers
    "C": [int],  # uint8
    "S": [int],  # uint16
    "I": [int],  # uint32
    "L": [int],  # uint64
    # Floating point
    "e": [float, int],  # float16
    "f": [float, int],  # float32
    "g": [float, int],  # float64
    # Strings
    "u": [str],  # utf8 string
    "U": [str],  # large utf8 string
    "z": [bytes, str],  # binary
    "Z": [bytes, str],  # large binary
    # Date/time types
    "tdD": [datetime.date, datetime.datetime],  # date32 (days)
    "tdm": [datetime.date, datetime.datetime],  # date64 (milliseconds)
    # Timestamp types (with timezone variations)
    "tss:": [datetime.datetime],  # timestamp seconds (no tz)
    "tsm:": [datetime.datetime],  # timestamp milliseconds (no tz)
    "tsu:": [datetime.datetime],  # timestamp microseconds (no tz)
    "tsn:": [datetime.datetime],  # timestamp nanoseconds (no tz)
    # Duration types
    "tDs": [datetime.timedelta],  # duration seconds
    "tDm": [datetime.timedelta],  # duration milliseconds
    "tDu": [datetime.timedelta],  # duration microseconds
    "tDn": [datetime.timedelta],  # duration nanoseconds
}


def _normalize_type(python_type: Type) -> Type:
    """Normalize a type annotation to handle Optional and Union types."""
    from typing import Union

    origin = get_origin(python_type)
    if origin is type(None):
        return type(None)

    # Handle Optional[X] which is Union[X, None]
    # Only simplify if the origin is Union (not List, Dict, etc.)
    if origin is Union:
        args = get_args(python_type)
        if args:
            # Filter out NoneType for Optional types
            non_none_args = [a for a in args if a is not type(None)]
            if len(non_none_args) == 1:
                return non_none_args[0]

    return python_type


def _is_list_type(python_type: Type) -> bool:
    """Check if a type is a List/list type."""
    # Handle bare list type
    if python_type is list:
        return True
    origin = get_origin(python_type)
    return origin in (list, List)


def _is_dict_type(python_type: Type) -> bool:
    """Check if a type is a Dict/dict type."""
    if python_type is dict:
        return True
    origin = get_origin(python_type)
    return origin is dict


def _is_compatible_type(arrow_format: str, python_type: Type) -> bool:
    """
    Check if an Arrow format is compatible with a Python type.

    Args:
        arrow_format: Arrow format string (e.g., 'l' for int64)
        python_type: Python type from dataclass annotation

    Returns:
        True if types are compatible
    """
    # Normalize the type to handle Optional
    python_type = _normalize_type(python_type)

    # Handle Any type - always compatible
    if python_type is Any:
        return True

    # Handle list types (+l = list, +L = large list)
    if _is_list_type(python_type):
        if arrow_format.startswith("+l") or arrow_format.startswith("+L"):
            return True
        return False

    # Handle struct/map types -> dict (bare dict or Dict[K, V])
    if _is_dict_type(python_type):
        return arrow_format.startswith("+s") or arrow_format.startswith("+m")

    # Handle timestamp formats with timezone info
    # Format like "tsm:UTC" or "tsu:America/Los_Angeles"
    for ts_prefix in ["tss:", "tsm:", "tsu:", "tsn:"]:
        if arrow_format.startswith(ts_prefix):
            compatible_types = ARROW_TO_PYTHON_COMPATIBLE.get(ts_prefix, [])
            return python_type in compatible_types

    # Handle decimal formats like "d:10,2"
    if arrow_format.startswith("d:"):
        return python_type in (float, int)

    # Handle date formats
    if arrow_format in ("tdD", "tdm"):
        compatible_types = ARROW_TO_PYTHON_COMPATIBLE.get(arrow_format, [])
        return python_type in compatible_types

    # Standard format lookup
    compatible_types = ARROW_TO_PYTHON_COMPATIBLE.get(arrow_format, [])
    return python_type in compatible_types


def _validate_column_types(
    columns: Dict[str, Any],
    expected: Type,
) -> List[str]:
    """
    Validate that column types match the expected dataclass field types.

    Args:
        columns: Dict mapping column names to column objects (with format attribute)
        expected: Dataclass type with type annotations

    Returns:
        List of error messages for type mismatches
    """
    errors: List[str] = []

    try:
        type_hints = get_type_hints(expected)
    except Exception:
        # If we can't get type hints, skip type validation
        return errors

    for field in fields(expected):
        field_name = field.name
        if field_name not in columns:
            continue  # Column existence is checked separately

        col = columns[field_name]
        if not hasattr(col, "_schema") or not hasattr(col._schema, "format"):
            continue  # Can't check type without schema info

        arrow_format = col._schema.format
        if isinstance(arrow_format, str):
            pass  # already a string
        elif isinstance(arrow_format, bytes):
            arrow_format = arrow_format.decode("utf-8")
        else:
            # Handle CFFI cdata 'char *' from Arrow C Data Interface
            try:
                raw = _get_ffi().string(arrow_format)
                arrow_format = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            except Exception:
                continue  # Skip type check for this column if format unreadable

        python_type = type_hints.get(field_name, Any)

        if not _is_compatible_type(arrow_format, python_type):
            errors.append(
                f"Column '{field_name}' has Arrow type '{arrow_format}' "
                f"which is not compatible with Python type '{python_type}'"
            )

    return errors


def validate_schema(
    columns: Dict[str, Any],
    actual_columns: Set[str],
    expected: Type[Any],
    strict: bool = False,
    check_types: bool = False,
) -> None:
    """
    Validate DataFrame columns against an expected dataclass schema.

    Args:
        columns: Dict mapping column names to column objects
        actual_columns: Set of column names in the DataFrame
        expected: Dataclass type defining expected columns and types
        strict: If True, DataFrame must have exactly the columns in schema (no extras)
        check_types: If True, validate column types match dataclass field types

    Raises:
        SchemaValidationError: If validation fails
    """
    if not is_dataclass(expected):
        raise SchemaValidationError(
            f"Expected schema must be a dataclass, got {type(expected).__name__}"
        )

    # Get expected column names from dataclass fields
    expected_columns = {f.name for f in fields(expected)}

    # Check for missing columns
    missing = expected_columns - actual_columns
    if missing:
        raise SchemaValidationError(
            f"Missing required columns: {sorted(missing)}. "
            f"DataFrame has columns: {sorted(actual_columns)}"
        )

    # In strict mode, check for extra columns
    if strict:
        extra = actual_columns - expected_columns
        if extra:
            raise SchemaValidationError(
                f"Unexpected columns in strict mode: {sorted(extra)}. "
                f"Expected only: {sorted(expected_columns)}"
            )

    # Optionally validate types
    if check_types:
        type_errors = _validate_column_types(columns, expected)
        if type_errors:
            raise SchemaValidationError(
                "Type validation failed:\n  " + "\n  ".join(type_errors)
            )
