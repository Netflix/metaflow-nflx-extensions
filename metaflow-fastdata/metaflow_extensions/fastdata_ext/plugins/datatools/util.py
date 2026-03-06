"""
Utility functions
"""

from .md import ffi


def as_array(data, length, offset=0, array_type="B", itemsize=1):
    return memoryview(ffi.buffer(data + offset * itemsize, length * itemsize)).cast(
        array_type
    )
