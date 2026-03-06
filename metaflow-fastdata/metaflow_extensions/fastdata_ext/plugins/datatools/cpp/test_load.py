import os
import cffi

ffi = cffi.FFI()
path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "build", "metaflow-data.so"
)
MD = ffi.dlopen(path)
