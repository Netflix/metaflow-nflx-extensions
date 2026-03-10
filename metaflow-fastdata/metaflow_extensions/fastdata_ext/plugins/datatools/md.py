import os
import cffi
from typing import TYPE_CHECKING

from metaflow.exception import MetaflowException

if TYPE_CHECKING:
    from cffi import FFI
    from typing import Any

# Module-level variables that may be defined conditionally
ffi: "FFI"
MD: "Any"


class MetaflowDataLoadingException(MetaflowException):
    headline = "Metaflow-data cannot be loaded"


# Define the C data structures.
c_source = """
struct ArrowSchema {
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

    // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowChunkedArray {
  int64_t n_chunks;
  struct ArrowArray** chunks;
};

// Forward declare wrapper types
struct CMetaflowDataFrame;
struct CMetaflowChunkedArray;
struct CMetaflowArray;

"""


if os.environ.get("METAFLOW_STUBGEN", None) is None:
    try:
        bindir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin")

        header_file = os.path.join(bindir, "c_api_funcs.h")
        if not os.path.exists(header_file):
            raise MetaflowDataLoadingException(
                "Cannot find header file: %s" % header_file
            )

        with open(header_file, "r") as f:
            c_source += f.read()

        ffi = cffi.FFI()
        ffi.cdef(c_source)

        mdfile = [f for f in os.listdir(bindir) if f.endswith(".so")]
        if not mdfile:
            raise MetaflowDataLoadingException(
                "Cannot find Metaflow-data shared object in '%s/'." % bindir
            )

        MD = ffi.dlopen(os.path.join(bindir, mdfile[0]))
    except OSError as e:
        raise MetaflowDataLoadingException("Cannot load Metaflow-data. %s" % str(e))
