import pickle
import platform
from metaflow.metaflow_config_funcs import from_conf

###
# Default configuration
###
#
DEFAULT_FUNCTIONS_S3_ROOT = from_conf(
    "DEFAULT_FUNCTIONS_S3_ROOT",
    None,
)

DEFAULT_FUNCTIONS_LOCAL_ROOT = from_conf(
    "DEFAULT_FUNCTIONS_LOCAL_ROOT", "/tmp/metaflow_functions"
)

FUNCTION_PACKAGE_USER_METAFLOW = from_conf("FUNCTION_PACKAGE_USER_METAFLOW", "0")

# Use /tmp on macOS since /mnt is read-only, /mnt on Linux
_default_runtime_path = "/tmp" if platform.system() == "Darwin" else "/mnt"
FUNCTION_RUNTIME_PATH = from_conf("FUNCTION_RUNTIME_PATH", _default_runtime_path)

FUNCTION_BACKEND = from_conf("FUNCTION_BACKEND", "memory")

FUNCTION_CLEAN_DIR_ON_DEL = from_conf("FUNCTION_CLEAN_DIR_ON_DEL", "1")

# Ray backend configuration
FUNCTION_RAY_ADDRESS = from_conf("FUNCTION_RAY_ADDRESS", None)
FUNCTION_RAY_OBJECT_STORE_MEMORY = from_conf(
    "FUNCTION_RAY_OBJECT_STORE_MEMORY", str(256 * 1024 * 1024)
)  # 256MB default


###
# Debug options for the functions extension
###
DEBUG_OPTIONS = ["functions"]


###
# Override pinned conda libraries to include libraries necessary for functions.
# This list should match metaflow-functions/setup.py install_requires.
#
# IMPORTANT: every entry must carry a real (non-empty) version specifier.
# Metaflow's extension merge logic joins duplicate keys with a comma:
#     d1[k] = v if k not in d1 else ",".join([d1[k], v])
# If this extension and another both pin the same package (e.g. cffi is also
# pinned by nflx-fastdata) and one side uses an empty string, the join produces
# `,>=X` (leading comma) or `>=X,` (trailing comma), which downstream becomes
# a malformed conda spec like `cffi==,>=1.13.0,!=1.15.0` and fails resolution.
# Use a real lower bound instead — duplicate constraints in the joined spec
# are harmless (conda/mamba dedupes them at solve time).
###
def get_pinned_conda_libs(python_version, datastore_type):
    return {
        "psutil": ">=5.8.0",
        # cffi 1.15.0 has an ABI regression (libffi mismatch on some platforms),
        # so exclude it explicitly — matches nflx-fastdata's pin.
        "cffi": ">=1.13.0,!=1.15.0",
        "fastavro": ">=1.6.0",
        "ray-default": ">=2.0",
    }
