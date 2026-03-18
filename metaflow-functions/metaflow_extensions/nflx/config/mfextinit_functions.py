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
# This list should match metaflow-functions/setup.py install_requires
###
def get_pinned_conda_libs(python_version, datastore_type):
    # Use "ray" (not "ray-default") so this works in both conda and pip environments.
    # ray-default is a conda-forge alias; pip only knows "ray".
    return {"psutil": ">=5.8.0", "cffi": "", "fastavro": "", "ray": ""}
