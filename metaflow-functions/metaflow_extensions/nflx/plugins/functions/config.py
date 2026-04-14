import os


class Config(object):
    VERSION = "v2"
    FUNCTIONS_ENV = os.environ.get("FUNCTIONS_ENVIRONMENT", "prod")
    RUNTIME_FUNCTION_DIR_PREFIX = "metaflow-function-"


# Shared memory configuration functions (evaluated lazily to avoid import order issues)
def get_default_buffer_size():
    """
    Get default buffer size for shared memory (512MB in production).
    Can be overridden via METAFLOW_FUNCTIONS_BUFFER_SIZE environment variable.
    """
    return int(os.environ.get("METAFLOW_FUNCTIONS_BUFFER_SIZE", 512 * 1024**2))


def get_min_buffer_size():
    """Get minimum buffer size (32MB)."""
    return 32 * 1024**2
