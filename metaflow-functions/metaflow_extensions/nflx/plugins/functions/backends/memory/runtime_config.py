import os


class RuntimeConfig(object):
    """
    A configuration class for all function runtime settings.
    """

    IS_DEBUG = os.environ.get("METAFLOW_DEBUG_FUNCTIONS", "") == "1"

    USER_EXIT_CODE = 100
    SYSTEM_EXIT_CODE = 101
    WAIT_TIMEOUT = float(os.environ.get("METAFLOW_FUNCTIONS_WAIT_TIMEOUT", "1"))
    STREAM_READ_BUFFER_SIZE = 1024
    MAX_SINGLE_LOG_FILE_SIZE_BYTES = 100 * 1024 * 1024
    MAX_TOTAL_LOG_FILE_SIZE_BYTES = 10 * MAX_SINGLE_LOG_FILE_SIZE_BYTES
