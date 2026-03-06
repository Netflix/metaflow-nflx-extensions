import sys

# Name of this customization
__mf_extensions__ = "fastdata"

# Datatools
from metaflow.plugins.datatools import MetaflowDataFrame
from metaflow.plugins.datatools import Cache, CacheError
from ..plugins.table import Table

# Functions (Python 3.10+ only)
if sys.version_info >= (3, 10):
    try:
        from ..plugins.dataframe_function.dataframe_function import (
            dataframe_function,
            DataFrameFunction,
        )
    except ModuleNotFoundError:
        # functions aren't installed
        pass

from .fastdata_version import _ext_version

__version__ = _ext_version
