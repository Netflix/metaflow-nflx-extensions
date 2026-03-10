# Name of this customization
__mf_extensions__ = "functions"

import sys

if sys.version_info >= (3, 10):
    from ..plugins import functions
    from ..plugins.functions.core.function import function_from_json, close_function
    from ..plugins.functions.core.function_pipeline import FunctionPipeline
    from ..plugins.functions.core.function_parameters import FunctionParameters
    from ..plugins.functions.backends.memory.supervisor.supervisor import (
        function_supervisor,
    )

from .functions_version import _ext_version

__version__ = _ext_version
