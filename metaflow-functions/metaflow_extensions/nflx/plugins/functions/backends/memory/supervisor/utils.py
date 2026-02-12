from dataclasses import dataclass
from collections import namedtuple
from typing import Optional
from metaflow_extensions.nflx.plugins.functions.core.function import (
    MetaflowFunction,
)
from metaflow_extensions.nflx.plugins.functions.backends.memory.runtime import (
    FunctionRuntime,
)


FunctionLease = namedtuple("FunctionLease", ["uuid", "runtime"])


@dataclass
class FunctionProcess:
    uuid: Optional[str] = None  # uuid of this system process
    leased: int = 0  # The number of times this process is leased
    function: Optional[MetaflowFunction] = None  # the function being run in the runtime
    runtime: Optional[FunctionRuntime] = None  # the runtime that backs this system
