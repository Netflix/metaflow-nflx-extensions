from dataclasses import dataclass
from collections import namedtuple
from typing import Optional, Tuple
from metaflow_extensions.nflx.plugins.functions.core.function import (
    MetaflowFunction,
)
from metaflow_extensions.nflx.plugins.functions.backends.memory.runtime import (
    FunctionRuntime,
)


# Identifies a runtime: the function's content-hash uuid, the process count
# it was created with, and the serialized runtime component specs it was
# created with. Two handles only share a runtime if all three match.
RuntimeKey = Tuple[str, int, Tuple[str, ...]]


FunctionLease = namedtuple("FunctionLease", ["key", "runtime"])


@dataclass
class FunctionProcess:
    key: Optional[RuntimeKey] = None  # runtime identity backing this process
    leased: int = 0  # The number of calls currently in flight against this process
    attached: int = 0  # The number of handles that currently own this process
    function: Optional[MetaflowFunction] = None  # the function being run in the runtime
    runtime: Optional[FunctionRuntime] = None  # the runtime that backs this system
