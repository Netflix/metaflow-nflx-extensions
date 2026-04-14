from .nflxext_version import nflxext_version

from ..plugins.conda.conda_flow_mutator import (
    ResolvedUVEnvFlowDecorator as resolved_uv,
    ResolvedReqFlowDecorator as resolved_req,
)

# Alias for Runner to make it more natural with environment command
from metaflow import Runner as FlowAPI

_addl_stubgen_modules = [
    "metaflow_extensions.nflx.plugins.environment_cli",
]

__mf_extensions__ = "netflix-ext"
__version__ = nflxext_version
