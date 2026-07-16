from ..nflx_compat import install as _install_nflx_compat

# Installs the nflx→netflixext backward-compat import shim.
# Limitation: the shim is only active after this toplevel module loads as part
# of Metaflow's extension bootstrap.  Code that imports metaflow_extensions.nflx.*
# before `import metaflow` (e.g. standalone scripts, remote bootstrap steps that
# run before metaflow initializes) will not be redirected and must use the
# netflixext.* path directly.
_install_nflx_compat()

try:
    from .nflxext_version import nflxext_version
except ImportError:
    try:
        from importlib.metadata import version
    except ImportError:
        try:
            from importlib_metadata import version
        except ImportError:
            version = None

    try:
        nflxext_version = version("metaflow-netflixext") if version else "0+unknown"
    except Exception:
        nflxext_version = "0+unknown"

from ..plugins.conda.conda_flow_mutator import (
    ResolvedUVEnvFlowDecorator as resolved_uv,
    ResolvedReqFlowDecorator as resolved_req,
)

# Alias for Runner to make it more natural with environment command
from metaflow import Runner as FlowAPI

_addl_stubgen_modules = [
    "metaflow_extensions.netflixext.plugins.environment_cli",
]

__mf_extensions__ = "netflix-ext"
__version__ = nflxext_version
