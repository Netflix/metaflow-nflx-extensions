from . import conda
from .conda.conda_flow_decorator import CondaFlowDecorator
from .conda.conda_step_decorator import CondaStepDecorator
from .conda.conda_environment import CondaEnvironment

FLOW_DECORATORS = [CondaFlowDecorator]
STEP_DECORATORS = [CondaStepDecorator]
ENVIRONMENTS = [CondaEnvironment]

def get_plugin_cli():
    from . import environment_cli
    return [environment_cli.cli]

__mf_promote_submodules__ = ["conda"]
