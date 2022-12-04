from . import conda
from .conda.conda_flow_decorator import CondaFlowDecorator
from .conda.conda_step_decorator import CondaStepDecorator
from .conda.conda_environment import CondaEnvironment

FLOW_DECORATORS = [CondaFlowDecorator]
STEP_DECORATORS = [CondaStepDecorator]
ENVIRONMENTS = [CondaEnvironment]

__mf_promot_submodules__ = ["conda"]
