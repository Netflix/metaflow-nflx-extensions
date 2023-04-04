CLIS_DESC = [("environment", ".environment_cli.cli")]

FLOW_DECORATORS_DESC = [
    ("conda_base", ".conda.conda_flow_decorator.CondaFlowDecorator"),
    ("pip_base", ".conda.conda_flow_decorator.PipFlowDecorator"),
]
STEP_DECORATORS_DESC = [
    ("conda", ".conda.conda_step_decorator.CondaStepDecorator"),
    ("pip", ".conda.conda_step_decorator.PipStepDecorator"),
]
ENVIRONMENTS_DESC = [("conda", ".conda.conda_environment.CondaEnvironment")]

__mf_promote_submodules__ = ["conda"]
