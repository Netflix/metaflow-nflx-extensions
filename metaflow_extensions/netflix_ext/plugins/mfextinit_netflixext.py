FLOW_DECORATORS_DESC = [
    ("conda_base", ".conda.conda_flow_decorator.CondaRequirementFlowDecorator"),
    ("pypi_base", ".conda.conda_flow_decorator.PypiRequirementFlowDecorator"),
]
STEP_DECORATORS_DESC = [
    ("conda", ".conda.conda_step_decorator.CondaRequirementStepDecorator"),
    ("pypi", ".conda.conda_step_decorator.PypiRequirementStepDecorator"),
    ("conda_env_internal", ".conda.conda_step_decorator.CondaEnvInternalDecorator"),
]
ENVIRONMENTS_DESC = [("conda", ".conda.conda_environment.CondaEnvironment")]

__mf_promote_submodules__ = ["conda"]
