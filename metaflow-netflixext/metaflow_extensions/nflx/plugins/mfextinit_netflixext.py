# CLIs
CLIS_DESC = [("environment", ".environment_cli.cli")]
RUNNER_CLIS_DESC = [("environment", ".environment_cli.RunnerCLI")]


FLOW_DECORATORS_DESC = [
    ("conda_base", ".conda.conda_flow_decorator.CondaRequirementFlowDecorator"),
    ("pip_base", ".conda.conda_flow_decorator.PipRequirementFlowDecorator"),
    ("pypi_base", ".conda.conda_flow_decorator.PypiRequirementFlowDecorator"),
    ("named_env_base", ".conda.conda_flow_decorator.NamedEnvRequirementFlowDecorator"),
    (
        "sys_packages_base",
        ".conda.conda_flow_decorator.SysPackagesRequirementFlowDecorator",
    ),
]
STEP_DECORATORS_DESC = [
    ("conda", ".conda.conda_step_decorator.CondaRequirementStepDecorator"),
    ("pip", ".conda.conda_step_decorator.PipRequirementStepDecorator"),
    (
        "pylock_toml_internal",
        ".conda.conda_step_decorator.PylockTomlInternalDecorator",
    ),
    ("pypi", ".conda.conda_step_decorator.PypiRequirementStepDecorator"),
    ("named_env", ".conda.conda_step_decorator.NamedEnvRequirementStepDecorator"),
    (
        "sys_packages",
        ".conda.conda_step_decorator.SysPackagesRequirementStepDecorator",
    ),
    ("conda_env_internal", ".conda.conda_step_decorator.CondaEnvInternalDecorator"),
    ("huggingface", ".huggingface.huggingface_decorator.HuggingFaceDecorator"),
]
ENVIRONMENTS_DESC = [("conda", ".conda.conda_environment.CondaEnvironment")]

__mf_promote_submodules__ = ["conda", "huggingface"]
