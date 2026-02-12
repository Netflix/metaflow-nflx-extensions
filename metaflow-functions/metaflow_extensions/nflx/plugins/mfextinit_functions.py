###
# Plugins
###
TOGGLE_CLI = [
    "-batch",
    "-kubernetes",
    "-step-functions",
    "-airflow",
    "-argo-workflows",
]

TOGGLE_FLOW_DECORATOR = [
    "-airflow_external_task_sensor",
    "-airflow_s3_key_sensor",
    "-exit_hook",
]
TOGGLE_STEP_DECORATOR = [
    "-batch",
    "-kubernetes",
    "-argo_workflows_internal",
    "-step_functions_internal",
    "-airflow_internal",
]

TOGGLE_ENVIRONMENT = ["-pypi"]
TOGGLE_METADATA_PROVIDER = ["-service"]
TOGGLE_DATASTORE = ["-azure", "-gs"]
TOGGLE_DATACLIENT = ["-azure", "-gs"]
TOGGLE_DEPLOYER_IMPL_PROVIDER = ["-argo-workflows", "-step-functions"]


__mf_promote_submodules__ = [
    "functions",
]
