import os

import pytest
from metaflow import Flow, Runner


@pytest.fixture(scope="session")
def bound_functions():
    """Run the flow once to bind the functions, without executing them --
    execution is driven directly by the tests below, across backends,
    against these same bound references."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    flow_path = os.path.join(current_dir, "flows/hellosimplefunction.py")

    # Add flows directory to PYTHONPATH so function_module can be imported
    flows_dir = os.path.join(current_dir, "flows")
    user_environment = {"PYTHONPATH": flows_dir + ":" + os.getenv("PYTHONPATH", "")}

    with Runner(flow_path, env=user_environment, environment="conda").run() as running:
        assert (
            running.status == "successful"
        ), f"Run failed with status {running.status}"

        flow = Flow("HelloSimpleFunction")
        run = flow[running.run.id]
        bind_step = run["bind_functions"].task

        references = {}
        for attr in (
            "avro_simple_function",
            "avro_pydash_function",
            "avro_error_function",
            "avro_optional_context_function",
            "avro_pipeline_function",
            "json_simple_function",
        ):
            assert hasattr(bind_step.data, attr), f"{attr} not found"
            reference = getattr(bind_step.data, attr).reference
            assert reference.startswith(
                "s3://"
            ), f"Expected S3 reference for {attr}, got {reference}"
            references[attr] = reference

        return references
