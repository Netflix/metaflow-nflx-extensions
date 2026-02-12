import pytest
from metaflow import Runner
import os

# Mark all tests in this file as functions
pytestmark = pytest.mark.functions


def test_functions_simple_avro_json():
    """Test basic Avro and JSON function binding and execution"""
    from metaflow import Flow

    current_dir = os.path.dirname(os.path.abspath(__file__))
    flow_path = os.path.join(current_dir, "flows/hellosimplefunction.py")

    # Add flows directory to PYTHONPATH so function_module can be imported
    flows_dir = os.path.join(current_dir, "flows")
    user_environment = {
        "PYTHONPATH": flows_dir + ":" + os.getenv("PYTHONPATH", "")
    }

    with Runner(flow_path, env=user_environment, environment="conda").run() as running:
        assert running.status == "successful", f"Run failed with status {running.status}"

        # Use Flow API to access the artifacts
        flow = Flow("HelloSimpleFunction")
        run = flow[running.run.id]

        # Verify the functions were bound correctly and have S3 references
        bind_step = run["bind_functions"].task
        assert hasattr(bind_step.data, 'avro_string_function'), "avro_string_function not found"
        assert hasattr(bind_step.data, 'json_object_function'), "json_object_function not found"
        assert hasattr(bind_step.data.avro_string_function, 'reference'), "avro_string_function.reference not found"
        assert hasattr(bind_step.data.json_object_function, 'reference'), "json_object_function.reference not found"

        # Verify references are S3 paths
        assert bind_step.data.avro_string_function.reference.startswith('s3://'), \
            f"Expected S3 reference, got {bind_step.data.avro_string_function.reference}"
        assert bind_step.data.json_object_function.reference.startswith('s3://'), \
            f"Expected S3 reference, got {bind_step.data.json_object_function.reference}"

        print("✓ Avro and JSON functions bound and executed successfully")
        print(f"  - Avro function reference: {bind_step.data.avro_string_function.reference}")
        print(f"  - JSON function reference: {bind_step.data.json_object_function.reference}")
