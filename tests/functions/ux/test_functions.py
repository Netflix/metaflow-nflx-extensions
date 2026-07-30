import importlib.util
import os

import pytest
from metaflow import Flow, Runner

# Mark all tests in this file as functions
pytestmark = pytest.mark.functions

BACKENDS = ["memory", "local", "ray"]

# The local backend runs in-process with whatever the calling Python process
# already has installed, so it can't guarantee pydash is present. memory and
# ray both isolate execution via a resolved conda environment, so they're the
# ones that actually exercise dependency resolution.
#
# NOTE: "ray" is intentionally excluded here, not because ray can't isolate
# dependencies in principle, but because the ray backend's conda isolation is
# currently broken -- see the TODO block in
# metaflow_extensions/nflx/plugins/functions/backends/ray/ray_backend.py
# (around _get_or_create_actor / _extract_conda_env_from_spec). Fixing that is
# out of scope here; re-add "ray" to this list once it's fixed.
PYDASH_BACKENDS = ["memory"]


def _skip_if_backend_unavailable(backend):
    if backend == "ray" and importlib.util.find_spec("ray") is None:
        pytest.skip("ray is not installed")


@pytest.fixture(scope="module")
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


@pytest.mark.parametrize("backend", BACKENDS)
def test_functions_simple_avro(bound_functions, backend):
    """A dependency-free avro function executes correctly on every backend."""
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        close_function,
        function_from_json,
    )

    _skip_if_backend_unavailable(backend)

    func = function_from_json(bound_functions["avro_simple_function"], backend=backend)
    try:
        result = func("hello")
        assert result == "HELLO_modified", f"Unexpected result: {result}"
    finally:
        close_function(func)


@pytest.mark.parametrize("backend", BACKENDS)
def test_functions_simple_avro_with_runtime_metrics(bound_functions, backend):
    """RuntimeMetrics fires for real against the dependency-free avro function."""
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        close_function,
        function_from_json,
    )
    from metaflow_extensions.nflx.plugins.functions.components.runtime_metrics import (
        RuntimeMetrics,
    )

    _skip_if_backend_unavailable(backend)

    metrics = RuntimeMetrics()
    func = function_from_json(
        bound_functions["avro_simple_function"],
        backend=backend,
        runtime_components=[metrics],
    )
    try:
        result = func("hello")
        assert result == "HELLO_modified", f"Unexpected result: {result}"

        assert metrics.output.keys() == {
            "call_count",
            "last_duration_s",
            "total_duration_s",
        }
        assert metrics.output["call_count"] == 1
        assert metrics.output["last_duration_s"] >= 0
        assert metrics.output["total_duration_s"] >= 0
    finally:
        close_function(func)


@pytest.mark.parametrize("backend", PYDASH_BACKENDS)
def test_functions_pydash_avro(bound_functions, backend):
    """An avro function that depends on pydash resolves correctly via the
    backend's conda environment (memory, ray only -- local has no isolation)."""
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        close_function,
        function_from_json,
    )

    _skip_if_backend_unavailable(backend)

    func = function_from_json(bound_functions["avro_pydash_function"], backend=backend)
    try:
        result = func("hello")
        assert result == "HELLO_modified", f"Unexpected result: {result}"
    finally:
        close_function(func)


@pytest.mark.parametrize("backend", PYDASH_BACKENDS)
def test_functions_pydash_avro_with_runtime_metrics(bound_functions, backend):
    """RuntimeMetrics fires for real against the pydash-dependent avro function."""
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        close_function,
        function_from_json,
    )
    from metaflow_extensions.nflx.plugins.functions.components.runtime_metrics import (
        RuntimeMetrics,
    )

    _skip_if_backend_unavailable(backend)

    metrics = RuntimeMetrics()
    func = function_from_json(
        bound_functions["avro_pydash_function"],
        backend=backend,
        runtime_components=[metrics],
    )
    try:
        result = func("hello")
        assert result == "HELLO_modified", f"Unexpected result: {result}"
        assert metrics.output["call_count"] == 1
    finally:
        close_function(func)


@pytest.mark.parametrize("backend", BACKENDS)
def test_functions_pipeline_avro(bound_functions, backend):
    """A two-stage avro FunctionPipeline executes correctly on every backend."""
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        close_function,
        function_from_json,
    )

    _skip_if_backend_unavailable(backend)

    func = function_from_json(
        bound_functions["avro_pipeline_function"], backend=backend
    )
    try:
        result = func({"value": 4})
        # avro_add_field: value=4, increment=10 -> incremented=14
        # avro_double_values: multiplier=3 -> value=12, incremented=42
        assert result == {
            "value": 12,
            "incremented": 42,
        }, f"Unexpected result: {result}"
    finally:
        close_function(func)


@pytest.mark.parametrize("backend", BACKENDS)
def test_functions_pipeline_avro_with_runtime_metrics(bound_functions, backend):
    """RuntimeMetrics fires for real against the avro FunctionPipeline."""
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        close_function,
        function_from_json,
    )
    from metaflow_extensions.nflx.plugins.functions.components.runtime_metrics import (
        RuntimeMetrics,
    )

    _skip_if_backend_unavailable(backend)

    metrics = RuntimeMetrics()
    func = function_from_json(
        bound_functions["avro_pipeline_function"],
        backend=backend,
        runtime_components=[metrics],
    )
    try:
        result = func({"value": 4})
        assert result == {
            "value": 12,
            "incremented": 42,
        }, f"Unexpected result: {result}"
        assert metrics.output["call_count"] == 1
    finally:
        close_function(func)


@pytest.mark.parametrize("backend", BACKENDS)
def test_functions_error_avro(bound_functions, backend):
    """A user error raised inside an avro function surfaces as
    MetaflowFunctionUserException on every backend."""
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        close_function,
        function_from_json,
    )
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionUserException,
    )

    _skip_if_backend_unavailable(backend)

    func = function_from_json(bound_functions["avro_error_function"], backend=backend)
    try:
        with pytest.raises(MetaflowFunctionUserException):
            func("hello")
    finally:
        close_function(func)


@pytest.mark.parametrize("backend", BACKENDS)
def test_functions_error_avro_with_runtime_metrics(bound_functions, backend):
    """User errors still surface correctly with runtime components active."""
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        close_function,
        function_from_json,
    )
    from metaflow_extensions.nflx.plugins.functions.components.runtime_metrics import (
        RuntimeMetrics,
    )
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionUserException,
    )

    _skip_if_backend_unavailable(backend)

    metrics = RuntimeMetrics()
    func = function_from_json(
        bound_functions["avro_error_function"],
        backend=backend,
        runtime_components=[metrics],
    )
    try:
        with pytest.raises(MetaflowFunctionUserException):
            func("hello")
    finally:
        close_function(func)


@pytest.mark.parametrize("backend", BACKENDS)
def test_functions_json_simple(bound_functions, backend):
    """A dependency-free json function executes correctly on every backend."""
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        close_function,
        function_from_json,
    )

    _skip_if_backend_unavailable(backend)

    func = function_from_json(bound_functions["json_simple_function"], backend=backend)
    try:
        result = func({"value": 42, "name": "test"})
        assert result["processed"] is True
        assert result["increment"] == 10
        assert result["value"] == 42
        assert result["name"] == "test"
    finally:
        close_function(func)
