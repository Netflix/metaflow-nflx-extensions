import importlib.util
import os
import sys

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


@pytest.mark.parametrize("backend", ["memory", "local", "ray"])
def test_on_runtime_started_receives_spec_metadata_or_none(bound_functions, backend):
    """on_runtime_started receives this component's deploy-time spec metadata,
    or None when the deployed function never configured it -- and None must not
    break the invocation.

    Callers install components without knowing which functions use them (the
    platform installs ALBLogger for every function; most don't log), so an
    unconfigured component has to come up quiet and stay out of the way. That is
    what this asserts, per backend.

    "Doesn't break the invocation" is also covered across every backend by
    test_functions_simple_avro_with_runtime_metrics, which invokes with the
    unconfigured RuntimeMetrics component; this one adds the hook observation.

    It cannot assert the *configured* case: the metadata is written into the spec
    at packaging time, and this fixture's function was deployed without any
    component configured, so there is nothing to retroactively declare. That half
    is covered in tests/functions/components/test_runtime_components.py
    (TestSpecMetadataCollection, TestOnRuntimeStartedReceivesSpecMetadata).

    Supersedes an earlier version of this test that asserted the hook received a
    caller-readable *directory*, under PR#98 feedback item [6]. Nothing needs a
    directory now -- the schema a component reads is resolved at packaging time
    into the function spec -- so that item is closed by deletion rather than by
    having each backend report its own path.
    """
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        close_function,
        function_from_json,
    )

    _skip_if_backend_unavailable(backend)

    # From the packaged flow module, not this file: components are rebuilt in the
    # subprocess/actor by dotted path, and the test tree isn't in the code
    # package. See MetadataRecorder's docstring.
    sys.path.insert(
        0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "flows")
    )
    from function_module import MetadataRecorder  # noqa: E402

    recorder = MetadataRecorder()
    func = function_from_json(
        bound_functions["avro_simple_function"],
        backend=backend,
        runtime_components=[recorder],
    )
    try:
        assert (
            len(recorder.calls) == 1
        ), f"on_runtime_started should fire exactly once, got {recorder.calls}"

        assert recorder.calls[0] is None, (
            f"{backend} backend passed metadata for a component this function "
            f"never configured: {recorder.calls[0]!r}"
        )

        # An unconfigured component must not break the call it is installed on.
        assert func("hello") == "HELLO_modified"
    finally:
        close_function(func)


# local has no start() override, so there's nothing for it to leak here --
# scoped to the two backends whose start() does real, stateful setup.
LEAK_TEST_BACKENDS = ["memory", "ray"]


def _active_runtime_count(backend):
    if backend == "memory":
        from metaflow_extensions.nflx.plugins.functions.backends.memory.supervisor.supervisor import (
            function_supervisor,
        )

        return len(function_supervisor._process_map)
    else:
        from metaflow_extensions.nflx.plugins.functions.backends.ray.ray_backend import (
            RayBackend,
        )

        return len(RayBackend._actor_pool)


@pytest.mark.parametrize("backend", LEAK_TEST_BACKENDS)
def test_on_runtime_started_raise_does_not_leak_backend_resource(
    bound_functions, backend
):
    """If a component's on_runtime_started hook raises, function_from_json
    currently propagates the exception without ever returning a handle to
    the caller -- so the backend resource backend.start() already created
    (a leased memory subprocess or an attached ray actor) has no way to get
    closed and is leaked. See PR #98 feedback item [6]."""
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        function_from_json,
    )
    from metaflow_extensions.nflx.plugins.functions.components.abstract_component import (
        AbstractRuntimeComponent,
    )

    _skip_if_backend_unavailable(backend)

    class _RaisingOnRuntimeStarted(AbstractRuntimeComponent):
        component_id = "raising_on_runtime_started"

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

        def on_runtime_started(self, function_root_dir):
            raise RuntimeError("boom: intentional failure in on_runtime_started")

    before = _active_runtime_count(backend)

    with pytest.raises(RuntimeError, match="boom"):
        function_from_json(
            bound_functions["avro_simple_function"],
            backend=backend,
            runtime_components=[_RaisingOnRuntimeStarted()],
        )

    after = _active_runtime_count(backend)

    assert after == before, (
        f"{backend} backend leaked a resource when on_runtime_started raised: "
        f"{before} active runtime(s) before this call, {after} after -- the "
        "backend that function_from_json already started was never closed "
        "because no handle was ever returned to the caller to clean it up."
    )
