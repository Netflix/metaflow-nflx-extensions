"""
Tests for Ray backend.
"""

import pytest


# Skip if Ray is not installed
pytest.importorskip("ray")


def test_ray_backend_import():
    """Test that Ray backend can be imported."""
    from metaflow_extensions.nflx.plugins.functions.backends.ray import RayBackend
    from metaflow_extensions.nflx.plugins.functions.backends.backend_type import (
        BackendType,
    )

    backend = RayBackend()
    assert backend.backend_type == BackendType.RAY


def test_ray_backend_via_factory():
    """Test that Ray backend can be instantiated via factory."""
    from metaflow_extensions.nflx.plugins.functions.backends import get_backend
    from metaflow_extensions.nflx.plugins.functions.backends.backend_type import (
        BackendType,
    )

    # Get backend by name
    backend = get_backend("ray")

    assert backend.backend_type == BackendType.RAY


def test_ray_cluster_initialization():
    """Test that Ray cluster can be initialized."""
    import ray
    from metaflow_extensions.nflx.plugins.functions.backends.ray import RayBackend

    # Shutdown any existing Ray instance
    if ray.is_initialized():
        ray.shutdown()

    try:
        # Initialize cluster
        RayBackend._ensure_cluster()

        # Verify Ray is initialized
        assert ray.is_initialized()

        # Check cluster resources
        resources = ray.cluster_resources()
        assert "CPU" in resources
        assert resources["CPU"] > 0

    finally:
        # Cleanup
        if ray.is_initialized():
            ray.shutdown()
        RayBackend._cluster_initialized = False


class _FakeFuncInstance:
    """Minimal stand-in for a MetaflowFunction, just enough for RayBackend.apply()."""

    def __init__(self, uuid, name):
        self.uuid = uuid
        self.name = name
        self._runtime_components = []


def test_ray_backend_hook_failure_raises_runtime_exception():
    """A RayTaskError wrapping a MetaflowFunctionRuntimeException (e.g. a component
    hook failure inside the actor) must surface as MetaflowFunctionRuntimeException,
    not get folded into MetaflowFunctionUserException."""
    import ray
    from metaflow_extensions.nflx.plugins.functions.backends.ray import RayBackend
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionRuntimeException,
    )

    if ray.is_initialized():
        ray.shutdown()

    @ray.remote
    class _FakeActor:
        def execute(self, data, **kwargs):
            raise MetaflowFunctionRuntimeException("hook failed")

    try:
        RayBackend._ensure_cluster()
        func_instance = _FakeFuncInstance("hook-fail-uuid", "hook_fail_fn")
        RayBackend._actor_pool[func_instance.uuid] = _FakeActor.remote()

        with pytest.raises(MetaflowFunctionRuntimeException):
            RayBackend.apply(func_instance, "data")

        # A hook failure (actor still alive) must NOT evict the actor from the pool.
        assert func_instance.uuid in RayBackend._actor_pool
    finally:
        RayBackend._actor_pool.clear()
        if ray.is_initialized():
            ray.shutdown()
        RayBackend._cluster_initialized = False


def test_ray_backend_user_failure_raises_user_exception():
    """A RayTaskError wrapping a plain user exception must surface as
    MetaflowFunctionUserException."""
    import ray
    from metaflow_extensions.nflx.plugins.functions.backends.ray import RayBackend
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionRuntimeException,
        MetaflowFunctionUserException,
    )

    if ray.is_initialized():
        ray.shutdown()

    @ray.remote
    class _FakeActor:
        def execute(self, data, **kwargs):
            raise MetaflowFunctionUserException("user code failed")

    try:
        RayBackend._ensure_cluster()
        func_instance = _FakeFuncInstance("user-fail-uuid", "user_fail_fn")
        RayBackend._actor_pool[func_instance.uuid] = _FakeActor.remote()

        with pytest.raises(MetaflowFunctionUserException) as exc_info:
            RayBackend.apply(func_instance, "data")
        assert not isinstance(exc_info.value, MetaflowFunctionRuntimeException)
    finally:
        RayBackend._actor_pool.clear()
        if ray.is_initialized():
            ray.shutdown()
        RayBackend._cluster_initialized = False


def test_shutdown_with_active_actors():
    """Test that shutdown doesn't kill cluster if actors are active (unless forced)."""
    import ray
    from metaflow_extensions.nflx.plugins.functions.backends.ray import RayBackend

    # Shutdown any existing Ray instance
    if ray.is_initialized():
        ray.shutdown()

    try:
        # Initialize cluster
        RayBackend._ensure_cluster()
        assert ray.is_initialized()

        # Add a fake actor to the pool
        RayBackend._actor_pool["test_uuid"] = "fake_actor"

        # Try to shutdown without force - should NOT shutdown cluster
        RayBackend.shutdown(force=False)
        assert (
            ray.is_initialized()
        ), "Cluster should still be running with active actors"
        assert len(RayBackend._actor_pool) == 1, "Actor should still be in pool"

        # Force shutdown - should kill everything
        RayBackend.shutdown(force=True)
        assert not ray.is_initialized(), "Cluster should be shut down with force=True"
        assert len(RayBackend._actor_pool) == 0, "Actor pool should be empty"

    finally:
        # Cleanup
        if ray.is_initialized():
            ray.shutdown()
        RayBackend._cluster_initialized = False
        RayBackend._actor_pool.clear()
