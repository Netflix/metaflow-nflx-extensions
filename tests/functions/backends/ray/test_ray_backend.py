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


def test_extract_resources_empty():
    """Resources default to empty dict when not in system_metadata."""
    from metaflow_extensions.nflx.plugins.functions.backends.ray import RayBackend
    from unittest.mock import MagicMock

    func_instance = MagicMock()
    func_instance.spec.system_metadata = {}

    resources = RayBackend._extract_resources_from_spec(func_instance)
    assert resources == {}


def test_extract_resources_present():
    """Resources are read from system_metadata["resources"]."""
    from metaflow_extensions.nflx.plugins.functions.backends.ray import RayBackend
    from unittest.mock import MagicMock

    func_instance = MagicMock()
    func_instance.spec.system_metadata = {
        "resources": {"num_cpus": 2, "num_gpus": 1, "memory": 4 * 1024**3}
    }

    resources = RayBackend._extract_resources_from_spec(func_instance)
    assert resources["num_cpus"] == 2
    assert resources["num_gpus"] == 1
    assert resources["memory"] == 4 * 1024**3


def test_extract_resources_missing_metadata():
    """Resources return empty dict when system_metadata is None."""
    from metaflow_extensions.nflx.plugins.functions.backends.ray import RayBackend
    from unittest.mock import MagicMock

    func_instance = MagicMock()
    func_instance.spec.system_metadata = None

    resources = RayBackend._extract_resources_from_spec(func_instance)
    assert resources == {}


def test_apply_async_is_coroutine():
    """apply_async must be a coroutine function (not a sync wrapper)."""
    import asyncio
    from metaflow_extensions.nflx.plugins.functions.backends.ray import RayBackend

    assert asyncio.iscoroutinefunction(RayBackend.apply_async)
