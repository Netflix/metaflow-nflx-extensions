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
        # RayBackend._get_or_create_actor's "already attached" fast path keys
        # directly off this, skipping _resolve_key/_extract_conda_env_from_spec
        # entirely -- which is what lets these tests pre-seed a fake actor
        # without also having to fake a real func_instance.spec.
        self._runtime_id = uuid


def _seed_fake_actor(func_instance, actor):
    """Register `actor` in RayBackend._actor_pool under the key that
    `func_instance._runtime_id` points at, so RayBackend.apply() resolves it
    via the "already attached" fast path instead of trying to create a real
    actor (which would require a real func_instance.spec)."""
    from metaflow_extensions.nflx.plugins.functions.backends.ray import RayBackend
    from metaflow_extensions.nflx.plugins.functions.backends.ray.ray_backend import (
        ActorEntry,
    )

    RayBackend._actor_pool[func_instance._runtime_id] = ActorEntry(actor=actor)


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
        _seed_fake_actor(func_instance, _FakeActor.remote())

        with pytest.raises(MetaflowFunctionRuntimeException):
            RayBackend.apply(func_instance, "data")

        # A hook failure (actor still alive) must NOT evict the actor from the pool.
        assert func_instance._runtime_id in RayBackend._actor_pool
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
        _seed_fake_actor(func_instance, _FakeActor.remote())

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
    from metaflow_extensions.nflx.plugins.functions.backends.ray.ray_backend import (
        ActorEntry,
    )

    # Shutdown any existing Ray instance
    if ray.is_initialized():
        ray.shutdown()

    @ray.remote
    class _FakeActor:
        def shutdown(self):
            pass

    try:
        # Initialize cluster
        RayBackend._ensure_cluster()
        assert ray.is_initialized()

        # Add a fake actor to the pool. shutdown(force=True) reads entry.actor
        # and calls actor.shutdown.remote()/ray.kill(actor), so this needs to be
        # a real ActorEntry wrapping a real Ray actor handle, not a bare string.
        RayBackend._actor_pool["test_uuid"] = ActorEntry(actor=_FakeActor.remote())

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


def _read_events(path):
    with open(path) as fh:
        return [line.strip() for line in fh if line.strip()]


def _tmp_log():
    import os
    import tempfile

    fd, path = tempfile.mkstemp(prefix="mff_ray_actor_test_", suffix=".log")
    os.close(fd)
    return path


def test_ray_actor_after_call_runs_when_execute_raises():
    """after_call() must fire even when the wrapped function raises, so
    components (e.g. metrics/logging) see every invocation.

    This runs a real FunctionActorClass through a real Ray actor and a real
    remote call/ray.get round trip (not a mocked/in-process stand-in), so it
    exercises the actual cross-process exception path Ray uses in production.
    __init__ is overridden on a subclass to skip code-package loading, which
    is irrelevant to this test and would require a real S3 function spec.
    """
    import os

    import ray
    from metaflow_extensions.nflx.plugins.functions.backends.ray import RayBackend
    from metaflow_extensions.nflx.plugins.functions.backends.ray.ray_backend import (
        FunctionActorClass,
    )
    from metaflow_extensions.nflx.plugins.functions.components.abstract_component import (
        AbstractRuntimeComponent,
    )
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionUserException,
    )

    if ray.is_initialized():
        ray.shutdown()

    log = _tmp_log()

    class _RecordingComponent(AbstractRuntimeComponent):
        component_id = "recording"
        _log_path = log

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            with open(type(self)._log_path, "a") as fh:
                fh.write("before_call\n")

        def after_call(self, *args, **kwargs):
            with open(type(self)._log_path, "a") as fh:
                fh.write("after_call\n")

    class _FailingFunction:
        name = "failing_fn"

        def __call__(self, data, **kwargs):
            raise MetaflowFunctionUserException("user error")

    @ray.remote
    class _TestActor(FunctionActorClass):
        def __init__(self):
            self.function = _FailingFunction()
            self._component_instances = [_RecordingComponent()]
            self.params = None

    try:
        RayBackend._ensure_cluster()
        actor = _TestActor.remote()

        with pytest.raises(MetaflowFunctionUserException, match="user error"):
            ray.get(actor.execute.remote("data"))

        assert _read_events(log) == ["before_call", "after_call"]
    finally:
        if ray.is_initialized():
            ray.shutdown()
        RayBackend._cluster_initialized = False
        os.unlink(log)


def test_ray_actor_after_call_failure_does_not_mask_user_exception():
    """A component failure in after_call() while a user exception is already
    in flight must not mask the original user exception, verified through a
    real Ray actor and a real remote call/ray.get round trip.
    """
    import ray
    from metaflow_extensions.nflx.plugins.functions.backends.ray import RayBackend
    from metaflow_extensions.nflx.plugins.functions.backends.ray.ray_backend import (
        FunctionActorClass,
    )
    from metaflow_extensions.nflx.plugins.functions.components.abstract_component import (
        AbstractRuntimeComponent,
    )
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionUserException,
    )

    if ray.is_initialized():
        ray.shutdown()

    class _FailingAfterCallComponent(AbstractRuntimeComponent):
        component_id = "failing_after_call"

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            raise ValueError("after_call blew up")

    class _FailingFunction:
        name = "failing_fn"

        def __call__(self, data, **kwargs):
            raise MetaflowFunctionUserException("user error")

    @ray.remote
    class _TestActor(FunctionActorClass):
        def __init__(self):
            self.function = _FailingFunction()
            self._component_instances = [_FailingAfterCallComponent()]
            self.params = None

    try:
        RayBackend._ensure_cluster()
        actor = _TestActor.remote()

        with pytest.raises(MetaflowFunctionUserException, match="user error"):
            ray.get(actor.execute.remote("data"))
    finally:
        if ray.is_initialized():
            ray.shutdown()
        RayBackend._cluster_initialized = False
