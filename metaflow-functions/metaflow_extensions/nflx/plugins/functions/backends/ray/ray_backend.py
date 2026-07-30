"""
Ray backend for Metaflow Functions.

Provides local Ray cluster execution with automatic resource allocation
from @resources decorator metadata.
"""

import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import ray

from metaflow_extensions.nflx.plugins.functions.backends.abstract_backend import (
    AbstractBackend,
)
from metaflow_extensions.nflx.plugins.functions.backends.backend_type import BackendType
from metaflow_extensions.nflx.plugins.functions.debug import debug
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionRuntimeException,
    MetaflowFunctionUserException,
)
from metaflow_extensions.nflx.config.mfextinit_functions import (
    FUNCTION_RAY_ADDRESS,
    FUNCTION_RAY_OBJECT_STORE_MEMORY,
)
from metaflow_extensions.nflx.plugins.functions.core.function import (
    function_from_json,
)
from metaflow_extensions.nflx.plugins.functions.common.runtime_utils import (
    create_function_parameters,
)
from metaflow_extensions.nflx.plugins.functions.serializers.ray_sync import (
    sync_ray_serializers,
)


# Identifies an actor: the function's content-hash uuid and the serialized
# runtime component specs it was created with. Two handles only share an
# actor if both match (Ray has no process-count knob, unlike the memory
# backend, so the key is narrower than RuntimeKey there).
RayActorKey = Tuple[str, Tuple[str, ...]]


@dataclass
class ActorEntry:
    actor: Any
    attached: int = 0  # number of handles currently attached to this actor


class RayBackend(AbstractBackend):
    """
    Local Ray cluster backend for Metaflow Functions.

    Features:
    - Starts local Ray cluster on first use (or connects to existing via RAY_ADDRESS)
    - Extracts resources from @resources decorator metadata
    - Creates Ray actors per function with proper resource allocation
    - Reuses actors across multiple calls for efficiency

    Configuration (environment variables):
    - METAFLOW_FUNCTION_RAY_ADDRESS: Ray cluster address (default: None, starts local cluster)
    - METAFLOW_FUNCTION_RAY_OBJECT_STORE_MEMORY: Object store memory in bytes (default: 268435456 / 256MB)
    """

    _cluster_initialized = False
    _actor_pool: Dict[RayActorKey, ActorEntry] = {}
    _lock = threading.Lock()

    @property
    def backend_type(self) -> BackendType:
        return BackendType.RAY

    @classmethod
    def _ensure_cluster(cls):
        """Initialize Ray cluster if not already started."""
        if cls._cluster_initialized:
            return

        if ray.is_initialized():
            debug.functions_exec("Ray already initialized")
            cls._cluster_initialized = True
            return

        # Check if we should connect to existing cluster
        ray_address = FUNCTION_RAY_ADDRESS

        if ray_address:
            # Connect to existing cluster (e.g., Mako-provisioned)
            ray.init(address=ray_address)
            debug.functions_exec(f"Connected to Ray cluster at {ray_address}")
        else:
            # Start local cluster with auto-detected resources
            # Ray will automatically detect available CPUs and GPUs
            object_store_memory = int(FUNCTION_RAY_OBJECT_STORE_MEMORY)
            ray.init(
                ignore_reinit_error=True,
                object_store_memory=object_store_memory,
            )

            debug.functions_exec(
                f"Started local Ray cluster with object_store_memory={object_store_memory} bytes"
            )

        cls._cluster_initialized = True

    @classmethod
    def _route_component_output(
        cls, func_instance, component_output: Dict[str, Any]
    ) -> None:
        """
        Stamp output onto the caller-side runtime component instances that
        match component_output entries, by ``component_id``.
        """
        if not component_output:
            return
        for component in getattr(func_instance, "_runtime_components", []):
            component_id = type(component).component_id
            if component_id in component_output:
                component.output = component_output[component_id]

    @classmethod
    def _sync_serializers(cls):
        """
        Sync Metaflow serializers to Ray's serialization system.

        Called before each function execution to pick up any newly registered serializers.
        """
        sync_ray_serializers()

    @classmethod
    def apply(cls, func_instance, data: Any, **kwargs) -> Any:
        """
        Execute function on Ray actor.

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance to execute
        data : Any
            Input data for the function
        **kwargs : Any
            Additional keyword arguments passed to the function

        Returns
        -------
        Any
            Result from function execution

        Raises
        ------
        MetaflowFunctionRuntimeException
            If Ray actor crashes or system error occurs
        MetaflowFunctionUserException
            If user code raises an exception
        """
        cls._ensure_cluster()
        cls._sync_serializers()

        # Get or create actor for this function
        actor = cls._get_or_create_actor(func_instance)

        # Execute on actor (Ray automatically puts data in object store)
        try:
            result_ref = actor.execute.remote(data, **kwargs)
            result, component_output = ray.get(result_ref)
            cls._route_component_output(func_instance, component_output)
            return result

        except ray.exceptions.RayActorError as e:
            # Actor crashed - remove from pool so it gets recreated
            key = func_instance._runtime_id
            if key is not None:
                with cls._lock:
                    cls._actor_pool.pop(key, None)
                func_instance._runtime_id = None
            raise MetaflowFunctionRuntimeException(
                f"Ray actor crashed while executing function '{func_instance.name}': {str(e)}"
            )

        except ray.exceptions.RayTaskError as e:
            # Ray dynamically subclasses RayTaskError with the original
            # exception's type, so isinstance() below recovers whether this
            # came from a runtime component hook or user code, even though
            # both cross the actor boundary as the same RayTaskError.
            if isinstance(e, MetaflowFunctionRuntimeException):
                raise MetaflowFunctionRuntimeException(
                    f"Runtime component exception in function '{func_instance.name}': {str(e)}"
                )
            raise MetaflowFunctionUserException(
                f"Function '{func_instance.name}' raised an exception: {str(e)}"
            )

    @classmethod
    async def apply_async(cls, func_instance, data: Any, **kwargs) -> Any:
        """
        Execute function on Ray actor

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance to execute
        data : Any
            Input data for the function
        **kwargs : Any
            Additional keyword arguments passed to the function

        Returns
        -------
        Any
            Result from function execution

        Raises
        ------
        MetaflowFunctionRuntimeException
            If Ray actor crashes or system error occurs
        MetaflowFunctionUserException
            If user code raises an exception
        """
        return cls.apply(func_instance, data, **kwargs)

    @classmethod
    def _extract_conda_env_from_spec(cls, func_instance) -> str:
        """
        Extract conda environment path from function spec.

        Uses the simple resolve_conda_environment utility.

        # TODO(ray-conda-isolation): This resolves a real conda environment and
        # returns a path to that environment's `python` binary (see
        # `resolve_conda_environment` in `metaflow_extensions/nflx/plugins/functions/environment.py`),
        # but that path is NOT currently wired up to actually put the Ray actor
        # inside that environment. See the TODO on `_get_or_create_actor` below
        # for the full explanation and fix options. Known-broken as of 2026-07-30;
        # `test_functions_pydash_avro[ray]` / `test_functions_pydash_avro_with_runtime_metrics[ray]`
        # are disabled in tests/functions/ux/test_functions.py until this is fixed.

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance

        Returns
        -------
        str
            Path to conda environment Python binary

        Raises
        ------
        MetaflowFunctionRuntimeException
            If system metadata is missing or environment cannot be resolved
        """
        from metaflow_extensions.nflx.plugins.functions.environment import (
            resolve_conda_environment,
        )

        system_metadata = getattr(func_instance.spec, "system_metadata", None)
        if not system_metadata:
            raise MetaflowFunctionRuntimeException(
                f"Function '{func_instance.name}' is missing system_metadata required for Ray backend"
            )

        python_path = resolve_conda_environment(system_metadata)
        return python_path

    @classmethod
    def _resolve_key(cls, func_instance) -> RayActorKey:
        """Identity of the actor a handle would attach to."""
        from metaflow_extensions.nflx.plugins.functions.components.runtime import (
            serialize_components,
        )

        component_specs = tuple(
            serialize_components(getattr(func_instance, "_runtime_components", []))
        )
        return (func_instance.uuid, component_specs)

    @classmethod
    def _get_or_create_actor(cls, func_instance):
        """
        Get existing actor or create new one with resource requirements.

        The first successful attach for a given handle attaches that handle
        to the resolved actor (tracked via ``func_instance._runtime_id``);
        the actor is only torn down once every attached handle has closed
        (see ``close``), so closing one handle cannot kill an actor still in
        use by another handle sharing the same (uuid, components) identity.

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance

        Returns
        -------
        ray.actor.ActorHandle
            Ray actor handle
        """
        # Once a handle has attached, reuse its resolved key rather than
        # recomputing it (which hashes runtime component specs) on every call.
        already_attached = func_instance._runtime_id is not None

        with cls._lock:
            key = func_instance._runtime_id if already_attached else cls._resolve_key(
                func_instance
            )

            entry = cls._actor_pool.get(key)
            if entry is None:
                # TODO(ray-conda-isolation): BROKEN. `runtime_env={"python": python_path}`
                # does NOT do what the surrounding comments/docstrings in this file
                # claim ("Ray actor is already running in the correct conda
                # environment thanks to the runtime_env passed to ray.remote()",
                # see FunctionActorClass docstring below).
                #
                # Root cause: `"python"` is not a key recognized by Ray's
                # RuntimeEnv schema. Ray's documented runtime_env keys are things
                # like `working_dir`, `py_modules`, `pip`, `conda`, `env_vars`,
                # `container`, `excludes`, `uv` -- there is no supported way to
                # point Ray at an arbitrary interpreter *binary path* the way
                # `subprocess.Popen([python_path, ...])` does. Ray silently
                # ignores the unrecognized `"python"` key (no validation error is
                # raised at actor-creation time), so the actor process just runs
                # inside whatever Python environment the Ray worker already has
                # (the ambient/cluster env), not the resolved conda env returned
                # by `_extract_conda_env_from_spec`.
                #
                # Contrast with the memory backend (see
                # `backends/memory/memory_backend.py`), which resolves the same
                # kind of conda alias and then genuinely execs a subprocess using
                # the resolved python binary -- that's a real interpreter swap,
                # so packages declared via `@conda(libraries=...)` (e.g. pydash)
                # are actually importable there. The ray backend has no
                # equivalent mechanism today.
                #
                # Symptom: any function whose body imports a package that's only
                # present in the resolved conda env (not in the ambient Ray
                # worker env) raises `ModuleNotFoundError` when executed via the
                # ray backend. Confirmed via
                # `tests/functions/ux/test_functions.py::test_functions_pydash_avro[ray]`
                # and `test_functions_pydash_avro_with_runtime_metrics[ray]`,
                # which are currently excluded from `PYDASH_BACKENDS` in that
                # file specifically because of this bug.
                #
                # Fix options (not yet implemented -- out of scope for that test
                # change):
                #   1. Use Ray's actual `conda` runtime_env key, e.g.
                #      `runtime_env = {"conda": <env name/path or inline spec>}`,
                #      pointing at the resolved conda environment rather than a
                #      bare python binary path. Requires the Ray cluster nodes
                #      to be able to resolve/activate that conda env.
                #   2. Use `runtime_env = {"pip": [...]}` with the resolved
                #      package list, if conda env activation isn't feasible in
                #      the Ray cluster's setup.
                #   3. Heavier fallback: mirror the memory backend and have
                #      `FunctionActorClass` invoke the function in a subprocess
                #      using the resolved `python_path`, instead of relying on
                #      Ray's `runtime_env` to swap the actor's own interpreter.
                #
                # Extract conda environment for runtime_env
                python_path = cls._extract_conda_env_from_spec(func_instance)
                runtime_env = {"python": python_path}
                debug.functions_exec(f"Using conda environment: {python_path}")

                # Create Ray actor with conda runtime_env
                # Ray will use all available resources by default
                FunctionActor = ray.remote(runtime_env=runtime_env)(FunctionActorClass)

                # Instantiate actor with function reference and component class names
                _, component_class_names = key
                actor = FunctionActor.remote(
                    func_instance.spec.reference, list(component_class_names)
                )
                entry = ActorEntry(actor=actor)
                cls._actor_pool[key] = entry

                debug.functions_exec(
                    f"Created Ray actor for function '{func_instance.name}' with runtime_env: {runtime_env}"
                )

            if not already_attached:
                func_instance._runtime_id = key
                entry.attached += 1

            return entry.actor

    @classmethod
    def start(cls, func_instance, **kwargs):
        """
        Pre-start the Ray actor (optional optimization).

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance
        """
        cls._ensure_cluster()
        cls._get_or_create_actor(func_instance)

    @classmethod
    def close(cls, func_instance, clean_dir: bool = True, **kwargs):
        """
        Terminate Ray actor and cleanup.

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance
        clean_dir : bool
            Whether to clean up directories (unused for Ray)
        """
        key = func_instance._runtime_id
        if key is None:
            return
        func_instance._runtime_id = None

        actor = None
        with cls._lock:
            entry = cls._actor_pool.get(key)
            if entry is not None:
                entry.attached -= 1
                if entry.attached <= 0:
                    actor = entry.actor
                    del cls._actor_pool[key]

        if actor:
            try:
                ray.get(actor.shutdown.remote())
            except Exception as e:
                debug.functions_exec(f"Error shutting down Ray actor components: {e}")
            try:
                ray.kill(actor)
                debug.functions_exec(
                    f"Terminated Ray actor for function '{func_instance.name}'"
                )
            except Exception as e:
                debug.functions_exec(f"Error terminating Ray actor: {e}")

    @classmethod
    def shutdown(cls, force: bool = False):
        """
        Shutdown the Ray cluster and cleanup all resources.

        This method only shuts down the cluster if no functions are using it
        (actor pool is empty), unless force=True.

        Parameters
        ----------
        force : bool, default False
            If True, kill all actors and shutdown cluster regardless of usage.
            If False, only shutdown if no actors remain in the pool.

        Notes
        -----
        The Ray cluster is shared across all functions. This method should only
        be called when you're done using ALL Ray functions, typically in test
        cleanup or application shutdown.
        """
        if force:
            # Gracefully stop components then kill all remaining actors
            for key, entry in list(cls._actor_pool.items()):
                actor = entry.actor
                try:
                    ray.get(actor.shutdown.remote())
                except Exception as e:
                    debug.functions_exec(
                        f"Error shutting down Ray actor components for key '{key}': {e}"
                    )
                try:
                    ray.kill(actor)
                    debug.functions_exec(f"Terminated Ray actor for key '{key}'")
                except Exception as e:
                    debug.functions_exec(f"Error terminating Ray actor: {e}")

            cls._actor_pool.clear()

        # Only shutdown cluster if no actors are using it
        if len(cls._actor_pool) == 0:
            if ray.is_initialized():
                ray.shutdown()
                debug.functions_exec("Ray cluster shut down")

            # Reset state
            cls._cluster_initialized = False
        else:
            debug.functions_exec(
                f"Ray cluster NOT shut down - {len(cls._actor_pool)} actors still in use"
            )


class FunctionActorClass:
    """
    Ray actor that executes Metaflow Functions.

    The actor loads the function once during initialization by:
    1. Extracting code packages immediately (not lazy)
    2. Loading the concrete function (not proxy) with all dependencies
    3. Executing in-process in the actor's conda environment (via Ray's runtime_env)

    This is decorated with @ray.remote in _get_or_create_actor() with
    resource requirements from the @resources decorator and runtime_env
    for the conda environment.

    TODO(ray-conda-isolation): point (3) and the "runtime_env for the conda
    environment" claim above are currently FALSE. See the detailed TODO in
    `_get_or_create_actor` in this module -- `runtime_env={"python": ...}` is
    not a real Ray RuntimeEnv mechanism, so this actor actually runs in
    whatever ambient Python environment the Ray worker started with, not a
    conda-isolated one. Dependencies declared only via `@conda(libraries=...)`
    on the flow (e.g. pydash) are NOT guaranteed to be importable here.
    """

    def __init__(
        self,
        function_reference: str,
        component_class_names: Optional[List[str]] = None,
    ):
        """
        Initialize actor by extracting code and loading concrete function.

        Parameters
        ----------
        function_reference : str
            S3 path to function specification JSON
        component_class_names : Optional[List[str]]
            Fully-qualified class names of runtime components to activate
        """
        component_class_names = component_class_names or []
        import os

        from metaflow_extensions.nflx.config.mfextinit_functions import (
            FUNCTION_RUNTIME_PATH,
        )
        from metaflow_extensions.nflx.plugins.functions.config import Config
        from metaflow_extensions.nflx.plugins.functions.core.function_spec import (
            FunctionSpec,
        )
        from metaflow_extensions.nflx.plugins.functions.environment import (
            extract_code_packages,
            generate_trampolines_for_directory,
        )

        # Register serializers in actor process
        sync_ray_serializers()

        # Load function spec to get code package information
        func_spec = FunctionSpec.from_json(function_reference)

        # Validate required fields
        if not func_spec.code_package:
            raise MetaflowFunctionRuntimeException("Function spec missing code_package")
        if not func_spec.task_code_path:
            raise MetaflowFunctionRuntimeException(
                "Function spec missing task_code_path"
            )

        # Extract code to the same deterministic directory the other backends
        # use (FUNCTION_RUNTIME_PATH/metaflow-function-<uuid>) so caller-side
        # code can compute this function's root dir without needing the
        # backend to report it back (e.g. for on_runtime_started()).
        function_dir = os.path.join(
            FUNCTION_RUNTIME_PATH,
            f"{Config.RUNTIME_FUNCTION_DIR_PREFIX}{func_spec.uuid}",
        )
        code_dir = extract_code_packages(
            func_spec.code_package,
            func_spec.task_code_path,
            function_dir,
        )

        debug.functions_exec(f"Code extracted to: {code_dir}")

        # Generate trampolines (for conda environments)
        generate_trampolines_for_directory(code_dir)

        # Load the concrete function (not proxy) with use_proxy=False
        # This loads the actual function with all dependencies in this process
        # TODO(ray-conda-isolation): the comment below is aspirational, not
        # actual, as of 2026-07-30 -- see the TODO on _get_or_create_actor.
        # The Ray actor is already running in the correct conda environment
        # thanks to the runtime_env passed to ray.remote()

        # Don't start runtime - function executes directly in Ray actor
        self.function = function_from_json(
            function_reference, use_proxy=False, backend="local", start_runtime=False
        )

        # Create function parameters once (caches artifacts)
        self.params = create_function_parameters(self.function.spec)

        # Store the code directory for cleanup
        self.code_dir = code_dir

        # Load and start runtime components inside the actor process
        from metaflow_extensions.nflx.plugins.functions.components.runtime import (
            load_component_instances,
            start_components,
        )

        self._component_instances = start_components(
            load_component_instances(component_class_names),
            function=self.function,
        )

    def shutdown(self):
        """Stop runtime components. Called before the actor is killed."""
        from metaflow_extensions.nflx.plugins.functions.components.runtime import (
            stop_components,
        )

        stop_components(self._component_instances)

    def execute(self, data, **kwargs):
        """
        Execute function on data in-process.

        Since we loaded a concrete function (not proxy), we can execute it directly
        in this process. The function runs in the conda environment specified by
        Ray's runtime_env.

        Parameters
        ----------
        data : Any
            Input data (Ray automatically transfers to actor)
        **kwargs : Any
            Additional keyword arguments passed to function

        Returns
        -------
        Any
            Result from function execution
        """
        from metaflow_extensions.nflx.plugins.functions.components.runtime import (
            before_call_components,
            after_call_components,
        )

        # Use params from kwargs if provided, otherwise use pre-created params
        params = kwargs.pop("params", self.params)

        try:
            before_call_components(self._component_instances)
        except Exception as e:
            raise MetaflowFunctionRuntimeException(
                f"Runtime component exception in function '{self.function.name}': {str(e)}"
            )

        # self.function is itself backed by LocalBackend, so this already
        # raises MetaflowFunctionUserException for user code failures.
        try:
            result = self.function(data, params=params, **kwargs)
        except Exception as e:
            user_exception = e
            result = None
        else:
            user_exception = None

        # after_call must run whether or not the function call itself failed,
        # so components (e.g. metrics/logging) see every invocation.
        try:
            component_output = after_call_components(self._component_instances)
        except Exception as e:
            if user_exception is None:
                raise MetaflowFunctionRuntimeException(
                    f"Runtime component exception in function '{self.function.name}': {str(e)}"
                )
            # A user exception is already in flight; don't let a component
            # failure on the error path mask it.
            debug.functions_exec(
                f"Runtime component exception in after_call for '{self.function.name}' "
                f"while handling a prior user exception: {e!r}"
            )
            component_output = {}

        if user_exception is not None:
            raise user_exception

        return result, component_output
