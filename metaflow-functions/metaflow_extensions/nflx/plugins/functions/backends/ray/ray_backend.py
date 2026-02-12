"""
Ray backend for Metaflow Functions.

Provides local Ray cluster execution with automatic resource allocation
from @resources decorator metadata.
"""

from typing import Any, Dict

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
    _actor_pool: Dict[str, Any] = {}  # uuid -> ray.ActorHandle

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
            result = ray.get(result_ref)
            return result

        except ray.exceptions.RayActorError as e:
            # Actor crashed - remove from pool so it gets recreated
            cls._actor_pool.pop(func_instance.uuid, None)
            raise MetaflowFunctionRuntimeException(
                f"Ray actor crashed while executing function '{func_instance.name}': {str(e)}"
            )

        except ray.exceptions.RayTaskError as e:
            # User code error - propagate as user exception
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
    def _get_or_create_actor(cls, func_instance):
        """
        Get existing actor or create new one with resource requirements.

        Parameters
        ----------
        func_instance : MetaflowFunction
            Function instance

        Returns
        -------
        ray.actor.ActorHandle
            Ray actor handle
        """
        uuid = func_instance.uuid

        # Return existing actor if available
        if uuid in cls._actor_pool:
            return cls._actor_pool[uuid]

        # Extract conda environment for runtime_env
        python_path = cls._extract_conda_env_from_spec(func_instance)
        runtime_env = {"python": python_path}
        debug.functions_exec(f"Using conda environment: {python_path}")

        # Create Ray actor with conda runtime_env
        # Ray will use all available resources by default
        FunctionActor = ray.remote(runtime_env=runtime_env)(FunctionActorClass)

        # Instantiate actor with function reference
        cls._actor_pool[uuid] = FunctionActor.remote(func_instance.spec.reference)

        debug.functions_exec(
            f"Created Ray actor for function '{func_instance.name}' with runtime_env: {runtime_env}"
        )

        return cls._actor_pool[uuid]

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
        uuid = func_instance.uuid
        actor = cls._actor_pool.pop(uuid, None)

        if actor:
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
            # Kill all remaining actors
            for uuid, actor in list(cls._actor_pool.items()):
                try:
                    ray.kill(actor)
                    debug.functions_exec(
                        f"Terminated Ray actor for function uuid '{uuid}'"
                    )
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
    """

    def __init__(self, function_reference: str):
        """
        Initialize actor by extracting code and loading concrete function.

        Parameters
        ----------
        function_reference : str
            S3 path to function specification JSON
        """
        import tempfile
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

        # Extract code packages immediately to a working directory
        code_dir = extract_code_packages(
            func_spec.code_package,
            func_spec.task_code_path,
            tempfile.mkdtemp(prefix="mf_ray_actor_"),
        )

        debug.functions_exec(f"Code extracted to: {code_dir}")

        # Generate trampolines (for conda environments)
        generate_trampolines_for_directory(code_dir)

        # Load the concrete function (not proxy) with use_proxy=False
        # This loads the actual function with all dependencies in this process
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
        # Use params from kwargs if provided, otherwise use pre-created params
        params = kwargs.pop("params", self.params)

        # Call the concrete function directly
        # Since we used use_proxy=False with backend="local", this goes through:
        # __call__ -> local_backend.apply() -> execute(data, params)
        # So we pass params separately, not in kwargs
        result = self.function(data, params=params, **kwargs)

        return result
