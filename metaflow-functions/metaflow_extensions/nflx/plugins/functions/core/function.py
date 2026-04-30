"""
Metaflow functions core

Metaflow Functions are a way to share computations with other
systems. These computations could be any aspect of an ML pipeline,
e.g. feature engineering, a model, a whole pipeline in your favorite
framework, etc. It could contain your own custom modules and
interfaces (e.g. all your custom code) alongside your favorite data
science packages. Metaflow Functions is about encapsulation and
integration other workflows, systems, or computation engines. We
handle describing and packaging up the runtime aspect of your project,
connecting it with your training flow, and exporting them to other
engines.

Functions can be shared without the user of the function needing to
know the details of the environment (e.g. python packages), code
(e.g. your function and supporting files), and parameters (e.g. the
weights, or configuration of a model, or details of your custom
framework), all these are provided from a Metaflow Task during
training. A Function can be rehydrated from its reference without
worry of these details, only data must be provided. This is a property
we call relocatable computation.

Different runtimes (e.g. default python, spark, Ray, quantum
computing) can interpret a Metaflow Function metadata and decide the
best runtime implementation. We provide a default one that works in
pure python with no dependencies, with all the encapsulation features
mentioned above.

Think of a Metaflow Function's as an abstract UX for creating
different types of relocatable computations for different user
experiences. You write your code once, and we target a back-end that
is suitable for your computation.

"""

import datetime
import hashlib
import json
import os
import shutil
from abc import ABC, abstractmethod
from dataclasses import asdict
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Dict, Optional, Type, cast

if TYPE_CHECKING:
    from metaflow import S3, Task

from metaflow_extensions.nflx.plugins.functions.config import Config
from metaflow_extensions.nflx.plugins.functions.core.function_decorator import (
    MetaflowFunctionDecorator,
)
from metaflow_extensions.nflx.plugins.functions.core.function_spec import FunctionSpec
from metaflow_extensions.nflx.plugins.functions.debug import debug
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
    MetaflowFunctionRuntimeException,
)
from metaflow_extensions.nflx.plugins.functions.utils import is_s3

if TYPE_CHECKING:
    from metaflow_extensions.nflx.plugins.functions.backends.abstract_backend import (
        AbstractBackend,
    )


class MetaflowFunction(ABC):
    """
    Base class for all Metaflow functions.
    """

    function_spec_cls: Type[FunctionSpec]

    def __init__(
        self,
        func: Optional["MetaflowFunctionDecorator"] = None,
        task: Optional["Task"] = None,
        **kwargs: Any,
    ):
        """
        Initializes the Metaflow function.

        This supports both legacy single-function initialization and
        new composition-style initialization.

        Parameters
        ----------
        func : Optional[MetaflowFunctionDecorator], default None
            The decorated function (for single functions)
        task : Optional[Task]
            The metaflow task (for single functions)
        **kwargs : Any
            Additional keyword arguments
        """
        self._func: Optional["MetaflowFunctionDecorator"] = func
        self.task: Optional["Task"] = task
        self._function_spec: Optional[FunctionSpec] = None
        self._backend: Optional["AbstractBackend"] = None

        # Only build function spec for legacy single-function initialization
        if func is not None and task is not None:
            # Build the function spec
            self._function_spec = self._build_function_spec(**kwargs)

            # Check that the task is completed, we need a valid task
            # that saved artifacts
            if task.successful != True:
                raise MetaflowFunctionException(
                    f"'{task.pathspec}' cannot be bound to function '{self.name}' because it did not complete successfully or is still running. Only finished and successful tasks can be bound to a function."
                )

            # Add package info, timestamp, and generate final hash
            package_suffixes = kwargs.get("package_suffixes", None)
            self._function_spec = self._export(self._function_spec, package_suffixes)

    @property
    def spec(self) -> FunctionSpec:
        """
        Returns the function specification for the Metaflow function bound to the task.

        Returns
        -------
        FunctionSpec
            The function specification object.
        """
        if self._function_spec is None:
            raise MetaflowFunctionException("Function spec is not set.")
        return self._function_spec

    @property
    def reference(self) -> str:
        """
        Return the path to the function reference JSON

        Returns
        ------
        str
            String path to function reference JSON

        """
        ref = self.spec.reference
        if ref is None:
            raise MetaflowFunctionException("Function reference not available")
        return ref

    @property
    def name(self) -> str:
        """Return the function name."""
        name = self.spec.name
        if name is None:
            raise MetaflowFunctionException("Function name not available")
        return name

    @property
    def uuid(self) -> str:
        """Return the function UUID."""
        uuid = self.spec.uuid
        if uuid is None:
            raise MetaflowFunctionException("Function uuid not available")
        return uuid

    @property
    def backend(self):
        """
        Get the backend for this function.
        Returns the instance-specific backend if available, otherwise raises an error.
        Returns
        -------
        AbstractBackend
            The backend to use for this function
        """
        if hasattr(self, "_backend") and self._backend is not None:
            return self._backend
        raise MetaflowFunctionException(
            "No backend has been set for this function instance. "
            "Please ensure you construct the function with a backend."
        )

    def _build_function_spec(self, **kwargs) -> FunctionSpec:
        """
        Builds the function specification for the Metaflow function.

        Parameters
        ----------
        **kwargs : Any
            Additional keyword arguments that can be used to configure the function.

        Returns
        -------
        FunctionSpec
            The function specification object.
        """

        def _package_artifact_metadata(task):
            artifact_meta = {}
            for artifact in task.artifacts:
                artifact_meta[artifact.id] = artifact._object
            return artifact_meta

        if self._func is None or self.task is None:
            raise MetaflowFunctionException(
                "Cannot build function spec without a function and a task."
            )

        func_spec = self.function_spec_cls()
        if not hasattr(self._func, "deco_spec"):
            raise MetaflowFunctionRuntimeException(
                "Function must have deco_spec attribute"
            )

        name_override = kwargs.get("name_override")
        if name_override:
            # Create a copy of the decorator spec with the overridden name
            from copy import deepcopy

            func_spec.function = deepcopy(self._func.deco_spec)
            func_spec.function.name = name_override
            func_spec.name = name_override
        else:
            func_spec.function = self._func.deco_spec
            func_spec.name = self._func.deco_spec.name

        func_spec.task_pathspec = self.task.pathspec
        # Use the task's code package path; fall back to the current run's code URL
        # (the first task in a local run may register metadata before the async
        # code-package upload completes, leaving task.code as None)
        if self.task.code is not None:
            func_spec.task_code_path = self.task.code.path
        elif os.environ.get("METAFLOW_CODE_URL"):
            func_spec.task_code_path = os.environ["METAFLOW_CODE_URL"]
        else:
            raise MetaflowFunctionException(
                "Cannot determine code package path: task '%s' has no code package "
                "and METAFLOW_CODE_URL is not set. Ensure code packaging is enabled."
                % self.task.pathspec
            )
        from metaflow.util import get_username

        func_spec.user = get_username()
        func_spec.system_metadata = {}
        if (
            "conda_env_id" not in self.task.metadata_dict
            or self.task.metadata_dict["conda_env_id"] == ""
        ):
            raise MetaflowFunctionException(
                "Tasks must use one of the environment "
                "decorators to bind to a 'Metaflow Function'"
            )

        conda_env_id = self.task.metadata_dict["conda_env_id"]
        conda_env_id = json.loads(conda_env_id)
        alias = f"{conda_env_id[0]}:{conda_env_id[1]}"
        arch = conda_env_id[2]
        func_spec.system_metadata.update(
            {
                "environment": {
                    "alias": alias,
                    "arch": arch,
                    "env_id": conda_env_id,
                }
            }
        )
        # Set user metadata if provided
        func_spec.user_metadata = kwargs.get("user_metadata", None)

        # Add class name and type specs
        func_spec.class_name = (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        )

        # Validate function name is set
        if func_spec.name is None:
            raise MetaflowFunctionException("Function name is required but not set")

        # Set type information
        if not func_spec.function:
            raise MetaflowFunctionException("No 'function' field in this function spec")

        func_spec.input_spec = func_spec.function.input_schema
        if not func_spec.input_spec or "type" not in func_spec.input_spec:
            raise MetaflowFunctionException(
                f"Invalid type information in input schema {func_spec.input_spec}"
            )

        func_spec.output_spec = func_spec.function.return_schema
        if not func_spec.output_spec or "type" not in func_spec.output_spec:
            raise MetaflowFunctionException(
                f"Invalid type information in output schema {func_spec.output_spec}"
            )

        # Set artifacts metadata
        func_spec.artifacts = _package_artifact_metadata(self.task)

        return func_spec

    @classmethod
    def _export(
        cls,
        func_spec: FunctionSpec,
        package_suffixes: Optional[str] = None,
    ) -> FunctionSpec:
        """
        Exports the function specification to a file or other storage medium
        and returns the path to the reference file.

        Parameters
        ----------
        func_spec : FunctionSpec
            The function specification to be exported.

        package_suffixes : Optional[str], default None
            A comma-separated string of suffixes to be used for packaging the function.

        Returns
        -------
        FunctionSpec
            The function specification with the updated reference path.
        """
        from metaflow_extensions.nflx.plugins.functions.package import (
            MetaflowFunctionPackage,
        )

        # TODO: Update this code to support other data stores, and make it compatible with OSS
        # TODO: Add support for versioning
        # TODO: Do not export if the function is already exported, use cache
        from metaflow.metaflow_config import (
            DEFAULT_DATASTORE,
            DEFAULT_FUNCTIONS_LOCAL_ROOT,
            DEFAULT_FUNCTIONS_S3_ROOT,
        )

        debug.functions_exec("Exporting function to S3 or local file system")
        root_path: str
        if DEFAULT_DATASTORE == "s3":
            # Export to S3
            root_path = DEFAULT_FUNCTIONS_S3_ROOT
        else:
            # Export to local file system
            root_path = DEFAULT_FUNCTIONS_LOCAL_ROOT
        root_path = os.path.join(root_path, Config.FUNCTIONS_ENV, Config.VERSION)
        debug.functions_exec(f"Exporting to {root_path}")
        func_package = MetaflowFunctionPackage(func_spec, package_suffixes)
        package_blob = func_package.get_package()
        if not func_package.package_hash:
            raise MetaflowFunctionException(
                "Function package hash is empty. "
                "Please ensure the function is properly decorated and has a valid package."
            )

        # Set package hash
        package_uuid: str = func_package.package_hash
        func_spec.package_uuid = package_uuid

        # Generate stable UUID from only the fields that should be stable
        stable_fields = {
            "name": func_spec.name,
            "class_name": func_spec.class_name,
            "function": asdict(func_spec.function) if func_spec.function else None,
            "task_pathspec": func_spec.task_pathspec,
            "task_code_path": func_spec.task_code_path,
            "package_uuid": func_spec.package_uuid,
            "system_metadata": func_spec.system_metadata,
            "input_spec": func_spec.input_spec,
            "output_spec": func_spec.output_spec,
            "code_package": func_spec.code_package,
            "user_metadata": func_spec.user_metadata,
            "artifacts": func_spec.artifacts,
        }
        json_blob = json.dumps(stable_fields, sort_keys=True).encode()
        func_spec.uuid = hashlib.sha256(json_blob).hexdigest()[:32]
        func_spec.timestamp_utc = int(
            datetime.datetime.now(datetime.timezone.utc).timestamp()
        )

        # Generate S3 paths and update package metadata
        blob_name = f"{package_uuid}.zip"
        # After generating uuid, we know uuid is not None, so cast it for type checker
        uuid_str = cast(str, func_spec.uuid)
        json_name = f"{uuid_str}.json"
        blob_path = os.path.join(root_path, "package", package_uuid[0:2], blob_name)
        json_path = os.path.join(root_path, "metadata", uuid_str[0:2], json_name)
        func_spec.code_package = blob_path
        func_spec.reference = json_path

        # Write to a temp directory then move to S3 or local
        with TemporaryDirectory(prefix="metaflow-functions-") as td:
            blob_path_temp = os.path.join(td, blob_name)
            json_path_temp = os.path.join(td, json_name)
            with open(blob_path_temp, "wb") as blob_f:
                blob_f.write(package_blob)

            with open(json_path_temp, "w") as json_f:
                json.dump(asdict(func_spec), json_f, sort_keys=True, indent=4)

            if is_s3(root_path):
                from metaflow import S3

                with S3() as s3:
                    s3.put_files(
                        [(blob_path, blob_path_temp), (json_path, json_path_temp)]
                    )
            else:
                os.makedirs(root_path, exist_ok=True)
                files = [(blob_path_temp, blob_path), (json_path_temp, json_path)]
                for f in files:
                    shutil.move(*f)
        return func_spec

    def execute(self, data: Any, params: "FunctionParameters", **kwargs: Any) -> Any:  # type: ignore
        """
        Core execution logic for functions.

        Parameters
        ----------
        data : Any
            Input data
        params : FunctionParameters
            Function parameters containing task artifacts
        **kwargs : Any
            Additional keyword arguments to pass to the function

        Returns
        -------
        Any
            The result of function execution
        """
        if self._func is None:
            raise MetaflowFunctionException("Function is not set")

        return self._func(data, params, **kwargs)

    def __call__(self, data: Any, **kwargs) -> Any:
        """
        Calls the function with the given data and keyword arguments.

        Parameters
        ----------
        data : Any
            The input data to which the function will be applied.

        **kwargs : dict
            Keyword arguments to be passed to the function.

        Returns
        -------
        Any
            The result of the function call.
        """
        return self.backend.apply(self, data, **kwargs)

    async def call_async(self, data: Any, **kwargs) -> Any:
        """
        Calls the function with the given data and keyword arguments.

        Parameters
        ----------
        data : Any
            The input data to which the function will be applied.

        **kwargs : dict
            Keyword arguments to be passed to the function.

        Returns
        -------
        Any
            The result of the function call.
        """
        return await self.backend.apply_async(self, data, **kwargs)

    @classmethod
    def from_json(
        cls,
        reference: str,
        base_path: Optional[str] = None,
    ) -> "MetaflowFunction":
        """
        Return a concrete class from a reference JSON by calling from_spec.

        This method loads a function specification from JSON and delegates to from_spec
        for the actual instantiation. This is a convenience method that combines JSON
        loading with function instantiation.

        Parameters
        ----------
        reference : str
            String to a reference file path containing the function metadata JSON
        base_path : Optional[str]
            Base path to expand function runtime directories

        Returns
        -------
        MetaflowFunction
            A fully instantiated function class derived from MetaflowFunction
        """
        if not base_path:
            from metaflow_extensions.nflx.config.mfextinit_functions import (
                FUNCTION_RUNTIME_PATH,
            )

            base_path = FUNCTION_RUNTIME_PATH
        func_spec = cls.function_spec_cls.from_json(reference)
        return cls.from_spec(func_spec, base_path)

    @classmethod
    def from_spec(
        cls,
        func_spec: FunctionSpec,
        base_path: Optional[str] = None,
        backend: Optional[str] = None,
    ) -> "MetaflowFunction":
        """
        Return a fully instantiated function class and create runtime directories.

        This method returns a fully instantiated class and creates the runtime directories
        but does not start a subprocess. This can only be used in runtime internal code
        or a compatible environment, as it is not relocatable. The function will have all
        dependencies extracted and be ready for execution.

        Note: This is primarily for internal use. Users should typically use
        function_from_json(url, start_runtime=True) for relocatable functions or load
        functions from a task object.

        Parameters
        ----------
        func_spec : FunctionSpec
            The function specification containing all metadata and dependencies
        base_path : Optional[str]
            Base path to expand function runtime directories and extract dependencies
        backend : Optional[str]
            Backend name to use. If not provided, uses the backend from config.

        Returns
        -------
        MetaflowFunction
            A fully instantiated function class with runtime environment prepared
        """
        if not base_path:
            from metaflow_extensions.nflx.config.mfextinit_functions import (
                FUNCTION_RUNTIME_PATH,
            )

            base_path = FUNCTION_RUNTIME_PATH

        def create_function_instance():
            dff = cls.__new__(cls)

            # Load the decorated function from the function spec
            if func_spec.function is None:
                raise MetaflowFunctionException(
                    "Function specification is missing from function spec"
                )

            # Load using consolidated function
            dff._func = load_decorated_function(func_spec.function)
            dff._function_spec = func_spec

            # Set the back-end
            if not hasattr(dff, "_backend") or dff._backend is None:
                from metaflow_extensions.nflx.plugins.functions.backends.factory import (
                    get_backend,
                )

                dff._backend = get_backend(backend)

            return dff

        # Create the environment if necessary
        # Lazy import to avoid circular imports
        from metaflow_extensions.nflx.plugins.functions.environment import (
            extract_code_packages,
            generate_trampolines_for_directory,
            run_in_path,
        )

        fpath = f"{Config.RUNTIME_FUNCTION_DIR_PREFIX}{func_spec.uuid}"
        function_dir = os.path.join(base_path, fpath)

        # Extract code
        code_dir = extract_code_packages(
            cast(str, func_spec.code_package),
            cast(str, func_spec.task_code_path),
            function_dir,
        )

        # Generate trampolines
        generate_trampolines_for_directory(code_dir)

        return run_in_path(create_function_instance, function_dir)

    @property
    @abstractmethod
    def input_types(self) -> Dict[str, Any]:
        """
        Get the input type specification for this function.

        Returns
        -------
        Dict[str, Any]
            Dictionary describing the input types
        """
        raise NotImplementedError(
            "The 'input_types' method is not implemented for this function. "
            "Please implement the method in the derived class."
        )

    @property
    @abstractmethod
    def output_types(self) -> Dict[str, Any]:
        """
        Get the output type specification for this function.

        Parameters
        ----------
        func_spec : FunctionSpec
            The function specification

        Returns
        -------
        Dict[str, Any]
            Dictionary describing the output types
        """
        raise NotImplementedError(
            "The 'output_types' method is not implemented for this function. "
            "Please implement the method in the derived class."
        )

    def get_environment(self) -> str:
        """
        Get the environment specification for this function.

        Returns
        -------
        str
            Environment identifier string
        """
        return self.backend.get_environment(self)

    @abstractmethod
    def is_compatible_with(self, other: "MetaflowFunction") -> bool:
        """
        Check if this function's output is compatible with another function's input.

        Parameters
        ----------
        other : MetaflowFunction
            The other function to check compatibility with

        Returns
        -------
        bool
            True if compatible, False otherwise
        """
        raise NotImplementedError(
            "The 'is_compatible_with' method is not implemented for this function. "
            "Please implement the method in the derived class."
        )

    def __getstate__(self) -> Dict[str, str]:
        """
        Prepares the instance for pickling.

        We only pickle the function spec reference

        Returns
        -------
        Dict[str, str]
            The state dictionary for pickling.
        """
        if self._function_spec is None or self._function_spec.reference is None:
            raise MetaflowFunctionException(
                "Cannot pickle function without a valid reference."
            )
        return {
            "reference": self._function_spec.reference,
        }

    def __setstate__(self, state: Dict[str, str]):
        """
        Restores the instance from a pickled state.

        Parameters
        ----------
        state : Dict[str, Any]
            The unpickled state dictionary.
        """
        # Restore instance attributes
        reference = state.get("reference")
        if reference is None:
            raise MetaflowFunctionException(
                "Cannot restore function state, reference is missing"
            )
        self._function_spec = self.function_spec_cls.from_json(reference)

    @classmethod
    def _from_decorator_spec(
        cls, func_decorator_spec, function_class_module: str, function_class_name: str
    ) -> "MetaflowFunction":
        """
        Reconstruct a function instance from its decorator spec.

        This is a utility method that can be used by pipelines and other
        compositions to reconstruct constituent functions.

        Parameters
        ----------
        func_decorator_spec : FunctionDecoratorSpec
            The decorator specification for the function
        function_class_module : str
            Module name of the function class
        function_class_name : str
            Class name of the function class

        Returns
        -------
        MetaflowFunction
            The reconstructed function instance
        """
        from metaflow_extensions.nflx.plugins.functions.utils import (
            get_function_from_path,
            load_class_from_string,
        )

        # Get the function class dynamically
        function_class = load_class_from_string(
            function_class_module, function_class_name
        )
        if function_class is None:
            raise MetaflowFunctionException(
                f"Could not load function class {function_class_name} "
                f"from module {function_class_module}"
            )

        # Load the actual decorated function
        function_path = func_decorator_spec.module + "::" + func_decorator_spec.name
        _, decorated_function = get_function_from_path(function_path)

        # Create the function instance
        return function_class(decorated_function)

    @classmethod
    def _create_proxy_from_spec(
        cls,
        func_spec: FunctionSpec,
        base_path: Optional[str] = None,
        backend: Optional[str] = None,
    ) -> "MetaflowFunction":
        """
        Create a function instance in proxy mode that doesn't import dependencies.

        This is used by function_from_json to create functions that can execute
        without importing the actual function code in the parent process.

        Parameters
        ----------
        func_spec : FunctionSpec
            The function specification loaded from JSON
        base_path : Optional[str]
            Base path for runtime (unused in proxy mode)
        backend : Optional[str]
            Backend name to use. If not provided, uses the backend from config.

        Returns
        -------
        MetaflowFunction
            The function instance in proxy mode
        """
        # Create instance without calling __init__ to avoid importing function code
        instance = cls.__new__(cls)

        # Initialize proxy function attributes
        instance._func = None
        instance.task = None
        instance._function_spec = func_spec

        # Set up backend
        from metaflow_extensions.nflx.plugins.functions.backends.factory import (
            get_backend,
        )

        instance._backend = get_backend(backend)

        return instance


def function_from_json(
    reference: str,
    base_path: Optional[str] = None,
    start_runtime: bool = True,
    use_proxy: bool = True,
    backend: Optional[str] = None,
    process: int = 1,
) -> "MetaflowFunction":
    """
    Load a relocatable function from a reference JSON.

    This is the primary entry point for loading functions from URLs or file paths.
    By default, it creates a lightweight proxy object that defers actual function
    loading until execution. This makes it suitable for most user scenarios.

    Use Cases:
    - Most users: Set start_runtime=True for a ready-to-use relocatable function
    - Advanced users: Use proxy mode for lazy loading and manual runtime control
    - Export scenarios: Use proxy mode for systems that control their own environment

    The proxy behavior means this function doesn't immediately load all dependencies,
    making it suitable for parent processes and export scenarios. The actual function
    and environment setup happens when needed.

    Parameters
    ----------
    reference : str
        String path or URL to the function reference JSON file
    base_path : Optional[str]
        Base path for runtime directories (mainly for internal use)
    start_runtime : bool, default True
        Start the runtime subprocess immediately after loading.
        Set to True for most user scenarios to get a ready-to-use function.
    use_proxy : bool, default True
        If True, return a lightweight proxy that defers loading (recommended).
        If False, return a concrete function with full dependencies loaded immediately.
    backend : Optional[str], default None
        Backend name to use (e.g., 'memory', 'local', 'ray').
        If not provided, uses the backend from METAFLOW_FUNCTION_BACKEND config.
    process: int, default 1
        Number of process to back this function

    Returns
    -------
    MetaflowFunction
        A function instance configured as specified by the parameters
    """
    # Load the spec from json reference
    fs = FunctionSpec.from_json(reference)

    # Setup interceptor and register serializer configs from JSON
    from metaflow_extensions.nflx.plugins.functions.serializers.import_interceptor import (
        _ensure_interceptor_installed,
    )
    from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
        register_serializer_config,
    )
    from metaflow_extensions.nflx.plugins.functions.serializers.config import (
        SerializerConfig,
    )

    # Ensure interceptor is installed to handle future imports
    _ensure_interceptor_installed()

    # Register serializer configs from the JSON
    if fs.serializer_configs:
        for canonical_type, config_dict in fs.serializer_configs.items():
            extra_kwargs = config_dict.get("extra_kwargs")
            config = SerializerConfig(
                canonical_type=canonical_type,
                serializer=config_dict["serializer"],
                supports_chunking=config_dict.get("supports_chunking", False),
                extra_kwargs=extra_kwargs if extra_kwargs is not None else {},
            )
            register_serializer_config(config)

    # Extract the class name to determine function type
    if not fs.class_name:
        raise MetaflowFunctionException(f"No class_name field found in function spec")

    # Load the subclass using the existing utility
    from metaflow_extensions.nflx.plugins.functions.utils import load_type_from_string

    subclass = load_type_from_string(fs.class_name)
    if subclass is None:
        raise MetaflowFunctionException(f"Cannot import class {fs.class_name}")

    if use_proxy:
        # Parent process - return lightweight proxy
        func = subclass._create_proxy_from_spec(fs, base_path, backend=backend)
    else:
        # Subprocess - return concrete function with full environment
        func = subclass.from_spec(fs, base_path, backend=backend)

    # Start the runtime if requested
    if start_runtime:
        # Store prefetch_artifacts on function instance so runtime can access it
        # when constructing the subprocess command
        func._prefetch_artifacts = True
        func.backend.start(func, process=process)

    return func


def close_function(function: MetaflowFunction, clean_dir: bool = True) -> None:
    """
    Force close a metaflow function, cleaning up any files.

    Parameters
    ----------
    function: MetaflowFunction
        Function to close
    """
    backend = function.backend
    backend.close(function, clean_dir)


def load_decorated_function(func_decorator_spec) -> MetaflowFunctionDecorator:
    """
    Load a decorated function from its decorator specification.

    Parameters
    ----------
    func_decorator_spec : FunctionDecoratorSpec
        The function decorator specification

    Returns
    -------
    MetaflowFunctionDecorator
        The loaded decorated function
    """
    from metaflow_extensions.nflx.plugins.functions.utils import (
        get_function_from_path,
    )

    if not func_decorator_spec.module or not func_decorator_spec.name:
        raise MetaflowFunctionRuntimeException(
            f"Function {func_decorator_spec.name} or module {func_decorator_spec.module} is missing"
        )

    # Handle __main__ module case
    module_name = func_decorator_spec.module
    if "__main__" in module_name and func_decorator_spec.file_name:
        module_name = func_decorator_spec.file_name

    function_path = module_name + "::" + func_decorator_spec.name
    _, decorated_func = get_function_from_path(function_path)

    return cast(MetaflowFunctionDecorator, decorated_func)
