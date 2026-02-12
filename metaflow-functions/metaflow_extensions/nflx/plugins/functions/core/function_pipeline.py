"""
Simple pipeline for composing functions.
"""

import os
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast
from dataclasses import dataclass, field, asdict

from metaflow_extensions.nflx.plugins.functions.core.function import MetaflowFunction



def _lazy_get_username():
    from metaflow.util import get_username

    return get_username()
from metaflow_extensions.nflx.plugins.functions.config import Config

from metaflow_extensions.nflx.plugins.functions.core.function_spec import FunctionSpec
from metaflow_extensions.nflx.plugins.functions.core.function_pipeline_spec import (
    FunctionPipelineSpec,
)
from metaflow_extensions.nflx.plugins.functions.core.function_decorator_spec import (
    FunctionDecoratorSpec,
)

from metaflow_extensions.nflx.plugins.functions.debug import debug
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
    MetaflowFunctionRuntimeException,
    MetaflowFunctionTypeException,
    MetaflowFunctionUserException,
)

if TYPE_CHECKING:
    from metaflow_extensions.nflx.plugins.functions.memory.memory import (
        ReadBuffer,
        WriteBuffer,
    )


@dataclass(kw_only=True)
class PipelineFunctionDecoratorSpec(FunctionDecoratorSpec):
    """Concrete FunctionDecoratorSpec for pipelines."""

    type: str = "pipeline"
    input_schema: Dict[str, Any] = field(default_factory=dict)
    parameter_schema: Dict[str, Any] = field(default_factory=dict)
    return_schema: Dict[str, Any] = field(default_factory=dict)


class FunctionPipeline(MetaflowFunction):
    """Simple linear pipeline of functions."""

    function_spec_cls = FunctionPipelineSpec

    def __init__(self, functions: List[MetaflowFunction], name: str) -> None:
        if not functions or not name:
            raise MetaflowFunctionException("Pipeline needs functions and name")

        # Validate pipeline first
        self._validate_input_functions(functions)

        # Initialize base class
        super().__init__()

        self.functions = functions
        self._name = name

        # Pipeline creates its own backend instance
        from metaflow_extensions.nflx.plugins.functions.backends import get_backend

        self._backend = get_backend()

        # Build the FunctionSpec for this pipeline
        self._function_spec = self._build_pipeline_spec()

        # Export the pipeline to set the reference field
        self._function_spec = self._export(self._function_spec)

    def _validate_input_functions(self, functions: List[MetaflowFunction]) -> None:
        """Validate pipeline structure."""

        # Check environment consistency
        envs = [f.get_environment() for f in functions]
        if len(set(envs)) > 1:
            raise MetaflowFunctionTypeException(
                f"Inconsistent environments: {set(envs)}"
            )

        # Check task pathspec consistency
        pathspecs = [f.spec.task_pathspec for f in functions if f.spec.task_pathspec]
        if len(set(pathspecs)) > 1:
            raise MetaflowFunctionTypeException(
                f"Inconsistent task pathspecs: {set(pathspecs)}"
            )

        # Check compatibility
        for i in range(len(functions) - 1):
            if not functions[i].is_compatible_with(functions[i + 1]):
                raise MetaflowFunctionTypeException(
                    f"{functions[i].name} incompatible with {functions[i + 1].name}"
                )

    def _build_pipeline_spec(self) -> FunctionSpec:
        """Build FunctionSpec for this pipeline."""
        # Compute pipeline I/O
        input_spec = self._compute_pipeline_input_spec()
        output_spec = self._compute_pipeline_output_spec()
        parameter_spec = self._compute_pipeline_parameter_spec()

        # Pipeline presents itself as a single function to the runtime
        pipeline_decorator_spec = PipelineFunctionDecoratorSpec(
            name=self._name,
            module=self.__class__.__module__,
            type="pipeline",
            input_schema=input_spec,
            parameter_schema=parameter_spec,
            return_schema=output_spec,
        )

        # Store function specs directly in system metadata
        function_specs = []
        for func in self.functions:
            function_specs.append(asdict(func.spec))

        # Extract environment metadata from the first function (all must have same environment)
        environment_metadata = {}
        if self.functions and self.functions[0].spec.system_metadata:
            environment_metadata = self.functions[0].spec.system_metadata.get(
                "environment", {}
            )

        # Collect serializer configs for pipeline I/O
        from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
            get_global_registry,
        )

        registry = get_global_registry()
        serializer_configs = {}
        for type_str in [input_spec.get("type"), output_spec.get("type")]:
            if type_str and type_str in registry._serializer_configs:
                config = registry._serializer_configs[type_str]
                serializer_configs[type_str] = {
                    "serializer": config.serializer,
                    "supports_chunking": config.supports_chunking,
                }
                if config.extra_kwargs:
                    serializer_configs[type_str]["extra_kwargs"] = config.extra_kwargs

        # Create the FunctionPipelineSpec - use same code package as first function
        # since all functions in pipeline share the same environment
        code_package = None
        if self.functions and hasattr(self.functions[0], "spec"):
            code_package = self.functions[0].spec.code_package

        return FunctionPipelineSpec(
            name=self._name,
            uuid=None,
            class_name=f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            function=pipeline_decorator_spec,
            input_spec=input_spec,
            output_spec=output_spec,
            code_package=code_package,  # Use first function's code package
            reference=None,  # Will be set during export
            task_pathspec=(
                self.functions[0].spec.task_pathspec if self.functions else None
            ),  # Use first function's task pathspec
            task_code_path=(
                self.functions[0].spec.task_code_path if self.functions else None
            ),  # Use first function's task code path
            artifacts=(
                self.functions[0].spec.artifacts if self.functions else None
            ),  # Use first function's artifacts metadata
            user=_lazy_get_username(),
            timestamp_utc=int(time.time()),
            system_metadata={
                "functions": function_specs,
                "environment": environment_metadata,
            },
            serializer_configs=serializer_configs,
        )

    @property
    def name(self) -> str:
        return self._name

    def _compute_pipeline_input_spec(self) -> Dict[str, Any]:
        """Compute external inputs needed by the pipeline."""
        if not self.functions:
            return {}

        # For linear pipeline, external inputs are inputs of first function
        first_func = self.functions[0]
        return first_func.spec.input_spec or {}

    def _compute_pipeline_output_spec(self) -> Dict[str, Any]:
        """Compute external outputs produced by the pipeline."""
        if not self.functions:
            return {}

        # For linear pipeline, external outputs are outputs of last function
        last_func = self.functions[-1]
        return last_func.spec.output_spec or {}

    def _compute_pipeline_parameter_spec(self) -> Dict[str, Any]:
        """Compute external parameters needed by the pipeline."""
        if not self.functions:
            return {}

        # For linear pipeline, external parameters are parameters of first function
        first_func = self.functions[0]
        if (
            first_func.spec
            and first_func.spec.function
            and hasattr(first_func.spec.function, "parameter_schema")
        ):
            return first_func.spec.function.parameter_schema or {}

        return {}

    def __call__(self, data: Any, **kwargs: Any) -> Any:
        """
        Calls the pipeline with the given data and keyword arguments.

        Parameters
        ----------
        data : Any
            The input data to which the pipeline will be applied.
        **kwargs : Any
            Additional keyword arguments to be passed to the pipeline.

        Returns
        -------
        Any
            The result of the pipeline call.
        """
        return self.backend.apply(self, data, **kwargs)

    def execute(self, data: Any, params: "FunctionParameters", **kwargs: Any) -> Any:  # type: ignore
        """
        Execute the pipeline by chaining all functions in the same process.

        Parameters
        ----------
        data : Any
            Input data
        params : FunctionParameters
            Function parameters containing task artifacts
        **kwargs : Any
            Additional keyword arguments to pass to functions

        Returns
        -------
        Any
            The result of the pipeline execution
        """
        # Chain all functions together in the same process
        result = data
        for func in self.functions:
            result = func.execute(result, params, **kwargs)
        return result

    def is_compatible_with(self, other: MetaflowFunction) -> bool:
        """Check if pipeline output is compatible with other function input."""
        if not self.functions:
            return False
        return self.functions[-1].is_compatible_with(other)

    def get_environment(self) -> str:
        """Get environment (all functions must have same environment)."""
        return self.functions[0].get_environment() if self.functions else ""

    @classmethod
    def _get_function_references(cls, func_spec) -> List[Dict[str, Any]]:
        """Extract function specs from spec with validation."""
        if not func_spec or not func_spec.system_metadata:
            raise MetaflowFunctionException("Pipeline spec is missing system metadata")

        function_specs = func_spec.system_metadata.get("functions", [])
        if not function_specs:
            raise MetaflowFunctionException("Pipeline spec is missing function specs")

        return function_specs

    @property
    def input_types(self) -> Dict[str, Any]:
        """
        Get the input type specification for this function.

        Returns
        -------
        Dict[str, Any]
            Dictionary describing the input types
        """
        # For pipeline, use the computed input_spec
        func_spec = self.spec
        if func_spec.input_spec:
            return func_spec.input_spec

        raise MetaflowFunctionException("Pipeline has no input schema defined")

    @property
    def output_types(self) -> Dict[str, Any]:
        """
        Get the output type specification for this function.

        Returns
        -------
        Dict[str, Any]
            Dictionary describing the output types
        """
        # For pipeline, use the computed output_spec
        func_spec = self.spec
        if func_spec.output_spec:
            return func_spec.output_spec

        raise MetaflowFunctionException("Pipeline has no output schema defined")

    @classmethod
    def _reconstruct_functions_from_metadata(
        cls, func_spec: FunctionSpec, use_proxy: bool = False
    ) -> List[MetaflowFunction]:
        """
        Reconstruct constituent functions from function specs.

        Parameters
        ----------
        func_spec : FunctionSpec
            The function spec containing function specs

        Returns
        -------
        List[MetaflowFunction]
            List of reconstructed functions
        """
        function_specs = cls._get_function_references(func_spec)

        functions = []
        for func_spec_dict in function_specs:
            # Create FunctionSpec from spec dictionary
            spec_class = FunctionSpec._detect_subclass_from_data(func_spec_dict)
            sub_spec_class = spec_class.function_spec_cls
            function_spec = sub_spec_class._from_json_impl_from_data(func_spec_dict)

            if function_spec.reference is None:
                raise MetaflowFunctionException(
                    f"Function spec for {function_spec.name} is missing reference path"
                )
            # Download S3 reference to local temp file
            local_reference = FunctionSpec.download_to_temp(function_spec.reference)
            # Load constituent functions
            from metaflow_extensions.nflx.plugins.functions.core.function import (
                function_from_json,
            )

            # Child functions run in-process within pipeline subprocess,
            # so don't start their own subprocesses
            func = function_from_json(
                local_reference, use_proxy=use_proxy, start_runtime=False
            )
            functions.append(func)

        return functions

    @classmethod
    def from_spec(
        cls,
        func_spec: FunctionSpec,
        base_path: Optional[str] = None,
        backend: Optional[str] = None,
    ) -> "FunctionPipeline":
        """
        Create concrete pipeline from FunctionSpec (used by function_from_json with use_proxy=False).

        Parameters
        ----------
        func_spec : FunctionSpec
            The function specification
        base_path : Optional[str]
            Base path for function reconstruction

        Returns
        -------
        FunctionPipeline
            The concrete pipeline with concrete constituent functions
        """
        # Create concrete pipeline with concrete constituent functions
        return cls._reconstruct_from_spec(func_spec, base_path)

    @classmethod
    def _reconstruct_from_spec(
        cls, spec: FunctionSpec, base_path: Optional[str] = None
    ) -> "FunctionPipeline":
        """Reconstruct pipeline from spec without loading decorated functions."""
        if not spec.class_name or "FunctionPipeline" not in spec.class_name:
            raise MetaflowFunctionException(
                f"Expected Pipeline spec, got {spec.class_name}"
            )

        # Set default base_path if not provided
        if not base_path:
            from metaflow_extensions.nflx.config.mfextinit_functions import FUNCTION_RUNTIME_PATH

            base_path = FUNCTION_RUNTIME_PATH

        # Create the environment if necessary
        from metaflow_extensions.nflx.plugins.functions.environment import (
            extract_code_packages,
            generate_trampolines_for_directory,
            run_in_path,
        )

        fpath = f"{Config.RUNTIME_FUNCTION_DIR_PREFIX}{spec.uuid}"
        function_dir = os.path.join(base_path, fpath)

        # Extract code
        code_dir = extract_code_packages(
            cast(str, spec.code_package),
            cast(str, spec.task_code_path),
            function_dir,
        )

        # Generate trampolines
        generate_trampolines_for_directory(code_dir)

        def reconstruct_functions():
            return cls._reconstruct_functions_from_metadata(spec, use_proxy=False)

        functions = run_in_path(reconstruct_functions, function_dir)
        return cls._create_from_spec(spec, functions)

    @classmethod
    def _create_proxy_from_spec(
        cls,
        func_spec: FunctionSpec,
        base_path: Optional[str] = None,
        backend: Optional[str] = None,
    ) -> "FunctionPipeline":
        """
        Create a pipeline proxy that doesn't import dependencies.

        This only registers serializers for pipeline input/output types, not
        intermediate types between functions in the pipeline.
        The actual pipeline execution happens in the subprocess.
        """
        # Create instance without calling __init__ to avoid importing function code
        instance = cls.__new__(cls)

        # Initialize pipeline proxy attributes
        instance._func = None
        instance.task = None
        instance._function_spec = func_spec
        if not func_spec.name:
            raise MetaflowFunctionException(
                "Pipeline function spec is missing required 'name' field"
            )
        if not func_spec.uuid:
            raise MetaflowFunctionException(
                "Pipeline function spec is missing required 'uuid' field"
            )

        instance._name = func_spec.name

        # Load constituent functions
        instance.functions = cls._reconstruct_functions_from_metadata(
            func_spec, use_proxy=True
        )

        # Set up backend
        from metaflow_extensions.nflx.plugins.functions.backends.factory import (
            get_backend,
        )

        instance._backend = get_backend()

        return instance

    @classmethod
    def from_json(
        cls, reference: str, base_path: Optional[str] = None
    ) -> "FunctionPipeline":
        """Create fully populated pipeline from FunctionSpec."""
        if not base_path:
            from metaflow_extensions.nflx.config.mfextinit_functions import FUNCTION_RUNTIME_PATH

            base_path = FUNCTION_RUNTIME_PATH

        # Download S3 reference to local temp file
        local_reference = FunctionSpec.download_to_temp(reference)

        # Load the FunctionSpec
        spec = FunctionSpec.from_json(local_reference)

        # Use the common reconstruction logic
        return cls._reconstruct_from_spec(spec, base_path)

    @classmethod
    def _create_from_spec(
        cls, spec: FunctionSpec, functions: List[MetaflowFunction]
    ) -> "FunctionPipeline":
        """Create pipeline from existing spec, bypassing validation and spec generation."""
        pipeline = cls.__new__(cls)

        # Initialize base class properly
        super(FunctionPipeline, pipeline).__init__(func=None, task=None)

        # Set function spec FIRST - other attributes may depend on it
        pipeline._function_spec = spec

        # Set pipeline-specific attributes
        pipeline.functions = functions
        if spec.name is None:
            raise MetaflowFunctionException("Pipeline spec is missing a name.")
        # Set UUID and name from the loaded spec to preserve original values
        if spec.uuid is None:
            raise MetaflowFunctionException("Pipeline spec is missing a uuid.")
        pipeline._name = spec.name

        # Pipeline creates its own backend instance
        from metaflow_extensions.nflx.plugins.functions.backends import get_backend

        pipeline._backend = get_backend()

        return pipeline

    @classmethod
    def runtime(
        cls, inp: "ReadBuffer", out: "WriteBuffer", func_spec: FunctionSpec
    ) -> None:
        """Handle runtime execution for pipeline in subprocess."""

        function_references = cls._get_function_references(func_spec)
        debug.functions_exec(
            f"Starting pipeline runtime for '{func_spec.name}' with {len(function_references)} functions"
        )

        # Reconstruct constituent functions from system metadata as concrete functions
        functions = cls._reconstruct_functions_from_metadata(func_spec, use_proxy=False)

        debug.functions_exec("Starting pipeline runtime loop")
        while True:
            # Get input buffer (blocking call)
            inp_ref = inp.acquire()
            if not inp_ref.valid:
                debug.functions_exec("Null input buffer, exiting pipeline runtime")
                break

            try:
                debug.functions_exec("Converting input buffer to bytes")
                current_data = bytes(inp_ref.buf)

                # Execute pipeline: chain all functions sequentially
                debug.functions_exec(
                    f"Executing pipeline with {len(functions)} functions"
                )
                for i, func in enumerate(functions):
                    debug.functions_exec(f"Executing function {i+1}/{len(functions)}")
                    current_data = func(current_data)

                debug.functions_exec("Pipeline execution completed")

            except Exception as user_error:
                debug.functions_exec("User exception in pipeline")
                inp.free(inp_ref)
                raise MetaflowFunctionUserException(str(user_error))

            # Get output buffer (blocking call)
            debug.functions_exec("Getting output buffer")
            out_ref = out.acquire()
            if not out_ref.valid:
                debug.functions_exec("Null output buffer")
                break

            # Write result to output buffer
            debug.functions_exec("Converting result to output buffer")
            if len(current_data) > len(out_ref.buf):
                raise MetaflowFunctionRuntimeException(
                    f"Output data size {len(current_data)} exceeds buffer size {len(out_ref.buf)}"
                )

            out_ref.buf[: len(current_data)] = current_data
            data_len = len(current_data)
            out_ref.size = data_len

            # Free input buffer
            debug.functions_exec("Free input buffer")
            inp.free(inp_ref)

            # Commit output buffer
            debug.functions_exec(f"Committing output buffer {data_len}")
            out.commit(out_ref)

            debug.functions_exec("Pipeline iteration completed")

        out.close()
