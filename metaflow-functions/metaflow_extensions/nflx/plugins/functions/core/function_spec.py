import os
import json
import tempfile
from dataclasses import dataclass
from typing import Optional, Dict, Any, TYPE_CHECKING
from abc import ABC, abstractmethod
from metaflow_extensions.nflx.plugins.functions.core.function_decorator_spec import (
    FunctionDecoratorSpec,
)


from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
)
from metaflow_extensions.nflx.plugins.functions.debug import debug
from metaflow_extensions.nflx.plugins.functions.utils import (
    is_s3,
    load_class_from_string,
)

if TYPE_CHECKING:
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        MetaflowFunction,
    )


@dataclass(kw_only=True)
class FunctionSpec(ABC):
    """
    Unified specification for functions and pipelines.

    All types are represented as a single callable function:
    - function: FunctionDecoratorSpec (the executable unit)
    - class_name: Fully qualified class name for the appropriate subclass

    For single functions:
    - function: FunctionDecoratorSpec of the function
    - class_name: Fully qualified class name (e.g., "my.module.FunctionSpec")

    For pipelines:
    - function: FunctionDecoratorSpec of the pipeline (executable unit)
    - class_name: Fully qualified class name (e.g., "my.module.PipelineSpec")
    - system_metadata contains function references

    Note: The main 'function' field represents the callable unit, while constituent
    functions are referenced in system_metadata for internal composition details.
    """

    # Basic metadata
    name: Optional[str] = None
    uuid: Optional[str] = None
    class_name: Optional[str] = None
    user: Optional[str] = None
    timestamp_utc: Optional[int] = None
    reference: Optional[str] = None

    # Function specification
    function: Optional[FunctionDecoratorSpec] = None

    # I/O specifications
    input_spec: Optional[Dict[str, Any]] = None
    output_spec: Optional[Dict[str, Any]] = None

    # Standard fields (used by all function types)
    code_package: Optional[str] = None
    task_pathspec: Optional[str] = None
    task_code_path: Optional[str] = None
    package_uuid: Optional[str] = None
    system_metadata: Optional[Dict[str, Any]] = None
    user_metadata: Optional[Dict[str, Any]] = None

    # Used for accessing artifacts
    artifacts: Optional[Dict[str, Any]] = None

    # Serializer configurations for types used by the function
    serializer_configs: Optional[Dict[str, Dict[str, Any]]] = None

    @classmethod
    def _build_deco_spec(cls, desc: Dict[str, Any]) -> FunctionDecoratorSpec:
        """
        Build the function decorator specification from the given description.

        Parameters
        ----------
        desc : Dict[str, Any]
            The description of the function decorator.

        Returns
        -------
        FunctionDecoratorSpec
            The created function decorator specification.
        """
        try:
            function_deco_spec = FunctionDecoratorSpec(**desc)
        except Exception as e:
            raise MetaflowFunctionException(
                f"Cannot create function deco spec from reference, error is: {str(e)}"
            )
        return function_deco_spec

    @classmethod
    def from_json(cls, reference_path: str) -> "FunctionSpec":
        """
        Create a FunctionSpec instance from a reference string.

        Parameters
        ----------
        reference_path : str
            The reference path to create the FunctionSpec from.

        Returns
        -------
        FunctionSpec
            The created FunctionSpec instance.
        """
        if cls is FunctionSpec:
            # Read file once and dispatch to appropriate subclass
            desc = cls._load_json_data(reference_path)
            subclass = cls._detect_subclass_from_data(desc)
            sub_spec_class = subclass.function_spec_cls
            return sub_spec_class._from_json_impl_from_data(desc)
        else:
            # Direct subclass call - read file and process
            desc = cls._load_json_data(reference_path)
            return cls._from_json_impl_from_data(desc)

    @classmethod
    def download_to_temp(cls, reference: str) -> str:
        """Download S3 reference to temp file, return local path"""
        if not is_s3(reference):
            return reference

        from metaflow import S3

        with S3() as s3:
            s3obj = s3.get(reference)
            fd, temp_path = tempfile.mkstemp(suffix=".json")
            try:
                with os.fdopen(fd, "w") as f:
                    f.write(s3obj.text or "")
                return temp_path
            except:
                os.unlink(temp_path)
                raise

    @classmethod
    def _load_json_data(cls, reference_path: str) -> Dict[str, Any]:
        """
        Load JSON data from the reference path.

        Parameters
        ----------
        reference_path : str
            The reference path to load the JSON from.

        Returns
        -------
        Dict[str, Any]
            The parsed JSON data.
        """
        debug.functions_exec(f"Loading JSON data from {reference_path}")
        desc: Dict[str, Any]
        if is_s3(reference_path):
            from metaflow import S3

            with S3() as s3:
                s3obj = s3.get(reference_path)
                desc = json.loads(s3obj.text or "")
        else:
            with open(reference_path, "r") as fh:
                desc = json.load(fh)
        debug.functions_exec(f"Function spec loaded from {reference_path}: {desc}")
        return desc

    @classmethod
    def _detect_subclass_from_data(
        cls, desc: Dict[str, Any]
    ) -> "type[MetaflowFunction]":
        """
        Detect the appropriate MetaflowFunction subclass based on the class_name field in the JSON data.

        Parameters
        ----------
        desc : Dict[str, Any]
            The parsed JSON data.

        Returns
        -------
        type[MetaflowFunction]
            The appropriate MetaflowFunction subclass to use.
        """
        class_name = desc.get("class_name")
        if not class_name:
            raise MetaflowFunctionException(
                f"No class_name field found in function spec"
            )

        # Import and return the class specified by the fully qualified class name
        module_name, class_name_only = class_name.rsplit(".", 1)
        subclass = load_class_from_string(module_name, class_name_only)
        if subclass is None:
            raise MetaflowFunctionException(f"Cannot import class {class_name}")
        return subclass

    @classmethod
    @abstractmethod
    def _from_json_impl_from_data(cls, desc: Dict[str, Any]) -> "FunctionSpec":
        """
        Subclasses implement actual loading logic from parsed JSON data.

        Parameters
        ----------
        desc : Dict[str, Any]
            The parsed JSON data.

        Returns
        -------
        FunctionSpec
            The created FunctionSpec instance.
        """
        # Build function spec from the function field
        function_spec = None
        if "function" in desc and desc["function"]:
            function_spec = cls._build_deco_spec(desc["function"])

        filtered_desc = {k: v for k, v in desc.items() if k != "function"}
        spec = cls(**filtered_desc)
        spec.function = function_spec

        return spec
