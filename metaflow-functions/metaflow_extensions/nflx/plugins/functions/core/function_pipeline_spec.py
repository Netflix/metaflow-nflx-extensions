from typing import Dict, Any
from metaflow_extensions.nflx.plugins.functions.core.function_spec import FunctionSpec


class FunctionPipelineSpec(FunctionSpec):
    """Concrete FunctionSpec subclass for pipelines."""

    @classmethod
    def _from_json_impl_from_data(cls, desc: Dict[str, Any]) -> "FunctionSpec":
        """Implement the abstract method for pipeline specs."""
        # Build function spec from the function field
        function_spec = None
        if "function" in desc and desc["function"]:
            function_spec = cls._build_deco_spec(desc["function"])

        filtered_desc = {k: v for k, v in desc.items() if k != "function"}
        spec = cls(**filtered_desc)
        spec.function = function_spec

        return spec
