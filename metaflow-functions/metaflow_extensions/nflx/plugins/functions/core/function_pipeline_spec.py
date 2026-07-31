import os
from typing import Dict, Any
from metaflow_extensions.nflx.plugins.functions.config import Config
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

    def resolve_function_root_dir(self, base_path: str) -> str:
        """
        A pipeline's own extraction dir is near-empty (see
        FunctionPipeline._build_pipeline_spec). Delegate to the first
        constituent function's directory instead, which actually holds the
        runtime files (config, schemas, etc). See the matching TODO in
        FunctionPipeline._reconstruct_from_spec about heterogeneous
        constituent modules -- both fixes consistently pick "first
        constituent" for now.
        """
        function_specs = (self.system_metadata or {}).get("functions") or []
        first_uuid = function_specs[0].get("uuid") if function_specs else None
        if not first_uuid:
            return super().resolve_function_root_dir(base_path)
        return os.path.join(
            base_path, f"{Config.RUNTIME_FUNCTION_DIR_PREFIX}{first_uuid}"
        )
