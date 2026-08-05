import os
from typing import Dict, Any, Optional
from metaflow_extensions.nflx.plugins.functions.config import Config
from metaflow_extensions.nflx.plugins.functions.core.function_spec import FunctionSpec

# Guard against a cycle in a hand-built/corrupt spec. Real pipelines nest at
# most a couple of levels; this only needs to be big enough not to be a
# ceiling in practice.
_MAX_PIPELINE_NESTING = 16


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

    def _first_constituent_desc(self) -> Optional[Dict[str, Any]]:
        """
        The first *non-pipeline* constituent function's serialized spec dict,
        as stashed in system_metadata by
        FunctionPipeline._build_pipeline_spec.

        Read straight off the dict rather than rehydrating a FunctionSpec:
        rehydrating goes through FunctionSpec._detect_subclass_from_data,
        which imports the constituent's class, and this runs on the caller
        side where the proxy path deliberately avoids importing function
        code.

        Descends through nested pipelines so a pipeline-of-pipelines still
        lands on a real function. Returns None when there are no constituent
        specs (e.g. a hand-built spec in a unit test), leaving callers to
        fall back to the pipeline's own values.

        TODO(pipeline first-constituent): "first" is a placeholder policy,
        inherited by resolve_function_root_dir. It only matters for choosing a
        directory that contains code; nothing reads per-function resources out
        of it anymore.
        """
        functions = (self.system_metadata or {}).get("functions") or []
        for _ in range(_MAX_PIPELINE_NESTING):
            if not functions:
                return None
            desc = functions[0]
            if not isinstance(desc, dict):
                return None
            if "FunctionPipeline" not in (desc.get("class_name") or ""):
                return desc
            functions = (desc.get("system_metadata") or {}).get("functions") or []
        return None

    @staticmethod
    def _root_dir_for(base_path: str, desc: Optional[Dict[str, Any]]) -> Optional[str]:
        """Extraction dir for a constituent spec dict, or None if it has no uuid."""
        uuid = desc.get("uuid") if desc else None
        if not uuid:
            return None
        return os.path.join(base_path, f"{Config.RUNTIME_FUNCTION_DIR_PREFIX}{uuid}")

    def resolve_function_root_dir(self, base_path: str) -> str:
        """
        A pipeline's own extraction dir is near-empty (see
        FunctionPipeline._build_pipeline_spec). Delegate to the first
        constituent function's directory instead, which is what actually
        holds the extracted code. See the matching TODO in
        FunctionPipeline._reconstruct_from_spec about heterogeneous
        constituent modules -- all of these consistently pick "first
        constituent" for now.
        """
        root = self._root_dir_for(base_path, self._first_constituent_desc())
        if root is None:
            return super().resolve_function_root_dir(base_path)
        return root
