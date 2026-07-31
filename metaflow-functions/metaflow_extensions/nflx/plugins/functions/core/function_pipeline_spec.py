import os
from typing import Dict, Any, Optional
from metaflow_extensions.nflx.plugins.functions.config import Config
from metaflow_extensions.nflx.plugins.functions.core.function_spec import FunctionSpec
from metaflow_extensions.nflx.plugins.functions.utils import resolve_package_dir

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

        TODO(pipeline first-constituent): "first" is a placeholder policy.
        Every caller of this method inherits it -- resolve_function_root_dir,
        resolve_function_package_dir and ensure_function_package_extracted --
        and it is only correct while a pipeline's functions share one module.
        See FunctionPipeline._reconstruct_from_spec for the full list of
        sites that must change together.
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

    def resolve_function_package_dir(self, base_path: str) -> str:
        """
        The first constituent function's package directory.

        Resolving the subdirectory from the *constituent's* module (rather
        than just borrowing its uuid and reusing the base implementation)
        matters: a pipeline's own `function` is a
        PipelineFunctionDecoratorSpec pointing at FunctionPipeline's module,
        which is never where the model owner's colocated files live.
        """
        desc = self._first_constituent_desc()
        root = self._root_dir_for(base_path, desc)
        if root is None:
            return super().resolve_function_package_dir(base_path)
        module = ((desc or {}).get("function") or {}).get("module")
        return resolve_package_dir(root, module)

    def ensure_function_package_extracted(self, base_path: str) -> None:
        """
        Extract the *first constituent function's* package, not the
        pipeline's own.

        A pipeline's own package is a near-empty placeholder, and the
        constituent packages are normally extracted only inside the runtime
        subprocess -- so without this a caller-side component resolves a
        correct path to a directory that doesn't exist in this process.
        """
        desc = self._first_constituent_desc()
        if desc is None:
            return super().ensure_function_package_extracted(base_path)
        self._extract_package_for(
            base_path,
            desc.get("uuid"),
            desc.get("code_package"),
            desc.get("task_code_path"),
        )
