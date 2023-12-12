# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

from typing import Dict, List, Optional, Tuple

from metaflow.debug import debug

from ..env_descr import EnvType, ResolvedEnvironment
from ..utils import arch_id, get_builder_envs_dep
from . import Resolver
from .conda_resolver import CondaResolver


class BuilderEnvsResolver(Resolver):
    TYPES = ["builderenv"]

    def resolve(
        self,
        env_type: EnvType,
        deps: Dict[str, List[str]],
        sources: Dict[str, List[str]],
        extras: Dict[str, List[str]],
        architecture: str,
        builder_envs: Optional[List[ResolvedEnvironment]] = None,
        base_env: Optional[ResolvedEnvironment] = None,
    ) -> Tuple[ResolvedEnvironment, Optional[List[ResolvedEnvironment]]]:
        conda_only_sources = sources.get("conda", [])
        python_deps = [d for d in deps.get("conda", []) if d.startswith("python==")]
        # We add a few more packages that we need to support building wheels
        # Conda typically includes pip but no harm adding it there too
        # All packages included are miniscule and have no dependencies.
        python_deps.extend(get_builder_envs_dep(deps.get("conda", [])))

        if arch_id() == architecture:
            conda_only_deps = {
                "npconda": deps.get("npconda", []),
                "sys": deps.get("sys", []),
                "conda": python_deps,
            }
            debug.conda_exec(
                "Building builder environment with %s" % str(conda_only_deps)
            )
            return CondaResolver(self._conda).resolve(
                EnvType.CONDA_ONLY,
                conda_only_deps,
                {"conda": conda_only_sources},
                extras,
                architecture,
                builder_envs,
                None,
            )

        debug.conda_exec("Using vanilla builder env with %s" % str(python_deps[0]))

        return CondaResolver(self._conda).resolve(
            EnvType.CONDA_ONLY,
            {"conda": python_deps},
            {"conda": conda_only_sources},
            extras,
            arch_id(),
            builder_envs,
            None,
        )
