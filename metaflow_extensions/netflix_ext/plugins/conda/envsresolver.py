# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import (
    cast,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from metaflow.debug import debug
from metaflow.metaflow_config import CONDA_PREFERRED_FORMAT
from metaflow.metaflow_environment import InvalidEnvironmentException

from .env_descr import EnvID, EnvType, ResolvedEnvironment, TStr
from .conda import Conda
from .conda_step_decorator import CondaStepDecorator
from .utils import AliasType, arch_id, plural_marker, resolve_env_alias


class EnvsResolver(object):
    def __init__(self, conda: Conda):
        # key: EnvID; value: dict containing:
        #  - "id": key
        #  - "steps": steps using this environment
        #  - "deps": array of requested dependencies
        #  - "sources": additional channels/sources to search
        #  - "extras": additional arguments (typically used for PIP)
        #  - "conda_format": list of formats we want the Conda packages in or ["_any"] if no pref
        #  - "base": optional base environment this environment derives from
        #  - "base_accurate": T/F; True if the "deps" for the base environment are known
        #  - "resolved": The resolved environment
        #  - "already_resolved": T/F: True if we had a resolved environment prior to resolving
        #  - "force": T/F: True if we need to re-resolve
        self._requested_envs = {}  # type: Dict[EnvID, Dict[str, Any]]
        self._builder_envs = {}  # type: Dict[EnvID, Dict[str, Any]]
        self._conda = conda
        self._non_step_envs = False
        self._co_resolved_force_resolve = set()  # type: Set[str]

    def add_environment(
        self,
        env_id: EnvID,
        deps: Sequence[TStr],
        sources: Sequence[TStr],
        extras: Sequence[TStr],
        base_env: Optional[ResolvedEnvironment] = None,
        clean_base: bool = False,
        base_from_full_id: bool = False,
        local_only: bool = False,
        force: bool = False,
        force_co_resolve: bool = False,
    ):
        self._non_step_envs = True
        if (
            not force
            and base_env
            and clean_base
            and base_env.env_id.arch == env_id.arch
        ):
            resolved_env = base_env
        else:
            resolved_env = (
                self._conda.environment(env_id, local_only) if not force else None
            )
        if env_id not in self._requested_envs:
            if force_co_resolve:
                if resolved_env is None:
                    # Invalidate any previously resolved environment with the same req_id
                    for other_env_id, other_env in self._requested_envs.items():
                        if other_env_id.req_id == env_id.req_id:
                            other_env["resolved"] = None
                            other_env["already_resolved"] = False
                    self._co_resolved_force_resolve.add(env_id.req_id)
                elif env_id.req_id in self._co_resolved_force_resolve:
                    # If another environment for the same req-id is not resolved, we
                    # need to resolve this one too.
                    resolved_env = None

            self._requested_envs[env_id] = {
                "id": env_id,
                "steps": ["ad-hoc"],
                "deps": deps,
                "sources": sources,
                "extras": extras,
                "conda_format": [CONDA_PREFERRED_FORMAT]
                if CONDA_PREFERRED_FORMAT
                else ["_any"],
                "base": base_env,
                "base_accurate": base_env
                and base_env.is_info_accurate
                and not base_from_full_id,
                "resolved": resolved_env,
                "already_resolved": resolved_env is not None,
                "force": force,
            }
            debug.conda_exec(
                "Added environment to resolve %s" % str(self._requested_envs[env_id])
            )

    def add_environment_for_step(
        self,
        step_name: str,
        decorator: CondaStepDecorator,
        force: bool = False,
        archs: Optional[List[str]] = None,
    ):
        # Make sure we use the same conda object to properly cache everything
        decorator.set_conda(self._conda)

        from_env = decorator.from_env
        if from_env:
            # We need to get this environment first so we can update it
            debug.conda_exec(
                "For step '%s', found base environment to be '%s'"
                % (step_name, from_env.env_id)
            )

        env_ids = decorator.env_ids
        base_req_id = env_ids[0].req_id
        base_full_id = env_ids[0].full_id
        env_ids = [
            EnvID(req_id=base_req_id, full_id=base_full_id, arch=arch)
            for arch in set((archs or []) + [env_id.arch for env_id in env_ids])
        ]

        for env_id in env_ids:
            if (
                not force
                and decorator.clean_from_env
                and env_id.arch == cast(ResolvedEnvironment, from_env).env_id.arch
            ):
                debug.conda_exec(
                    "For step '%s', found a clean base environment -- using that"
                    % step_name
                )
                resolved_env = from_env
            else:
                resolved_env = self._conda.environment(env_id) if not force else None
            if env_id not in self._requested_envs:
                self._requested_envs[env_id] = {
                    "id": env_id,
                    "steps": [step_name],
                    "deps": decorator.step_deps,
                    "sources": decorator.source_deps,
                    "extras": [],
                    "conda_format": [CONDA_PREFERRED_FORMAT]
                    if CONDA_PREFERRED_FORMAT
                    else ["_any"],
                    "base": from_env,
                    "base_accurate": from_env
                    and from_env.is_info_accurate
                    and resolve_env_alias(cast(str, decorator.from_env_name))[0]
                    != AliasType.FULL_ID,
                    "resolved": resolved_env,
                    "already_resolved": resolved_env is not None,
                    "force": force,
                }
                debug.conda_exec(
                    "Added environment to resolve %s"
                    % str(self._requested_envs[env_id])
                )
            else:
                self._requested_envs[env_id]["steps"].append(step_name)
                debug.conda_exec(
                    "Environment '%s' is also needed by '%s'" % (env_id, step_name)
                )

    def resolve_environments(self, echo: Callable[..., None]):
        # At this point, we check in our backend storage if we have the files we need
        need_resolution = [
            env_id
            for env_id, req in self._requested_envs.items()
            if req["resolved"] is None
        ]
        if debug.conda:
            debug.conda_exec("Resolving environments:")
            for env_id in need_resolution:
                info = self._requested_envs[env_id]
                debug.conda_exec(
                    "%s (%s): %s" % (env_id.req_id, env_id.full_id, str(info))
                )
        if len(need_resolution):
            self._resolve_environments(echo, need_resolution)

    def all_environments(
        self, include_builder_envs: bool = False
    ) -> Iterator[Tuple[EnvID, Optional[ResolvedEnvironment], List[str]]]:
        src_dicts = [self._requested_envs]
        if include_builder_envs:
            src_dicts.append(self._builder_envs)
        for d in src_dicts:
            for env_id, req in d.items():
                yield env_id, cast(
                    Optional[ResolvedEnvironment], req["resolved"]
                ), cast(List[str], req["steps"])

    def resolved_environments(
        self, include_builder_envs: bool = False
    ) -> Iterator[Tuple[EnvID, ResolvedEnvironment, List[str]]]:
        src_dicts = [self._requested_envs]
        if include_builder_envs:
            src_dicts.append(self._builder_envs)
        for d in src_dicts:
            for env_id, req in d.items():
                if req["resolved"] is not None:
                    yield env_id, cast(ResolvedEnvironment, req["resolved"]), cast(
                        List[str], req["steps"]
                    )

    def non_resolved_environments(
        self,
    ) -> Iterator[Tuple[EnvID, List[str]]]:
        for env_id, req in self._requested_envs.items():
            if req["resolved"] is None:
                yield env_id, cast(List[str], req["steps"])

    def need_caching_environments(
        self, include_builder_envs: bool = False
    ) -> Iterator[Tuple[EnvID, ResolvedEnvironment, List[str], List[str]]]:
        src_dicts = [self._requested_envs]
        if include_builder_envs:
            src_dicts.append(self._builder_envs)
        for d in src_dicts:
            for env_id, req in d.items():
                # Resolved environments that were not already resolved or not fully
                # cached
                if req["resolved"] is not None and (
                    not req["already_resolved"]
                    or not req["resolved"].is_cached({"conda": req["conda_format"]})
                ):
                    yield env_id, cast(ResolvedEnvironment, req["resolved"]), cast(
                        List[str], req["conda_format"]
                    ), cast(List[str], req["steps"])

    def new_environments(
        self, include_builder_envs: bool = False
    ) -> Iterator[Tuple[EnvID, ResolvedEnvironment, List[str]]]:
        src_dicts = [self._requested_envs]
        if include_builder_envs:
            src_dicts.append(self._builder_envs)
        for d in src_dicts:
            for env_id, req in d.items():
                # Resolved environments that were not already resolved
                if req["resolved"] is not None and not req["already_resolved"]:
                    yield env_id, cast(ResolvedEnvironment, req["resolved"]), cast(
                        List[str], req["steps"]
                    )

    def _resolve_environments(
        self, echo: Callable[..., None], env_ids: Sequence[EnvID]
    ):
        start = time.time()
        if len(env_ids) == len(self._requested_envs):
            echo(
                "    Resolving %d environment%s %s..."
                % (
                    len(env_ids),
                    plural_marker(len(env_ids)),
                    "in flow " if not self._non_step_envs else "",
                ),
                nl=False,
            )
        else:
            echo(
                "    Resolving %d of %d environment%s %s(others are cached) ..."
                % (
                    len(env_ids),
                    len(self._requested_envs),
                    plural_marker(len(self._requested_envs)),
                    "in flow " if not self._non_step_envs else "",
                ),
                nl=False,
            )

        def _resolve(
            env_desc: Mapping[str, Any],
            builder_environments: Optional[Dict[str, EnvID]],
        ) -> Tuple[EnvID, ResolvedEnvironment]:
            env_id = cast(EnvID, env_desc["id"])
            builder_env_id = (
                builder_environments.get(env_id.req_id, None)
                if builder_environments
                else None
            )
            builder_env = cast(
                Optional[ResolvedEnvironment],
                (
                    self._builder_envs[builder_env_id]["resolved"]
                    if builder_env_id
                    else None
                ),
            )
            if env_desc["base"] is not None:
                return (
                    env_id,
                    self._conda.add_to_resolved_env(
                        env_desc["base"],
                        env_desc["steps"],
                        env_desc["deps"],
                        env_desc["sources"],
                        env_desc["extras"],
                        env_id.arch,
                        inputs_are_addl=False,
                        cur_is_accurate=env_desc["base_accurate"],
                        builder_env=builder_env,
                    ),
                )
            return (
                env_id,
                self._conda.resolve(
                    env_desc["steps"],
                    env_desc["deps"],
                    env_desc["sources"],
                    env_desc["extras"],
                    env_id.arch,
                    env_desc.get("env_type"),
                    builder_env=builder_env,
                ),
            )

        # In PIP-ONLY scenarios, we actually need a sub-environment to properly resolve
        # the pip environment (due to https://github.com/pypa/pip/issues/11664).
        # We try to mutualize these environments.

        # Similarly, PIP-ONLY environments have a base conda environment that contains
        # all the non-python dependencies + Python itself.
        builders_by_req_id = {}  # type: Dict[str, Any]
        my_arch = arch_id()
        for env_id in env_ids:
            if env_id.arch != my_arch:
                builders_by_req_id.setdefault(
                    env_id.req_id, self._requested_envs[env_id]
                )
                continue

            to_resolve_info = self._requested_envs[env_id]
            if to_resolve_info["base"]:
                (
                    builder_env_id,
                    builder_sources,
                    builder_user_deps,
                    _,
                    _,
                ) = self._conda.info_for_add_to_resolved_env(
                    to_resolve_info["base"],
                    to_resolve_info["steps"],
                    to_resolve_info["deps"],
                    to_resolve_info["sources"],
                    to_resolve_info["extras"],
                    env_id.arch,
                    False,
                )
                builder_sources = [s for s in builder_sources if s.category == "conda"]
                builder_user_deps = [
                    d for d in builder_user_deps if d.category in ("conda", "npconda")
                ]
                debug.conda_exec(
                    "Need builder environment: %s" % str(builder_user_deps)
                )
            else:
                builder_user_deps = [
                    d
                    for d in to_resolve_info["deps"]
                    if d.category in ("conda", "npconda")
                ]
                builder_sources = [
                    s for s in to_resolve_info["sources"] if s.category == "conda"
                ]
                builder_env_id = EnvID(
                    ResolvedEnvironment.get_req_id(
                        builder_user_deps, builder_sources, []
                    ),
                    "_default",
                    env_id.arch,
                )
            if builder_env_id in self._builder_envs:
                builder_env_info = self._builder_envs[builder_env_id]
                builder_env_info["steps"].extend(to_resolve_info["steps"])
            else:
                builder_env = None
                if not to_resolve_info["force"]:
                    builder_env = self._conda.environment(builder_env_id)
                builder_env_info = {
                    "id": builder_env_id,
                    "steps": to_resolve_info["steps"],
                    "deps": builder_user_deps,
                    "sources": builder_sources,
                    "extras": [],
                    "conda_format": to_resolve_info["conda_format"],
                    "base": None,
                    "base_accurate": False,
                    "resolved": builder_env,
                    "already_resolved": builder_env is not None,
                    "env_type": EnvType.CONDA_ONLY,
                }

            builders_by_req_id[env_id.req_id] = builder_env_id
            self._builder_envs[builder_env_id] = builder_env_info

        # We need to make sure we have a builder environment for this architecture
        # at least. If we can, we reuse the one that is requested but if not, we create
        # an empty one that simply has the python version we need and nothing else.

        # NOTE: Given the pip bug, it is possible that cross-arch pip-only resolution
        # produces the wrong result since none of the platform/etc markers are honored.
        # We chose not to disable this for now as this is hopefully a rare case but
        # could consider printing a warning.

        for req_id, v in builders_by_req_id.items():
            if not isinstance(v, EnvID):
                base_info = v
                builder_deps = [
                    d for d in base_info["deps"] if d.value.startswith("python==")
                ]
                builder_env_id = EnvID(
                    ResolvedEnvironment.get_req_id(
                        builder_deps,
                        [s for s in base_info["sources"] if s.category == "conda"],
                        [],
                    ),
                    "_default",
                    my_arch,
                )
                if builder_env_id in self._builder_envs:
                    builder_env_info = self._builder_envs[builder_env_id]
                    builder_env_info["steps"].extend(base_info["steps"])
                else:
                    # Here we never force rebuild -- it is only to resolve PIP so don't
                    # really care too much
                    builder_env = self._conda.environment(builder_env_id)
                    builder_env_info = {
                        "id": builder_env_id,
                        "steps": base_info["steps"],
                        "deps": builder_deps,
                        "sources": base_info["sources"],
                        "extras": [],
                        "conda_format": base_info["conda_format"],
                        "base": None,
                        "base_accurate": False,
                        "resolved": builder_env,
                        "already_resolved": builder_env is not None,
                        "env_type": EnvType.CONDA_ONLY,
                    }
                builders_by_req_id[req_id] = builder_env_id
                self._builder_envs[builder_env_id] = builder_env_info
        if len(self._builder_envs):
            debug.conda_exec("Builder environments: %s" % str(self._builder_envs))

            # Build the environments
            with ThreadPoolExecutor() as executor:
                resolution_result = [
                    executor.submit(_resolve, v, None)
                    for v in self._builder_envs.values()
                    if not v["already_resolved"]
                ]
                for f in as_completed(resolution_result):
                    env_id, resolved = f.result()
                    self._builder_envs[env_id]["resolved"] = resolved

        # NOTE: Co-resolved environments allow you to resolve a bunch of "equivalent"
        # environments for different platforms. This is great as it can allow you to
        # run code on Linux and then instantiate an environment to look at it on Mac.
        # One issue though is that the set of packages on Linux may change while those
        # on mac may not (or vice versa) so it is possible to get in the following
        # situation:
        # - Co-resolve at time A:
        #   - Get linux full_id 123 and mac full_id 456
        # - Co-resolve later at time B:
        #   - Get linux full_id 123 and mac full_id 789
        # This is a problem because now the 1:1 correspondence between co-resolved
        # environments (important for figuring out which environment to use) is broken
        #
        # To solve this problem, we consider that co-resolved environments participate
        # in the computation of the full_id (basically a concatenation of all packages
        # across all co-resolved environments). This maintains the 1:1 correspondence.
        # It has a side benefit that we can use that same full_id for all co-resolved
        # environment making one easier to find from the other (instead of using indirect
        # links)
        co_resolved_envs = (
            {}
        )  # type: Dict[str, List[Tuple[EnvID, ResolvedEnvironment]]]
        if len(env_ids):
            with ThreadPoolExecutor() as executor:
                resolution_result = [
                    executor.submit(_resolve, v, builders_by_req_id)
                    for k, v in self._requested_envs.items()
                    if k in env_ids
                ]
                for f in as_completed(resolution_result):
                    env_id, resolved_env = f.result()
                    co_resolved_envs.setdefault(env_id.req_id, []).append(
                        (env_id, resolved_env)
                    )

            # Now we know all the co-resolved environments so we can compute the full
            # ID for all those environments
            for envs in co_resolved_envs.values():
                if len(envs) > 1:
                    ResolvedEnvironment.set_coresolved_full_id([x[1] for x in envs])

                for orig_env_id, resolved_env in envs:
                    resolved_env_id = resolved_env.env_id
                    cached_resolved_env = self._conda.environment(resolved_env_id)
                    # This checks if there is the same resolved environment already
                    # cached (in which case, we don't have to check a bunch of things
                    # so makes it nicer)
                    if cached_resolved_env:
                        resolved_env = cached_resolved_env
                        self._requested_envs[orig_env_id]["already_resolved"] = True

                    self._requested_envs[orig_env_id]["resolved"] = resolved_env
                    debug.conda_exec(
                        "For environment %s (%s) (deps: %s), need packages %s"
                        % (
                            orig_env_id.req_id,
                            orig_env_id.full_id,
                            ";".join(map(str, resolved_env.deps)),
                            ", ".join([p.filename for p in resolved_env.packages]),
                        )
                    )

        duration = int(time.time() - start)
        echo(" done in %d second%s." % (duration, plural_marker(duration)))
