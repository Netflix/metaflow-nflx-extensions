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

from .env_descr import EnvID, ResolvedEnvironment
from .conda import Conda
from .conda_step_decorator import CondaStepDecorator
from .env_descr import TStr
from .utils import AliasType, plural_marker, resolve_env_alias


class EnvsResolver(object):
    def __init__(self, conda: Conda):
        # key: EnvID; value: dict containing:
        #  - "id": key
        #  - "steps": steps using this environment
        #  - "arch": architecture of the environment
        #  - "deps": array of requested dependencies
        #  - "channels": additional channels to search
        #  - "resolved": ResolvedEnvironment or None
        #  - "already_resolved": T/F
        self._requested_envs = {}  # type: Dict[EnvID, Dict[str, Any]]
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
        base_from_full_id: bool = False,
        local_only: bool = False,
        force: bool = False,
        force_co_resolve: bool = False,
    ):
        self._non_step_envs = True
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
        self,
    ) -> Iterator[Tuple[EnvID, Optional[ResolvedEnvironment], List[str]]]:
        for env_id, req in self._requested_envs.items():
            yield env_id, cast(Optional[ResolvedEnvironment], req["resolved"]), cast(
                List[str], req["steps"]
            )

    def resolved_environments(
        self,
    ) -> Iterator[Tuple[EnvID, ResolvedEnvironment, List[str]]]:
        for env_id, req in self._requested_envs.items():
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
        self,
    ) -> Iterator[Tuple[EnvID, ResolvedEnvironment, List[str], List[str]]]:
        for env_id, req in self._requested_envs.items():
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
        self,
    ) -> Iterator[Tuple[EnvID, ResolvedEnvironment, List[str]]]:
        for env_id, req in self._requested_envs.items():
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

        def _resolve(env_desc: Mapping[str, Any]) -> Tuple[EnvID, ResolvedEnvironment]:
            env_id = cast(EnvID, env_desc["id"])
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
                ),
            )

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
                    executor.submit(_resolve, v)
                    for k, v in self._requested_envs.items()
                    if k in env_ids
                ]
                for f in as_completed(resolution_result):
                    env_id, resolved_env = f.result()
                    # This checks if there is the same resolved environment already
                    # cached (in which case, we don't have to check a bunch of things
                    # so makes it nicer)
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
