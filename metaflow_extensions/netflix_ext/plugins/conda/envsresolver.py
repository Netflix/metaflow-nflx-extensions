# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

import time

from itertools import chain
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

from .env_descr import EnvID, EnvType, PackageSpecification, ResolvedEnvironment, TStr
from .conda import Conda
from .conda_step_decorator import CondaStepDecorator
from .utils import (
    AliasType,
    CondaException,
    channel_from_url,
    arch_id,
    merge_dep_dicts,
    plural_marker,
    resolve_env_alias,
    split_into_dict,
)


class EnvsResolver(object):
    def __init__(self, conda: Conda):
        # key: EnvID; value: dict containing:
        #  - "id": key
        #  - "steps": steps using this environment
        #  - "user_deps": array of requested dependencies
        #  - "deps": full array of dependencies -- typically user_deps except if base_env
        #  - "sources": full array of sources
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
        architecture: str,
        user_deps: Sequence[TStr],
        user_sources: Sequence[TStr],
        extras: Sequence[TStr],
        base_env: Optional[ResolvedEnvironment] = None,
        env_type: Optional[EnvType] = None,
        base_from_full_id: bool = False,
        local_only: bool = False,
        force: bool = False,
        force_co_resolve: bool = False,
    ):
        """
        Add an environment to resolve to this EnvsResolver. The EnvsResolver will resolve
        the same environment only once and will mutualize anything it can.


        Parameters
        ----------
        architecture : str
            Architecture this environment is requested for
        user_deps : Sequence[TStr]
            The user dependencies of this environment -- if there is a base environment,
            these are the *additional* dependencies.
        user_sources : Sequence[TStr]
            The sources of this environment -- if there is a base environment, these are
            the *additional* sources.
        extras : Sequence[TStr]
            Any extra information to pass to Conda/Pip (currently just pip) -- if there
            is a base environment, these are the *additional* extras.
        base_env : Optional[ResolvedEnvironment], optional
            The base environment, if any, this environment is derived from, by default None
        env_type : Optional[EnvType], optional
            The environment type -- this is only used if base_env is not None and
            we know what the derived environment should be. Else it is automatically
            determined.
        base_from_full_id : bool, optional
            True if the base environment was extracted using just the full ID of the
            environment which makes it imprecise, by default False
        local_only : bool, optional
            True if we should only look for resolved environments locally and not
            remotely, by default False
        force : bool, optional
            True if we should force resolution even if this environment is known,
            by default False
        force_co_resolve : bool, optional
            True if we should force co-resolution, by default False
        """
        self._non_step_envs = True

        # If there is a base environment, get all the resolved information
        if base_env:
            (
                env_id,
                user_sources,
                user_deps,
                deps,
                extras,
            ) = self.extract_info_from_base(
                self._conda, base_env, user_deps, user_sources, extras, architecture
            )
        else:
            env_id = EnvID(
                ResolvedEnvironment.get_req_id(user_deps, user_sources, extras),
                "_default",
                architecture,
            )
            deps = user_deps

        if (
            not force
            and base_env
            and base_env.env_id.req_id == env_id.req_id
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
                "user_deps": user_deps,
                "deps": deps,
                "sources": user_sources,
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
                "env_type": env_type,
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
        """
        This is the same as `add_environment` except a lot of the information needed
        for `add_environment` is extracted from the step decorator passed in

        Parameters
        ----------
        step_name : str
            Step name for which this environment is being resolved (used for debugging
            messages)
        decorator : CondaStepDecorator
            Decorator to use to extract information from
        force : bool, optional
            True if we should resolve the environment even if we already
            know about it, by default False
        archs : Optional[List[str]], optional
            List of architectures to resolve for. If None, resolves for
            the current architecture, by default None
        """
        # Make sure we use the same conda object to properly cache everything
        decorator.set_conda(self._conda)

        from_env = decorator.from_env
        if from_env:
            (
                env_id,
                user_sources,
                user_deps,
                deps,
                extras,
            ) = self.extract_info_from_base(
                self._conda,
                from_env,
                decorator.non_base_step_deps,
                decorator.source_deps,
                [],
                arch_id(),
            )
            env_ids = [env_id]
        else:
            user_deps = decorator.non_base_step_deps
            user_sources = decorator.source_deps
            deps = decorator.step_deps

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
                and from_env
                and env_id.req_id == from_env.env_id.req_id
                and env_id.arch == from_env.env_id.arch
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
                    "user_deps": user_deps,
                    "deps": deps,
                    "sources": user_sources,
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
                    "env_type": decorator.env_type,
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
        """
        Resolve the environments added using `add_environment` and `add_environment_for_step`.

        Parameters
        ----------
        echo : Callable[..., None]
            Method to use to print things to the console
        """
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
        """
        Returns an iterator over all the environments (resolved or not) this
        EnvsResolver knows about

        Parameters
        ----------
        include_builder_envs : bool, optional
            If True, will also return builder environments that were built
            while resolving the main environments. In the case of PIP environments,
            we sometimes need to build another environment to build PIP packages
            and/or resolve the PIP dependencies, by default False

        Yields
        ------
        Iterator[Tuple[EnvID, Optional[ResolvedEnvironment], List[str]]]
            Each tuple element contains:
                - the environment ID for the environment
                - the ResolvedEnvironment (if it was resolved)
                - the list of step names this environment pertains to.
        """
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
        """
        Same as `all_environments` except for resolved environments only

        Parameters
        ----------
        include_builder_envs : bool, optional
            If True, will also return builder environments that were built
            while resolving the main environments. In the case of PIP environments,
            we sometimes need to build another environment to build PIP packages
            and/or resolve the PIP dependencies, by default False

        Yields
        ------
        Iterator[Tuple[EnvID, ResolvedEnvironment, List[str]]]
            Each tuple element contains:
                - the environment ID for the environment
                - the ResolvedEnvironment
                - the list of step names this environment pertains to.
        """
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
        """
        The opposite of `resolved_environments`. It never includes builder environments
        as those are always resolved

        Yields
        ------
        Iterator[Tuple[EnvID, List[str]]]
            Each tuple element contains:
                - the environment ID for the environment
                - the list of step names this environment pertains to.
        """
        for env_id, req in self._requested_envs.items():
            if req["resolved"] is None:
                yield env_id, cast(List[str], req["steps"])

    def need_caching_environments(
        self, include_builder_envs: bool = False
    ) -> Iterator[Tuple[EnvID, ResolvedEnvironment, List[str], List[str]]]:
        """
        Same as `resolved_environments` but further limits the output to environments
        that need to be cached.

        Parameters
        ----------
        include_builder_envs : bool, optional
            If True, will also return builder environments that were built
            while resolving the main environments. In the case of PIP environments,
            we sometimes need to build another environment to build PIP packages
            and/or resolve the PIP dependencies, by default False

        Yields
        ------
        Iterator[Tuple[EnvID, ResolvedEnvironment, List[str], List[str]]]
            Each tuple element contains:
                - the environment ID for the environment
                - the ResolvedEnvironment
                - the Conda formats that need caching
                - the list of step names this environment pertains to.
        """
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
        """
        Same as `resolved_environments` but further limits the output to environments
        that were newly resolved (ie: excludes ones that were already resolved prior
        to starting this EnvsResolver)

        Parameters
        ----------
        include_builder_envs : bool, optional
            If True, will also return builder environments that were built
            while resolving the main environments. In the case of PIP environments,
            we sometimes need to build another environment to build PIP packages
            and/or resolve the PIP dependencies, by default False

        Yields
        ------
        Iterator[Tuple[EnvID, ResolvedEnvironment, List[str]]]
            Each tuple element contains:
                - the environment ID for the environment
                - the ResolvedEnvironment
                - the list of step names this environment pertains to.
        """
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
        ) -> Tuple[EnvID, ResolvedEnvironment, Optional[ResolvedEnvironment]]:
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
            resolved_env, builder_env = self._conda.resolve(
                env_desc["steps"],
                env_desc["deps"],
                env_desc["sources"],
                env_desc["extras"],
                env_id.arch,
                env_desc.get("env_type"),
                builder_env=builder_env,
                base_env=env_desc["base"],
            )
            if env_desc["base"]:
                # We try to copy things over from the base environment as it contains
                # potential caching information we don't need to rebuild. This also
                # properly sets the user dependencies to what we want as opposed to
                # including everything we resolved for.
                merged_packages = []  # type: List[PackageSpecification]
                base_packages = {
                    p.filename: p.to_dict()
                    for p in cast(ResolvedEnvironment, env_desc["base"]).packages
                }
                for p in resolved_env.packages:
                    existing_info = base_packages.get(p.filename)
                    if existing_info:
                        merged_packages.append(
                            PackageSpecification.from_dict(existing_info)
                        )
                    else:
                        merged_packages.append(p)
                resolved_env = ResolvedEnvironment(
                    env_desc["user_deps"],
                    env_desc["sources"],
                    env_desc["extras"],
                    env_id.arch,
                    all_packages=merged_packages,
                    env_type=resolved_env.env_type,
                    accurate_source=env_desc["base_accurate"],
                )
            return env_id, resolved_env, builder_env

        # In PIP-ONLY scenarios, we actually need a sub-environment to properly resolve
        # the pip environment (due to https://github.com/pypa/pip/issues/11664).
        # We try to mutualize these environments.
        # There are other cases where we need a builder env but we only build it
        # for user in PIP-ONLY scenarios because we are not sure we need it in other
        # cases. If we do need it in the other cases, it will be lazily built and returned
        # so we can cache them for later use (they just won't be re-used and cached
        # this time so it is possible that we resolve the same builder environment
        # multiple times)

        builders_by_req_id = {}  # type: Dict[str, Any]
        my_arch = arch_id()
        for env_id in env_ids:
            env_type = self._requested_envs[env_id].get("env_type")
            if env_type is None:
                env_type = self._conda.env_type_for_deps(
                    self._requested_envs[env_id]["deps"]
                )
            if env_type != EnvType.PIP_ONLY:
                # If this is not a PIP_ONLY environment -- do not build builder_env
                continue

            if env_id.arch != my_arch:
                # If this is not for this architecture, we will either build a vanilla
                # builder env (later) or wait for the environment for this architecture
                # (if it is another env_id in env_ids)
                builders_by_req_id.setdefault(
                    env_id.req_id, self._requested_envs[env_id]
                )
                continue

            # Builder environments only have pip and npconda dependencies

            to_resolve_info = self._requested_envs[env_id]

            builder_user_deps = [
                d for d in to_resolve_info["user_deps"] if d.category == "npconda"
            ] + [
                d
                for d in to_resolve_info["deps"]
                if d.category == "conda" and d.value.startswith("python==")
            ]
            builder_sources = [
                s for s in to_resolve_info["sources"] if s.category == "conda"
            ]

            builder_env_id = EnvID(
                ResolvedEnvironment.get_req_id(builder_user_deps, builder_sources, []),
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
                    "user_deps": builder_user_deps,
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
        for req_id, v in builders_by_req_id.items():
            if not isinstance(v, EnvID):
                base_info = v
                builder_deps = [
                    d for d in base_info["deps"] if d.value.startswith("python==")
                ]
                builder_sources = [
                    s for s in base_info["sources"] if s.category == "conda"
                ]
                builder_env_id = EnvID(
                    ResolvedEnvironment.get_req_id(
                        builder_deps,
                        builder_sources,
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
                        "user_deps": builder_deps,
                        "deps": builder_deps,
                        "sources": builder_sources,
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

        # Resolve all builder enviornments
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
                    env_id, resolved, _ = f.result()
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
                    env_id, resolved_env, addl_builder_env = f.result()
                    co_resolved_envs.setdefault(env_id.req_id, []).append(
                        (env_id, resolved_env)
                    )
                    if (
                        addl_builder_env
                        and addl_builder_env.env_id not in self._builder_envs
                    ):
                        # We "hack" the extract_from_base to get the proper user and
                        # full requirements from addl_builder_env
                        _, _, user_deps, full_deps, _ = self.extract_info_from_base(
                            self._conda,
                            addl_builder_env,
                            [],
                            [],
                            [],
                            addl_builder_env.env_id.arch,
                        )
                        self._builder_envs[addl_builder_env.env_id] = {
                            "id": addl_builder_env.env_id,
                            "steps": self._requested_envs[env_id]["steps"],
                            "user_deps": user_deps,
                            "deps": full_deps,
                            "sources": addl_builder_env.sources,
                            "extras": [],
                            "conda_format": self._requested_envs[env_id][
                                "conda_format"
                            ],
                            "base": None,
                            "base_accurate": False,
                            "resolved": addl_builder_env,
                            "already_resolved": False,
                            "env_type": EnvType.CONDA_ONLY,
                        }

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
                        "For environment %s (%s) %s need packages %s"
                        % (
                            resolved_env_id.req_id,
                            resolved_env_id.full_id,
                            self._requested_envs[orig_env_id],
                            ", ".join([p.filename for p in resolved_env.packages]),
                        )
                    )

        duration = int(time.time() - start)
        echo(" done in %d second%s." % (duration, plural_marker(duration)))

    @staticmethod
    def extract_info_from_base(
        conda: Conda,
        base_env: ResolvedEnvironment,
        deps: Sequence[TStr],
        sources: Sequence[TStr],
        extras: Sequence[TStr],
        architecture: str,
    ) -> Tuple[EnvID, Sequence[TStr], Sequence[TStr], Sequence[TStr], Sequence[TStr]]:
        """
        Returns the information about an environment extended from a given environment
        (in other words, taking the base environment first and then extending it
        with other dependencies).

        Parameters
        ----------
        base_env : ResolvedEnvironment
            The base environment to use
        deps : Sequence[TStr]
            The user dependencies to add/update
        sources : Sequence[TStr]
            The sources to add/update
        extras : Sequence[TStr]
            Additional information passed to Conda/Pip
        architecture : str
            The architecture to resolve for

        Returns
        -------
        Tuple[EnvID, Sequence[TStr], Sequence[TStr], Sequence[TStr], Sequence[TStr]]
            The tuple contains:
                - the new EnvID
                - the set of sources
                - the set of user dependencies
                - the exact set of dependencies
                - the set of extras
        """
        if architecture != base_env.env_id.arch:
            raise CondaException(
                "Mismatched architecture when extending an environment"
            )
        # We form the new list of dependencies based on the ones we have in cur_env
        # and the new ones.
        # We need to split things up and reform

        incoming_conda_deps = split_into_dict(
            [d for d in deps if d.category == "conda"]
        )
        incoming_npconda_deps = split_into_dict(
            [d for d in deps if d.category == "npconda"]
        )
        incoming_pip_deps = split_into_dict([d for d in deps if d.category == "pip"])

        user_conda_deps = merge_dep_dicts(
            split_into_dict([d for d in base_env.deps if d.category == "conda"]),
            incoming_conda_deps,
        )
        user_pip_deps = merge_dep_dicts(
            split_into_dict([d for d in base_env.deps if d.category == "pip"]),
            incoming_pip_deps,
        )
        user_npconda_deps = merge_dep_dicts(
            split_into_dict([d for d in base_env.deps if d.category == "npconda"]),
            incoming_npconda_deps,
        )

        # Extract the implicit channels that are already listed somewhere
        sources = list(set(chain(base_env.sources, sources)))
        extras = list(set(chain(base_env.extras, extras)))

        included_channels = set()  # type: Set[str]
        for s in chain(sources, conda.default_conda_channels):
            channel = channel_from_url(s.value if isinstance(s, TStr) else s)
            if channel:
                included_channels.add(channel)

        included_channels_list = list(included_channels)
        conda_deps = {
            p.package_name_with_channel(included_channels_list): p.package_version
            for p in base_env.packages
            if p.TYPE == "conda"
        }
        conda_deps = merge_dep_dicts(conda_deps, incoming_conda_deps)
        conda_deps = merge_dep_dicts(conda_deps, incoming_npconda_deps)

        pip_deps = {
            p.package_name: p.package_version
            for p in base_env.packages
            if p.TYPE == "pip"
        }
        pip_deps = merge_dep_dicts(pip_deps, incoming_pip_deps)

        deps = list(
            chain(
                [
                    TStr("conda", "%s==%s" % (k, v) if v else k)
                    for k, v in conda_deps.items()
                ],
                [
                    TStr("pip", "%s==%s" % (k, v) if v else k)
                    for k, v in pip_deps.items()
                ],
            )
        )

        user_deps = list(
            chain(
                [
                    TStr("conda", "%s==%s" % (k, v) if v else k)
                    for k, v in user_conda_deps.items()
                ],
                [
                    TStr("npconda", "%s==%s" % (k, v) if v else k)
                    for k, v in user_npconda_deps.items()
                ],
                [
                    TStr("pip", "%s==%s" % (k, v) if v else k)
                    for k, v in user_pip_deps.items()
                ],
            )
        )
        new_env_id = EnvID(
            ResolvedEnvironment.get_req_id(user_deps, sources, extras),
            "_default",
            architecture,
        )

        return new_env_id, sources, user_deps, deps, extras
