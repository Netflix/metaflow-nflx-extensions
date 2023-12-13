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
    Type,
)

from metaflow.debug import debug
from metaflow.metaflow_config import (
    CONDA_DEPENDENCY_RESOLVER,
    CONDA_PREFERRED_FORMAT,
    CONDA_MIXED_DEPENDENCY_RESOLVER,
    CONDA_PYPI_DEPENDENCY_RESOLVER,
    CONDA_SYS_DEPENDENCIES,
    CONDA_SYS_DEFAULT_PACKAGES,
)
from metaflow.metaflow_environment import InvalidEnvironmentException

from metaflow._vendor.packaging.version import parse as parse_version
from .env_descr import (
    EnvID,
    EnvType,
    PackageSpecification,
    ResolvedEnvironment,
    env_type_for_deps,
)
from .conda import Conda

from .resolvers import Resolver
from .utils import (
    CondaException,
    channel_or_url,
    arch_id,
    get_builder_envs_dep,
    merge_dep_dicts,
    plural_marker,
    split_into_dict,
    tstr_to_dict,
)


class EnvsResolver(object):
    def __init__(self, conda: Conda):
        # key: EnvID; value: dict containing:
        #  - "id": key
        #  - "steps": steps using this environment
        #  - "user_deps": array of requested dependencies
        #  - "deps": full array of dependencies -- typically user_deps except if base_env
        #  - "sources": full array of sources
        #  - "extras": additional arguments (typically used for PYPI)
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
        user_deps: Dict[str, List[str]],
        user_sources: Dict[str, List[str]],
        extras: Dict[str, List[str]],
        step_name: str = "ad-hoc",
        base_env: Optional[ResolvedEnvironment] = None,
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
        user_deps : SDict[str, List[str]]
            The user dependencies of this environment -- if there is a base environment,
            these are the *additional* dependencies.
        user_sources : Dict[str, List[str]]
            The sources of this environment -- if there is a base environment, these are
            the *additional* sources.
        extras : Dict[str, List[str]]
            Any extra information to pass to Conda/Pypi (currently just pypi) -- if there
            is a base environment, these are the *additional* extras.
        step_name : str
            Step needing this environment or "ad-hoc" if not needed by a step
        base_env : Optional[ResolvedEnvironment], optional
            The base environment, if any, this environment is derived from, by default None
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
                env_type,
                env_id,
                user_sources,
                user_deps,
                deps,
                extras,
            ) = self.extract_info_from_base(
                self._conda, base_env, user_deps, user_sources, extras, architecture
            )
        else:
            env_type = env_type_for_deps(user_deps)
            env_id = EnvID(
                ResolvedEnvironment.get_req_id(user_deps, user_sources, extras),
                "_default",
                architecture,
            )
            deps = user_deps

        # Check if we have the environment resolved already
        resolved_env = None
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

        # Check if we have already requested this environment
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
                "steps": [step_name],
                "user_deps": user_deps,
                "deps": deps,
                "sources": user_sources,
                "extras": extras,
                "conda_format": [CONDA_PREFERRED_FORMAT]
                if CONDA_PREFERRED_FORMAT and CONDA_PREFERRED_FORMAT != "none"
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
            while resolving the main environments. In the case of PYPI environments,
            we sometimes need to build another environment to build PYPI packages
            and/or resolve the PYPI dependencies, by default False

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
            while resolving the main environments. In the case of PYPI environments,
            we sometimes need to build another environment to build PYPI packages
            and/or resolve the PYPI dependencies, by default False

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
            while resolving the main environments. In the case of PYPI environments,
            we sometimes need to build another environment to build PYPI packages
            and/or resolve the PYPI dependencies, by default False

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
            while resolving the main environments. In the case of PYPI environments,
            we sometimes need to build another environment to build PYPI packages
            and/or resolve the PYPI dependencies, by default False

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

        # In PYPI-ONLY scenarios, we actually need a sub-environment to properly resolve
        # the pypi environment (due to https://github.com/pypa/pip/issues/11664).
        # We try to mutualize these environments.
        # There are other cases where we need a builder env but we only build it
        # for user in PYPI-ONLY scenarios because we are not sure we need it in other
        # cases. If we do need it in the other cases, it will be lazily built and returned
        # so we can cache them for later use (they just won't be re-used and cached
        # this time so it is possible that we resolve the same builder environment
        # multiple times). This happens, for example, in mixed mode when we need to build
        # PYPI packages from source.

        # In the case of cross architecture build we need two things:
        #  - a "vanilla" builder environment for this machine's architecture
        #    (we do not need the npconda packages in
        #    that since we won't use them for anything) -- this will be the actual
        #    "builder" environment
        #  - a base conda environment for the target architecture (which includes the
        #    npconda packages). We will use this as the base environment.
        # For simplicity, we call both of them the "builder" environments (the first is
        # an environment with which we build the pypi environment and the second is the
        # building block of the final environment).

        # Figure out the set of builder environments we need to build. The key is the
        # requirement ID for which we are building these builder environments and the
        # value is the envid of the environments built for it.
        builders_by_req_id = {}  # type: Dict[str, List[EnvID]]
        my_arch = arch_id()
        for env_id in env_ids:
            env_type = self._requested_envs[env_id].get("env_type")
            if env_type is None:
                env_type = env_type_for_deps(self._requested_envs[env_id]["deps"])
            if env_type != EnvType.PYPI_ONLY:
                # If this is not a PYPI_ONLY environment -- do not build builder_envs
                continue

            to_resolve_info = self._requested_envs[env_id]

            # Extract just the conda sources
            builder_sources = {"conda": to_resolve_info["sources"].get("conda", [])}

            # Figure out the python dependency
            python_dep = [
                d
                for d in to_resolve_info["user_deps"].get("conda", [])
                if d.startswith("python==")
            ]
            # Make sure we have everything we need in the builder environment
            python_dep.extend(
                get_builder_envs_dep(to_resolve_info["user_deps"].get("conda", []))
            )

            # The user dependencies are everything in npconda, sys and the pythondep
            builder_user_deps = {
                env_id.arch: {
                    "conda": python_dep,
                    "npconda": to_resolve_info["user_deps"].get("npconda", []),
                    "sys": to_resolve_info["user_deps"].get("sys", []),
                }
            }
            if env_id.arch != my_arch:
                # Here we need an extra "vanilla" environment with just the python
                # dependency
                builder_user_deps[my_arch] = {"conda": python_dep}

            for builder_arch, builder_deps in builder_user_deps.items():
                builder_env_id = EnvID(
                    ResolvedEnvironment.get_req_id(builder_deps, builder_sources, {}),
                    "_default",
                    builder_arch,
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
                        "user_deps": builder_deps,
                        "deps": builder_deps,
                        "sources": builder_sources,
                        "extras": {},
                        "conda_format": to_resolve_info["conda_format"],
                        "base": None,
                        "base_accurate": False,
                        "resolved": builder_env,
                        "already_resolved": builder_env is not None,
                        "env_type": EnvType.CONDA_ONLY,
                    }
                self._builder_envs[builder_env_id] = builder_env_info
                builders_by_req_id.setdefault(env_id.req_id, []).append(builder_env_id)

        # Resolve all builder environments first so we can share them across steps
        if len(self._builder_envs):
            debug.conda_exec("Builder environments: %s" % str(self._builder_envs))

            # Build the environments
            with ThreadPoolExecutor() as executor:
                resolution_result = [
                    executor.submit(self._resolve, v, None)
                    for v in self._builder_envs.values()
                    if not v["already_resolved"]
                ]
                for f in as_completed(resolution_result):
                    env_id, resolved, _ = f.result()
                    self._builder_envs[env_id]["resolved"] = resolved

        # Now that we have all the builder environments, we go ahead and resolve the
        # actual environments. We can pass down the builder environments as needed

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
                    executor.submit(self._resolve, v, builders_by_req_id)
                    for k, v in self._requested_envs.items()
                    if k in env_ids
                ]
                for f in as_completed(resolution_result):
                    env_id, resolved_env, addl_builder_envs = f.result()
                    co_resolved_envs.setdefault(env_id.req_id, []).append(
                        (env_id, resolved_env)
                    )

                    # Record any other builder environment that was built so we can
                    # properly cache it later
                    if addl_builder_envs is None:
                        addl_builder_envs = []
                    addl_builder_envs = [
                        env
                        for env in addl_builder_envs
                        if env.env_id not in self._builder_envs
                    ]
                    for addl_builder_env in addl_builder_envs:
                        # We "hack" the extract_from_base to get the proper user and
                        # full requirements from addl_builder_env
                        _, _, _, user_deps, full_deps, _ = self.extract_info_from_base(
                            self._conda,
                            addl_builder_env,
                            {},
                            {},
                            {},
                            addl_builder_env.env_id.arch,
                        )
                        self._builder_envs[addl_builder_env.env_id] = {
                            "id": addl_builder_env.env_id,
                            "steps": self._requested_envs[env_id]["steps"],
                            "user_deps": user_deps,
                            "deps": full_deps,
                            "sources": addl_builder_env.sources,
                            "extras": {},
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
                            ", ".join(
                                sorted([p.filename for p in resolved_env.packages])
                            ),
                        )
                    )

        duration = int(time.time() - start)
        echo(" done in %d second%s." % (duration, plural_marker(duration)))

    def _resolve(
        self,
        env_desc: Mapping[str, Any],
        builder_environments: Optional[Dict[str, List[EnvID]]],
    ) -> Tuple[EnvID, ResolvedEnvironment, Optional[List[ResolvedEnvironment]]]:
        env_id = cast(EnvID, env_desc["id"])
        if builder_environments is None:
            builder_environments = {}

        builder_envs = [
            self._builder_envs[builder_env_id]["resolved"]
            for builder_env_id in builder_environments.get(env_id.req_id, [])
        ]

        # Figure out the env_type
        env_type = cast(
            EnvType, env_desc.get("env_type") or env_type_for_deps(env_desc["deps"])
        )

        # Create the resolver object
        resolver = self.get_resolver(env_type)(self._conda)

        # Resolve the environment
        if env_type == EnvType.PYPI_ONLY:
            # Pypi only mode
            # In this mode, we also allow (as a workaround for poor support for
            # more advanced options in conda-lock (like git repo, local support,
            # etc)) the inclusion of conda packages that are *not* python packages.
            # To ensure this, we check the npconda packages, create an environment
            # for it and check if that environment doesn't contain python deps.
            # If that is the case, we then create the actual environment including
            # both conda and npconda packages and re-resolve. We could maybe
            # optimize to not resolve from scratch twice but given this is a rare
            # situation and the cost is only during resolution, it doesn't seem
            # worth it.
            npconda_deps = env_desc["deps"].get("npconda", [])
            if npconda_deps:
                npcondaenv, _ = self.get_resolver(EnvType.CONDA_ONLY)(
                    self._conda
                ).resolve(
                    EnvType.CONDA_ONLY,
                    {"npconda": npconda_deps},
                    env_desc["sources"],
                    {},
                    env_id.arch,
                )
                if any((p.filename.startswith("python-") for p in npcondaenv.packages)):
                    raise InvalidEnvironmentException(
                        "Cannot specify a non-python Conda dependency that uses "
                        "python: %s. Please use the mixed mode instead."
                        % ", ".join([d.value for d in npconda_deps])
                    )
        resolved_env, builder_envs = resolver.resolve(
            env_type,
            env_desc["deps"],
            env_desc["sources"],
            env_desc["extras"],
            env_id.arch,
            builder_envs,
            env_desc["base"],
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
        return env_id, resolved_env, builder_envs

    @staticmethod
    def extract_info_from_base(
        conda: Conda,
        base_env: ResolvedEnvironment,
        deps: Dict[str, List[str]],
        sources: Dict[str, List[str]],
        extras: Dict[str, List[str]],
        architecture: str,
    ) -> Tuple[
        EnvType,
        EnvID,
        Dict[str, List[str]],
        Dict[str, List[str]],
        Dict[str, List[str]],
        Dict[str, List[str]],
    ]:
        """
        Returns the information about an environment extended from a given environment
        (in other words, taking the base environment first and then extending it
        with other dependencies).

        Parameters
        ----------
        base_env : ResolvedEnvironment
            The base environment to use
        deps : Dict[str, List[str]]
            The user dependencies to add/update
        sources : Dict[str, List[str]]
            The sources to add/update
        extras : Dict[str, List[str]]
            Additional information passed to Conda/Pypi
        architecture : str
            The architecture to resolve for

        Returns
        -------
        Tuple[EnvType, EnvID, Dict[str, List[str]], Dict[str, List[str]],Dict[str, List[str]], Dict[str, List[str]]]
            The tuple contains:
                - the EnvType
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
        # incoming_* is basically stuff that comes from everything outside of the
        # base_env
        incoming_conda_deps = split_into_dict(deps.get("conda", []))
        incoming_npconda_deps = split_into_dict(deps.get("npconda", []))
        incoming_pypi_deps = split_into_dict(deps.get("pypi", []))
        incoming_sys_deps = split_into_dict(deps.get("sys", []))

        base_deps = tstr_to_dict(base_env.deps)
        base_sources = tstr_to_dict(base_env.sources)
        base_extras = tstr_to_dict(base_env.extras)

        user_conda_deps = merge_dep_dicts(
            split_into_dict(base_deps.get("conda", [])),
            incoming_conda_deps,
        )
        user_pypi_deps = merge_dep_dicts(
            split_into_dict(base_deps.get("pypi", [])),
            incoming_pypi_deps,
        )
        user_npconda_deps = merge_dep_dicts(
            split_into_dict(base_deps.get("npconda", [])),
            incoming_npconda_deps,
        )

        # Special handling of sys deps -- it specifies things like __cuda or __glibc.
        # In both cases, it specifies the maximum allowed version so we take the
        # smallest.

        base_sys_deps = split_into_dict(base_deps.get("sys", []))
        d = set(base_sys_deps.keys()).difference(CONDA_SYS_DEPENDENCIES)
        if d:
            raise CondaException("Unhandled sys deps: %s" % ", ".join(d))
        d = set(incoming_sys_deps.keys()).difference(CONDA_SYS_DEPENDENCIES)
        if d:
            raise CondaException("Unhandled sys deps: %s" % ", ".join(d))

        user_sys_deps = cast(
            Dict[str, str], CONDA_SYS_DEFAULT_PACKAGES.get(architecture, {})
        )
        debug.conda_exec(
            "Default system deps are: %s; base env: %s; incoming: %s"
            % (user_sys_deps, base_sys_deps, incoming_sys_deps)
        )
        if base_sys_deps or incoming_sys_deps:
            for d in CONDA_SYS_DEPENDENCIES:
                d = cast(str, d)
                v1 = base_sys_deps.get(d, "9999")
                v2 = incoming_sys_deps.get(d, "9999")
                if "=" in v1:
                    v1, v1_build = v1.split("=", 1)
                else:
                    v1_build = None
                if "=" in v2:
                    v2, v2_build = v2.split("=", 1)
                else:
                    v2_build = None
                v_min, v_min_build = v1, v1_build
                if parse_version(v2) < parse_version(v1):
                    v_min, v_min_build = v2, v2_build
                if v_min != "9999":
                    user_sys_deps[d] = (
                        "%s=%s" % (v_min, v_min_build) if v_min_build else v_min
                    )

        for category, extra in base_extras.items():
            extras.setdefault(category, []).extend(extra)

        conda_deps = {
            p.package_name_with_channel(): p.package_version
            for p in base_env.packages
            if p.TYPE == "conda"
        }

        conda_deps = merge_dep_dicts(
            merge_dep_dicts(conda_deps, incoming_conda_deps), incoming_npconda_deps
        )

        pypi_deps = {
            p.package_name: p.package_version
            for p in base_env.packages
            if p.TYPE == "pypi"
        }
        pypi_deps = merge_dep_dicts(pypi_deps, incoming_pypi_deps)

        # The dependencies we need to resolve for are:
        #  - the packages from the base environment
        #  - the packages the user requested
        deps = {
            "conda": ["%s==%s" % (p, v) if v else p for p, v in conda_deps.items()],
            "npconda": [
                "%s==%s" % (k, v) if v else k for k, v in user_npconda_deps.items()
            ],
            "pypi": ["%s==%s" % (p, v) if v else p for p, v in pypi_deps.items()],
            "sys": ["%s==%s" % (k, v) if v else k for k, v in user_sys_deps.items()],
        }

        # The user requested dependencies are the ones that were requested for the
        # base environment as well as the ones that were added here
        user_deps = {
            "conda": [
                "%s==%s" % (k, v) if v else k for k, v in user_conda_deps.items()
            ],
            "npconda": list(deps["npconda"]),
            "pypi": ["%s==%s" % (k, v) if v else k for k, v in user_pypi_deps.items()],
            "sys": list(deps["sys"]),
        }

        if base_env.env_type == EnvType.PYPI_ONLY and len(incoming_conda_deps) == 0:
            env_type = EnvType.PYPI_ONLY
        else:
            env_type = env_type_for_deps(deps)

        # Sources will be:
        #   - for Conda: base_sources, then user sources then default sources
        #   - for Pypi: default sources then base_sources then user sources

        new_sources = {
            "conda": list(
                dict.fromkeys(
                    map(
                        channel_or_url,
                        chain(
                            base_sources.get("conda", []),
                            sources.get("conda", []),
                            conda.default_conda_channels,
                        ),
                    )
                )
            )
        }

        if env_type != EnvType.CONDA_ONLY:
            new_sources["pypi"] = list(
                dict.fromkeys(
                    chain(
                        conda.default_pypi_sources,
                        base_sources.get("pypi", []),
                        sources.get("pypi", []),
                    )
                )
            )

        new_env_id = EnvID(
            ResolvedEnvironment.get_req_id(user_deps, new_sources, extras),
            "_default",
            architecture,
        )

        return env_type, new_env_id, new_sources, user_deps, deps, extras

    @staticmethod
    def get_resolver(env_type: EnvType) -> Type[Resolver]:
        if env_type == EnvType.CONDA_ONLY:
            resolver_name = CONDA_DEPENDENCY_RESOLVER
        elif env_type == EnvType.PYPI_ONLY:
            resolver_name = CONDA_PYPI_DEPENDENCY_RESOLVER
        elif env_type == EnvType.MIXED:
            resolver_name = CONDA_MIXED_DEPENDENCY_RESOLVER
        else:
            raise CondaException("Unhandled environment type %s" % env_type.value)
        if resolver_name is None or resolver_name == "none":
            raise CondaException(
                "Cannot resolve environments in %s mode because no resolver is configured"
                % env_type.value
            )
        return Resolver.get_resolver(resolver_name)
