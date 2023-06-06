# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

import errno
import json
import os
import requests
import shutil
import socket
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from datetime import datetime
from distutils.version import LooseVersion
from itertools import chain, product
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)
from shutil import which
from urllib.parse import urlparse, unquote

from metaflow.plugins.datastores.local_storage import LocalStorage
from metaflow.datastore.datastore_storage import DataStoreStorage

from metaflow.debug import debug
from metaflow.exception import MetaflowException, MetaflowNotFound
from metaflow.metaflow_config import (
    CONDA_DEPENDENCY_RESOLVER,
    CONDA_PIP_DEPENDENCY_RESOLVER,
    CONDA_MIXED_DEPENDENCY_RESOLVER,
    CONDA_LOCAL_DIST_DIRNAME,
    CONDA_LOCAL_DIST,
    CONDA_LOCAL_PATH,
    CONDA_LOCK_TIMEOUT,
    CONDA_PACKAGES_DIRNAME,
    CONDA_ENVS_DIRNAME,
    CONDA_PREFERRED_FORMAT,
    CONDA_REMOTE_INSTALLER,
    CONDA_REMOTE_INSTALLER_DIRNAME,
    CONDA_DEFAULT_PIP_SOURCE,
    CONDA_USE_REMOTE_LATEST,
)
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow.util import get_username

from metaflow_extensions.netflix_ext.vendor.packaging.requirements import Requirement
from metaflow_extensions.netflix_ext.vendor.packaging.tags import Tag
from metaflow_extensions.netflix_ext.vendor.packaging.utils import parse_wheel_filename


from .utils import (
    CONDA_FORMATS,
    AliasType,
    CondaException,
    CondaStepException,
    arch_id,
    change_pip_package_version,
    correct_splitext,
    get_conda_root,
    is_alias_mutable,
    merge_dep_dicts,
    parse_explicit_url_conda,
    parse_explicit_url_pip,
    parse_explicit_path_pip,
    pip_tags_from_arch,
    plural_marker,
    resolve_env_alias,
    split_into_dict,
)

from .env_descr import (
    CondaCachePackage,
    CondaPackageSpecification,
    EnvID,
    EnvType,
    PackageSpecification,
    PipCachePackage,
    PipPackageSpecification,
    ResolvedEnvironment,
    TStr,
    read_conda_manifest,
    write_to_conda_manifest,
)

from .conda_lock_micromamba_server import glue_script

_CONDA_DEP_RESOLVERS = ("conda", "mamba", "micromamba")

_FAKE_WHEEL = "_fake-1.0-py3-none-any.whl"
_DEV_TRANS = str.maketrans("abcdef", "123456")
ParseURLResult = NamedTuple(
    "ParseURLResult",
    [("filename", str), ("format", str), ("hash", str)],
)


class Conda(object):
    _cached_info = None

    def __init__(
        self, echo: Callable[..., None], datastore_type: str, mode: str = "local"
    ):
        from metaflow.cli import logger, echo_dev_null

        if id(echo) != id(logger):

            def _modified_logger(*args: Any, **kwargs: Any):
                if "timestamp" in kwargs:
                    del kwargs["timestamp"]
                echo(*args, **kwargs)

            self._echo = _modified_logger
        else:
            self._echo = echo

        self._no_echo = echo_dev_null

        self._datastore_type = datastore_type
        self._mode = mode
        self._bins = None  # type: Optional[Dict[str, Optional[str]]]
        self._conda_executable_type = None  # type: Optional[str]

        # For remote mode, make sure we are not going to try to check too much
        # We don't need much at all in remote mode.
        if self._mode == "local":
            self._resolvers = {
                EnvType.PIP_ONLY: CONDA_PIP_DEPENDENCY_RESOLVER,
                EnvType.MIXED: CONDA_MIXED_DEPENDENCY_RESOLVER,
                EnvType.CONDA_ONLY: CONDA_DEPENDENCY_RESOLVER,
            }  # type: Dict[EnvType, Optional[str]]
        else:
            self._resolvers = {
                EnvType.PIP_ONLY: None,
                EnvType.MIXED: None,
                EnvType.CONDA_ONLY: "micromamba",
            }  # type: Dict[EnvType, Optional[str]]

        self._have_micromamba_server = False  # type: bool
        self._micromamba_server_port = None  # type: Optional[int]
        self._micromamba_server_process = (
            None
        )  # type: Optional[subprocess.Popen[bytes]]

        self._found_binaries = False  # We delay resolving binaries until we need them
        # because in the remote case, in conda_environment.py
        # we create this object but don't use it.

        # Figure out what environments we know about locally
        self._local_root = LocalStorage.get_datastore_root_from_config(
            echo
        )  # type: str
        self._cached_environment = read_conda_manifest(self._local_root)

        # Initialize storage
        if self._datastore_type != "local":
            # Prevent circular dep
            from metaflow.plugins import DATASTORES

            # We will be able to cache things -- currently no caching for local
            storage_impl = [d for d in DATASTORES if d.TYPE == self._datastore_type][0]
            self._storage = storage_impl(
                get_conda_root(self._datastore_type)
            )  # type: Optional[DataStoreStorage]
        else:
            self._storage = None

    def __del__(self):
        if self._micromamba_server_process:
            self._micromamba_server_process.kill()
        if not debug.conda and self._bins:
            server_bin = self._bins.get("micromamba_server")
            if server_bin:
                os.unlink(server_bin)

    def binary(self, binary: str) -> Optional[str]:
        if not self._found_binaries:
            self._find_conda_binary()
        if self._bins:
            return self._bins.get(binary)
        return None

    @staticmethod
    def env_type_for_deps(deps: Sequence[TStr]) -> EnvType:
        """
        Returns the environment type based on a set of dependencies

        Parameters
        ----------
        deps : Sequence[TStr]
            User-requested dependencies for this environment

        Returns
        -------
        EnvType
            The environment type, either CONDA_ONLY, PIP_ONLY or MIXED
        """
        conda_packages = [d for d in deps if d.category == "conda"]
        pip_packages = [d for d in deps if d.category == "pip"]
        env_type = EnvType.CONDA_ONLY
        if len(conda_packages) == 1:
            # This is a pip only mode
            env_type = EnvType.PIP_ONLY
        elif len(pip_packages) > 0:
            env_type = EnvType.MIXED
        return env_type

    def resolve(
        self,
        using_steps: Sequence[str],
        deps: Sequence[TStr],
        sources: Sequence[TStr],
        extras: Sequence[TStr],
        architecture: str,
        env_type: Optional[EnvType] = None,
        builder_env: Optional[ResolvedEnvironment] = None,
        base_env: Optional[ResolvedEnvironment] = None,
    ) -> Tuple[ResolvedEnvironment, Optional[ResolvedEnvironment]]:
        """
        Resolve an environment. This only resolves the environment and does not
        actually download any package (except as required by the resolver) nor does
        it create it.

        Parameters
        ----------
        using_steps : Sequence[str]
            Steps that this environment is being resolved from (for printing purposes only)
        deps : Sequence[TStr]
            User dependencies for this environment
        sources : Sequence[TStr]
            Sources for this environment
        extras : Sequence[TStr]
            Additional information passed to Conda/Pip
        architecture : str
            Architecture to resolve for
        env_type : Optional[EnvType], optional
            If specified, forces a specific type of environment resolution, by default None
        builder_env : Optional[ResolvedEnvironment], optional
            An environment used as a builder environment (used in PIP cases), by default None
        base_env : Optional[ResolvedEnvironment], optional
            Needs to sometimes be used to determine if we can fully resolve this.

        Returns
        -------
        Tuple[ResolvedEnvironment, Optional[ResolvedEnvironment]]
            Each element of the tuple contains:
                - the resolved environment
                - the builder environment: either the one passed in or one that was
                  lazily created.
        """
        returned_builder_env = builder_env
        if self._mode != "local":
            # TODO: Maybe relax this later but for now assume that the remote environment
            # is a "lighter" conda.
            raise CondaException("Cannot resolve environments in a remote environment")

        if not self._found_binaries:
            self._find_conda_binary()

        # We determine the resolver method based on what we have to resolve. There are
        # three cases:
        #  - only conda packages (conda-only resolver)
        #  - mix of conda packages and pip packages (mixed resolver)
        #  - a single conda package (the python version) and all pip packages
        #    (conda-only for the python version and pip-only for the rest)
        if env_type is None:
            env_type = self.env_type_for_deps(deps)
        debug.conda_exec("Environment is of type %s" % env_type.value)
        # We now check we have a resolver
        resolver_bin = self._resolvers[env_type]
        if resolver_bin is None:
            raise InvalidEnvironmentException(
                "Cannot resolve environments in %s mode because no resolver is configured"
                % env_type.value
            )
        if self._bins and self._bins.get(resolver_bin) is None:
            raise InvalidEnvironmentException(
                "Resolver '%s' for %s mode is not installed"
                % (resolver_bin, env_type.value)
            )

        if extras and env_type != EnvType.PIP_ONLY:
            raise InvalidEnvironmentException(
                "Passing extra arguments is only supported for pip-only environments, "
                "found '%s' but environment type is %s" % (str(extras), env_type.value)
            )
        try:
            if env_type == EnvType.CONDA_ONLY:
                packages, returned_builder_env = self._resolve_env_with_conda(
                    deps, sources, architecture, builder_env, base_env
                )
            elif env_type == EnvType.MIXED:
                if resolver_bin == "conda-lock":
                    packages, returned_builder_env = self._resolve_env_with_conda_lock(
                        deps, sources, architecture, builder_env, base_env
                    )
                else:
                    # Should never happen and be caught earlier but being clean
                    # add other mixed resolvers here if needed
                    raise InvalidEnvironmentException(
                        "Resolver '%s' is not supported" % resolver_bin
                    )
            else:
                # Pip only mode
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
                npconda_deps = [d for d in deps if d.category == "npconda"]
                if npconda_deps:
                    npconda_pkgs, _ = self._resolve_env_with_conda(
                        npconda_deps, sources, architecture, None, None
                    )
                    if any((p.filename.startswith("python-") for p in npconda_pkgs)):
                        raise InvalidEnvironmentException(
                            "Cannot specify a non-python Conda dependency that uses "
                            "python: %s. Please use the mixed mode instead."
                            % ", ".join([d.value for d in npconda_deps])
                        )
                if resolver_bin == "pip":
                    packages, returned_builder_env = self._resolve_env_with_pip(
                        deps, sources, extras, architecture, builder_env, base_env
                    )
                    assert returned_builder_env
                    packages += list(returned_builder_env.packages)
                else:
                    # Should also never happen and be caught earlier but being clean
                    # add other pip resolvers here if needed
                    raise InvalidEnvironmentException(
                        "Resolver '%s' is not supported" % resolver_bin
                    )

            return (
                ResolvedEnvironment(
                    deps,
                    sources,
                    extras,
                    architecture,
                    all_packages=packages,
                    env_type=env_type,
                ),
                returned_builder_env,
            )
        except CondaException as e:
            raise CondaStepException(e, using_steps)

    def create_for_step(
        self,
        step_name: str,
        env: ResolvedEnvironment,
        do_symlink: bool = False,
    ) -> None:
        """
        Creates a local instance of the resolved environment

        Parameters
        ----------
        step_name : str
            The step name this environment is being created for
        env : ResolvedEnvironment
            The resolved environment to create an instance of
        do_symlink : bool, optional
            If True, creates a `__conda_python` symlink in the current directory
            pointing to the created Conda Python executable, by default False
        """

        if not self._found_binaries:
            self._find_conda_binary()

        try:
            env_name = self._env_directory_from_envid(env.env_id)
            return self.create_for_name(env_name, env, do_symlink)
        except CondaException as e:
            raise CondaStepException(e, [step_name])

    def create_for_name(
        self, name: str, env: ResolvedEnvironment, do_symlink: bool = False
    ) -> None:
        """
        Creates a local instance of the resolved environment

        Parameters
        ----------
        name : str
            The name of the environment to create
        env : ResolvedEnvironment
            The resolved environment to create an instance of
        do_symlink : bool, optional
            If True, creates a `__conda_python` symlink in the current directory
            pointing to the created Conda Python executable, by default False
        """

        if not self._found_binaries:
            self._find_conda_binary()

        # We lock two things here:
        #   - the directory in which we are going to create the environment so
        #     that we don't create the same named environment twice
        #   - the directories we are going to fetch/expand files from/to to prevent
        #     the packages themselves from being accessed concurrently.
        #
        # This is not great but doing so prevents errors when multiple runs run in
        # parallel. In a typical use-case, the locks are non-contended and it should
        # be very fast.
        with CondaLock(self._echo, self._env_lock_file(name)):
            with CondaLockMultiDir(
                self._echo, self._package_dirs, self._package_dir_lockfile_name
            ):
                self._create(env, name)
        if do_symlink:
            python_path = self.python(name)
            if python_path:
                os.symlink(python_path, os.path.join(os.getcwd(), "__conda_python"))

    def remove_for_step(self, step_name: str, env_id: EnvID) -> None:
        """
        Removes a locally created environment.

        This can only remove environments created by Metaflow

        Parameters
        ----------
        step_name : str
            The step for which this environment is being removed
        env_id : EnvID
            The environment we are removing
        """
        if not self._found_binaries:
            self._find_conda_binary()

        try:
            env_name = self._env_directory_from_envid(env_id)
            return self.remove_for_name(env_name)

        except CondaException as e:
            raise CondaStepException(e, [step_name])

    def remove_for_name(self, name: str) -> None:
        """
        Removes a locally created environment.

        Parameters
        ----------
        name : str
            The name of the Conda environment being removed
        """
        if not self._found_binaries:
            self._find_conda_binary()
        with CondaLock(self._echo, self._env_lock_file(name)):
            self._remove(name)

    def python(self, env_desc: Union[EnvID, str]) -> Optional[str]:
        """
        Returns the path to the python interpreter for the given environment.

        Note that this does not create environments and will only return a non None
        value if the environment is already locally created. Note also that this only
        looks at environments created by Metaflow.

        Parameters
        ----------
        env_desc : Union[EnvID, str]
            Environment to get the path of. This is either an environment ID or a
            name of the Conda environment

        Returns
        -------
        Optional[str]
            The path to the python binary if the environment exists locally
        """
        # Get Python interpreter for the conda environment
        if not self._found_binaries:
            self._find_conda_binary()
        env_path = None
        if isinstance(env_desc, EnvID):
            env_path = self.created_environment(env_desc)
            if env_path:
                env_path = env_path[1]
        else:
            env_paths = self._created_envs(env_desc, full_match=True)
            if len(env_paths) == 1:
                env_path = list(env_paths.values())[0]
                if len(env_path) == 1:
                    env_path = env_path[0]
                else:
                    env_path = None  # We don't do ambiguous
        if env_path:
            return os.path.join(env_path, "bin/python")
        return None

    def created_environment(
        self, env_desc: Union[EnvID, str]
    ) -> Optional[Tuple[EnvID, str]]:
        """
        Returns a locally created environment matching the passed in environment
        description.

        Note that this does not create the environment but instead returns it if it
        exists. Note also that this only returns environments created by Metaflow.

        Parameters
        ----------
        env_desc : Union[EnvID, str]
            Either an EnvID or the name of the environment to look for.

        Returns
        -------
        Optional[Tuple[EnvID, str]]
            The environment ID for the environment as well as the path to the environment
        """
        if not self._found_binaries:
            self._find_conda_binary()

        if isinstance(env_desc, EnvID):
            prefix = "metaflow_%s_%s" % (env_desc.req_id, env_desc.full_id)
        else:
            prefix = env_desc
        envs = self._created_envs(prefix, full_match=True)
        if len(envs) == 1:
            return [(k, v[0]) for k, v in envs.items()][0]
        return None

    def created_environments(
        self, req_id: Optional[str] = None
    ) -> Dict[EnvID, List[str]]:
        """
        This is similar to `created_environment` but returns *ALL* Metaflow created
        environments that satisfy the user environment requirements. This will include
        the various resolved environments (if they exist) as well as environments
        that may have been created by the `metaflow environment` command.

        Parameters
        ----------
        req_id : Optional[str], optional
            Requirement ID that is being searched for. If None, returns all
            environments created by Metaflow, by default None

        Returns
        -------
        Dict[EnvID, List[str]]
            The key is the environment ID for the environment and the value is a list
            of paths to all the environments for that environment ID.
        """
        # List all existing metaflow environments; this can include environments that
        # were created with the `environment` command and therefore have a different
        # name
        if not self._found_binaries:
            self._find_conda_binary()
        prefix = "metaflow_%s_" % req_id if req_id else ""
        return {
            k: v
            for k, v in self._created_envs(prefix).items()
            if k.req_id != "_invalid"
        }

    def environment(
        self, env_id: EnvID, local_only: bool = False
    ) -> Optional[ResolvedEnvironment]:
        """
        Returns the resolved environment for a given environment ID.

        Note that this does not return the instance of the environment (use
        a variant of `created_environment` for that) but instead returns if we know
        how to resolve the environment.

        Parameters
        ----------
        env_id : EnvID
            Environment ID we are looking for
        local_only : bool, optional
            If True, does not look in the remote cache for resolved environments,
            by default False

        Returns
        -------
        Optional[ResolvedEnvironment]
            The ResolvedEnvironment corresponding to the input EnvID
        """
        # First look if we have from_env_id locally
        env = self._cached_environment.env_for(*env_id)

        debug.conda_exec("%s%sfound locally" % (str(env_id), " " if env else " not "))
        if env:
            return env

        # We never have a "_default" remotely so save time and don't go try to
        # look for one UNLESS the user configured to use the latest as the default
        if not local_only and self._storage:
            if env_id.full_id != "_default":
                env = self._remote_env_fetch([env_id])
                env = env[0] if env else None
            elif CONDA_USE_REMOTE_LATEST != ":none:":
                # Here we fetch all the environments first and sort them so most
                # recent (biggest date) is first
                all_environments = self.environments(env_id.req_id, env_id.arch)
                current_user = get_username()
                if CONDA_USE_REMOTE_LATEST == ":username:":
                    current_user = cast(str, get_username())
                    filter_func = lambda x: x[1].resolved_by == current_user
                elif CONDA_USE_REMOTE_LATEST == ":any:":
                    filter_func = lambda x: True
                else:
                    allowed_usernames = list(
                        map(
                            lambda x: x.strip(),
                            cast(str, CONDA_USE_REMOTE_LATEST).split(","),
                        )
                    )
                    filter_func = lambda x: x in allowed_usernames

                env = list(
                    filter(
                        filter_func,
                        sorted(
                            all_environments,
                            key=lambda x: x[1].resolved_on,
                            reverse=True,
                        ),
                    )
                )
                env = env[0][1] if env else None
                if env:
                    debug.conda_exec(
                        "%s found as latest remotely: %s"
                        % (str(env_id), str(env.env_id))
                    )
        return env

    def environments(
        self, req_id: str, arch: Optional[str] = None, local_only: bool = False
    ) -> List[Tuple[EnvID, ResolvedEnvironment]]:
        """
        Similar to `environment` but looks for all resolutions for a given user
        requirement ID (as opposed to a full environment ID)

        Parameters
        ----------
        req_id : str
            The requirement ID to look for
        arch : Optional[str], optional
            The architecture to look for. If None, defaults to the current architecture,
            by default None
        local_only : bool, optional
            If True, does not look in the remote cache for resolved environments,
            by default False

        Returns
        -------
        List[Tuple[EnvID, ResolvedEnvironment]]
            A list of ResolvedEnvironments found
        """
        arch = arch or arch_id()
        if not local_only and self._storage is not None:
            # If we are looking for remote stuff, we fetch all the full IDs the remote
            # side knows about and get all those in our cache first and then return
            # what we have
            base_path = self.get_datastore_path_to_env(EnvID(req_id, "_unknown", arch))
            base_path = "/".join(base_path.split("/")[:-2])
            full_ids = [
                os.path.basename(path.rstrip("/"))
                for path, is_file in self._storage.list_content([base_path])
                if not is_file
            ]  # type: List[str]
            self._remote_env_fetch(
                [EnvID(req_id=req_id, full_id=x, arch=arch) for x in full_ids]
            )

        return list(self._cached_environment.envs_for(req_id, arch))

    def env_id_from_alias(
        self, env_alias: str, arch: Optional[str] = None, local_only: bool = False
    ) -> Optional[EnvID]:
        """
        Resolves an environment alias and returns the environment ID for it, if the
        alias exists for that architecture

        Parameters
        ----------
        env_alias : str
            Alias for the environment
        arch : Optional[str], optional
            Architecture to look for. If None, defaults to the current one, by default None
        local_only : bool, optional
            If True, do not look at the remote environment cache, by default False

        Returns
        -------
        Optional[EnvID]
            If found, returns the environment ID corresponding to the alias
        """
        arch = arch or arch_id()

        alias_type, resolved_alias = resolve_env_alias(env_alias)
        if alias_type == AliasType.REQ_FULL_ID:
            req_id, full_id = env_alias.split(":", 1)
            return EnvID(req_id=req_id, full_id=full_id, arch=arch)

        if alias_type == AliasType.PATHSPEC:
            # Late import to prevent cycles
            from metaflow.client.core import Step

            env_id = None
            try:
                s = Step(resolved_alias, _namespace_check=False)
                req_id, full_id, _ = json.loads(
                    s.task.metadata_dict.get("conda_env_id", '["", "", ""]')
                )
                if len(req_id) != 0:
                    env_id = EnvID(req_id=req_id, full_id=full_id, arch=arch)
            except MetaflowNotFound as e:
                raise MetaflowNotFound(
                    "Cannot locate step while looking for Conda environment: %s"
                    % e.message
                )
            if not env_id:
                raise CondaException("Step %s is not a Conda step" % resolved_alias)

            self._cached_environment.add_alias(
                alias_type, resolved_alias, env_id.req_id, env_id.full_id
            )
            debug.conda_exec(
                "%s (type %s) found locally (env_id: %s)"
                % (env_alias, alias_type.value, str(env_id))
            )
            return env_id

        env_id = self._cached_environment.env_id_for_alias(
            alias_type, resolved_alias, arch
        )

        debug.conda_exec(
            "%s (type %s)%sfound locally (resolved %s)"
            % (env_alias, alias_type.value, " " if env_id else " not ", resolved_alias)
        )

        if env_id:
            return env_id

        if not local_only and self._storage is not None:
            env_id = self._remote_fetch_alias([(alias_type, resolved_alias)], arch)
            if env_id:
                return env_id[0]
        return None

    def environment_from_alias(
        self, env_alias: str, arch: Optional[str] = None, local_only: bool = False
    ) -> Optional[ResolvedEnvironment]:
        """
        Convenience function allowing you to go from an alias to a ResolvedEnvironment
        directly

        Parameters
        ----------
        env_alias : str
            Alias for the environment
        arch : Optional[str], optional
            Architecture to look for. If None, defaults to the current one, by default None
        local_only : bool, optional
            If True, do not look at the remote environment cache, by default False

        Returns
        -------
        Optional[ResolvedEnvironment]
            If found, returns the ResolvedEnvironment for the given alias.
        """

        env_id = self.env_id_from_alias(env_alias, arch, local_only)
        if env_id:
            return self.environment(env_id, local_only)

    def aliases_for_env_id(self, env_id: EnvID) -> Tuple[List[str], List[str]]:
        """
        Returns the list of aliases for a given environment id

        Parameters
        ----------
        env_id : EnvID
            The environment ID

        Returns
        -------
        Tuple[List[str], List[str]]
            A tuple representing the immutable aliases and the mutable aliases
        """
        return self._cached_environment.aliases_for_env(env_id)

    def set_default_environment(self, env_id: EnvID) -> None:
        """
        Sets the default environment (the one used if "_default" is passed as the
        full ID) for the requirement ID embedded in the environemnt ID

        Parameters
        ----------
        env_id : EnvID
            The environment ID
        """
        self._cached_environment.set_default(env_id)

    def get_default_environment(
        self, req_id: str, arch: Optional[str]
    ) -> Optional[EnvID]:
        """
        If present, returns the default environment ID for the given requirement ID

        Parameters
        ----------
        req_id : str
            The requirement ID
        arch : Optional[str]
            The architecture to look for

        Returns
        -------
        Optional[EnvID]
            If it exists, returns the environment ID for the default environment
        """
        return self._cached_environment.get_default(req_id, arch)

    def clear_default_environment(self, req_id: str, arch: Optional[str]):
        """
        Removes the default mapping for the given requirement ID and architecture

        Parameters
        ----------
        req_id : str
            The requirement ID
        arch : Optional[str]
            The architecture
        """
        self._cached_environment.clear_default(req_id, arch)

    def add_environments(self, resolved_envs: List[ResolvedEnvironment]) -> None:
        """
        Adds the resolved environments to our local cache of known environments

        Parameters
        ----------
        resolved_envs : List[ResolvedEnvironment]
            The list of environments to add to the local cache
        """
        for resolved_env in resolved_envs:
            self._cached_environment.add_resolved_env(resolved_env)

    def alias_environment(self, env_id: EnvID, aliases: List[str]) -> None:
        """
        Add aliases to a given environment.

        This immediately updates the remote cache if present.

        Parameters
        ----------
        env_id : EnvID
            The environment ID
        aliases : List[str]
            The list of aliases -- note that you can only update mutable aliases or
            add new ones.
        """
        if self._datastore_type != "local":
            # We first fetch any aliases we have remotely because that way
            # we will catch any non-mutable changes
            resolved_aliases = [resolve_env_alias(a) for a in aliases]
            aliases_to_fetch = [
                (t, a) for t, a in resolved_aliases if t == AliasType.GENERIC
            ]

            self._remote_fetch_alias(aliases_to_fetch)
            # We are going to write out our aliases
            mutable_upload_files = []  # type: List[Tuple[str, str]]
            immutable_upload_files = []  # type: List[Tuple[str, str]]
            with tempfile.TemporaryDirectory() as aliases_dir:
                for alias in aliases:
                    alias_type, resolved_alias = resolve_env_alias(alias)

                    if alias_type == AliasType.GENERIC:
                        local_filepath = os.path.join(
                            aliases_dir, resolved_alias.replace("/", "~")
                        )
                        cache_path = self.get_datastore_path_to_env_alias(
                            alias_type, resolved_alias
                        )
                        with open(local_filepath, mode="w", encoding="utf-8") as f:
                            json.dump([env_id.req_id, env_id.full_id], f)
                        if is_alias_mutable(alias_type, resolved_alias):
                            mutable_upload_files.append((cache_path, local_filepath))
                        else:
                            immutable_upload_files.append((cache_path, local_filepath))
                        debug.conda_exec(
                            "Aliasing and will upload alias %s to %s"
                            % (str(env_id), cache_path)
                        )
                    else:
                        debug.conda_exec(
                            "Aliasing (but not uploading) %s as %s"
                            % (str(env_id), resolved_alias)
                        )

                    self._cached_environment.add_alias(
                        alias_type, resolved_alias, env_id.req_id, env_id.full_id
                    )
                if mutable_upload_files:
                    self._upload_to_ds(mutable_upload_files, overwrite=True)
                if immutable_upload_files:
                    self._upload_to_ds(immutable_upload_files)
        else:
            for alias in aliases:
                alias_type, resolved_alias = resolve_env_alias(alias)
                self._cached_environment.add_alias(
                    alias_type, resolved_alias, env_id.req_id, env_id.full_id
                )

    def cache_environments(
        self,
        resolved_envs: List[ResolvedEnvironment],
        cache_formats: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        """
        Downloads and caches all packages needed for the environments passed in.

        This will update the environments with the new cache information.

        Note that if packages are already cached, the information will be updated but
        the packages will not be re-downloaded.

        This will also upload the resolved environment's description to the remote
        cache.

        Parameters
        ----------
        resolved_envs : List[ResolvedEnvironment]
            List of ResolvedEnvironments to cache
        cache_formats : Optional[Dict[str, List[str]]], optional
            The formats we need to cache for. This is primarily used for
            Conda packages. The dictionary keys are `pip` or `conda` and the
            values are the formats that are needed or ["_any"] if any cached
            format is ok. If the None default is used, the value will be
            {"conda": CONDA_PREFERRED_FORMAT or ["_any"], "pip": ["_any"]}.
        """
        # The logic behind this function is as follows:
        #  - check in the S3/Azure/GS storage to see if the file exists
        #  - if it does, we are all good and we update the cache_urls
        #  - if it does not, check if the file is locally installed (if same arch)
        #    + if installed, check if it matches the MD5 hash and if all checks out,
        #      use to upload
        #    + if not, download it
        #  - at this point, we have the tarballs so upload to S3/Azure/GS

        # We cache multiple environments at once because we can benefit from overlaps
        # between environments which is likely across multiple similar environments

        if self._storage is None:
            raise CondaException(
                "Cannot cache environments since no datastore configured"
            )
        if not self._found_binaries:
            self._find_conda_binary()

        my_arch_id = arch_id()
        cache_formats = cache_formats or {
            "pip": ["_any"],
            "conda": [CONDA_PREFERRED_FORMAT] if CONDA_PREFERRED_FORMAT else ["_any"],
        }

        # key: URL
        # value: architecture, list of packages that need this URL
        url_to_pkgs = {}  # type: Dict[str, Tuple[str, List[PackageSpecification]]]

        # key: base cache URL
        # value: key in url_to_pkgs
        cache_path_to_key = {}  # type: Dict[str, str]

        # We fetch the latest information from the cache about the environment.
        # This is to have a quick path for:
        #  - environment is resolved locally
        #  - it resolves to something already in cache
        cached_resolved_envs = self._remote_env_fetch(
            [env.env_id for env in resolved_envs], ignore_co_resolved=True
        )

        # Here we take the resolved env (which has more information but maybe not
        # all if we request a different package format for example) if it exists or
        # the one we just resolved
        resolved_env = [
            cached_env if cached_env else env
            for cached_env, env in zip(cached_resolved_envs, resolved_envs)
        ]

        for resolved_env in resolved_envs:
            # We are now going to try to figure out all the locations we need to check
            # in the cache for the presence of the files we need
            env_id = resolved_env.env_id
            for req_pkg in resolved_env.packages:
                if req_pkg.url in url_to_pkgs:
                    old_arch, all_pkgs = url_to_pkgs[req_pkg.url]
                    all_pkgs.append(req_pkg)
                    # It is possible that arch is different (for noarch packages). In
                    # that case, we prefer to download it for our own arch to place the
                    # file in the right place for later. It doesn't matter for anything
                    # else
                    if env_id.arch == my_arch_id and old_arch != env_id.arch:
                        url_to_pkgs[req_pkg.url] = (my_arch_id, all_pkgs)
                else:
                    url_to_pkgs[req_pkg.url] = (env_id.arch, [req_pkg])

        # At this point, we can check if we are missing any cache information
        # Note that the packages may have the same url but different filenames (for example
        # if there are different wheels built from the same source). This is why we
        # check across all packages. Concretely, we may need to check for
        # foo/bar/<pkg1.url>/<pkg1.filename>.whl and foo/bar/<pkg2.url>/pkg2.filename>whl
        # where both pkg1 and pkg2 have the same URL (the git repo from where they were
        # built for example) but one was built for version 3.8 and another version 3.9.
        for base_url, (_, all_pkgs) in url_to_pkgs.items():
            if not all(
                [
                    pkg.cached_version(f) != None
                    for pkg in all_pkgs
                    for f in cache_formats.get(pkg.TYPE, ["_any"])
                ]
            ):
                # We are missing some information so we will look at all formats
                # we have available in the cache
                base_cache_url = (
                    all_pkgs[0]
                    .cache_pkg_type()
                    .make_partial_cache_url(base_url, all_pkgs[0].is_downloadable_url())
                )
                cache_path_to_key[base_cache_url] = base_url

        found_files = self._storage.list_content(cache_path_to_key.keys())
        cache_path_to_pkgs = {}  # type: Dict[str, List[PackageSpecification]]
        for cache_path, is_file in found_files:
            cache_path = cast(str, cache_path).rstrip("/")
            is_file = cast(bool, is_file)
            if is_file:
                raise CondaException("Invalid cache content at '%s'" % cache_path)
            base_cache_path, cache_filename_with_ext = os.path.split(cache_path)
            all_pkgs = url_to_pkgs[cache_path_to_key[base_cache_path]][1]
            for pkg in all_pkgs:
                if pkg.can_add_filename(cache_filename_with_ext):
                    # This means this package is interested in this cached package and
                    # can have it added. It's not always the case for example for PIP
                    # packages that can have multiple built wheels
                    cache_path_to_pkgs.setdefault(cache_path, []).append(pkg)

        # We now list one level down from everything in cache_url_to_pkgs which
        # will give us the hash
        found_files = self._storage.list_content(cache_path_to_pkgs.keys())
        for cache_path, is_file in found_files:
            cache_path = cast(str, cache_path).rstrip("/")
            is_file = cast(bool, is_file)
            if is_file:
                raise CondaException("Invalid cache content at '%s'" % cache_path)
            base_cache_path = os.path.split(cache_path)[0]
            cache_filename_with_ext = os.path.split(base_cache_path)[1]
            cache_ext = correct_splitext(cache_filename_with_ext)[1]
            for pkg in cache_path_to_pkgs[base_cache_path]:
                debug.conda_exec(
                    "%s:%s -> Found cache file %s"
                    % (
                        pkg.filename,
                        cache_ext,
                        os.path.join(cache_path, cache_filename_with_ext),
                    )
                )
                cache_pkg = pkg.cache_pkg_type()(
                    os.path.join(cache_path, cache_filename_with_ext)
                )
                pkg.add_cached_version(cache_ext, cache_pkg)

        # Build the list of packages we need to fetch something from. We basically fetch
        # anything for which we don't have a cached or local version of the file for url_format.
        pkgs_to_fetch_per_arch = {}  # type: Dict[str, List[PackageSpecification]]

        for arch, all_pkgs in url_to_pkgs.values():
            # We can only fetch the URL format from something other than the cache
            # so we only see if we need to fetch that one
            pkg = all_pkgs[0]
            if (
                pkg.is_downloadable_url()
                and not pkg.is_cached([pkg.url_format])
                and not pkg.local_file(pkg.url_format)
            ):
                pkgs_to_fetch_per_arch.setdefault(arch, []).append(pkg)

        # Fetch what we need; we fetch all formats for all packages (this is a superset
        # but simplifies the code)

        # Contains cache_path, local_path
        upload_files = []  # type: List[Tuple[str, str]]

        def _cache_pkg(pkg: PackageSpecification, pkg_fmt: str, local_path: str) -> str:
            file_hash = cast(str, pkg.pkg_hash(pkg_fmt))
            cache_path = pkg.cache_pkg_type().make_cache_url(
                pkg.url, pkg.filename, pkg_fmt, file_hash, pkg.is_downloadable_url()
            )
            upload_files.append((cache_path, local_path))
            debug.conda_exec(
                "%s:%s -> will upload %s to %s"
                % (pkg.filename, pkg_fmt, local_path, cache_path)
            )
            pkg.add_cached_version(pkg_fmt, pkg.cache_pkg_type()(cache_path))
            return cache_path

        with tempfile.TemporaryDirectory() as download_dir:
            for arch, pkgs in pkgs_to_fetch_per_arch.items():
                if arch == arch_id():
                    search_dirs = self._package_dirs
                else:
                    search_dirs = [os.path.join(download_dir, arch)]
                    os.mkdir(search_dirs[0])
                dest_dir = search_dirs[0]

                with CondaLockMultiDir(
                    self._echo, search_dirs, self._package_dir_lockfile_name
                ):
                    require_conda_format = cache_formats.get("conda", [])
                    if len(require_conda_format) > 0 and "_any" in require_conda_format:
                        require_conda_format = []
                    self._lazy_fetch_packages(
                        pkgs,
                        dest_dir,
                        require_conda_format=require_conda_format,
                        require_url_format=True,
                        requested_arch=arch,
                        search_dirs=search_dirs,
                    )
            # Upload everything for all packages. We iterate over all packages in case
            # there are different filenames. We only upload things once though
            saw_local_file = set()  # type: Set[str]
            for _, all_pkgs in url_to_pkgs.values():
                for pkg in all_pkgs:
                    for local_fmt, local_path in pkg.local_files:
                        if (
                            not pkg.cached_version(local_fmt)
                            and local_path not in saw_local_file
                        ):
                            _cache_pkg(pkg, local_fmt, local_path)
                            saw_local_file.add(local_path)

            # We fetched and updated the cache information of only the first package
            # in the list so we update all others.
            for _, all_pkgs in url_to_pkgs.values():
                cannonical_pkg = all_pkgs[0]
                for pkg in all_pkgs[1:]:
                    for pkg_fmt, pkg_cache in cannonical_pkg.cached_versions:
                        filename_with_ext = os.path.split(pkg_cache.url)[1]
                        if pkg.can_add_filename(filename_with_ext):
                            pkg.add_cached_version(
                                pkg_fmt, pkg.cache_pkg_type()(pkg_cache.url)
                            )

            if upload_files:
                start = time.time()
                self._echo(
                    "    Caching %d item%s to %s ..."
                    % (
                        len(upload_files),
                        plural_marker(len(upload_files)),
                        self._datastore_type,
                    ),
                    nl=False,
                )
                self._upload_to_ds(upload_files)
                delta_time = int(time.time() - start)
                self._echo(
                    " done in %d second%s." % (delta_time, plural_marker(delta_time))
                )
            else:
                self._echo(
                    "    All packages already cached in %s." % self._datastore_type
                )
            # If this is successful, we cache the environments. We do this *after*
            # in case some packages fail to upload so we don't write corrupt
            # information
            upload_files = []
            for resolved_env in resolved_envs:
                if not resolved_env.dirty:
                    continue
                env_id = resolved_env.env_id
                local_filepath = os.path.join(download_dir, "%s_%s_%s.env" % env_id)
                cache_path = self.get_datastore_path_to_env(env_id)
                with open(local_filepath, mode="w", encoding="utf-8") as f:
                    json.dump(resolved_env.to_dict(), f)
                upload_files.append((cache_path, local_filepath))
                debug.conda_exec("Will upload env %s to %s" % (str(env_id), cache_path))

                # We also cache the full-id as an alias so we can access it directly
                # later
                local_filepath = os.path.join(download_dir, "%s.alias" % env_id.full_id)
                if not os.path.isfile(local_filepath):
                    # Don't upload the same thing for multiple arch
                    cache_path = self.get_datastore_path_to_env_alias(
                        AliasType.FULL_ID, env_id.full_id
                    )
                    with open(local_filepath, mode="w", encoding="utf-8") as f:
                        json.dump([env_id.req_id, env_id.full_id], f)
                    upload_files.append((cache_path, local_filepath))
                    debug.conda_exec(
                        "Will upload alias for env %s to %s" % (str(env_id), cache_path)
                    )
            if upload_files:
                start = time.time()
                self._echo(
                    "    Caching %d environments and aliases to %s ..."
                    % (
                        len(upload_files),
                        self._datastore_type,
                    ),
                    nl=False,
                )
                self._upload_to_ds(upload_files)
                delta_time = int(time.time() - start)
                self._echo(
                    " done in %d second%s." % (delta_time, plural_marker(delta_time))
                )
            else:
                self._echo(
                    "    All environments already cached in %s." % self._datastore_type
                )

    def write_out_environments(self) -> None:
        """
        Writes out the local cache environment information.
        """
        write_to_conda_manifest(self._local_root, self._cached_environment)

    @staticmethod
    def get_datastore_path_to_env(env_id: EnvID) -> str:
        # Returns a path to where we store a resolved environment's information
        return os.path.join(
            cast(str, CONDA_ENVS_DIRNAME),
            env_id.arch,
            env_id.req_id,
            env_id.full_id,
            "env",
        )

    @staticmethod
    def get_datastore_path_to_env_alias(alias_type: AliasType, env_alias: str) -> str:
        return os.path.join(
            cast(str, CONDA_ENVS_DIRNAME), "aliases", alias_type.value, env_alias
        )

    def _lazy_fetch_packages(
        self,
        packages: Iterable[PackageSpecification],
        dest_dir: str,
        require_conda_format: Optional[Sequence[str]] = None,
        require_url_format: bool = False,
        requested_arch: str = arch_id(),
        search_dirs: Optional[List[str]] = None,
    ):
        # Lazily fetch all packages specified by the PackageSpecification
        # for requested_arch.
        #
        # At the end of this function, all packages will either be:
        #  - present as a local directory (pkg.local_dir() is not None)
        #  - present as a local file (pkg.local_file(fmt) is not None for some fmt) and
        #    points to a tarball representing the package
        #
        # You can optionally request formats which will force the presence of a particular
        # local file (ie: even if a directory is present for the package, it will
        # still fetch the tarball)
        #
        # Tarballs are fetched from cache if available and the web if not. Package
        # transmutation also happens if the requested format is not found (only for
        # conda packages))
        #
        # A lock needs to be held on all directories of search_dirs and dest_dir

        if not self._found_binaries:
            self._find_conda_binary()

        if require_conda_format is None:
            require_conda_format = []

        cache_downloads = []  # type: List[Tuple[PackageSpecification, str, str]]
        web_downloads = []  # type: List[Tuple[PackageSpecification, str]]
        transmutes = []  # type: List[Tuple[PackageSpecification, str]]
        url_adds = []  # type: List[str]

        if search_dirs is None:
            search_dirs = [dest_dir]

        # Helper functions
        def _add_to_fetch_lists(
            pkg_spec: PackageSpecification, pkg_format: str, mode: str, dst: str
        ):
            if pkg_spec.TYPE != "conda":  # type: ignore
                # We may have to create the directory
                dir_path = os.path.split(dst)[0]
                if not os.path.exists(dir_path):
                    os.mkdir(dir_path)
            if mode == "cache":
                cache_downloads.append((pkg_spec, pkg_format, dst))
            elif mode == "web":
                # In some cases -- when we build packages locally, the URL is actually
                # not a valid URL but a fake one we use as a key. In that case, we
                # error out because we won't be able to fetch the environment. Note that
                # this won't happen if we have a cache since we prefer cache over web
                # download
                if pkg_spec.is_downloadable_url():
                    web_downloads.append((pkg_spec, dst))
                else:
                    raise CondaException(
                        "No non-web source for non web-downloadable package '%s'"
                        % pkg_spec.package_name
                    )

        def _download_web(
            session: requests.Session, entry: Tuple[PackageSpecification, str]
        ) -> Tuple[PackageSpecification, Optional[Exception]]:
            pkg_spec, local_path = entry
            base_hash = pkg_spec.base_hash()
            debug.conda_exec(
                "%s -> download %s to %s"
                % (pkg_spec.filename, pkg_spec.url, local_path)
            )
            try:
                with open(local_path, "wb") as f:
                    with session.get(pkg_spec.url, stream=True) as r:
                        for chunk in r.iter_content(chunk_size=None):
                            base_hash.update(chunk)
                            f.write(chunk)
                pkg_spec.add_local_file(
                    pkg_spec.url_format,
                    local_path,
                    pkg_hash=base_hash.hexdigest(),
                    downloaded=True,
                )
            except Exception as e:
                return (pkg_spec, e)
            return (pkg_spec, None)

        def _transmute(
            entry: Tuple[PackageSpecification, str]
        ) -> Tuple[PackageSpecification, Optional[str], Optional[Exception]]:
            pkg_spec, src_format = entry
            if pkg_spec.TYPE != "conda":
                raise CondaException("Transmutation only supported for Conda packages")
            debug.conda_exec("%s -> transmute %s" % (pkg_spec.filename, src_format))

            def _cph_transmute(src_file: str, dst_file: str, dst_format: str):
                args = [
                    "t",
                    "--processes",
                    "1",
                    "--zstd-compression-level",
                    "3",
                    "--force",
                    "--out-folder",
                    os.path.dirname(dst_file),
                    src_file,
                    dst_format,
                ]
                self._call_binary(args, binary="cph")

            def _micromamba_transmute(src_file: str, dst_file: str, dst_format: str):
                args = ["package", "transmute", "-c", "3", src_file]
                self._call_binary(args, binary="micromamba")

            try:
                src_file = pkg_spec.local_file(src_format)
                if src_file is None:
                    raise CondaException(
                        "Need to transmute %s but %s does not have a local file in that format"
                        % (src_format, pkg_spec.filename)
                    )
                dst_format = [f for f in CONDA_FORMATS if f != src_format][0]
                dst_file = src_file[: -len(src_format)] + dst_format

                # Micromamba transmute still has an issue with case insensitive
                # OSs so force CPH use for cross-arch packages for now but use
                # micromamba otherwise if available
                # https://github.com/mamba-org/mamba/issues/2328

                # Micromamba transmute also has issues going from .conda to .tar.bz2
                # with filenames so we use CPH
                if (
                    "micromamba" in cast(Dict[str, str], self._bins)
                    and requested_arch == arch_id()
                    and src_format != ".conda"
                ):
                    _micromamba_transmute(src_file, dst_file, dst_format)
                elif "cph" in cast(Dict[str, str], self._bins):
                    _cph_transmute(src_file, dst_file, dst_format)
                else:
                    if requested_arch != arch_id() and "micromamba" not in cast(
                        Dict[str, str], self._bins
                    ):
                        raise CondaException(
                            "Transmuting a package with micromamba is not supported "
                            "across architectures or from .conda due to "
                            "https://github.com/mamba-org/mamba/issues/2328. "
                            "Please install conda-package-handling."
                        )
                    raise CondaException(
                        "Requesting to transmute package without conda-package-handling "
                        " or micromamba"
                    )

                pkg_spec.add_local_file(dst_format, dst_file)
            except CondaException as e:
                return (pkg_spec, None, e)
            return (pkg_spec, dst_format, None)

        # Iterate over all the filenames that we want to fetch.
        for pkg_spec in packages:
            found_dir = False
            for d in search_dirs:
                extract_path = os.path.join(d, pkg_spec.filename)
                if (
                    pkg_spec.TYPE == "conda"
                    and not require_conda_format
                    and not require_url_format
                    and os.path.isdir(extract_path)
                ):
                    debug.conda_exec(
                        "%s -> using existing directory %s"
                        % (pkg_spec.filename, extract_path)
                    )
                    pkg_spec.add_local_dir(extract_path)
                    found_dir = True
                    break

                # At this point, we don't have a directory or we need specific tarballs
                for f in pkg_spec.allowed_formats():
                    # We may have found the file in another directory
                    if pkg_spec.local_file(f):
                        continue
                    if pkg_spec.TYPE == "conda":
                        tentative_path = os.path.join(
                            d, "%s%s" % (pkg_spec.filename, f)
                        )
                    else:
                        tentative_path = os.path.join(
                            d, pkg_spec.TYPE, "%s%s" % (pkg_spec.filename, f)
                        )
                    if os.path.isfile(tentative_path):
                        try:
                            pkg_spec.add_local_file(f, tentative_path)
                        except ValueError:
                            debug.conda_exec(
                                "%s -> rejecting %s due to hash mismatch (expected %s)"
                                % (
                                    pkg_spec.filename,
                                    tentative_path,
                                    pkg_spec.pkg_hash(f),
                                )
                            )
            if found_dir:
                # We didn't need specific tarballs and we found a directory -- happy clam
                continue

            # This code extracts the most preferred source of filename as well as a list
            # of where we can get a given format
            # The most_preferred_source will be:
            #  - if it exists, a local file (among local files, prefer the first file in
            #    the allowed_formats list)
            #  - if no local files, if they exist, a cached version
            #    (among cached versions, prefer the first format in the allowed_formats
            #    list)
            #  - if no cached version, the web download
            most_preferred_source = None
            most_preferred_format = None
            available_formats = {}  # type: Dict[str, Tuple[str, str]]
            if pkg_spec.TYPE != "conda":
                dl_local_path = os.path.join(
                    dest_dir, pkg_spec.TYPE, "%s{format}" % pkg_spec.filename
                )
                # Make sure the destination directory exists
                os.makedirs(os.path.join(dest_dir, pkg_spec.TYPE), exist_ok=True)
            else:
                dl_local_path = os.path.join(dest_dir, "%s{format}" % pkg_spec.filename)
            # Check for local files first
            for f in pkg_spec.allowed_formats():
                local_path = pkg_spec.local_file(f)
                if local_path:
                    src = ("local", local_path)
                    if most_preferred_source is None:
                        most_preferred_source = src
                        most_preferred_format = f
                    available_formats[f] = src

            # Check for cache paths next
            for f in pkg_spec.allowed_formats():
                cache_info = pkg_spec.cached_version(f)
                if cache_info and cache_info.url:
                    src = ("cache", dl_local_path.format(format=f))
                    if most_preferred_source is None:
                        most_preferred_source = src
                        most_preferred_format = f
                    if f not in available_formats:
                        available_formats[f] = src

            # And finally, fall back on the web
            web_src = (
                "web",
                dl_local_path.format(format=pkg_spec.url_format),
            )
            if most_preferred_source is None:
                most_preferred_source = web_src
                most_preferred_format = pkg_spec.url_format
            if pkg_spec.url_format not in available_formats:
                available_formats[pkg_spec.url_format] = web_src

            debug.conda_exec(
                "%s -> preferred %s @ %s"
                % (
                    pkg_spec.filename,
                    most_preferred_format,
                    most_preferred_source[0],
                )
            )

            assert most_preferred_format
            fetched_formats = [most_preferred_format]
            _add_to_fetch_lists(pkg_spec, most_preferred_format, *most_preferred_source)

            # Conda packages, with their multiple formats, require a bit more handling
            if pkg_spec.TYPE == "conda":
                for f in [
                    f
                    for f in require_conda_format
                    if f in available_formats and f != most_preferred_format
                ]:
                    _add_to_fetch_lists(pkg_spec, f, *available_formats[f])
                    fetched_formats.append(f)
                if require_url_format and pkg_spec.url_format not in fetched_formats:
                    # Guaranteed to be in available_formats because we add the web_src
                    # as a last resort above
                    _add_to_fetch_lists(
                        pkg_spec,
                        pkg_spec.url_format,
                        *available_formats[pkg_spec.url_format]
                    )
                    fetched_formats.append(pkg_spec.url_format)
                # For anything that we need and we don't have an available source for,
                # we transmute from our most preferred source.
                for f in [f for f in require_conda_format if f not in fetched_formats]:
                    transmutes.append((pkg_spec, most_preferred_format))

        # Done going over all the files
        do_download = web_downloads or cache_downloads
        if do_download:
            start = time.time()
            self._echo(
                "    Downloading %d(web) + %d(cache) package%s for arch %s ..."
                % (
                    len(web_downloads),
                    len(cache_downloads),
                    plural_marker(len(web_downloads) + len(cache_downloads)),
                    requested_arch,
                ),
                nl=False,
            )

        pending_errors = []  # type: List[str]
        if do_download or len(transmutes) > 0:
            # Ensure the packages directory exists at the very least
            if not os.path.isdir(dest_dir):
                os.makedirs(dest_dir)
            url_file = os.path.join(dest_dir, "urls.txt")
            known_urls = set()  # type: Set[str]
            if os.path.isfile(url_file):
                with open(url_file, "rb") as f:
                    known_urls.update([l.strip().decode("utf-8") for l in f])

            if web_downloads:
                with ThreadPoolExecutor() as executor:
                    with requests.Session() as s:
                        a = requests.adapters.HTTPAdapter(
                            pool_connections=executor._max_workers,
                            pool_maxsize=executor._max_workers,
                            max_retries=3,
                        )
                        s.mount("https://", a)
                        download_results = [
                            executor.submit(_download_web, s, entry)
                            for entry in web_downloads
                        ]
                        for f in as_completed(download_results):
                            pkg_spec, error = f.result()
                            if error is None:
                                if (
                                    pkg_spec.TYPE == "conda"
                                    and pkg_spec.url not in known_urls
                                ):
                                    url_adds.append(pkg_spec.url)
                            else:
                                pending_errors.append(
                                    "Error downloading package for '%s': %s"
                                    % (pkg_spec.filename, str(error))
                                )

            if cache_downloads:
                from metaflow.plugins import DATASTORES

                storage = [d for d in DATASTORES if d.TYPE == self._datastore_type][0](
                    get_conda_root(self._datastore_type)
                )  # type: DataStoreStorage
                keys_to_info = (
                    {}
                )  # type: Dict[str, Tuple[PackageSpecification, str, str, str]]
                for pkg_spec, pkg_format, local_path in cache_downloads:
                    cache_info = pkg_spec.cached_version(pkg_format)
                    if cache_info:
                        keys_to_info[cache_info.url] = (
                            pkg_spec,
                            pkg_format,
                            cache_info.hash,
                            local_path,
                        )
                    else:
                        pending_errors.append(
                            "Internal error: trying to download a non-existent cache item for %s"
                            % pkg_spec.filename
                        )
                with storage.load_bytes(keys_to_info.keys()) as load_results:  # type: ignore
                    for (key, tmpfile, _) in load_results:  # type: ignore
                        pkg_spec, pkg_format, pkg_hash, local_path = keys_to_info[key]  # type: ignore
                        if not tmpfile:
                            pending_errors.append(
                                "Error downloading package from cache for '%s': not found at %s"
                                % (pkg_spec.filename, key)
                            )
                        else:
                            if pkg_spec.TYPE == "conda":
                                url_to_add = self._make_urlstxt_from_cacheurl(key)  # type: ignore
                                if url_to_add not in known_urls:
                                    url_adds.append(url_to_add)
                            shutil.move(tmpfile, local_path)  # type: ignore
                            # We consider stuff in the cache to be clean in terms of hash
                            # so we don't want to recompute it.
                            pkg_spec.add_local_file(
                                pkg_format, local_path, pkg_hash, downloaded=True
                            )

            if do_download:
                delta_time = int(time.time() - start)
                self._echo(
                    " done in %d second%s." % (delta_time, plural_marker(delta_time)),
                    timestamp=False,
                )
            if not pending_errors and transmutes:
                start = time.time()
                self._echo(
                    "    Transmuting %d package%s for arch %s..."
                    % (
                        len(transmutes),
                        plural_marker(len(transmutes)),
                        requested_arch,
                    ),
                    nl=False,
                )
                with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                    transmut_results = [
                        executor.submit(_transmute, entry) for entry in transmutes
                    ]
                    for f in as_completed(transmut_results):
                        pkg_spec, dst_format, error = f.result()
                        if error:
                            pending_errors.append(
                                "Error transmuting '%s': %s"
                                % (pkg_spec.filename, error)
                            )
                        else:
                            new_url = self._make_urlstxt_from_url(pkg_spec, dst_format)

                            if new_url not in known_urls:
                                url_adds.append(new_url)
                delta_time = int(time.time() - start)
                self._echo(
                    " done in %d second%s." % (delta_time, plural_marker(delta_time)),
                    timestamp=False,
                )
            if url_adds:
                # Update the urls file in the packages directory so that Conda knows that the
                # files are there
                debug.conda_exec(
                    "Adding the following URLs to %s: %s"
                    % (os.path.join(dest_dir, "urls.txt"), str(url_adds))
                )
                with open(
                    os.path.join(dest_dir, "urls.txt"),
                    mode="a",
                    encoding="utf-8",
                ) as f:
                    f.writelines(["%s\n" % l for l in url_adds])
        if pending_errors:
            print(
                "Got the following errors while loading packages:\n%s"
                % "\n".join(pending_errors),
                file=sys.stderr,
            )
            raise CondaException(
                "Could not fetch packages -- see pretty-printed errors above."
            )

    def _resolve_env_with_micromamba_server(
        self, deps: Sequence[TStr], channels: Sequence[TStr], architecture: str
    ) -> List[PackageSpecification]:

        deps = [d for d in deps if d.category in ("conda", "npconda")]

        if not self._have_micromamba_server:
            raise CondaException(
                "Micromamba server not supported by installed version of micromamba"
            )

        self._start_micromamba_server()
        # Form the payload to send to the server
        req = {
            "specs": [d.value for d in deps if d.category in ("conda", "npconda")]
            + ["pip"],
            "platform": architecture,
            "channels": [c.value for c in channels if c.category == "conda"],
        }
        if arch_id() == architecture:
            # Use the same virtual packages as the ones used for conda/mamba
            req["virtual_packages"] = [
                "%s=%s=%s" % (pkg_name, pkg_version, pkg_id)
                for pkg_name, pkg_version, pkg_id in self._info["virtual_pkgs"]
            ]
        # Make the request to the micromamba server
        debug.conda_exec(
            "Payload for micromamba server on port %d: %s"
            % (self._micromamba_server_port, str(req))
        )
        resp = requests.post(
            "http://localhost:%d" % self._micromamba_server_port, json=req
        )
        if resp.status_code != 200:
            raise CondaException(
                "Got unexpected return code from micromamba server: %d"
                % resp.status_code
            )
        else:
            json_response = resp.json()
            if "error_msg" in json_response:
                raise CondaException(
                    "Cannot resolve environment: %s" % json_response["error_msg"]
                )
            else:
                return [
                    CondaPackageSpecification(
                        filename=pkg["filename"],
                        url=pkg["url"],
                        url_format=correct_splitext(pkg["filename"])[1],
                        hashes={correct_splitext(pkg["filename"])[1]: pkg["md5"]},
                    )
                    for pkg in json_response["packages"]
                ]

    def _resolve_env_with_conda(
        self,
        deps: Sequence[TStr],
        sources: Sequence[TStr],
        architecture: str,
        builder_env: Optional[ResolvedEnvironment],
        base_env: Optional[ResolvedEnvironment],
    ) -> Tuple[List[PackageSpecification], Optional[ResolvedEnvironment]]:

        if base_env:
            local_packages = [
                p for p in base_env.packages if not p.is_downloadable_url()
            ]
            if local_packages:
                raise CondaException(
                    "Local packages are not allowed in Conda: %s"
                    % ", ".join([p.package_name for p in local_packages])
                )
        deps = [d for d in deps if d.category in ("conda", "npconda")]

        result = []
        with tempfile.TemporaryDirectory() as mamba_dir:
            args = [
                "create",
                "--prefix",
                os.path.join(mamba_dir, "prefix"),
                "--dry-run",
            ]
            have_channels = False
            for c in sources:
                if c.category == "conda":
                    have_channels = True
                    args.extend(["-c", c.value])

            if not have_channels:
                have_channels = any(
                    [
                        "::" in d.value
                        for d in deps
                        if d.category in ("conda", "npconda")
                    ]
                )
            if have_channels:
                # Add no-channel-priority because otherwise if a version
                # is present in the other channels but not in the higher
                # priority channels, it is not found.
                args.append("--no-channel-priority")
            args.extend([d.value for d in deps])

            addl_env = {
                "CONDA_SUBDIR": architecture,
                "CONDA_PKGS_DIRS": mamba_dir,
                "CONDA_ROOT": self._info["root_prefix"],
                "CONDA_UNSATISFIABLE_HINTS_CHECK_DEPTH": "0",
            }
            conda_result = json.loads(self._call_conda(args, addl_env=addl_env))

        # This returns a JSON blob with:
        #  - actions:
        #    - FETCH: List of objects to fetch -- this is where we get hash and URL
        #    - LINK: Packages to actually install (in that order)
        # On micromamba, we can just use the LINK blob since it has all information we need
        if not conda_result["success"]:
            print(
                "Pretty-printed Conda create result:\n%s" % conda_result,
                file=sys.stderr,
            )
            raise CondaException(
                "Could not resolve environment -- see above pretty-printed error."
            )

        if self._conda_executable_type == "micromamba":
            for lnk in conda_result["actions"]["LINK"]:
                parse_result = parse_explicit_url_conda(
                    "%s#%s" % (lnk["url"], lnk["md5"])
                )
                result.append(
                    CondaPackageSpecification(
                        filename=parse_result.filename,
                        url=parse_result.url,
                        url_format=parse_result.url_format,
                        hashes={parse_result.url_format: cast(str, parse_result.hash)},
                    )
                )
        else:

            def _pkg_key(
                name: str, platform: str, build_string: str, build_number: str
            ) -> str:
                return "%s_%s_%s_%s" % (name, platform, build_string, build_number)

            fetched_packages = {}  # type: Dict[str, Tuple[str, str]]
            result = []  # type: List[PackageSpecification]
            for pkg in conda_result["actions"]["FETCH"]:
                fetched_packages[
                    _pkg_key(
                        pkg["name"], pkg["subdir"], pkg["build"], pkg["build_number"]
                    )
                ] = (pkg["url"], pkg["md5"])
            for lnk in conda_result["actions"]["LINK"]:
                k = _pkg_key(
                    lnk["name"],
                    lnk["platform"],
                    lnk["build_string"],
                    lnk["build_number"],
                )
                url, md5_hash = fetched_packages[k]
                if not url.startswith(lnk["base_url"]):
                    raise CondaException(
                        "Unexpected record for %s: %s" % (k, str(conda_result))
                    )
                parse_result = parse_explicit_url_conda("%s#%s" % (url, md5_hash))
                result.append(
                    CondaPackageSpecification(
                        filename=parse_result.filename,
                        url=parse_result.url,
                        url_format=parse_result.url_format,
                        hashes={parse_result.url_format: cast(str, parse_result.hash)},
                    )
                )
        return result, builder_env

    def _resolve_env_with_pip(
        self,
        deps: Sequence[TStr],
        sources: Sequence[TStr],
        extras: Sequence[TStr],
        architecture: str,
        builder_env: Optional[ResolvedEnvironment],
        base_env: Optional[ResolvedEnvironment],
    ) -> Tuple[List[PackageSpecification], Optional[ResolvedEnvironment]]:

        if base_env:
            local_packages = [
                p
                for p in base_env.packages
                if p.TYPE == "pip" and (not p.is_downloadable_url() or p.is_derived())
            ]
        else:
            local_packages = None
        # Some args may be two actual arguments like "-f <something>" thus the map
        extra_args = list(
            chain.from_iterable(
                map(
                    lambda x: x.split(maxsplit=1),
                    (e.value for e in extras if e.category == "pip"),
                )
            )
        )

        if not builder_env:
            builder_env = self._build_builder_env(deps, sources, architecture)

        deps = [d for d in deps if d.category == "pip"]
        # We get the python version for this builder env
        python_version = None  # type: Optional[str]
        for p in builder_env.packages:
            if p.filename.startswith("python-"):
                python_version = p.package_version
                break
        if python_version is None:
            raise CondaException(
                "Could not determine version of Python from conda packages"
            )

        # Create the environment in which we will call pip
        debug.conda_exec("Creating builder conda environment")
        techo = self._echo
        self._echo = self._no_echo
        self.create_for_name(
            self._env_builder_directory_from_envid(builder_env.env_id),
            builder_env,
        )
        self._echo = techo

        builder_python = cast(
            str,
            self.python(self._env_builder_directory_from_envid(builder_env.env_id)),
        )

        result = []
        with tempfile.TemporaryDirectory() as pip_dir:
            args = [
                "-m",
                "pip",
                "--isolated",
                "install",
                "--ignore-installed",
                "--dry-run",
                "--target",
                pip_dir,
                "--report",
                os.path.join(pip_dir, "out.json"),
            ]
            args.extend(extra_args)
            if CONDA_DEFAULT_PIP_SOURCE:
                args.extend(["-i", CONDA_DEFAULT_PIP_SOURCE])
            for c in chain(
                (s.value for s in sources if s.category == "pip"),
            ):
                args.extend(["--extra-index-url", c])

            supported_tags = pip_tags_from_arch(python_version, architecture)

            if architecture != arch_id():

                implementations = []  # type: List[str]
                abis = []  # type: List[str]
                platforms = []  # type: List[str]
                for tag in supported_tags:
                    implementations.append(tag.interpreter)
                    abis.append(tag.abi)
                    platforms.append(tag.platform)
                implementations = [x.interpreter for x in supported_tags]
                extra_args = (
                    "--only-binary=:all:",
                    # This seems to overly constrain things so skipping for now
                    # *(
                    #    chain.from_iterable(
                    #        product(["--implementation"], set(implementations))
                    #    )
                    # ),
                    *(chain.from_iterable(product(["--abi"], set(abis)))),
                    *(chain.from_iterable(product(["--platform"], set(platforms)))),
                )

                args.extend(extra_args)

            # If we have local packages, we download them to a directory and point
            # pip to it using the `--find-links` argument.
            local_packages_dict = {}  # type: Dict[str, PackageSpecification]
            if local_packages:
                os.makedirs(os.path.join(pip_dir, "local_packages"))
                self._lazy_fetch_packages(
                    local_packages, os.path.join(pip_dir, "local_packages")
                )
                args.extend(
                    ["--find-links", os.path.join(pip_dir, "local_packages", "pip")]
                )

                for p in local_packages:
                    for _, f in p.local_files:
                        local_packages_dict[os.path.realpath(f)] = p
                debug.conda_exec(
                    "Locally present files: %s" % ", ".join(local_packages_dict)
                )
            # Unfortunately, pip doesn't like things like ==<= so we need to strip
            # the ==
            for d in deps:
                splits = d.value.split("==", 1)
                if len(splits) == 1:
                    args.append(d.value)
                else:
                    if splits[1][0] in ("=", "<", ">", "!", "~"):
                        # Something originally like pkg==<=ver
                        args.append("".join(splits))
                    else:
                        # Something originally like pkg==ver
                        args.append(d.value)

            self._call_binary(args, binary=builder_python)

            # We should now have a json blob in out.json
            result = []  # type: List[PackageSpecification]
            with open(
                os.path.join(pip_dir, "out.json"), mode="r", encoding="utf-8"
            ) as f:
                desc = json.load(f)
            packages_to_build = []  # type: List[Dict[str, Any]]
            for package_desc in desc["install"]:
                # We can either have a direct URL (in which case we have an "archive_info"
                # blob), a directory (in which case we have a "dir_info" blob) or
                # a repo (in which case we have a "vcs_info" blob).
                # The first case is easy.
                # For the second case, if we are in editable mode, we error out (it
                # shouldn't get to here). If not, we add the file directly
                # For the third case, we will build the package (only if the same
                # arch) and then add it.
                # The spec is given here:
                # https://packaging.python.org/en/latest/specifications/direct-url-data-structure/
                if debug.conda_exec:
                    package_desc = {
                        k: v
                        for k, v in package_desc.items()
                        if k in ("download_info", "vcs_info", "url", "subdirectory")
                    }
                    debug.conda_exec("Need to install %s" % str(package_desc))
                dl_info = package_desc["download_info"]
                url = dl_info["url"]
                if "dir_info" in dl_info or url.startswith("file://"):
                    url = unquote(url)
                    local_path = url[7:]
                    if "dir_info" in dl_info and dl_info["dir_info"].get(
                        "editable", False
                    ):
                        raise CondaException(
                            "Cannot include an editable PIP package: '%s'" % url
                        )
                    if os.path.isdir(local_path):
                        packages_to_build.append(dl_info)
                    else:
                        # A local wheel or tarball
                        if url in local_packages_dict:
                            debug.conda_exec("This is a known local package")
                            # We are going to move this file to a less "temporary"
                            # location so that it can be installed if needed
                            pkg_spec = local_packages_dict[local_path]
                            filename = os.path.split(local_path)[1]
                            file_format = correct_splitext(filename)[1]
                            shutil.move(local_path, self._package_dirs[0])
                            pkg_spec.add_local_file(
                                file_format,
                                os.path.join(self._package_dirs[0], filename),
                            )
                            result.append(pkg_spec)
                        else:
                            parse_result = parse_explicit_path_pip(url)
                            if parse_result.url_format != ".whl":
                                # This is a source package so we need to build it
                                packages_to_build.append(dl_info)
                            else:
                                package_spec = PipPackageSpecification(
                                    parse_result.filename,
                                    parse_result.url,
                                    is_real_url=False,
                                    url_format=parse_result.url_format,
                                )
                                # We extract the actual local file so we can use that for now
                                # Note that the url in PipPackageSpecication is a fake
                                # one that looks like file://local-file/... which is meant
                                # to act as a key for the package.
                                package_spec.add_local_file(
                                    parse_result.url_format, url[7:]
                                )
                                result.append(package_spec)
                elif "vcs_info" in dl_info:
                    packages_to_build.append(dl_info)
                else:
                    if "hashes" in dl_info["archive_info"]:
                        hashes = dl_info["archive_info"]["hashes"]
                        for fmt, val in hashes.items():
                            if fmt == PipPackageSpecification.base_hash_name():
                                hash = "%s=%s" % (fmt, val)
                                break
                        else:
                            raise CondaException(
                                "Cannot find hash '%s' for package at '%s'"
                                % (
                                    PipPackageSpecification.base_hash_name(),
                                    url,
                                )
                            )
                    else:
                        # Fallback on older "hash" field
                        hash = dl_info["archive_info"]["hash"]

                    parse_result = parse_explicit_url_pip("%s#%s" % (url, hash))
                    if parse_result.url_format != ".whl":
                        packages_to_build.append(dl_info)
                    else:
                        result.append(
                            PipPackageSpecification(
                                parse_result.filename,
                                parse_result.url,
                                url_format=parse_result.url_format,
                                hashes={parse_result.url_format: parse_result.hash}
                                if parse_result.hash
                                else None,
                            )
                        )
            if packages_to_build:
                # Will contain:
                #  - build_url: url to use to build with pip wheel
                #  - cache_url: url in the cache
                #  - pkg_filename: name of the package
                #  - pkg_spec: PackageSpecification for the package
                # Keyed by the cannonical URL we assign for it
                to_build_pkg_info = {}  # type: Dict[str, Dict[str, Any]]
                for package_desc in packages_to_build:
                    if "vcs_info" in package_desc:
                        base_build_url = "%s+%s@%s" % (
                            package_desc["vcs_info"]["vcs"],
                            package_desc["url"],
                            package_desc["vcs_info"]["commit_id"],
                        )
                        # We form a "fake" URL which will give us a unique key so we can
                        # look up the package build in the cache. Given we have the
                        # commit_id, we assume that for a combination of repo, commit_id
                        # and subdirectory, we have a uniquely built package. This will
                        # give users some consistency as well in the sense that an
                        # environment that uses the same git package reference will
                        # actually use the same package
                        base_pkg_url = "%s/%s" % (
                            package_desc["url"],
                            package_desc["vcs_info"]["commit_id"],
                        )
                        if "subdirectory" in package_desc:
                            base_build_url += (
                                "#subdirectory=%s" % package_desc["subdirectory"]
                            )
                            base_pkg_url += "/%s" % package_desc["subdirectory"]
                        cache_base_url = PipCachePackage.make_partial_cache_url(
                            base_pkg_url, is_real_url=False
                        )
                        to_build_pkg_info[cache_base_url] = {
                            "build_url": base_build_url,
                            # We get the name once we build the package
                            "spec": PipPackageSpecification(
                                _FAKE_WHEEL,
                                base_pkg_url,
                                is_real_url=False,
                                url_format=".whl",
                            ),
                        }
                    elif "dir_info" in package_desc:
                        # URL starts with file://
                        local_path = package_desc["url"][7:]
                        if os.path.isdir(local_path):
                            # For now support only setup.py packages.
                            if not os.path.isfile(os.path.join(local_path, "setup.py")):
                                raise CondaException(
                                    "Local directory '%s' is not supported as it is "
                                    "missing a 'setup.py'" % local_path
                                )
                            package_name, package_version = (
                                self._call_binary(
                                    [
                                        os.path.join(local_path, "setup.py"),
                                        "-q",
                                        "--name",
                                        "--version",
                                    ],
                                    binary=sys.executable,
                                )
                                .decode(encoding="utf-8")
                                .splitlines()
                            )
                            local_path = os.path.join(
                                package_desc["url"],
                                "%s-%s.whl" % (package_name, package_version),
                            )
                        parse_result = parse_explicit_path_pip(local_path)
                        cache_base_url = PipCachePackage.make_partial_cache_url(
                            parse_result.url, is_real_url=False
                        )

                        to_build_pkg_info[cache_base_url] = {
                            "build_url": package_desc["url"],
                            # We get the name once we build the package
                            "spec": PipPackageSpecification(
                                _FAKE_WHEEL,
                                parse_result.url,
                                is_real_url=False,
                                url_format=".whl",
                            ),
                        }
                    else:
                        # Just a regular .tar.gz or .zip package
                        url_parse_result = urlparse(cast(str, package_desc["url"]))

                        is_real_url = False
                        if url_parse_result.scheme == "file":
                            parse_result = parse_explicit_path_pip(package_desc["url"])
                            cache_base_url = PipCachePackage.make_partial_cache_url(
                                parse_result.url, is_real_url=False
                            )
                        else:
                            # We don't have the hash so we ignore.
                            parse_result = parse_explicit_url_pip(
                                "%s#" % package_desc["url"]
                            )
                            cache_base_url = PipCachePackage.make_partial_cache_url(
                                parse_result.url, is_real_url=True
                            )
                            is_real_url = True

                        spec = PipPackageSpecification(
                            parse_result.filename,
                            parse_result.url,
                            is_real_url=is_real_url,
                            url_format=parse_result.url_format,
                        )
                        to_build_pkg_info[cache_base_url] = {
                            "build_url": package_desc["url"],
                            "spec": spec,
                        }
                        if not is_real_url:
                            # We have a local file for this tar-ball
                            spec.add_local_file(
                                parse_result.url_format, url_parse_result.path
                            )
                            to_build_pkg_info[cache_base_url]["found"] = [
                                parse_result.url_format
                            ]

                if self._storage:
                    built_pip_packages, builder_env = self._build_pip_packages(
                        python_version,
                        to_build_pkg_info,
                        builder_env,
                        pip_dir,
                        architecture,
                        supported_tags,
                    )
                    result.extend(built_pip_packages)
                else:
                    non_relocatable_packages = [
                        k for k, v in to_build_pkg_info.items() if not v["url"]
                    ]
                    if non_relocatable_packages:
                        raise CondaException(
                            "Cannot create a relocatable environment as it depends on "
                            "local files or non tarballs: %s"
                            % ", ".join(non_relocatable_packages)
                        )
                    result.extend(
                        [
                            cast(PackageSpecification, v["spec"])
                            for v in to_build_pkg_info.values()
                        ]
                    )
        return result, builder_env

    def _resolve_env_with_conda_lock(
        self,
        deps: Sequence[TStr],
        channels: Sequence[TStr],
        architecture: str,
        builder_env: Optional[ResolvedEnvironment],
        base_env: Optional[ResolvedEnvironment],
    ) -> Tuple[List[PackageSpecification], Optional[ResolvedEnvironment]]:
        outfile_name = None
        my_arch = arch_id()
        if any([d.category not in ("pip", "conda", "npconda") for d in deps]):
            raise CondaException(
                "Cannot resolve dependencies that include non-Conda/Pip dependencies: %s"
                % "; ".join(map(str, deps))
            )
        if base_env:
            local_packages = [
                p for p in base_env.packages if not p.is_downloadable_url()
            ]
            if local_packages:
                # We actually only care about things that are not online. Derived packages
                # are OK because we can reconstruct them if needed (or they may even
                # be cached)

                raise CondaException(
                    "Local PIP packages are not supported in MIXED mode: %s"
                    % ", ".join([p.package_name for p in local_packages])
                )

        self._start_micromamba_server()

        def _poetry_exec(cmd: str, *args: str):
            # Execute where conda-lock is installed since we are using that
            # anyways
            if CONDA_LOCAL_PATH:
                python_exec = os.path.join(CONDA_LOCAL_PATH, "bin", "python")
            else:
                python_exec = sys.executable
            try:
                arg_list = [
                    python_exec,
                    "-c",
                    cmd,
                    *args,
                ]
                debug.conda_exec("Poetry repo management call: %s" % " ".join(arg_list))
                subprocess.check_output(arg_list, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                print(
                    "Pretty-printed STDOUT:\n%s" % e.output.decode("utf-8")
                    if e.output
                    else "<None>",
                    file=sys.stderr,
                )
                print(
                    "Pretty-printed STDERR:\n%s" % e.stderr.decode("utf-8")
                    if e.stderr
                    else "<None>",
                    file=sys.stderr,
                )
                raise CondaException(
                    "Could not manage poetry's dependency using '{cmd}' -- got error"
                    "code {code}'; see pretty-printed error above".format(
                        cmd=e.cmd, code=e.returncode
                    )
                )

        pip_channels = (
            [CONDA_DEFAULT_PIP_SOURCE] if CONDA_DEFAULT_PIP_SOURCE else []
        ) + [
            c.value for c in channels if c.category == "pip"
        ]  # type: List[str]
        salt = str(uuid.uuid4())[:8]
        try:
            # We resolve the environment using conda-lock

            # Write out the TOML file. It's easy enough that we don't use another tool
            # to write it out. We use TOML so that we can disable pypi if needed

            pip_deps = [d.value for d in deps if d.category == "pip"]
            conda_deps = [d.value for d in deps if d.category in ("conda", "npconda")]
            # We only add pip if not present
            if not any([d.startswith("pip==") for d in conda_deps]):
                conda_deps.append("pip")
            toml_lines = [
                "[build-system]\n",
                'requires = ["poetry>=0.12"]\n',
                'build-backend = "poetry.masonry.api"\n',
                "\n" "[tool.conda-lock]\n",
            ]
            # Add channels
            all_channels = [c.value for c in channels if c.category == "conda"]
            for c in self._info["channels"]:
                all_channels.append(c.replace(my_arch, architecture))

            toml_lines.append(
                "channels = [%s]\n" % ", ".join(["'%s'" % c for c in all_channels])
            )

            if CONDA_DEFAULT_PIP_SOURCE:
                toml_lines.append("allow-pypi-requests = false\n")

            toml_lines.append("\n")
            if any(["::" in conda_deps]) or any(
                [c.value for c in channels if c.category == "conda"]
            ):
                addl_env = {"CONDA_CHANNEL_PRIORITY": "flexible"}
            else:
                addl_env = {}
            # For poetry unfortunately, conda-lock does not support setting the
            # sources manually so we need to actually update the config.
            # We do this using the vendored version of poetry in conda_lock because
            # poetry may not be installed and, more importantly, there has been
            # some change in where the config file lives on mac so if there is
            # a version mismatch, if we set with poetry, conda-lock may not be able
            # to read it.
            if pip_channels:
                # This works with 1.1.15 which is what is bundled in conda-lock.
                # In newer versions, there is a Config.create() directly
                # TODO: Check what to do based on version of conda-lock if conda-lock
                # changes the bundled version.
                python_cmd = (
                    "import json; import sys; "
                    "from pathlib import Path; "
                    "from conda_lock._vendor.poetry.factory import Factory; "
                    "translation_table = str.maketrans(':/.', '___'); "
                    "poetry_config = Factory.create_config(); "
                    "Path(poetry_config.config_source.name).parent.mkdir(parents=True, exist_ok=True); "
                    "channels = json.loads(sys.argv[1]); "
                    "[poetry_config.config_source.add_property("
                    "'repositories.metaflow_inserted%s_%%s.url' %% "
                    "c.translate(translation_table), c) "
                    "for c in channels]" % salt
                )
                _poetry_exec(python_cmd, json.dumps(pip_channels))

            # Add deps
            toml_lines.append("[tool.conda-lock.dependencies]\n")
            for d in conda_deps:
                splits = d.split("==", 1)
                if len(splits) == 2:
                    toml_lines.append('"%s" = "%s"\n' % (splits[0], splits[1]))
                else:
                    toml_lines.append('"%s" = "*"\n' % d)
            toml_lines.append("\n")
            toml_lines.append("[tool.poetry.dependencies]\n")
            # In some cases (when we build packages), we may actually have the same
            # dependency multiple times. We keep just the URL one in this case
            pip_dep_lines = {}  # type: Dict[str, Dict[str, str]]
            for d in pip_deps:
                splits = d.split("==", 1)
                # Here we re-parse the requirement. It will be one of the four options:
                #  - <package_name>
                #  - <package_name>[extras]
                #  - <package_name>@<url>
                #  - <package_name>[extras]@<url>
                parsed_req = Requirement(splits[0])
                if parsed_req.extras:
                    extra_part = "extras = [%s]," % ", ".join(
                        ['"%s"' % e for e in parsed_req.extras]
                    )
                else:
                    extra_part = ""

                version_str = splits[1] if len(splits) == 2 else "*"
                if parsed_req.url:
                    if len(splits) == 2:
                        raise CondaException(
                            "Unexpected version on URL requirement %s" % splits[0]
                        )
                    pip_dep_lines.setdefault(parsed_req.name, {}).update(
                        {"url": parsed_req.url, "url_extras": extra_part}
                    )
                else:
                    pip_dep_lines.setdefault(parsed_req.name, {}).update(
                        {"version": version_str, "extras": extra_part}
                    )
            for pip_name, info in pip_dep_lines.items():
                if "url" in info:
                    toml_lines.append(
                        '"%s" = {url = "%s", %s source="pypi"}\n'
                        % (pip_name, info["url"], info["url_extras"])
                    )
                else:
                    toml_lines.append(
                        '"%s" = {version = "%s", %s source="pypi"}\n'
                        % (pip_name, info["version"], info["extras"])
                    )

            assert self._bins

            with tempfile.TemporaryDirectory() as conda_lock_dir:
                outfile_name = "/tmp/conda-lock-gen-%s" % os.path.basename(
                    conda_lock_dir
                )

                args = [
                    "lock",
                    "-f",
                    "pyproject.toml",
                    "-p",
                    architecture,
                    "--filename-template",
                    outfile_name,
                    "-k",
                    "explicit",
                    "--conda",
                ]
                if "micromamba_server" in self._bins:
                    args.extend([self._bins["micromamba_server"], "--micromamba"])
                else:
                    args.extend(
                        [
                            self._bins[self._conda_executable_type],
                            "--%s" % self._conda_executable_type,
                        ]
                    )

                # If arch_id() == architecture, we also use the same virtual packages
                # as the ones that exist on the machine to mimic the current behavior
                # of conda/mamba
                if arch_id() == architecture:
                    lines = ["subdirs:\n", "  %s:\n" % architecture, "    packages:\n"]
                    if "virtual_pkgs" in self._info:
                        virtual_pkgs = self._info["virtual_pkgs"]
                        lines.extend(
                            [
                                "      %s: %s-%s\n" % (pkg_name, pkg_version, pkg_id)
                                for pkg_name, pkg_version, pkg_id in virtual_pkgs
                                if pkg_name != "__glibc"
                            ]
                        )
                    elif "virtual packages" in self._info:
                        # Micromamba does its own thing
                        virtual_pkgs = self._info["virtual packages"]
                        for virtpkg in virtual_pkgs:
                            pkg_name, pkg_version, pkg_id = virtpkg.split("=")
                            if pkg_name != "__glibc":
                                lines.append(
                                    "      %s: %s-%s\n"
                                    % (pkg_name, pkg_version, pkg_id)
                                )

                    with open(
                        os.path.join(conda_lock_dir, "virtual_yml.spec"),
                        mode="w",
                        encoding="ascii",
                    ) as virtual_yml:
                        virtual_yml.writelines(lines)
                    args.extend(["--virtual-package-spec", "virtual_yml.spec"])

                with WithDir(conda_lock_dir):
                    # conda-lock will only consider a `pyproject.toml` as a TOML file which
                    # is somewhat annoying.
                    with open(
                        "pyproject.toml", mode="w", encoding="ascii"
                    ) as input_toml:
                        input_toml.writelines(toml_lines)
                        debug.conda_exec(
                            "TOML configuration:\n%s" % "".join(toml_lines)
                        )
                    self._call_binary(args, binary="conda-lock", addl_env=addl_env)
            # At this point, we need to read the explicit dependencies in the file created
            emit = False
            result = []  # type: List[PackageSpecification]
            packages_to_build = {}  # type: Dict[str, Any]
            with open(outfile_name, "r", encoding="utf-8") as out:
                for l in out:
                    if emit:
                        if l.startswith("#"):
                            components = l.split()
                            # Line should be # pip <pkg> @ <url>
                            if len(components) != 5:
                                raise CondaException(
                                    "Unexpected package specification line: %s" % l
                                )
                            parse_result = parse_explicit_url_pip(components[4])
                            if parse_result.url_format != ".whl":
                                cache_base_url = PipCachePackage.make_partial_cache_url(
                                    parse_result.url, is_real_url=True
                                )
                                packages_to_build[cache_base_url] = {
                                    "build_url": parse_result.url,
                                    "spec": PipPackageSpecification(
                                        parse_result.filename,
                                        parse_result.url,
                                        is_real_url=True,
                                        url_format=parse_result.url_format,
                                    ),
                                }
                            else:
                                result.append(
                                    PipPackageSpecification(
                                        parse_result.filename,
                                        parse_result.url,
                                        url_format=parse_result.url_format,
                                        hashes={
                                            parse_result.url_format: parse_result.hash
                                        }
                                        if parse_result.hash
                                        else None,
                                    )
                                )
                        else:
                            parse_result = parse_explicit_url_conda(l.strip())
                            result.append(
                                CondaPackageSpecification(
                                    parse_result.filename,
                                    parse_result.url,
                                    url_format=parse_result.url_format,
                                    hashes={
                                        parse_result.url_format: cast(
                                            str, parse_result.hash
                                        )
                                    },
                                )
                            )
                    if not emit and l.strip() == "@EXPLICIT":
                        emit = True
            if packages_to_build:
                with tempfile.TemporaryDirectory() as build_dir:
                    python_version = None  # type: Optional[str]
                    for p in result:
                        if p.filename.startswith("python-"):
                            python_version = p.package_version
                            break
                    if python_version is None:
                        raise CondaException(
                            "Could not determine version of Python from conda packages"
                        )
                    supported_tags = pip_tags_from_arch(python_version, architecture)
                    if self._storage:
                        built_pip_packages, builder_env = self._build_pip_packages(
                            python_version,
                            packages_to_build,
                            builder_env,
                            build_dir,
                            architecture,
                            supported_tags,
                        )
                        result.extend(built_pip_packages)
                    else:
                        # Here it was just URLs so we are good
                        result.extend(
                            [
                                cast(PackageSpecification, v["spec"])
                                for v in packages_to_build.values()
                            ]
                        )
            return result, builder_env
        finally:
            if outfile_name and os.path.isfile(outfile_name):
                os.unlink(outfile_name)
            if pip_channels:
                # Clean things up in poetry
                python_cmd = (
                    "from pathlib import Path; "
                    "from conda_lock._vendor.poetry.factory import Factory; "
                    "poetry_config = Factory.create_config(); "
                    "Path(poetry_config.config_source.name).parent.mkdir(parents=True, exist_ok=True); "
                    "[poetry_config.config_source.remove_property('repositories.%%s' %% p) for p in "
                    "poetry_config.all().get('repositories', {}) "
                    "if p.startswith('metaflow_inserted%s_')]; " % salt
                )
                _poetry_exec(python_cmd)

    def _build_builder_env(
        self, deps: Sequence[TStr], sources: Sequence[TStr], architecture: str
    ) -> ResolvedEnvironment:

        python_dep = [
            d for d in deps if d.category == "conda" and d.value.startswith("python==")
        ]
        conda_only_sources = [s for s in sources if s.category == "conda"]

        if arch_id() == architecture:
            conda_only_deps = [d for d in deps if d.category == "npconda"] + python_dep
            debug.conda_exec(
                "Building builder environment with %s" % str(conda_only_deps)
            )
            packages, _ = self._resolve_env_with_conda(
                conda_only_deps, conda_only_sources, architecture, None, None
            )

            return ResolvedEnvironment(
                conda_only_deps,
                conda_only_sources,
                None,
                architecture,
                all_packages=packages,
                env_type=EnvType.CONDA_ONLY,
            )
        debug.conda_exec("Using vanilla builder env with %s" % str(python_dep[0]))
        python_only_packages, _ = self._resolve_env_with_conda(
            python_dep, conda_only_sources, arch_id(), None, None
        )
        return ResolvedEnvironment(
            python_dep,
            conda_only_sources,
            None,
            arch_id(),
            all_packages=python_only_packages,
            env_type=EnvType.CONDA_ONLY,
        )

    def _build_pip_packages(
        self,
        python_version: str,
        to_build_pkg_info: Dict[str, Any],
        builder_env: Optional[ResolvedEnvironment],
        build_dir: str,
        architecture: str,
        supported_tags: List[Tag],
    ) -> Tuple[List[PackageSpecification], Optional[ResolvedEnvironment]]:

        # We check in the cache -- we don't actually have the filename or
        # hash so we check things starting with the partial URL.
        # The URL in cache will be:
        #  - <base url>/<filename>/<hash>/<filename>

        debug.conda_exec(
            "Checking for pre-built packages: %s"
            % ", ".join(
                ["%s @ %s" % (v["spec"], k) for k, v in to_build_pkg_info.items()]
            )
        )
        found_files = self._storage.list_content(to_build_pkg_info.keys())

        keys_to_check = set()  # type: Set[str]

        # Key: key in to_build_pkg_info
        # Value: list of possible cache paths
        possible_wheels = {}  # type: Dict[str, List[str]]
        for cache_path, is_file in found_files:
            cache_path = cast(str, cache_path).rstrip("/")
            is_file = cast(bool, is_file)
            if is_file:
                raise CondaException("Invalid cache content at '%s'" % cache_path)
            keys_to_check.add(cache_path)
            base_cache_path, cache_filename_with_ext = os.path.split(cache_path)
            cache_format = os.path.splitext(cache_filename_with_ext)[1]
            if cache_format != ".whl":
                # This is a source format -- we add it to the keys_to_check so we can
                keys_to_check.add(cache_path)
            else:
                # There may be multiple wheel files so we want to pick the best one
                # so we record for now and then we will pick the best one.
                possible_wheels.setdefault(base_cache_path, []).append(cache_path)
            debug.conda_exec("Found potential pre-built package at '%s'" % cache_path)

        # We now check and pick the best wheel if one is compatible and then we will
        # check it further
        for key, wheel_potentials in possible_wheels.items():
            for t in supported_tags:
                # Tags are ordered from most-preferred to least preferred
                for p in wheel_potentials:
                    # Potentials are in no particular order but we will
                    # effectively get a package with the most preferred tag
                    # if one exists
                    wheel_name = os.path.split(p)[1]
                    _, _, _, tags = parse_wheel_filename(wheel_name)
                    if t in tags:
                        keys_to_check.add(p)
                        debug.conda_exec("%s: matching package @ %s" % (key, p))
                        break
                else:
                    # If we don't find a match, continue to next tag (and
                    # skip break of outer loop on next line)
                    continue
                break

        # We now check for hashes for those packages we did find (it's the
        # next level down in the cache)
        found_files = self._storage.list_content(keys_to_check)
        for cache_path, is_file in found_files:
            cache_path = cast(str, cache_path).rstrip("/")
            is_file = cast(bool, is_file)
            if is_file:
                raise CondaException("Invalid cache content at '%s'" % cache_path)
            head, _ = os.path.split(cache_path)
            base_cache_path, cache_filename_with_ext = os.path.split(head)
            cache_filename, cache_format = correct_splitext(cache_filename_with_ext)

            pkg_info = to_build_pkg_info[base_cache_path]
            pkg_spec = cast(PipPackageSpecification, pkg_info["spec"])
            pkg_info.setdefault("found", []).append(cache_format)
            if cache_format == ".whl":
                # In some cases, we don't know the filename so we change it here (or
                # we need to update it since a tarball has a generic name without
                # ABI, etc but a wheel name has more information)
                if pkg_spec.filename != cache_filename:
                    pkg_spec = pkg_spec.clone_with_filename(cache_filename)
                    pkg_info["spec"] = pkg_spec
            debug.conda_exec(
                "%s:%s adding cache file %s"
                % (
                    pkg_spec.filename,
                    cache_format,
                    os.path.join(cache_path, cache_filename_with_ext),
                )
            )
            pkg_spec.add_cached_version(
                cache_format,
                PipCachePackage(os.path.join(cache_path, cache_filename_with_ext)),
            )

        if arch_id() != architecture:
            # We can't build here so we make sure we have at least something for each
            # pip package; either we have something in cache (a source of wheel or both)
            # or we have an actual URL pointing to a source tarball.
            not_in_cache_or_local = [
                k for k, v in to_build_pkg_info.items() if not v.get("found")
            ]

            not_downloadable = [
                k
                for k, v in to_build_pkg_info.items()
                if not cast(PackageSpecification, v["spec"]).is_downloadable_url()
            ]
            no_info = set(not_in_cache_or_local).intersection(not_downloadable)
            if no_info:
                raise CondaException(
                    "Cannot build PIP package across architectures. "
                    "Requirements would have us build: %s. "
                    "This may be because you are specifying non wheel dependencies or "
                    "no wheel dependencies exist." % ", ".join(no_info)
                )
            return [
                cast(PackageSpecification, v["spec"])
                for v in to_build_pkg_info.values()
            ], builder_env

        # Determine what we need to build -- all non wheels
        keys_to_build = [
            k for k, v in to_build_pkg_info.items() if ".whl" not in v.get("found", [])
        ]

        if not keys_to_build:
            return [
                cast(PackageSpecification, v["spec"])
                for v in to_build_pkg_info.values()
            ], builder_env

        debug.conda_exec(
            "Going to build packages %s"
            % ", ".join([to_build_pkg_info[k]["spec"].filename for k in keys_to_build])
        )
        # Here we are the same architecture so we can go ahead and build the wheel and
        # add it.
        self._echo(" (building PIP packages from repositories)", nl=False)
        debug.conda_exec("Creating builder environment to build PIP packages")

        # Create the environment in which we will call pip
        if not builder_env:
            builder_env = self._build_builder_env(
                [TStr(category="conda", value="python==%s" % python_version)],
                [],
                architecture,
            )
        techo = self._echo
        self._echo = self._no_echo
        self.create_for_name(
            self._env_builder_directory_from_envid(builder_env.env_id),
            builder_env,
        )
        self._echo = techo

        builder_python = cast(
            str,
            self.python(self._env_builder_directory_from_envid(builder_env.env_id)),
        )

        # Download any source either from cache or the web. We can use our typical
        # lazy fetch to do this. We just make sure that we only pass it packages that
        # it has something to fetch
        target_directory = self._package_dirs[0]
        os.makedirs(os.path.join(target_directory, "pip"), exist_ok=True)
        pkgs_to_fetch = cast(
            List[PipPackageSpecification],
            [to_build_pkg_info[k]["spec"] for k in keys_to_build],
        )
        pkgs_to_fetch = list(
            filter(
                lambda x: x.is_downloadable_url() or x.cached_version(x.url_format),
                pkgs_to_fetch,
            )
        )
        debug.conda_exec(
            "Going to fetch sources for %s"
            % ", ".join([p.filename for p in pkgs_to_fetch])
        )
        if pkgs_to_fetch:
            self._lazy_fetch_packages(pkgs_to_fetch, target_directory)

        def _build_with_pip(
            identifier: int, key: str, spec: PipPackageSpecification, build_url: str
        ):
            dest_path = os.path.join(build_dir, "build_%d" % identifier)
            src = spec.local_file(spec.url_format) or build_url
            debug.conda_exec("%s: building from '%s' in '%s'" % (key, src, dest_path))

            self._call_binary(
                [
                    "-m",
                    "pip",
                    "--isolated",
                    "wheel",
                    "--no-deps",
                    "--progress-bar",
                    "off",
                    "-w",
                    dest_path,
                    src,
                ],
                binary=builder_python,
            )
            return key, dest_path

        with ThreadPoolExecutor() as executor:
            build_result = [
                executor.submit(
                    _build_with_pip,
                    idx,
                    key,
                    cast(PipPackageSpecification, to_build_pkg_info[key]["spec"]),
                    cast(str, to_build_pkg_info[key]["build_url"]),
                )
                for idx, key in enumerate(keys_to_build)
            ]
            for f in as_completed(build_result):
                key, build_dir = f.result()
                wheel_files = [
                    f
                    for f in os.listdir(build_dir)
                    if os.path.isfile(os.path.join(build_dir, f)) and f.endswith(".whl")
                ]
                if len(wheel_files) != 1:
                    raise CondaException(
                        "Could not build '%s' -- found built packages: %s"
                        % (key, wheel_files)
                    )

                pkg_spec = cast(PipPackageSpecification, to_build_pkg_info[key]["spec"])
                wheel_file = os.path.join(build_dir, wheel_files[0])
                # Move the built wheel to a less temporary location
                wheel_file = shutil.copy(
                    wheel_file, os.path.join(target_directory, "pip")
                )

                parse_result = parse_explicit_path_pip("file://%s" % wheel_file)
                # If the source is not an actual URL, we are going to change the name
                # of the package to avoid any potential conflict. We consider that
                # packages derived from internet URLs (so likely a source package)
                # do not need name changes
                if not pkg_spec.is_downloadable_url():
                    pkg_version = parse_wheel_filename(parse_result.filename + ".whl")[
                        1
                    ]

                    pkg_version_str = str(pkg_version)
                    if not pkg_version.dev:
                        wheel_hash = PipPackageSpecification.hash_pkg(wheel_file)
                        pkg_version_str += ".dev" + wheel_hash[:8].translate(_DEV_TRANS)
                    pkg_version_str += "+mfbuild"
                    wheel_file = change_pip_package_version(wheel_file, pkg_version_str)
                    parse_result = parse_explicit_path_pip("file://%s" % wheel_file)

                debug.conda_exec("Package for '%s' built in '%s'" % (key, wheel_file))

                # We update because we need to change the filename mostly so that it
                # now reflects the abi, etc and all that goes in a wheel filename.
                pkg_spec = pkg_spec.clone_with_filename(parse_result.filename)
                to_build_pkg_info[key]["spec"] = pkg_spec
                pkg_spec.add_local_file(".whl", wheel_file)

        return [
            cast(PackageSpecification, v["spec"]) for v in to_build_pkg_info.values()
        ], builder_env

    def _find_conda_binary(self):
        # Lock as we may be trying to resolve multiple environments at once and therefore
        # we may be trying to validate the installation multiple times.
        with CondaLock(self._echo, "/tmp/mf-conda-check.lock"):
            if self._found_binaries:
                return
            if self._resolvers[EnvType.CONDA_ONLY] not in _CONDA_DEP_RESOLVERS:
                raise CondaException(
                    "Invalid Conda dependency resolver %s, valid candidates are %s."
                    % (self._resolvers[EnvType.CONDA_ONLY], _CONDA_DEP_RESOLVERS)
                )
            if self._mode == "local":
                self._ensure_local_conda()
            else:
                # Remote mode -- we install a conda environment or make sure we have
                # one already there
                self._ensure_remote_conda()
                err = self._validate_conda_installation()
                if err:
                    raise err
            self._found_binaries = True

    def _ensure_local_conda(self):
        self._conda_executable_type = cast(str, self._resolvers[EnvType.CONDA_ONLY])
        if CONDA_LOCAL_PATH is not None:
            # We need to look in a specific place
            self._bins = {
                self._conda_executable_type: os.path.join(
                    CONDA_LOCAL_PATH, "bin", self._conda_executable_type
                ),
                "conda-lock": os.path.join(CONDA_LOCAL_PATH, "bin", "conda-lock"),
                "micromamba": os.path.join(CONDA_LOCAL_PATH, "bin", "micromamba"),
                "cph": os.path.join(CONDA_LOCAL_PATH, "bin", "cph"),
                "pip": os.path.join(CONDA_LOCAL_PATH, "bin", "pip"),
            }
            self._bins["conda"] = self._bins[self._conda_executable_type]
            if self._validate_conda_installation():
                # This means we have an exception so we are going to try to install
                with CondaLock(
                    self._echo,
                    os.path.abspath(
                        os.path.join(CONDA_LOCAL_PATH, "..", ".conda-install.lock")
                    ),
                ):
                    if self._validate_conda_installation():
                        self._install_local_conda()
        else:
            self._bins = {
                self._conda_executable_type: which(self._conda_executable_type),
                "conda-lock": which("conda-lock"),
                "micromamba": which("micromamba"),
                "cph": which("cph"),
                "pip": which("pip"),
            }
            self._bins["conda"] = self._bins[self._conda_executable_type]

    def _install_local_conda(self):
        from metaflow.plugins import DATASTORES

        start = time.time()
        path = CONDA_LOCAL_PATH  # type: str
        self._echo("    Installing Conda environment at %s ..." % path, nl=False)
        shutil.rmtree(path, ignore_errors=True)

        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        path_to_fetch = os.path.join(
            CONDA_LOCAL_DIST_DIRNAME,
            CONDA_LOCAL_DIST.format(arch=arch_id()),
        )
        debug.conda_exec(
            "Fetching remote conda distribution at %s"
            % os.path.join(get_conda_root(self._datastore_type), path_to_fetch)
        )
        storage = [d for d in DATASTORES if d.TYPE == self._datastore_type][0](
            get_conda_root(self._datastore_type)
        )  # type: DataStoreStorage
        with tempfile.NamedTemporaryFile() as tmp:
            with storage.load_bytes([path_to_fetch]) as load_results:
                for _, tmpfile, _ in load_results:
                    if tmpfile is None:
                        raise CondaException(
                            "Cannot find Conda installation tarball '%s'"
                            % os.path.join(
                                get_conda_root(self._datastore_type), path_to_fetch
                            )
                        )
                    shutil.move(tmpfile, tmp.name)
            try:
                tar = tarfile.open(tmp.name)
                tar.extractall(path)
                tar.close()
            except Exception as e:
                raise CondaException("Could not extract environment: %s" % str(e))
        delta_time = int(time.time() - start)
        self._echo(
            " done in %d second%s." % (delta_time, plural_marker(delta_time)),
            timestamp=False,
        )

        # We write a file to say that the local conda installation is good to go. We can
        # use this to check if the installation was complete in case multiple processes
        # try to check at the same time.
        with open(
            os.path.join(path, ".metaflow-local-env"), mode="w", encoding="utf-8"
        ) as f:
            json.dump(
                {
                    "src": os.path.join(
                        get_conda_root(self._datastore_type), path_to_fetch
                    ),
                    "install_time": datetime.fromtimestamp(time.time()).isoformat(),
                },
                f,
            )

    def _ensure_remote_conda(self):
        if CONDA_REMOTE_INSTALLER is not None:
            self._install_remote_conda()
        else:
            # If we don't have a REMOTE_INSTALLER, we check if we need to install one
            args = [
                "/bin/bash",
                "-c",
                "if ! type micromamba  >/dev/null 2>&1; then "
                "mkdir -p ~/.local/bin >/dev/null 2>&1; "
                "curl -Ls https://micro.mamba.pm/api/micromamba/%s/latest | "
                "tar -xvj -C ~/.local/bin/ --strip-components=1 bin/micromamba >/dev/null 2>&1; "
                "echo $HOME/.local/bin/micromamba; "
                "else which micromamba; fi" % arch_id(),
            ]
            self._bins = {
                "conda": subprocess.check_output(args).decode("utf-8").strip()
            }
            self._bins["micromamba"] = self._bins["conda"]
            self._conda_executable_type = "micromamba"

    def _install_remote_conda(self):
        from metaflow.plugins import DATASTORES

        # We download the installer and return a path to it
        final_path = os.path.join(os.getcwd(), "__conda_installer")

        path_to_fetch = os.path.join(
            CONDA_REMOTE_INSTALLER_DIRNAME,
            CONDA_REMOTE_INSTALLER.format(arch=arch_id()),
        )
        storage_opts = [d for d in DATASTORES if d.TYPE == self._datastore_type]
        if len(storage_opts) == 0:
            raise MetaflowException(
                msg="Downloading conda remote installer from backend %s is unimplemented!"
                % self._datastore_type
            )
        storage = storage_opts[0](
            get_conda_root(self._datastore_type)
        )  # type: DataStoreStorage
        with storage.load_bytes([path_to_fetch]) as load_results:
            for _, tmpfile, _ in load_results:
                if tmpfile is None:
                    raise MetaflowException(
                        msg="Cannot find Conda remote installer '%s'"
                        % os.path.join(
                            get_conda_root(self._datastore_type), path_to_fetch
                        )
                    )
                shutil.move(tmpfile, final_path)
        os.chmod(
            final_path,
            stat.S_IRUSR
            | stat.S_IXUSR
            | stat.S_IRGRP
            | stat.S_IXGRP
            | stat.S_IROTH
            | stat.S_IXOTH,
        )
        self._bins = {"conda": final_path, "micromamba": final_path}
        self._conda_executable_type = "micromamba"

    def _validate_conda_installation(self) -> Optional[Exception]:

        # If this is installed in CONDA_LOCAL_PATH look for special marker
        if self._mode == "local" and CONDA_LOCAL_PATH is not None:
            if not os.path.isfile(
                os.path.join(CONDA_LOCAL_PATH, ".metaflow-local-env")
            ):
                return InvalidEnvironmentException(
                    "Missing special marker .metaflow-local-env in locally installed environment"
                )
            # We consider that locally installed environments are OK
            return None

        # Check if the dependency solver exists.
        if self._bins is None:
            return InvalidEnvironmentException("No binaries configured for Conda")
        # We check we have what we need to resolve
        for resolver_type, resolver in self._resolvers.items():
            if resolver is None:
                continue
            if resolver not in self._bins:
                raise InvalidEnvironmentException(
                    "Unknown resolver '%s' for %s environments"
                    % (resolver, resolver_type.value)
                )
            elif self._bins[resolver] is None or not os.path.isfile(
                self._bins[resolver]
            ):
                if resolver_type == EnvType.CONDA_ONLY:
                    # We absolutely need a conda resolver for everything -- others
                    # are optional
                    return InvalidEnvironmentException(
                        self._install_message_for_resolver(resolver)
                    )
        # Check for other binaries
        to_remove = [
            k for k, v in self._bins.items() if v is None or not os.path.isfile(v)
        ]  # type: List[str]
        if to_remove:
            for k in to_remove:
                del self._bins[k]

        # Check version requirements
        if "cph" in self._bins:
            cph_version = (
                self._call_binary(["--version"], binary="cph")
                .decode("utf-8")
                .split()[-1]
            )
            if LooseVersion(cph_version) < LooseVersion("1.9.0"):
                self._echo(
                    "cph is installed but not recent enough (1.9.0 or later is required) "
                    "-- ignoring"
                )
                del self._bins["cph"]

        if "conda-lock" in self._bins:
            conda_lock_version = (
                self._call_binary(["--version"], binary="conda-lock")
                .decode("utf-8")
                .split()[-1]
            )
            if LooseVersion(conda_lock_version) < LooseVersion("2.0.0"):
                self._echo(
                    "conda-lock is installed but not recent enough (2.0.0 or later "
                    "is required) --ignoring"
                )
                del self._bins["conda-lock"]
        if "pip" in self._bins:
            pip_version = self._call_binary(["--version"], binary="pip").split(b" ", 2)[
                1
            ]
            # 22.3 has PEP 658 support which is a bit performance boost
            if LooseVersion(pip_version.decode("utf-8")) < LooseVersion("22.3"):
                self._echo(
                    "pip is installed but not recent enough (22.3 or later is required) "
                    "-- ignoring"
                )
                del self._bins["pip"]

        if "micromamba version" in self._info:
            if LooseVersion(self._info["micromamba version"]) < LooseVersion("1.4.0"):
                return InvalidEnvironmentException(
                    self._install_message_for_resolver("micromamba")
                )
        else:
            if LooseVersion(self._info["conda_version"]) < LooseVersion("4.14.0"):
                return InvalidEnvironmentException(
                    self._install_message_for_resolver(self._conda_executable_type)
                )
            # TODO: We should check mamba version too

        # TODO: Re-add support for micromamba server when it is actually out
        # Even if we have conda/mamba as the dependency solver, we can also possibly
        # use the micromamba server. Micromamba is typically used "as conda" only on
        # remote environments.
        # if "micromamba" in self._bins:
        #    try:
        #        self._call_binary(
        #            ["server", "--help"],
        #            binary="micromamba",
        #            pretty_print_exception=False,
        #        )
        #        self._have_micromamba_server = False  # FIXME: Check on version
        #    except CondaException:
        #        self._have_micromamba_server = False
        if self._mode == "local":
            # Check if conda-forge is available as a channel to pick up Metaflow's
            # dependencies.
            if "conda-forge" not in "\t".join(self._info["channels"]):
                return InvalidEnvironmentException(
                    "Conda channel 'conda-forge' is required. "
                    "Specify it with CONDA_CHANNELS environment variable."
                )

        return None

    def _is_valid_env(self, dir_name: str) -> Optional[EnvID]:
        mf_env_file = os.path.join(dir_name, ".metaflowenv")
        if os.path.isfile(mf_env_file):
            with open(mf_env_file, mode="r", encoding="utf-8") as f:
                env_info = json.load(f)
            debug.conda_exec("Found %s at dir_name %s" % (EnvID(*env_info), dir_name))
            return EnvID(*env_info)
        return None

    def _created_envs(
        self, prefix: str, full_match: bool = False
    ) -> Dict[EnvID, List[str]]:
        ret = {}  # type: Dict[EnvID, List[str]]

        debug.conda_exec(
            "Locating MF environment %s%s"
            % ("starting with " if not full_match else "", prefix)
        )

        def _check_match(dir_name: str) -> Optional[EnvID]:
            dir_lastcomponent = os.path.basename(dir_name)
            if (full_match and dir_lastcomponent == prefix) or (
                not full_match and dir_lastcomponent.startswith(prefix)
            ):
                possible_env_id = self._is_valid_env(dir_name)
                if possible_env_id:
                    return possible_env_id
                elif full_match:
                    self._echo(
                        "Removing potentially corrupt directory at %s" % dir_name
                    )
                    self._remove(os.path.basename(dir_name))
            return None

        if self._conda_executable_type == "micromamba" or CONDA_LOCAL_PATH is not None:
            # For micromamba OR if we are using a specific conda installation
            # (so with CONDA_LOCAL_PATH), only search there
            env_dir = os.path.join(self._info["root_prefix"], "envs")
            with CondaLock(self._echo, self._env_lock_file(os.path.join(env_dir, "_"))):
                # Grab a lock *once* on the parent directory so we pick anyname for
                # the "directory".
                for entry in os.scandir(env_dir):
                    if entry.is_dir():
                        possible_env_id = _check_match(entry.path)
                        if possible_env_id:
                            ret.setdefault(possible_env_id, []).append(entry.path)
        else:
            envs = self._info["envs"]  # type: List[str]
            for env in envs:
                with CondaLock(self._echo, self._env_lock_file(env)):
                    possible_env_id = _check_match(env)
                    if possible_env_id:
                        ret.setdefault(possible_env_id, []).append(env)
        return ret

    def _remote_env_fetch(
        self, env_ids: List[EnvID], ignore_co_resolved: bool = False
    ) -> List[Optional[ResolvedEnvironment]]:
        result = OrderedDict(
            {self.get_datastore_path_to_env(env_id): env_id for env_id in env_ids}
        )  # type: OrderedDict[str, Union[EnvID, ResolvedEnvironment]]
        addl_env_ids = []  # type: List[EnvID]
        with self._storage.load_bytes(result.keys()) as loaded:
            for key, tmpfile, _ in loaded:
                env_id = cast(EnvID, result[cast(str, key)])
                if tmpfile:
                    with open(tmpfile, mode="r", encoding="utf-8") as f:
                        resolved_env = ResolvedEnvironment.from_dict(
                            env_id, json.load(f)
                        )
                        result[cast(str, key)] = resolved_env
                        # We need to fetch the co-resolved ones as well since they
                        # may be requested
                        if (
                            not ignore_co_resolved
                            and len(resolved_env.co_resolved_archs) > 1
                        ):
                            addl_env_ids.extend(
                                [
                                    EnvID(
                                        req_id=resolved_env.env_id.req_id,
                                        full_id=resolved_env.env_id.full_id,
                                        arch=arch,
                                    )
                                    for arch in resolved_env.co_resolved_archs
                                    if arch != resolved_env.env_id.arch
                                ]
                            )
                        self._cached_environment.add_resolved_env(resolved_env)
                debug.conda_exec(
                    "%s%sfound remotely at %s"
                    % (str(env_id), " " if tmpfile else " not ", key)
                )
        if addl_env_ids:
            self._remote_env_fetch(addl_env_ids, ignore_co_resolved=True)
        return [
            x if isinstance(x, ResolvedEnvironment) else None for x in result.values()
        ]

    def _remote_fetch_alias(
        self, env_aliases: List[Tuple[AliasType, str]], arch: Optional[str] = None
    ) -> List[Optional[EnvID]]:
        arch = arch or arch_id()
        result = OrderedDict(
            {
                self.get_datastore_path_to_env_alias(alias_type, env_alias): (
                    alias_type,
                    env_alias,
                )
                for alias_type, env_alias in env_aliases
            }
        )  # type: OrderedDict[str, Optional[Union[Tuple[AliasType, str], EnvID]]]
        with self._storage.load_bytes(result.keys()) as loaded:
            for key, tmpfile, _ in loaded:
                alias_type, env_alias = cast(
                    Tuple[AliasType, str], result[cast(str, key)]
                )
                if tmpfile:
                    with open(tmpfile, mode="r", encoding="utf-8") as f:
                        req_id, full_id = json.load(f)
                    self._cached_environment.add_alias(
                        alias_type, env_alias, req_id, full_id
                    )
                    result[cast(str, key)] = EnvID(req_id, full_id, arch)
                debug.conda_exec(
                    "%s%sfound remotely at %s"
                    % (env_alias, " " if tmpfile else " not ", key)
                )
        return [x if isinstance(x, EnvID) else None for x in result.values()]

    # TODO: May not be needed
    def _remote_env_fetch_alias(
        self,
        env_aliases: List[Tuple[AliasType, str]],
        arch: Optional[str] = None,
    ) -> List[Optional[ResolvedEnvironment]]:
        arch = arch or arch_id()
        to_fetch = []  # type: List[EnvID]
        to_fetch_idx = []  # type: List[int]
        env_ids = self._remote_fetch_alias(env_aliases, arch)
        result = [None] * len(env_ids)  # type: List[Optional[ResolvedEnvironment]]

        # We may have the environments locally even if we didn't have the alias so
        # look there first before going remote again
        for idx, env_id in enumerate(env_ids):
            if env_id:
                e = self.environment(env_id, local_only=True)
                if e:
                    result[idx] = e
                else:
                    to_fetch_idx.append(idx)
                    to_fetch.append(env_id)

        # Now we look at all the other ones that we don't have locally but have
        # actual EnvIDs for
        found_envs = self._remote_env_fetch(to_fetch, ignore_co_resolved=True)
        idx = 0
        for idx, e in zip(to_fetch_idx, found_envs):
            result[idx] = e
        return result

    @property
    def _package_dirs(self) -> List[str]:
        info = self._info
        if self._conda_executable_type == "micromamba":
            pkg_dir = os.path.join(info["root_prefix"], "pkgs")
            if not os.path.exists(pkg_dir):
                os.makedirs(pkg_dir)
            return [pkg_dir]
        return info["pkgs_dirs"]

    @property
    def _root_env_dir(self) -> str:
        info = self._info
        if self._conda_executable_type == "micromamba":
            return os.path.join(info["root_prefix"], "envs")
        return info["envs_dirs"][0]

    @property
    def _info(self) -> Dict[str, Any]:
        if self._cached_info is None:
            self._cached_info = json.loads(self._call_conda(["info", "--json"]))
            # Micromamba is annoying because if there are multiple installations of it
            # executing the binary doesn't necessarily point us to the root directory
            # we are in so we kind of look for it heuristically
            if self._conda_executable_type == "micromamba":
                # Best info if we don't have something else
                self._cached_info["root_prefix"] = self._cached_info["base environment"]
                cur_dir = os.path.dirname(self._bins[self._conda_executable_type])
                while True:
                    if os.path.isdir(os.path.join(cur_dir, "pkgs")) and os.path.isdir(
                        os.path.join(cur_dir, "envs")
                    ):
                        self._cached_info["root_prefix"] = cur_dir
                        break
                    if cur_dir == "/":
                        break
                    cur_dir = os.path.dirname(cur_dir)

        return self._cached_info

    def _create(self, env: ResolvedEnvironment, env_name: str) -> None:

        # We first check to see if the environment exists -- if it does, we skip it
        env_dir = os.path.join(self._root_env_dir, env_name)

        self._cached_info = None

        if os.path.isdir(env_dir):
            possible_env_id = self._is_valid_env(env_dir)
            if possible_env_id and possible_env_id == env.env_id:
                # The environment is already created -- we can skip
                self._echo("Environment at '%s' already created and valid" % env_dir)
                return
            else:
                # Invalid environment
                if possible_env_id is None:
                    self._echo(
                        "Environment at '%s' is incomplete -- re-creating" % env_dir
                    )
                else:
                    self._echo(
                        "Environment at '%s' expected to be for %s (%s) but found %s (%s) -- re-creating"
                        % (
                            env_dir,
                            env.env_id.req_id,
                            env.env_id.full_id,
                            possible_env_id.req_id,
                            possible_env_id.full_id,
                        )
                    )
                self._remove(env_name)

        # We first get all the packages needed
        self._lazy_fetch_packages(
            env.packages, self._package_dirs[0], search_dirs=self._package_dirs
        )

        # We build the list of explicit URLs to pass to conda to create the environment
        # We know here that we have all the packages present one way or another, we just
        # need to format the URLs appropriately.
        explicit_urls = []  # type: List[str]
        pip_paths = []  # type: List[str]
        for p in env.packages:
            if p.TYPE == "pip":
                for f in p.allowed_formats():
                    local_path = p.local_file(f)
                    if local_path:
                        debug.conda_exec(
                            "For %s, using PIP package at '%s'"
                            % (p.filename, local_path)
                        )
                        pip_paths.append("%s\n" % local_path)
                        break
                else:
                    raise CondaException(
                        "Local file for package %s expected; looked at %s"
                        % (p.filename, ", ".join([f[1] for f in p.local_files]))
                    )
            elif p.TYPE == "conda":
                local_dir = p.local_dir
                found_local_dir = False
                if local_dir:
                    # If this is a local directory, we make sure we use the URL for that
                    # directory (so the conda system uses it properly)
                    debug.conda_exec(
                        "For %s, using local Conda directory at '%s'"
                        % (p.filename, local_dir)
                    )
                    with open(
                        os.path.join(local_dir, "info", "repodata_record.json"),
                        mode="r",
                        encoding="utf-8",
                    ) as f:
                        info = json.load(f)
                        # Some packages don't have a repodata_record.json with url so
                        # we will download again
                        if "url" in info and "md5" in info:
                            explicit_urls.append("%s#%s\n" % (info["url"], info["md5"]))
                            found_local_dir = True
                        else:
                            debug.conda_exec(
                                "Skipping local directory as no URL information"
                            )

                if not found_local_dir:
                    for f in CONDA_FORMATS:
                        cache_info = p.cached_version(f)
                        if cache_info:
                            debug.conda_exec(
                                "For %s, using cached package from '%s'"
                                % (p.filename, cache_info.url)
                            )
                            explicit_urls.append(
                                "%s#%s\n"
                                % (
                                    self._make_urlstxt_from_cacheurl(cache_info.url),
                                    cache_info.hash,
                                )
                            )
                            break
                    else:
                        debug.conda_exec(
                            "For %s, using package from '%s'" % (p.filename, p.url)
                        )
                        # Here we don't have any cache format so we just use the base URL
                        explicit_urls.append(
                            "%s#%s\n" % (p.url, p.pkg_hash(p.url_format))
                        )
            else:
                raise CondaException(
                    "Package of type %s is not supported in Conda environments" % p.TYPE
                )

        start = time.time()
        if "micromamba" not in self._bins:
            self._echo(
                "WARNING: conda/mamba do not properly handle installing .conda "
                "packages in offline mode. Creating environments may fail -- if so, "
                "please install `micromamba`. "
                "See https://github.com/conda/conda/issues/11775."
            )
        self._echo("    Extracting and linking Conda environment ...", nl=False)

        if pip_paths:
            self._echo(" (conda packages) ...", timestamp=False, nl=False)
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", delete=not debug.conda
        ) as explicit_list:
            # We create an explicit file
            lines = ["@EXPLICIT\n"] + explicit_urls
            explicit_list.writelines(lines)
            explicit_list.flush()
            self._call_binary(
                [
                    "create",
                    "--yes",
                    "--quiet",
                    "--offline",
                    "--no-deps",
                    "--name",
                    env_name,
                    "--file",
                    explicit_list.name,
                ],
                # Creating with micromamba is faster as it extracts in parallel. Prefer
                # it if it exists.
                binary="micromamba"
                if self._bins and "micromamba" in self._bins
                else "conda",
            )

        if pip_paths:
            self._echo(" (pip packages) ...", timestamp=False, nl=False)
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", delete=not debug.conda
            ) as pip_list:
                pip_list.writelines(pip_paths)
                pip_list.flush()
                python_exec = os.path.join(env_dir, "bin", "python")
                try:
                    arg_list = [
                        python_exec,
                        "-m",
                        "pip",
                        "install",
                        "--no-deps",
                        "--no-input",
                        "-r",
                        pip_list.name,
                    ]
                    debug.conda_exec("Pip call: %s" % " ".join(arg_list))
                    subprocess.check_output(arg_list, stderr=subprocess.STDOUT)
                except subprocess.CalledProcessError as e:
                    print(
                        "Pretty-printed STDOUT:\n%s" % e.output.decode("utf-8")
                        if e.output
                        else "No STDOUT",
                        file=sys.stderr,
                    )
                    print(
                        "Pretty-printed STDERR:\n%s" % e.stderr.decode("utf-8")
                        if e.stderr
                        else "No STDERR",
                        file=sys.stderr,
                    )
                    raise CondaException(
                        "Could not install pip dependencies using '{cmd}' -- got error"
                        "code {code}'; see pretty-printed error above".format(
                            cmd=e.cmd, code=e.returncode
                        )
                    )

        self._cached_info = None

        # We write a `.metaflowenv` file to be able to get back the env_id from it in
        # case the name doesn't contain it. We also write it at the end to be able to
        # better determine if an environment is corrupt (if conda succeeds but not pip)
        with open(
            os.path.join(env_dir, ".metaflowenv"), mode="w", encoding="utf-8"
        ) as f:
            json.dump(env.env_id, f)

        delta_time = int(time.time() - start)
        self._echo(
            " done in %s second%s." % (delta_time, plural_marker(delta_time)),
            timestamp=False,
        )

    def _remove(self, env_name: str):
        # TODO: Verify that this is a proper metaflow environment to remove
        self._cached_info = None
        self._call_conda(["env", "remove", "--name", env_name, "--yes", "--quiet"])

    def _upload_to_ds(
        self, files: List[Tuple[str, str]], overwrite: bool = False
    ) -> None:
        def paths_and_handles():
            for cache_path, local_path in files:
                with open(local_path, mode="rb") as f:
                    yield cache_path, f

        self._storage.save_bytes(
            paths_and_handles(), overwrite=overwrite, len_hint=len(files)
        )

    def _env_lock_file(self, env_directory: str):
        # env_directory is either a name or a directory -- if name, it is assumed
        # to be rooted at _root_env_dir
        parent_dir = os.path.split(env_directory)[0]
        if parent_dir == "":
            parent_dir = self._root_env_dir
        return os.path.join(parent_dir, "mf_env-creation.lock")

    @property
    def _package_dir_lockfile_name(self) -> str:
        return "mf_pkgs-update.lock"

    def _make_urlstxt_from_url(
        self,
        pkg_spec: PackageSpecification,
        file_format: Optional[str] = None,
    ):
        if not file_format or file_format == pkg_spec.url_format:
            return pkg_spec.url
        # If not, we return the path to the cached version of this
        return os.path.join(
            get_conda_root(self._datastore_type),
            CondaCachePackage.make_cache_url(
                pkg_spec.url,
                pkg_spec.filename,
                file_format,
                cast(str, pkg_spec.pkg_hash(file_format)),
            ),
        )

    def _make_urlstxt_from_cacheurl(self, cache_url: str) -> str:
        # Format for a cache URL is CONDA_PACKAGES_DIRNAME/conda/url/file/hash/file
        # If this is the actual file available, the url will end with file too. If not
        # it is a transmuted package and we just return the cache URL
        splits = cache_url.split("/")
        if splits[-1] != splits[-3]:
            raise ValueError("Invalid cache address: %s" % cache_url)
        if splits[-1] != splits[-4]:
            # This is not the real thing
            return os.path.join(
                get_conda_root(self._datastore_type),
                cast(str, CONDA_PACKAGES_DIRNAME),
                "conda",
                cache_url,
            )
        else:
            # This is the real thing -- we strip out the last 3 components and up until
            # "conda" since that is the last component before the url
            start_idx = 0
            while splits[start_idx] != "conda":
                start_idx += 1
            return "https://" + "/".join(splits[start_idx + 1 : -3])

    @staticmethod
    def _env_directory_from_envid(env_id: EnvID) -> str:
        return "metaflow_%s_%s" % (env_id.req_id, env_id.full_id)

    @staticmethod
    def _env_builder_directory_from_envid(env_id: EnvID) -> str:
        return "metaflow_builder_%s_%s" % (env_id.req_id, env_id.full_id)

    @staticmethod
    def _install_message_for_resolver(resolver: str) -> str:
        if resolver == "mamba":
            return (
                "Mamba version 1.4.0 or newer is required. "
                "Visit https://mamba.readthedocs.io/en/latest/installation.html "
                "for installation instructions."
            )
        elif resolver == "conda":
            return (
                "Conda version 4.14.0 or newer is required. "
                "Visit https://docs.conda.io/en/latest/miniconda.html "
                "for installation instructions."
            )
        elif resolver == "micromamba":
            return (
                "Micromamba version 1.4.0 or newer is required. "
                "Visit https://mamba.readthedocs.io/en/latest/installation.html "
                "for installation instructions."
            )
        else:
            return "Unknown resolver '%s'" % resolver

    def _call_conda(
        self,
        args: List[str],
        binary: str = "conda",
        addl_env: Optional[Mapping[str, str]] = None,
        pretty_print_exception: bool = True,
    ) -> bytes:
        if self._bins is None or self._bins[binary] is None:
            raise InvalidEnvironmentException("Binary '%s' unknown" % binary)
        try:
            env = {"CONDA_JSON": "True"}
            if self._conda_executable_type == "mamba":
                env.update({"MAMBA_NO_BANNER": "1", "MAMBA_JSON": "True"})
            if addl_env:
                env.update(addl_env)

            if (
                args
                and args[0] not in ("package", "info")
                and (
                    self._conda_executable_type == "micromamba"
                    or binary == "micromamba"
                )
            ):
                args.extend(["-r", self._info["root_prefix"], "--json"])
            debug.conda_exec("Conda call: %s" % str([self._bins[binary]] + args))
            return subprocess.check_output(
                [self._bins[binary]] + args,
                stderr=subprocess.PIPE,
                env=dict(os.environ, **env),
            ).strip()
        except subprocess.CalledProcessError as e:
            try:
                output = json.loads(e.output)
                if isinstance(output, dict) and "error" in output:
                    err = [output["err"]]  # type: List[str]
                else:
                    err = [output]  # type: List[str]
                for error in output.get("errors", []):
                    err.append(error["error"])
                if pretty_print_exception:
                    print(
                        "Pretty-printed exception:\n%s" % "\n".join(err),
                        file=sys.stderr,
                    )
                raise CondaException(
                    "Conda command '{cmd}' returned an error ({code}); "
                    "see pretty-printed error above".format(
                        cmd=e.cmd, code=e.returncode
                    )
                )
            except (TypeError, ValueError):
                pass
            print(
                "Pretty-printed STDOUT:\n%s" % e.output.decode("utf-8")
                if e.output
                else "No STDOUT",
                file=sys.stderr,
            )
            print(
                "Pretty-printed STDERR:\n%s" % e.stderr.decode("utf-8")
                if e.stderr
                else "No STDERR",
                file=sys.stderr,
            )
            raise CondaException(
                "Conda command '{cmd}' returned error ({code}); "
                "see pretty-printed error above".format(cmd=e.cmd, code=e.returncode)
            )

    def _call_binary(
        self,
        args: List[str],
        binary: str,
        addl_env: Optional[Mapping[str, str]] = None,
        pretty_print_exception: bool = True,
    ) -> bytes:
        if binary in _CONDA_DEP_RESOLVERS:
            return self._call_conda(args, binary, addl_env, pretty_print_exception)
        if self._bins and self._bins.get(binary) is not None:
            binary = cast(str, self._bins[binary])
        elif not os.path.isfile(binary) or not os.access(binary, os.X_OK):
            raise InvalidEnvironmentException("Binary '%s' unknown" % binary)
        if addl_env is None:
            addl_env = {}
        try:
            debug.conda_exec("Binary call: %s" % str([binary] + args))
            return subprocess.check_output(
                [binary] + args,
                stderr=subprocess.PIPE,
                env=dict(os.environ, **addl_env),
            ).strip()
        except subprocess.CalledProcessError as e:
            print(
                "Pretty-printed STDOUT:\n%s" % e.output.decode("utf-8")
                if e.output
                else "No STDOUT",
                file=sys.stderr,
            )
            print(
                "Pretty-printed STDERR:\n%s" % e.stderr.decode("utf-8")
                if e.stderr
                else "No STDERR",
                file=sys.stderr,
            )
            raise CondaException(
                "Binary command for Conda '{cmd}' returned error ({code}); "
                "see pretty-printed error above".format(cmd=e.cmd, code=e.returncode)
            )

    def _start_micromamba_server(self):
        if not self._have_micromamba_server:
            return
        if self._micromamba_server_port:
            return

        def _find_port():
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                s.bind(("", 0))
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                return s.getsockname()[1]

        assert self._bins
        attempt = 1
        while True:
            cur_port = _find_port()
            p = subprocess.Popen(
                [
                    self._bins["micromamba"],
                    "-r",
                    os.path.dirname(self._package_dirs[0]),
                    "server",
                    "-p",
                    str(cur_port),
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(1)
            # If we can't start, we try again with a new port
            debug.conda_exec(
                "Attempted micromamba server start on port %d (attempt %d)"
                % (cur_port, attempt)
            )
            attempt += 1
            if not p.poll():
                break
        self._micromamba_server_port = cur_port
        self._micromamba_server_process = p
        debug.conda_exec("Micromamba server started on port %d" % cur_port)
        # We also create a script to pretend to be micromamba but still use the server
        # This allows us to integrate with conda-lock without modifying conda-lock.
        # This can go away if/when conda-lock integrates directly with micromamba server
        # Conda-lock checks for the file ending in "micromamba" so we name it
        # appropriately
        with tempfile.NamedTemporaryFile(
            suffix="micromamba", delete=False, mode="w", encoding="utf-8"
        ) as f:
            f.write(
                glue_script.format(
                    python_executable=sys.executable,
                    micromamba_exec=self._bins["micromamba"],
                    micromamba_root=os.path.dirname(self._package_dirs[0]),
                    server_port=cur_port,
                )
            )
            debug.conda_exec("Micromamba server glue script in %s" % cur_port)
            self._bins["micromamba_server"] = f.name
        os.chmod(
            self._bins["micromamba_server"],
            stat.S_IRUSR
            | stat.S_IXUSR
            | stat.S_IRGRP
            | stat.S_IXGRP
            | stat.S_IROTH
            | stat.S_IXOTH,
        )


class CondaLock(object):
    def __init__(
        self,
        echo: Callable[..., None],
        lock: str,
        timeout: Optional[int] = CONDA_LOCK_TIMEOUT,
        delay: int = 10,
    ):
        self.lock = lock
        self.locked = False
        self.timeout = timeout
        self.delay = delay
        self.echo = echo

    def _acquire(self) -> None:
        start = time.time()
        try:
            os.makedirs(os.path.dirname(self.lock))
        except OSError as x:
            if x.errno != errno.EEXIST:
                raise
        try_count = 0
        while True:
            try:
                self.fd = os.open(self.lock, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                self.locked = True
                break
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

                if try_count < 3:
                    try_count += 1
                elif try_count == 3:
                    self.echo(
                        "Waited %ds to acquire lock at '%s' -- if unexpected, "
                        "please remove that file and retry"
                        % (try_count * self.delay, self.lock)
                    )
                    try_count += 1

                if self.timeout is None:
                    raise CondaException("Could not acquire lock {}".format(self.lock))
                if (time.time() - start) >= self.timeout:
                    raise CondaException(
                        "Timeout occurred while acquiring lock {}".format(self.lock)
                    )
                time.sleep(self.delay)

    def _release(self) -> None:
        if self.locked:
            os.close(self.fd)
            os.unlink(self.lock)
            self.locked = False

    def __enter__(self: "CondaLock") -> "CondaLock":
        if not self.locked:
            self._acquire()
        return self

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        self.__del__()

    def __del__(self) -> None:
        self._release()


class CondaLockMultiDir(object):
    def __init__(
        self,
        echo: Callable[..., None],
        dirs: List[str],
        lockfile: str,
        timeout: Optional[int] = CONDA_LOCK_TIMEOUT,
        delay: int = 10,
    ):
        self.lockfile = lockfile
        self.dirs = sorted(dirs)
        self.locked = False
        self.timeout = timeout
        self.delay = delay
        self.fd = []  # type: List[int]
        self.echo = echo

    def _acquire(self) -> None:
        start = time.time()
        for d in self.dirs:
            full_file = os.path.join(d, self.lockfile)
            try_count = 0
            try:
                os.makedirs(d)
            except OSError as x:
                if x.errno != errno.EEXIST:
                    raise
            while True:
                try:
                    self.fd.append(
                        os.open(full_file, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                    )
                    break
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise

                    if try_count < 3:
                        try_count += 1
                    elif try_count == 3:
                        self.echo(
                            "Waited %ds to acquire lock at '%s' -- if unexpected, "
                            "please remove that file and retry"
                            % (try_count * self.delay, full_file)
                        )
                        try_count += 1

                    if self.timeout is None:
                        raise CondaException(
                            "Could not acquire lock {}".format(full_file)
                        )
                    if (time.time() - start) >= self.timeout:
                        raise CondaException(
                            "Timeout occurred while acquiring lock {}".format(full_file)
                        )
                    time.sleep(self.delay)
        self.locked = True

    def _release(self) -> None:
        if self.locked:
            for d, fd in zip(self.dirs, self.fd):
                os.close(fd)
                os.unlink(os.path.join(d, self.lockfile))
        self.locked = False

    def __enter__(self: "CondaLockMultiDir") -> "CondaLockMultiDir":
        if not self.locked:
            self._acquire()
        return self

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        self.__del__()

    def __del__(self) -> None:
        self._release()


class WithDir:
    def __init__(self, new_dir: str):
        self._current_dir = os.getcwd()
        self._new_dir = new_dir

    def __enter__(self):
        os.chdir(self._new_dir)
        return self._new_dir

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.chdir(self._current_dir)
