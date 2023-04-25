# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

import errno
import json
import os
import platform
import re
import requests
import shutil
import socket
import stat
import subprocess
import sys
import tarfile
import tempfile
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from distutils.version import LooseVersion
from itertools import chain, product
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    OrderedDict,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)
from shutil import which
from urllib.parse import urlparse

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
)
from metaflow.metaflow_environment import InvalidEnvironmentException

from metaflow_extensions.netflix_ext.vendor.packaging.tags import Tag
from metaflow_extensions.netflix_ext.vendor.packaging.utils import parse_wheel_filename

from .utils import (
    CONDA_FORMATS,
    TRANSMUT_PATHCOMPONENT,
    AliasType,
    CondaException,
    CondaStepException,
    arch_id,
    convert_filepath,
    get_conda_root,
    is_alias_mutable,
    parse_explicit_url_conda,
    parse_explicit_url_pip,
    parse_explicit_path_pip,
    pip_tags_from_arch,
    plural_marker,
    resolve_env_alias,
)

from .env_descr import (
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

ParseURLResult = NamedTuple(
    "ParseURLResult",
    [("filename", str), ("format", str), ("hash", str), ("is_transmuted", bool)],
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

    def resolve(
        self,
        using_steps: Sequence[str],
        deps: Sequence[TStr],
        sources: Sequence[TStr],
        extras: Sequence[TStr],
        architecture: str,
        env_type: Optional[EnvType] = None,
    ) -> ResolvedEnvironment:
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
            conda_packages = [d for d in deps if d.category == "conda"]
            pip_packages = [d for d in deps if d.category == "pip"]
            env_type = EnvType.CONDA_ONLY
            if len(conda_packages) == 1:
                # This is a pip only mode
                env_type = EnvType.PIP_ONLY
            elif len(pip_packages) > 0:
                env_type = EnvType.MIXED
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
                packages = self._resolve_env_with_conda(deps, sources, architecture)
            elif env_type == EnvType.MIXED:
                if resolver_bin == "conda-lock":
                    packages = self._resolve_env_with_conda_lock(
                        deps, sources, architecture
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
                    npconda_pkgs = self._resolve_env_with_conda(
                        npconda_deps, sources, architecture
                    )
                    if any((p.filename.startswith("python-") for p in npconda_pkgs)):
                        raise InvalidEnvironmentException(
                            "Cannot specify a non-python Conda dependency that uses "
                            "python: %s. Please use the mixed mode instead."
                            % ", ".join([d.value for d in npconda_deps])
                        )
                packages = self._resolve_env_with_conda(deps, sources, architecture)
                conda_only_deps = [
                    d for d in deps if d.category in ("conda", "npconda")
                ]
                conda_only_sources = [s for s in sources if s.category == "conda"]
                builder_resolved_env = ResolvedEnvironment(
                    conda_only_deps,
                    conda_only_sources,
                    None,
                    architecture,
                    all_packages=packages,
                    env_type=EnvType.CONDA_ONLY,
                )
                if resolver_bin == "pip":
                    # We need to get the python package to get the version
                    python_version = None  # type: Optional[str]
                    for p in packages:
                        if p.filename.startswith("python-"):
                            python_version = p.package_version
                            break
                    if python_version is None:
                        raise CondaException(
                            "Could not determine version of Python from conda packages"
                        )

                    packages.extend(
                        self._resolve_env_with_pip(
                            python_version,
                            deps,
                            sources,
                            extras,
                            architecture,
                            builder_resolved_env,
                        )
                    )
                else:
                    # Should also never happen and be caught earlier but being clean
                    # add other pip resolvers here if needed
                    raise InvalidEnvironmentException(
                        "Resolver '%s' is not supported" % resolver_bin
                    )

            return ResolvedEnvironment(
                deps,
                sources,
                extras,
                architecture,
                all_packages=packages,
                env_type=env_type,
            )
        except CondaException as e:
            raise CondaStepException(e, using_steps)

    def add_to_resolved_env(
        self,
        cur_env: ResolvedEnvironment,
        using_steps: Sequence[str],
        deps: Sequence[TStr],
        sources: Sequence[TStr],
        extras: Sequence[TStr],
        architecture: str,
        inputs_are_addl: bool = True,
        cur_is_accurate: bool = True,
    ) -> ResolvedEnvironment:
        if self._mode != "local":
            # TODO: Maybe relax this later but for now assume that the remote environment
            # is a "lighter" conda.
            raise CondaException("Cannot resolve environments in a remote environment")

        if not self._found_binaries:
            self._find_conda_binary()

        if architecture != cur_env.env_id.arch:
            raise CondaException(
                "Mismatched architecture when extending an environment"
            )
        # We form the new list of dependencies based on the ones we have in cur_env
        # and the new ones.
        if inputs_are_addl:
            sources = list(chain(cur_env.sources, sources))
            user_deps = list(chain(cur_env.deps, deps))
            extras = list(chain(cur_env.extras, extras))
        else:
            sources = sources
            user_deps = deps
            extras = extras
        deps = list(
            chain(
                [
                    TStr(p.TYPE, "%s==%s" % (p.package_name, p.package_version))
                    for p in cur_env.packages
                ],
                user_deps,
            )
        )

        # We check if we already resolved this environment and bypass resolving for
        # if it we already have solved for it.
        new_env_id = EnvID(
            ResolvedEnvironment.get_req_id(user_deps, sources, extras),
            "_default",
            architecture,
        )
        new_resolved_env = self.environment(new_env_id)
        if new_resolved_env:
            return new_resolved_env

        # Figure out the env_type
        new_env_type = cur_env.env_type
        if cur_env.env_type == EnvType.PIP_ONLY and any(
            [d.value for d in deps if d.category == "conda"]
        ):
            self._echo(
                "Upgrading an environment from %s to %s due to new Conda dependencies"
                % (EnvType.PIP_ONLY, EnvType.MIXED)
            )
        elif cur_env.env_type == EnvType.CONDA_ONLY and any(
            [d.value for d in deps if d.category == "pip"]
        ):
            self._echo(
                "Upgrading an environment from %s to %s due to new Pip dependencies"
                % (EnvType.CONDA_ONLY, EnvType.MIXED)
            )

        new_resolved_env = self.resolve(
            using_steps,
            deps,
            sources,
            extras,
            architecture,
            env_type=new_env_type,
        )

        # We now try to copy as much information as possible from the current environment
        # which includes information about cached packages
        cur_packages = {p.filename: p.to_dict() for p in cur_env.packages}
        merged_packages = []  # type: List[PackageSpecification]
        for p in new_resolved_env.packages:
            existing_info = cur_packages.get(p.filename)
            if existing_info:
                merged_packages.append(PackageSpecification.from_dict(existing_info))
            else:
                merged_packages.append(p)
        return ResolvedEnvironment(
            user_deps,
            sources,
            extras,
            architecture,
            all_packages=merged_packages,
            env_type=new_resolved_env.env_type,
            accurate_source=cur_is_accurate,
        )

    def create_for_step(
        self,
        step_name: str,
        env: ResolvedEnvironment,
        do_symlink: bool = False,
    ):

        if not self._found_binaries:
            self._find_conda_binary()

        try:
            env_name = self._env_directory_from_envid(env.env_id)
            return self.create_for_name(env_name, env, do_symlink)
        except CondaException as e:
            raise CondaStepException(e, [step_name])

    def create_for_name(
        self, name: str, env: ResolvedEnvironment, do_symlink: bool = False
    ):

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

    def remove_for_step(self, step_name: str, env_id: EnvID):
        # Remove the conda environment
        if not self._found_binaries:
            self._find_conda_binary()

        try:
            env_name = self._env_directory_from_envid(env_id)
            return self.remove_for_name(env_name)

        except CondaException as e:
            raise CondaStepException(e, [step_name])

    def remove_for_name(self, name: str):
        if not self._found_binaries:
            self._find_conda_binary()
        with CondaLock(self._echo, self._env_lock_file(name)):
            self._remove(name)

    def python(self, env_desc: Union[EnvID, str]) -> Optional[str]:
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
        # First look if we have from_env_id locally
        env = self._cached_environment.env_for(*env_id)

        debug.conda_exec("%s%sfound locally" % (str(env_id), " " if env else " not "))
        if env:
            return env

        # We never have a "_default" remotely so save time and don't go try to
        # look for one
        if not local_only and self._storage and env_id.full_id != "_default":
            env = self._remote_env_fetch([env_id])
            if env:
                env = env[0]
            else:
                env = None
        return env

    def environments(
        self, req_id: str, arch: Optional[str] = None, local_only: bool = False
    ) -> List[Tuple[EnvID, ResolvedEnvironment]]:
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
        arch = arch or arch_id()

        alias_type, resolved_alias = resolve_env_alias(env_alias)
        if alias_type == AliasType.REQ_FULL_ID:
            req_id, full_id = env_alias.split(":", 1)
            return EnvID(req_id=req_id, full_id=full_id, arch=arch)

        env_id = self._cached_environment.env_id_for_alias(
            alias_type, resolved_alias, arch
        )

        if env_id is None and alias_type == AliasType.PATHSPEC:
            # TODO: Add _namespace_check = False
            # Late import to prevent cycles
            from metaflow.client.core import Step

            try:
                s = Step(resolved_alias)
                req_id, full_id, _ = json.loads(
                    s.task.metadata_dict.get("conda_env_id", '["", "", ""]')
                )
                if len(req_id) != 0:
                    env_id = EnvID(req_id=req_id, full_id=full_id, arch=arch)
            except MetaflowNotFound:
                pass
            if env_id:
                self._cached_environment.add_alias(
                    alias_type, resolved_alias, env_id.req_id, env_id.full_id
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

        env_id = self.env_id_from_alias(env_alias, arch, local_only)
        if env_id:
            return self.environment(env_id, local_only)

    def aliases_for_env_id(self, env_id: EnvID) -> Tuple[List[str], List[str]]:
        # Returns immutable and mutable aliases
        return self._cached_environment.aliases_for_env(env_id)

    def set_default_environment(self, env_id: EnvID) -> None:
        self._cached_environment.set_default(env_id)

    def get_default_environment(
        self, req_id: str, arch: Optional[str]
    ) -> Optional[EnvID]:
        return self._cached_environment.get_default(req_id, arch)

    def clear_default_environment(self, req_id: str, arch: Optional[str]):
        self._cached_environment.clear_default(req_id, arch)

    def add_environments(self, resolved_envs: List[ResolvedEnvironment]) -> None:
        for resolved_env in resolved_envs:
            self._cached_environment.add_resolved_env(resolved_env)

    def alias_environment(self, env_id: EnvID, aliases: List[str]) -> None:
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

        cache_paths_to_check = []  # type: List[Tuple[str, str, str]]
        my_arch_id = arch_id()
        cache_formats = cache_formats or {
            "pip": ["_any"],
            "conda": [CONDA_PREFERRED_FORMAT] if CONDA_PREFERRED_FORMAT else ["_any"],
        }
        # Contains the architecture, the list of packages that need the URL
        url_to_pkgs = {}  # type: Dict[str, Tuple[str, List[PackageSpecification]]]

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
                    # We add ourself to the end of the list and update the
                    # first package with any additional information we have about
                    # cache files as it will be used to lazily fetch things (so should
                    # have all up-to-date info).
                    old_arch, all_pkgs = url_to_pkgs[req_pkg.url]
                    all_pkgs.append(req_pkg)
                    to_update_pkg = all_pkgs[0]
                    for pkg_fmt, pkg_hash in req_pkg.pkg_hashes:
                        to_update_pkg.add_pkg_hash(pkg_fmt, pkg_hash)
                    for pkg_fmt, cache_pkg in req_pkg.cached_versions:
                        to_update_pkg.add_cached_version(
                            pkg_fmt, to_update_pkg.cache_pkg_type()(cache_pkg.url)
                        )
                    # It is possible that arch is different (for noarch packages). In
                    # that case, we prefer to download it for our own arch to place the
                    # file in the right place for later. It doesn't matter for anything
                    # else
                    if env_id.arch == my_arch_id and old_arch != env_id.arch:
                        url_to_pkgs[req_pkg.url] = (my_arch_id, all_pkgs)
                else:
                    url_to_pkgs[req_pkg.url] = (env_id.arch, [req_pkg])

        # At this point, we can check if we are missing any cache information
        for base_url, (_, all_pkgs) in url_to_pkgs.items():
            pkg = all_pkgs[0]
            if not all(
                [
                    pkg.cached_version(f) != None
                    for f in cache_formats.get(pkg.TYPE, ["_any"])
                ]
            ):
                # We are missing some cache information. We will check for the base
                # format and any links to other formats
                base_cache_url = pkg.cache_pkg_type().make_cache_url(
                    base_url, cast(str, pkg.pkg_hash(pkg.url_format))
                )
                cache_paths_to_check.append((base_cache_url, base_url, pkg.url_format))
                debug.conda_exec(
                    "%s:%s -> check file @ %s"
                    % (pkg.filename, pkg.url_format, base_cache_url)
                )
                for f in cache_formats.get(pkg.TYPE, ["_any"]):
                    if (
                        f == "_any"
                        or f == pkg.url_format
                        or pkg.cached_version(f) is not None
                    ):
                        continue
                    lnk_path = self._lnk_path_for_pkg(pkg, f)
                    cache_paths_to_check.append((lnk_path, base_url, f))
                    debug.conda_exec(
                        "%s:%s -> check link @ %s" % (pkg.filename, f, lnk_path)
                    )

        files_exist = self._storage.is_file(
            [x[0] for x in cache_paths_to_check]
        )  # type: List[bool]

        link_files_to_get = {}  # type: Dict[str, Tuple[str, str]]
        pkgs_to_fetch_per_arch = {}  # type: Dict[str, List[PackageSpecification]]
        # NOTE: saw_url allows us to fetch only *one* package. Note that some packages
        # are shared across arch (the noarch packages). This will ensure we download it
        # only once and basically for this arch if we are using this arch (so it will
        # be in the right place for later -- it will also be properly cached and
        # other archs will be updated)
        saw_url = set()  # type: Set[str]
        for exist, (req_url, base_url, pkg_fmt) in zip(
            files_exist, cache_paths_to_check
        ):
            arch, all_pkgs = url_to_pkgs[base_url]
            pkg = all_pkgs[0]
            if exist:
                if pkg_fmt != pkg.url_format:
                    # This means that we have a .lnk file, we need to actually fetch
                    # it and determine where it points to so we can update the cache URLs
                    # and hashes
                    debug.conda_exec(
                        "%s:%s -> Found link file" % (pkg.filename, pkg_fmt)
                    )
                    link_files_to_get[req_url] = (base_url, pkg_fmt)
                else:
                    debug.conda_exec(
                        "%s -> Found cache file %s" % (pkg.filename, req_url)
                    )
                    cache_pkg = pkg.cache_pkg_type()(req_url)
                    pkg.add_cached_version(pkg_fmt, cache_pkg)
            else:
                if base_url not in saw_url:
                    # If we don't have it in cache, we will need to actually fetch it
                    pkgs_to_fetch_per_arch.setdefault(arch, []).append(pkg)
                    saw_url.add(base_url)

        # Get the link files to properly update the cache packages and hashes
        if link_files_to_get:
            cache_paths_to_check = []
            with self._storage.load_bytes(link_files_to_get.keys()) as loaded:
                for key, tmpfile, _ in loaded:
                    base_url, pkg_fmt = link_files_to_get[key]
                    _, all_pkgs = url_to_pkgs[base_url]
                    pkg = all_pkgs[0]
                    with open(tmpfile, mode="r", encoding="utf-8") as f:
                        cached_path = f.read().strip()
                        debug.conda_exec(
                            "%s:%s -> check file at %s"
                            % (pkg.filename, pkg_fmt, cached_path)
                        )
                        cache_paths_to_check.append((cached_path, base_url, pkg_fmt))
                        pkg.add_cached_version(
                            pkg_fmt, pkg.cache_pkg_type()(cached_path)
                        )
            # We also double check that whatever is being pointed to is actually there.
            # It is possible that when caching, some link files get uploaded but not the
            # actual file.
            files_exist = self._storage.is_file(
                [x[0] for x in cache_paths_to_check]
            )  # type: List[bool]
            for exist, (req_url, base_url, pkg_fmt) in zip(
                files_exist, cache_paths_to_check
            ):
                arch, all_pkgs = url_to_pkgs[base_url]
                pkg = all_pkgs[0]
                if exist:
                    debug.conda_exec(
                        "%s -> Found cache file %s" % (pkg.filename, req_url)
                    )
                    cache_pkg = pkg.cache_pkg_type()(req_url)
                    pkg.add_cached_version(pkg_fmt, cache_pkg)
                else:
                    if base_url not in saw_url:
                        # If we don't have it in cache, we will need to actually fetch it
                        pkgs_to_fetch_per_arch.setdefault(arch, []).append(pkg)
                        saw_url.add(base_url)

        # Fetch what we need; we fetch all formats for all packages (this is a superset
        # but simplifies the code)

        # Contains cache_path, local_path
        upload_files = []  # type: List[Tuple[str, str]]

        def _cache_pkg(pkg: PackageSpecification, pkg_fmt: str, local_path: str) -> str:
            file_hash = cast(str, pkg.pkg_hash(pkg_fmt))
            cache_path = pkg.cache_pkg_type().make_cache_url(
                pkg.url,
                file_hash,
                pkg_fmt,
                pkg.is_transmuted(pkg_fmt),
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
                    self._lazy_fetch_packages(
                        pkgs,
                        dest_dir,
                        require_conda_format=cache_formats.get("conda", []),
                        require_url_format=True,
                        requested_arch=arch,
                        search_dirs=search_dirs,
                    )

                # Upload what we need to upload and update the representative package with
                # the new cache information. For all packages, we basically check if we have
                # a local file but not a cached version and upload that.
                for pkg in pkgs:
                    # We first need to check for url_format because the links are then
                    # created from there
                    if not pkg.cached_version(pkg.url_format):
                        _cache_pkg(
                            pkg,
                            pkg.url_format,
                            cast(str, pkg.local_file(pkg.url_format)),
                        )
                    for local_fmt, local_path in pkg.local_files:
                        if not pkg.cached_version(local_fmt):
                            # Since we just cached the url_format version (if needed),
                            # we know that here this is not it so we always create a link
                            cache_path = _cache_pkg(pkg, local_fmt, local_path)
                            lnk_path = self._lnk_path_for_pkg(pkg, local_fmt)
                            # Files will get deleted when the outer directory gets
                            # deleted and we need to keep it around to upload it.
                            with tempfile.NamedTemporaryFile(
                                delete=False, mode="w", encoding="utf-8"
                            ) as lnk_file:
                                debug.conda_exec(
                                    "%s:%s -> will upload link @@%s@@ to %s"
                                    % (
                                        pkg.filename,
                                        local_fmt,
                                        cache_path,
                                        lnk_path,
                                    )
                                )
                                lnk_file.write(cache_path)
                                upload_files.append((lnk_path, lnk_file.name))

            # We now update all packages with the information from the first package which
            # we have been updating all this time. They are all the same but are referenced
            # from different environments.
            for _, all_pkgs in url_to_pkgs.values():
                cannonical_pkg = all_pkgs[0]
                for pkg in all_pkgs[1:]:
                    # We only really care about hashes and cached versions
                    for pkg_fmt, pkg_hash in cannonical_pkg.pkg_hashes:
                        pkg.add_pkg_hash(pkg_fmt, pkg_hash)
                    for pkg_fmt, pkg_cache in cannonical_pkg.cached_versions:
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
                web_downloads.append((pkg_spec, dst))

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
                if (
                    "micromamba" in cast(Dict[str, str], self._bins)
                    and requested_arch == arch_id()
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
                            "across architectures due to "
                            "https://github.com/mamba-org/mamba/issues/2328. "
                            "Please install conda-package-handling."
                        )
                    raise CondaException(
                        "Requesting to transmute package without conda-package-handling "
                        " or micromamba"
                    )

                pkg_spec.add_local_file(dst_format, dst_file, transmuted=True)
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
                            url_to_add = self._make_urlstxt_from_cacheurl(key)  # type: ignore
                            if (
                                pkg_spec.TYPE == "conda"
                                and url_to_add not in known_urls
                            ):
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
                            new_url = self._make_urlstxt_from_url(
                                pkg_spec.url, dst_format, is_transmuted=True
                            )

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
                        url_format=os.path.splitext(pkg["filename"])[1],
                        hashes={os.path.splitext(pkg["filename"])[1]: pkg["md5"]},
                    )
                    for pkg in json_response["packages"]
                ]

    def _resolve_env_with_conda(
        self,
        deps: Sequence[TStr],
        sources: Sequence[TStr],
        architecture: str,
    ) -> List[PackageSpecification]:

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
        return result

    def _resolve_env_with_pip(
        self,
        python_version: str,
        deps: Sequence[TStr],
        sources: Sequence[TStr],
        extras: Sequence[TStr],
        architecture: str,
        builder_env: ResolvedEnvironment,
    ) -> List[PackageSpecification]:

        deps = [d for d in deps if d.category == "pip"]

        # Some args may be two actual arguments like "-f <something>" thus the map
        extra_args = list(
            chain.from_iterable(
                map(
                    lambda x: x.split(maxsplit=1),
                    (e.value for e in extras if e.category == "pip"),
                )
            )
        )
        result = []
        with tempfile.TemporaryDirectory() as pip_dir:
            args = [
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
            pip_python_version = self._call_binary(["-V"], binary="pip").decode(
                encoding="utf-8"
            )
            matched_version = re.search(
                r"\(python ([23])\.([0-9]+)\)", pip_python_version
            )
            if not matched_version:
                raise InvalidEnvironmentException(
                    "Cannot identify Python for PIP; got %s" % pip_python_version
                )
            clean_python_version = ".".join(python_version.split(".")[:2])
            if (
                clean_python_version
                != ".".join([matched_version.group(1), matched_version.group(2)])
                or architecture != arch_id()
            ):
                args.extend(
                    ["--only-binary=:all:", "--python-version", clean_python_version]
                )

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
                    *(
                        chain.from_iterable(
                            product(["--implementation"], implementations)
                        )
                    ),
                    *(chain.from_iterable(product(["--abi"], abis))),
                    *(chain.from_iterable(product(["--platform"], platforms))),
                )

                args.extend(extra_args)

            # Unfortunately, pip doesn't like things like ==<= so we need to strip
            # the ==
            for d in deps:
                splits = d.value.split("==")
                if len(splits) == 1:
                    args.append(d.value)
                else:
                    if splits[1][0] in ("=", "<", ">"):
                        # Something originally like pkg==<=ver
                        args.append("".join(splits))
                    else:
                        # Something originally like pkg==ver
                        args.append(d.value)

            self._call_binary(args, binary="pip")

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
                dl_info = package_desc["download_info"]
                url = dl_info["url"]
                if "dir_info" in dl_info or url.startswith("file://"):
                    if dl_info["dir_info"].get("editable", False):
                        raise CondaException(
                            "Cannot include an editable PIP package: '%s'" % url
                        )
                    if os.path.isdir(url[7:]):
                        packages_to_build.append(dl_info)
                    else:
                        parse_result = parse_explicit_path_pip(url)
                        if parse_result.url_format != ".whl":
                            # This is a source package so we need to build it
                            packages_to_build.append(dl_info)
                        else:
                            package_spec = PipPackageSpecification(
                                parse_result.filename,
                                parse_result.url,
                                parse_result.url_format,
                                None,
                            )
                            # Use url to not have the `file://` and not use the canonical
                            # file://local-file/ path
                            package_spec.add_local_file(parse_result.url_format, url)
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
                                parse_result.url_format,
                                {parse_result.url_format: parse_result.hash}
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
                to_build_pkg_info = (
                    {}
                )  # type: Dict[str, Dict[str, Union[str, PackageSpecification, List[Tuple[FrozenSet[Tag], str]]]]]
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
                        to_build_pkg_info[base_pkg_url] = {
                            "build_url": base_build_url,
                            "cache_url": PipCachePackage.make_partial_cache_url(
                                base_pkg_url
                            ),
                        }
                    elif "dir_info" in package_desc:
                        local_path = package_desc["url"][7:]
                        if os.path.isdir(local_path):
                            # For now support only setup.py packages.
                            if not os.path.isfile(os.path.join(local_path, "setup.py")):
                                raise InvalidEnvironmentException(
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
                        to_build_pkg_info[parse_result.url] = {
                            "build_url": package_desc["url"],
                            "cache_url": PipCachePackage.make_partial_cache_url(
                                parse_result.url
                            ),
                        }
                    else:
                        # Just a regular .tar.gz package
                        if package_desc["url"].endswith(".tar.gz"):
                            cache_url = package_desc["url"][:-7] + ".whl"
                        else:
                            raise InvalidEnvironmentException(
                                "Expected a '.tar.gz' package: '%s'"
                                % package_desc["url"]
                            )
                        to_build_pkg_info[cache_url] = {
                            "build_url": package_desc["url"],
                            "cache_url": PipCachePackage.make_partial_cache_url(
                                cache_url
                            ),
                        }

                # We check in the cache -- we don't actually have the filename or
                # hash so we check things starting with the partial URL
                debug.conda_exec(
                    "Checking for pre-built packages: %s" % str(to_build_pkg_info)
                )
                found_files = self._storage.list_content(
                    (x["cache_url"] for x in to_build_pkg_info.values())
                )
                keys_to_check = []  # type: List[str]
                for cache_path, is_file in found_files:
                    cache_path = cast(str, cache_path)
                    is_file = cast(bool, is_file)
                    if is_file:
                        raise CondaException(
                            "Invalid cache content at '%s'" % cache_path
                        )

                    debug.conda_exec(
                        "Found potential pre-built package at '%s'" % cache_path
                    )
                    for k, v in to_build_pkg_info.items():
                        if cache_path.startswith(cast(str, v["cache_url"])):
                            keys_to_check.append(k)
                            # We now have a potential filename for the package. We
                            # note it so that we can later match it based on the
                            # supported tags for this platform
                            filename = os.path.split(cache_path.rstrip("/"))[1]
                            cast(
                                List[Tuple[FrozenSet[Tag], str]],
                                v.setdefault("pkg_filenames", []),
                            ).append((parse_wheel_filename(filename)[3], cache_path))
                            if "pkg_filename" in v:
                                raise CondaException(
                                    "File at '%s' is a duplicate of '%s'"
                                    % (cache_path, v["cache_url"])
                                )
                # We now check all the keys_to_check (basically where we found potential
                # matches) for files that have the right tag for the platform we are
                # building for
                for k in keys_to_check:
                    potentials = cast(
                        List[Tuple[FrozenSet[Tag], str]],
                        to_build_pkg_info[k]["pkg_filenames"],
                    )
                    for t in supported_tags:
                        # Tags are ordered from most-preferred to least preferred
                        for p in potentials:
                            # Potentials are in no particular order but we will
                            # effectively get a package with the most preferred tag
                            # if one exists
                            if t in p[0]:
                                to_build_pkg_info[k]["cache_url"] = p[1]
                                to_build_pkg_info[k]["pkg_filename"] = os.path.split(
                                    p[1].rstrip("/")
                                )[1]
                                debug.conda_exec(
                                    "For '%s', found matching package at %s" % (k, p[1])
                                )
                                break
                        else:
                            # If we don't find a match, continue to next tag (and
                            # skip break of outer loop on next line)
                            continue
                        break

                # We now check for hashes for those packages we did find (it's the
                # next level down in the cache)
                found_files = self._storage.list_content(
                    (
                        x["cache_url"]
                        for x in to_build_pkg_info.values()
                        if "pkg_filename" in x
                    )
                )
                for cache_path, is_file in found_files:
                    cache_path = cast(str, cache_path)
                    is_file = cast(bool, is_file)
                    if is_file:
                        raise CondaException(
                            "Invalid cache content at '%s'" % cache_path
                        )

                    debug.conda_exec("Found package with hash at '%s'" % cache_path)
                    for k, v in to_build_pkg_info.items():
                        if cache_path.startswith(cast(str, v["cache_url"])):
                            # We now have the hash for the package
                            pkg_hash = os.path.split(cache_path.rstrip("/"))[1]
                            pkg_url = os.path.join(k, cast(str, v["pkg_filename"]))
                            v["cache_url"] = PipCachePackage.make_cache_url(
                                pkg_url, pkg_hash
                            )
                            v["pkg_spec"] = PipPackageSpecification(
                                cast(str, v["pkg_filename"])[:-4],
                                pkg_url,
                                ".whl",
                                {".whl": pkg_hash},
                                {".whl": PipCachePackage(v["cache_url"])},
                            )
                            break
                    else:
                        raise CondaException(
                            "Found unexpected content at '%s'" % cache_path
                        )

                if any("pkg_spec" not in v for v in to_build_pkg_info.values()):
                    self._echo(" (building PIP packages from repositories)", nl=False)
                    # We need to build packages -- we only allow this if the architecture
                    # is the same to avoid potential cross-building. We could relax this to
                    # noarch packages but playing it safe for now
                    if arch_id() != architecture:
                        raise CondaException(
                            "Specifying PIP packages from repositories requires "
                            "building the wheels and this is only allowed if the target "
                            "architecture is the same as this one"
                        )
                    debug.conda_exec(
                        "Creating builder environment to build PIP packages"
                    )

                    target_directory = os.path.join(self._package_dirs[0], "pip")
                    os.makedirs(target_directory, exist_ok=True)

                    techo = self._echo
                    self._echo = self._no_echo
                    self.create_for_name(
                        self._env_builder_directory_from_envid(builder_env.env_id),
                        builder_env,
                    )
                    self._echo = techo

                    builder_python = cast(
                        str,
                        self.python(
                            self._env_builder_directory_from_envid(builder_env.env_id)
                        ),
                    )

                    def _build_with_pip(identifier: int, key: str, url: str):
                        dest_path = os.path.join(pip_dir, "build_%d" % identifier)
                        debug.conda_exec(
                            "Building package '%s'  for '%s' in '%s'"
                            % (url, key, dest_path)
                        )
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
                                url,
                            ],
                            binary=builder_python,
                        )
                        return key, dest_path

                    with ThreadPoolExecutor() as executor:
                        build_result = [
                            executor.submit(
                                _build_with_pip, idx, key, cast(str, v["build_url"])
                            )
                            for idx, (key, v) in enumerate(to_build_pkg_info.items())
                            if not "pkg_filename" in v
                        ]
                        for f in as_completed(build_result):
                            key, build_dir = f.result()
                            wheel_files = [
                                f
                                for f in os.listdir(build_dir)
                                if os.path.isfile(os.path.join(build_dir, f))
                                and f.endswith(".whl")
                            ]
                            if len(wheel_files) != 1:
                                raise CondaException(
                                    "Could not build '%s' -- found built packages: %s"
                                    % (key, wheel_files)
                                )

                            wheel_file = os.path.join(build_dir, wheel_files[0])
                            # Move the built wheel to a less temporary location
                            wheel_file = shutil.copy(wheel_file, target_directory)
                            debug.conda_exec(
                                "Package for '%s' built in '%s'" % (key, wheel_file)
                            )

                            parse_result = parse_explicit_path_pip(
                                "file://%s" % wheel_file
                            )
                            package_spec = PipPackageSpecification(
                                parse_result.filename,
                                os.path.join(key, "%s.whl" % parse_result.filename),
                                parse_result.url_format,
                                None,
                            )
                            package_spec.add_local_file(
                                parse_result.url_format, wheel_file
                            )

                            to_build_pkg_info[key][
                                "pkg_filename"
                            ] = parse_result.filename
                            to_build_pkg_info[key]["pkg_spec"] = package_spec

                for v in to_build_pkg_info.values():
                    result.append(cast(PackageSpecification, v["pkg_spec"]))

        return result

    def _resolve_env_with_conda_lock(
        self,
        deps: Sequence[TStr],
        channels: Sequence[TStr],
        architecture: str,
    ) -> List[PackageSpecification]:
        outfile_name = None
        my_arch = arch_id()
        if any([d.category not in ("pip", "conda", "npconda") for d in deps]):
            raise CondaException(
                "Cannot resolve dependencies that include non-Conda/Pip dependencies: %s"
                % "; ".join(map(str, deps))
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
        try:
            # We resolve the environment using conda-lock

            # Write out the requirement yml file. It's easy enough so don't use a YAML
            # library to avoid adding another dep

            pip_deps = [d.value for d in deps if d.category == "pip"]
            conda_deps = [
                d.value for d in deps if d.category in ("conda", "npconda")
            ] + ["pip"]
            # Add channels
            lines = ["channels:\n"]
            lines.extend(
                ["  - %s\n" % c.value for c in channels if c.category == "conda"]
            )
            for c in self._info["channels"]:
                lines.append("  - %s\n" % c.replace(my_arch, architecture))

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
                    "'repositories.metaflow_inserted_%s.url' % "
                    "c.translate(translation_table), c) "
                    "for c in channels]"
                )
                _poetry_exec(python_cmd, json.dumps(pip_channels))

            # Add deps

            lines.append("dependencies:\n")
            lines.extend(["  - %s\n" % d for d in conda_deps])
            if pip_deps:
                lines.append("  - pip:\n")
                lines.extend(["    - %s\n" % d for d in pip_deps])

            assert self._bins

            with tempfile.NamedTemporaryFile(
                mode="w", encoding="ascii", delete=not debug.conda
            ) as input_yml:
                input_yml.writelines(lines)
                input_yml.flush()
                outfile_name = "conda-lock-gen-%s" % os.path.basename(input_yml.name)
                args = [
                    "lock",
                    "-f",
                    input_yml.name,
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

                    with tempfile.NamedTemporaryFile(
                        mode="w", encoding="ascii", delete=not debug.conda
                    ) as virtual_yml:
                        virtual_yml.writelines(lines)
                        virtual_yml.flush()
                        args.extend(["--virtual-package-spec", virtual_yml.name])

                        self._call_binary(args, binary="conda-lock", addl_env=addl_env)
                else:
                    self._call_binary(args, binary="conda-lock", addl_env=addl_env)
            # At this point, we need to read the explicit dependencies in the file created
            emit = False
            result = []  # type: List[PackageSpecification]
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
                            result.append(
                                PipPackageSpecification(
                                    parse_result.filename,
                                    parse_result.url,
                                    parse_result.url_format,
                                    {
                                        parse_result.url_format: cast(
                                            str, parse_result.hash
                                        )
                                    },
                                )
                            )
                        else:
                            parse_result = parse_explicit_url_conda(l.strip())
                            result.append(
                                CondaPackageSpecification(
                                    parse_result.filename,
                                    parse_result.url,
                                    parse_result.url_format,
                                    {
                                        parse_result.url_format: cast(
                                            str, parse_result.hash
                                        )
                                    },
                                )
                            )
                    if not emit and l.strip() == "@EXPLICIT":
                        emit = True
            return result
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
                    "[poetry_config.config_source.remove_property('repositories.%s' % p) for p in "
                    "poetry_config.all().get('repositories', {}) "
                    "if p.startswith('metaflow_inserted_')]; "
                )
                _poetry_exec(python_cmd)

    def _find_conda_binary(self):
        # Lock as we may be trying to resolve multiple environments at once and therefore
        # we may be trying to validate the installation multiple times.
        with CondaLock(self._echo, "/tmp/mf-conda-check.lock"):
            if self._found_binaries:
                return
            if self._resolvers[EnvType.CONDA_ONLY] not in _CONDA_DEP_RESOLVERS:
                raise InvalidEnvironmentException(
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
                        raise InvalidEnvironmentException(
                            msg="Cannot find Conda installation tarball '%s'"
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
                raise InvalidEnvironmentException(
                    msg="Could not extract environment: %s" % str(e)
                )
        delta_time = int(time.time() - start)
        self._echo(
            " done in %d second%s." % (delta_time, plural_marker(delta_time)),
            timestamp=False,
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

        if "pip" in self._bins:
            pip_version = self._call_binary(["--version"], binary="pip").split(b" ", 2)[
                1
            ]
            if LooseVersion(pip_version.decode("utf-8")) < LooseVersion("23.0"):
                self._echo(
                    "pip is installed but not recent enough (23.0 or later is required) "
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

        if self._conda_executable_type == "micromamba":
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
                local_path = p.local_file(p.url_format)
                if local_path:
                    debug.conda_exec(
                        "For %s, using PIP package at '%s'" % (p.filename, local_path)
                    )
                    pip_paths.append("%s\n" % local_path)
                else:
                    raise CondaException(
                        "Local file for package %s expected" % p.filename
                    )
            elif p.TYPE == "conda":
                local_dir = p.local_dir
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
                        explicit_urls.append("%s#%s\n" % (info["url"], info["md5"]))
                else:
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
        base_url: str,
        file_format: Optional[str] = None,
        is_transmuted: bool = False,
    ):
        if not is_transmuted:
            return base_url
        url = urlparse(base_url)
        file_path, filename = convert_filepath(url.path, file_format)
        return os.path.join(
            get_conda_root(self._datastore_type),
            cast(str, CONDA_PACKAGES_DIRNAME),
            "conda",
            TRANSMUT_PATHCOMPONENT,
            url.netloc,
            file_path.lstrip("/"),
            filename,
        )

    def _make_urlstxt_from_cacheurl(self, cache_url: str) -> str:
        if TRANSMUT_PATHCOMPONENT in cache_url:
            return os.path.join(
                get_conda_root(self._datastore_type),
                cast(str, CONDA_PACKAGES_DIRNAME),
                "conda",
                os.path.split(os.path.split(cache_url)[0])[
                    0
                ],  # Strip off last two (hash and filename)
            )
        else:
            # Format is CONDA_PACKAGES_DIRNAME/conda/url/hash/file so we strip
            # first 2 and last 2
            components = cache_url.split("/")
            return "https://" + "/".join(components[2:-2])

    @staticmethod
    def _env_directory_from_envid(env_id: EnvID) -> str:
        return "metaflow_%s_%s" % (env_id.req_id, env_id.full_id)

    @staticmethod
    def _env_builder_directory_from_envid(env_id: EnvID) -> str:
        return "metaflow_builder_%s_%s" % (env_id.req_id, env_id.full_id)

    @staticmethod
    def _lnk_path_for_pkg(pkg: PackageSpecification, pkg_fmt: str) -> str:
        cached_base_version = pkg.cached_version(pkg.url_format)
        if cached_base_version:
            base_url = os.path.split(cached_base_version.url)[0]
        else:
            base_url = os.path.split(
                pkg.cache_pkg_type().make_cache_url(
                    pkg.url, cast(str, pkg.pkg_hash(pkg.url_format))
                )
            )[0]
        return "/".join([base_url, "%s.lnk" % pkg_fmt])

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
