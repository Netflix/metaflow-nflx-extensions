# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

import errno
import json
import os
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

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from datetime import datetime
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

from requests.auth import AuthBase

from metaflow.plugins.datastores.local_storage import LocalStorage
from metaflow.datastore.datastore_storage import DataStoreStorage

from metaflow.debug import debug
from metaflow.exception import MetaflowException, MetaflowNotFound
from metaflow.metaflow_config import (
    CONDA_DEPENDENCY_RESOLVER,
    CONDA_LOCAL_DIST_DIRNAME,
    CONDA_LOCAL_DIST,
    CONDA_LOCAL_PATH,
    CONDA_LOCK_TIMEOUT,
    CONDA_PACKAGES_DIRNAME,
    CONDA_ENVS_DIRNAME,
    CONDA_PREFERRED_FORMAT,
    CONDA_REMOTE_INSTALLER,
    CONDA_REMOTE_INSTALLER_DIRNAME,
    CONDA_DEFAULT_PYPI_SOURCE,
    CONDA_USE_REMOTE_LATEST,
)
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow.util import get_username

from metaflow._vendor.packaging.version import parse as parse_version

from .utils import (
    CONDA_FORMATS,
    AliasType,
    CondaException,
    CondaStepException,
    arch_id,
    auth_from_urls,
    correct_splitext,
    get_conda_root,
    is_alias_mutable,
    plural_marker,
    resolve_env_alias,
)

from .env_descr import (
    CondaCachePackage,
    EnvID,
    PackageSpecification,
    ResolvedEnvironment,
    read_conda_manifest,
    write_to_conda_manifest,
)

from .conda_lock_micromamba_server import glue_script


_CONDA_DEP_RESOLVERS = ("conda", "mamba", "micromamba")


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

            self.echo = _modified_logger
        else:
            self.echo = echo

        self._no_echo = echo_dev_null

        self._datastore_type = datastore_type
        self._mode = mode
        self._bins = None  # type: Optional[Dict[str, Optional[str]]]
        self._conda_executable_type = None  # type: Optional[str]

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

    def package_dir(self, package_type: str) -> str:
        return os.path.join(self._package_dirs[0], package_type)

    @property
    def conda_executable_type(self) -> Optional[str]:
        if not self._found_binaries:
            self._find_conda_binary()
        return self._conda_executable_type

    @property
    def default_conda_channels(self) -> List[str]:
        if not self._found_binaries:
            self._find_conda_binary()
        return list(self._info["channels"] or [])

    @property
    def default_pypi_sources(self) -> List[str]:
        # TODO: Maybe we also need to get this from poetry but that gets a bit tricky
        # since we don't actually check for a poetry install and use the one within
        # conda lock. This is, for now, used to get authentication values so we should
        # be ok relying on the pypi repo. We also provide a side mechanism to specify
        # other authentication values so we don't need to worry too much here.
        sources = []  # type: List[str]
        if not self._found_binaries:
            self._find_conda_binary()

        if "pip" not in self._bins:
            return sources

        config_values = self.call_binary(["config", "list"], binary="pip").decode(
            encoding="utf-8"
        )

        # If we have CONDA_DEFAULT_PYPI_SOURCE, we use that as the index-url and then
        # add on all the extra-index-url.
        have_index = False
        if CONDA_DEFAULT_PYPI_SOURCE is not None:
            sources.append(CONDA_DEFAULT_PYPI_SOURCE)
            have_index = True
        for line in config_values.splitlines():
            key, value = line.split("=", 1)
            _, key = key.split(".")
            if key in ("index-url", "extra-index-url"):
                if key == "index-url":
                    if have_index:
                        continue
                    have_index = True
                sources.extend(
                    map(lambda x: x.strip("'\""), re.split("\s+", value, re.M))
                )
        if not have_index:
            sources = ["https://pypi.org/simple"] + sources[1:]
        return sources

    @property
    def virtual_packages(self) -> Dict[str, str]:
        if "virtual_pkgs" in self._info:
            return {
                name: "%s=%s" % (version, build)
                for name, version, build in self._info["virtual_pkgs"]
            }
        elif "virtual packages" in self._info:
            # Micromamba outputs them differently for some reason
            return {
                name: build_str
                for name, build_str in map(
                    lambda x: x.split("=", 1),
                    cast(List[str], self._info["virtual packages"]),
                )
            }
        else:
            raise CondaException("Cannot extract virtual package information")

    @property
    def root_prefix(self) -> str:
        return cast(str, self._info["root_prefix"])

    @property
    def storage(self) -> Optional[DataStoreStorage]:
        return self._storage

    def call_conda(
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
            return cast(
                bytes,
                subprocess.check_output(
                    [self._bins[binary]] + args,
                    stderr=subprocess.PIPE,
                    env=dict(os.environ, **env),
                ),
            ).strip()
        except subprocess.CalledProcessError as e:
            if pretty_print_exception:
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
                    "see pretty-printed error above".format(
                        cmd=e.cmd, code=e.returncode
                    )
                ) from None
            else:
                raise CondaException(
                    "Conda command '{cmd}' returned error ({code})".format(
                        cmd=e.cmd, code=e.returncode
                    )
                ) from None

    def call_binary(
        self,
        args: List[str],
        binary: str,
        addl_env: Optional[Mapping[str, str]] = None,
        cwd: Optional[str] = None,
        pretty_print_exception: bool = True,
    ) -> bytes:
        if binary in _CONDA_DEP_RESOLVERS:
            return self.call_conda(args, binary, addl_env, pretty_print_exception)
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
                cwd=cwd,
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
            ) from None

    def create_for_step(
        self,
        step_name: str,
        env: ResolvedEnvironment,
        do_symlink: bool = False,
    ) -> str:
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
        Returns
        -------
        str: Path to the environment (add bin/python for the python binary)
        """

        if not self._found_binaries:
            self._find_conda_binary()

        try:
            env_name = self._env_directory_from_envid(env.env_id)
            return self.create_for_name(env_name, env, do_symlink)
        except CondaException as e:
            raise CondaStepException(e, [step_name]) from None

    def create_for_name(
        self, name: str, env: ResolvedEnvironment, do_symlink: bool = False
    ) -> str:
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

        Returns
        -------
        str: Path to the environment (add bin/python for the python binary)
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
        with CondaLock(self.echo, self._env_lock_file(name)):
            with CondaLockMultiDir(
                self.echo, self._package_dirs, self._package_dir_lockfile_name
            ):
                env_path = self._create(env, name)

        if do_symlink:
            os.symlink(
                os.path.join(env_path, "bin", "python"),
                os.path.join(os.getcwd(), "__conda_python"),
            )
        return env_path

    def create_builder_env(self, builder_env: ResolvedEnvironment) -> str:
        # A helper to build a named environment specifically for builder environments.
        # We are more quiet and have a specific name for it
        techo = self.echo
        self.echo = self._no_echo
        r = self.create_for_name(
            self._env_builder_directory_from_envid(builder_env.env_id), builder_env
        )
        self.echo = techo

        return r

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
            raise CondaStepException(e, [step_name]) from None

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
        with CondaLock(self.echo, self._env_lock_file(name)):
            self._remove(name)

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
            req_id = ""
            full_id = ""
            old_metadata_value = None  # type: Optional[str]
            try:
                s = Step(resolved_alias, _namespace_check=False)
                try:
                    req_id, full_id, _ = json.loads(
                        s.task.metadata_dict.get("conda_env_id", '["", "", ""]')
                    )
                except json.decoder.JSONDecodeError:
                    # Most likely old format -- raise a nicer exception
                    old_metadata_value = s.task.metadata_dict.get("conda_env_id")
                if old_metadata_value is None and len(req_id) != 0:
                    env_id = EnvID(req_id=req_id, full_id=full_id, arch=arch)
            except MetaflowNotFound as e:
                raise MetaflowNotFound(
                    "Cannot locate step while looking for Conda environment"
                ) from e
            if old_metadata_value:
                raise CondaException(
                    "Step %s was created by an older Conda" % resolved_alias
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

        if (
            not local_only
            and self._storage is not None
            and (env_id is None or is_alias_mutable(alias_type, resolved_alias))
        ):
            env_id_list = self._remote_fetch_alias([(alias_type, resolved_alias)], arch)
            if env_id_list:
                env_id = env_id_list[0]
        return env_id

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
            Conda packages. The dictionary keys are `pypi` or `conda` and the
            values are the formats that are needed or ["_any"] if any cached
            format is ok. If the None default is used, the value will be
            {"conda": CONDA_PREFERRED_FORMAT or ["_any"], "pypi": ["_any"]}.
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
            "pypi": ["_any"],
            "conda": [CONDA_PREFERRED_FORMAT]
            if CONDA_PREFERRED_FORMAT and CONDA_PREFERRED_FORMAT != "none"
            else ["_any"],
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
        all_sources = []  # type: List[str]

        for resolved_env in resolved_envs:
            # First check the sources so we can figure out any authentication that
            # may be needed
            all_sources.extend([s.value for s in resolved_env.sources])

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
                    # can have it added. It's not always the case for example for PYPI
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
                    self.echo, search_dirs, self._package_dir_lockfile_name
                ):
                    require_conda_format = cache_formats.get("conda", [])
                    if len(require_conda_format) > 0 and "_any" in require_conda_format:
                        require_conda_format = []
                    self.lazy_fetch_packages(
                        pkgs,
                        auth_from_urls(all_sources),
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
                self.echo(
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
                self.echo(
                    " done in %d second%s." % (delta_time, plural_marker(delta_time))
                )
            else:
                self.echo(
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
                self.echo(
                    "    Caching %d environments and aliases to %s ..."
                    % (
                        len(upload_files),
                        self._datastore_type,
                    ),
                    nl=False,
                )
                self._upload_to_ds(upload_files)
                delta_time = int(time.time() - start)
                self.echo(
                    " done in %d second%s." % (delta_time, plural_marker(delta_time))
                )
            else:
                self.echo(
                    "    All environments already cached in %s." % self._datastore_type
                )

    def lazy_fetch_packages(
        self,
        packages: Iterable[PackageSpecification],
        auth_info: Optional[AuthBase],
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

        if self._mode == "remote":
            # If we are in remote mode, no point searching for local packages -- they
            # are not there.
            search_dirs = []
        else:
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
                    with session.get(pkg_spec.url, stream=True, auth=auth_info) as r:
                        r.raise_for_status()
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
                self.call_binary(args, binary="cph")

            def _micromamba_transmute(src_file: str, dst_file: str, dst_format: str):
                args = ["package", "transmute", "-c", "3", src_file]
                self.call_binary(args, binary="micromamba")

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
            if self._mode != "remote":
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
        start = time.time()
        do_download = web_downloads or cache_downloads
        if do_download:
            self.echo(
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

            if cache_downloads and self._storage:
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
                with self._storage.load_bytes(keys_to_info.keys()) as load_results:  # type: ignore
                    for key, tmpfile, _ in load_results:  # type: ignore
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
                self.echo(
                    " done in %d second%s." % (delta_time, plural_marker(delta_time)),
                    timestamp=False,
                )
            if not pending_errors and transmutes:
                start = time.time()
                self.echo(
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
                self.echo(
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

    def _find_conda_binary(self):
        # Lock as we may be trying to resolve multiple environments at once and therefore
        # we may be trying to validate the installation multiple times.
        with CondaLock(self.echo, "/tmp/mf-conda-check.lock"):
            if self._found_binaries:
                return

            if self._mode == "local":
                self._ensure_local_conda()
            else:
                self._ensure_remote_conda()
                err = self._validate_conda_installation()
                if err:
                    raise err
            self._found_binaries = True

    def _ensure_local_conda(self):
        self._conda_executable_type = cast(str, CONDA_DEPENDENCY_RESOLVER)
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
                    self.echo,
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
                # We can install micromamba in $HOME/.local/bin so we look there too
                "micromamba": which(
                    "micromamba",
                    path=":".join(
                        [
                            os.environ.get("PATH", ""),
                            "%s/.local/bin" % os.environ.get("HOME", ""),
                        ]
                    ),
                ),
                "cph": which("cph"),
                "pip": which("pip"),
            }
            self._bins["conda"] = self._bins[self._conda_executable_type]
            err = self._validate_conda_installation()
            if err:
                raise err

    def _ensure_micromamba(self) -> str:
        args = [
            "/bin/bash",
            "-c",
            "if ! type micromamba  >/dev/null 2>&1; then "
            "mkdir -p ~/.local/bin >/dev/null 2>&1; "
            'python -c "import requests, bz2, sys; '
            "data = requests.get('https://micro.mamba.pm/api/micromamba/%s/latest').content; "
            'sys.stdout.buffer.write(bz2.decompress(data))" | '
            "tar -xv -C ~/.local/bin/ --strip-components=1 bin/micromamba > /dev/null 2>&1; "
            "echo $HOME/.local/bin/micromamba; "
            "else which micromamba; fi" % arch_id(),
        ]
        return subprocess.check_output(args).decode("utf-8").strip()

    def _install_local_conda(self):
        start = time.time()
        path = CONDA_LOCAL_PATH  # type: str
        self.echo("    Installing Conda environment at %s ..." % path, nl=False)
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
        with tempfile.NamedTemporaryFile() as tmp:
            with self._storage.load_bytes([path_to_fetch]) as load_results:
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
                raise CondaException("Could not extract environment") from e
        delta_time = int(time.time() - start)
        self.echo(
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
            self._bins = {"conda": self._ensure_micromamba()}
            self._bins["micromamba"] = self._bins["conda"]
            self._conda_executable_type = "micromamba"

    def _install_remote_conda(self):
        # We download the installer and return a path to it
        # To be clean, we try to be in the parent of our current directory to avoid
        # polluting the user code. If that is not possible though, we create something
        # inside the current directory
        parent_dir = os.path.dirname(os.getcwd())
        if os.access(parent_dir, os.W_OK):
            final_path = os.path.join(parent_dir, "conda_env", "__conda_installer")
        else:
            final_path = os.path.join(os.getcwd(), "conda_env", "__conda_installer")

        if os.path.isfile(final_path):
            os.unlink(final_path)
        os.makedirs(os.path.dirname(final_path), exist_ok=True)

        path_to_fetch = os.path.join(
            CONDA_REMOTE_INSTALLER_DIRNAME,
            CONDA_REMOTE_INSTALLER.format(arch=arch_id()),
        )
        if self._storage is None:
            raise MetaflowException(
                msg="Downloading conda remote installer from backend %s is unimplemented!"
                % self._datastore_type
            )
        with self._storage.load_bytes([path_to_fetch]) as load_results:
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
        # In some cases, we were seeing text file busy errors. This will hopefully
        # force everything to be written out (including the chmod above) before we need
        # to use it.
        os.sync()
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

        # Remove anything that has an invalid path
        to_remove = [
            k for k, v in self._bins.items() if v is None or not os.path.isfile(v)
        ]  # type: List[str]
        if to_remove:
            for k in to_remove:
                del self._bins[k]

        if "conda" not in self._bins:
            return InvalidEnvironmentException(
                "No %s binary found" % self._conda_executable_type
            )

        # Check version requirements
        if "cph" in self._bins:
            cph_version = (
                self.call_binary(["--version"], binary="cph")
                .decode("utf-8")
                .split()[-1]
            )
            if parse_version(cph_version) < parse_version("1.9.0"):
                self.echo(
                    "cph is installed but not recent enough (1.9.0 or later is required) "
                    "-- ignoring"
                )
                del self._bins["cph"]

        if "conda-lock" in self._bins:
            conda_lock_version = (
                self.call_binary(["--version"], binary="conda-lock")
                .decode("utf-8")
                .split()[-1]
            )
            if parse_version(conda_lock_version) < parse_version("2.4.0"):
                self.echo(
                    "conda-lock is installed but not recent enough (2.4.0 or later "
                    "is required) --ignoring"
                )
                del self._bins["conda-lock"]
        if "pip" in self._bins:
            pip_version = self.call_binary(["--version"], binary="pip").split(b" ", 2)[
                1
            ]
            # 22.3 has PEP 658 support which can be a big performance boost
            if parse_version(pip_version.decode("utf-8")) < parse_version("22.3"):
                self.echo(
                    "pip is installed but not recent enough (22.3 or later is required) "
                    "-- ignoring"
                )
                del self._bins["pip"]

        if "micromamba version" in self._info_no_lock:
            if parse_version(self._info_no_lock["micromamba version"]) < parse_version(
                "1.4.0"
            ):
                return InvalidEnvironmentException(
                    self._install_message_for_resolver("micromamba")
                )
        else:
            if parse_version(self._info_no_lock["conda_version"]) < parse_version(
                "4.14.0"
            ):
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
        #        self.call_binary(
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
            if "conda-forge" not in "\t".join(self._info_no_lock["channels"]):
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
                    self.echo("Removing potentially corrupt directory at %s" % dir_name)
                    self._remove(os.path.basename(dir_name))
            return None

        if self._conda_executable_type == "micromamba" or CONDA_LOCAL_PATH is not None:
            # For micromamba OR if we are using a specific conda installation
            # (so with CONDA_LOCAL_PATH), only search there
            env_dir = os.path.join(self._info["root_prefix"], "envs")
            with CondaLock(self.echo, self._env_lock_file(os.path.join(env_dir, "_"))):
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
                with CondaLock(self.echo, self._env_lock_file(env)):
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
        if not self._found_binaries:
            self._find_conda_binary()
        return self._info_no_lock

    @property
    def _info_no_lock(self) -> Dict[str, Any]:
        if self._cached_info is None:
            self._cached_info = json.loads(self.call_conda(["info", "--json"]))
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

    def _create(self, env: ResolvedEnvironment, env_name: str) -> str:
        # We first check to see if the environment exists -- if it does, we skip it
        env_dir = os.path.join(self._root_env_dir, env_name)

        self._cached_info = None

        if os.path.isdir(env_dir):
            possible_env_id = self._is_valid_env(env_dir)
            if possible_env_id and possible_env_id == env.env_id:
                # The environment is already created -- we can skip
                self.echo("Environment at '%s' already created and valid" % env_dir)
                return env_dir
            else:
                # Invalid environment
                if possible_env_id is None:
                    self.echo(
                        "Environment at '%s' is incomplete -- re-creating" % env_dir
                    )
                else:
                    self.echo(
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
        self.lazy_fetch_packages(
            env.packages,
            auth_from_urls([s.value for s in env.sources]),
            self._package_dirs[0],
            search_dirs=self._package_dirs,
        )

        # We build the list of explicit URLs to pass to conda to create the environment
        # We know here that we have all the packages present one way or another, we just
        # need to format the URLs appropriately.
        explicit_urls = []  # type: List[str]
        pypi_paths = []  # type: List[str]
        for p in env.packages:
            if p.TYPE == "pypi":
                for f in p.allowed_formats():
                    local_path = p.local_file(f)
                    if local_path:
                        debug.conda_exec(
                            "For %s, using PYPI package at '%s'"
                            % (p.filename, local_path)
                        )
                        pypi_paths.append("%s\n" % local_path)
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
            self.echo(
                "WARNING: conda/mamba do not properly handle installing .conda "
                "packages in offline mode (See https://github.com/conda/conda/issues/11775)."
            )
            self.echo("Going to install micromamba to create environment ...", nl=False)
            self._bins["micromamba"] = self._ensure_micromamba()
            self.echo(" installed at %s" % self._bins["micromamba"])

        self.echo("    Extracting and linking Conda environment ...", nl=False)

        if pypi_paths:
            self.echo(" (conda packages) ...", timestamp=False, nl=False)
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", delete=not debug.conda
        ) as explicit_list:
            # We create an explicit file
            lines = ["@EXPLICIT\n"] + explicit_urls
            explicit_list.writelines(lines)
            explicit_list.flush()
            self.call_binary(
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

        if pypi_paths:
            self.echo(" (pypi packages) ...", timestamp=False, nl=False)
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", delete=not debug.conda
            ) as pypi_list:
                pypi_list.writelines(pypi_paths)
                pypi_list.flush()
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
                        pypi_list.name,
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
                        "Could not install pypi dependencies using '{cmd}' -- got error"
                        "code {code}'; see pretty-printed error above".format(
                            cmd=e.cmd, code=e.returncode
                        )
                    ) from None

        self._cached_info = None

        # We write a `.metaflowenv` file to be able to get back the env_id from it in
        # case the name doesn't contain it. We also write it at the end to be able to
        # better determine if an environment is corrupt (if conda succeeds but not pypi)
        with open(
            os.path.join(env_dir, ".metaflowenv"), mode="w", encoding="utf-8"
        ) as f:
            json.dump(env.env_id, f)

        delta_time = int(time.time() - start)
        self.echo(
            " done in %s second%s." % (delta_time, plural_marker(delta_time)),
            timestamp=False,
        )
        return env_dir

    def _remove(self, env_name: str):
        # TODO: Verify that this is a proper metaflow environment to remove
        self._cached_info = None
        self.call_conda(["env", "remove", "--name", env_name, "--yes", "--quiet"])

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
                    raise CondaException(
                        "Could not acquire lock {}".format(self.lock)
                    ) from e
                if (time.time() - start) >= self.timeout:
                    raise CondaException(
                        "Timeout occurred while acquiring lock {}".format(self.lock)
                    ) from e
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
                        ) from e
                    if (time.time() - start) >= self.timeout:
                        raise CondaException(
                            "Timeout occurred while acquiring lock {}".format(full_file)
                        ) from e
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
