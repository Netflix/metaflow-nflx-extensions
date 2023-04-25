# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

import json
import os
import tarfile

from io import BytesIO
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    cast,
)

from metaflow.plugins.datastores.local_storage import LocalStorage
from metaflow.flowspec import FlowSpec

from metaflow.metaflow_config import (
    CONDA_MAGIC_FILE_V2,
)

from metaflow.metaflow_environment import MetaflowEnvironment

from .envsresolver import EnvsResolver
from .utils import get_conda_manifest_path

from .env_descr import CachedEnvironmentInfo, EnvID, ResolvedEnvironment
from .conda import Conda
from .conda_step_decorator import get_conda_decorator


class CondaEnvironment(MetaflowEnvironment):
    TYPE = "conda"
    _filecache = None
    _conda = None

    def __init__(self, flow: FlowSpec):
        self._flow = flow

        self._conda = None  # type: Optional[Conda]

        # A conda environment sits on top of whatever default environment
        # the user has so we get that environment to be able to forward
        # any calls we don't handle specifically to that one.
        from metaflow.plugins import ENVIRONMENTS
        from metaflow.metaflow_config import DEFAULT_ENVIRONMENT

        if DEFAULT_ENVIRONMENT == self.TYPE:
            # If the default environment is Conda itself then fallback on
            # the default 'default environment'
            self.base_env = MetaflowEnvironment(self._flow)
        else:
            self.base_env = [
                e
                for e in ENVIRONMENTS + [MetaflowEnvironment]
                if e.TYPE == DEFAULT_ENVIRONMENT
            ][0](self._flow)

    def init_environment(self, echo: Callable[..., None]):
        # Print a message for now
        echo("Bootstrapping Conda environment... (this could take a few minutes)")

        self._conda = cast(Conda, self._conda)

        resolver = EnvsResolver(self._conda)

        for step in self._flow:
            # Figure out the environments that we need to resolve for all steps
            # We will resolve all unique environments in parallel
            step_conda_dec = get_conda_decorator(self._flow, step.name)
            resolver.add_environment_for_step(step.name, step_conda_dec)

        resolver.resolve_environments(echo)

        update_envs = []  # type: List[ResolvedEnvironment]
        if self._datastore_type != "local":
            # We may need to update caches
            # Note that it is possible that something we needed to resolve, we don't need
            # to cache (if we resolved to something already cached).
            formats = set()  # type: Set[str]
            for _, resolved_env, f, _ in resolver.need_caching_environments():
                update_envs.append(resolved_env)
                formats.update(f)

            self._conda.cache_environments(update_envs, {"conda": list(formats)})
        else:
            update_envs = [
                resolved_env for _, resolved_env, _ in resolver.new_environments()
            ]

        self._conda.add_environments(update_envs)

        # Update the default environment
        for env_id, resolved_env, _ in resolver.resolved_environments():
            if env_id.full_id == "_default":
                self._conda.set_default_environment(resolved_env.env_id)

        # We are done -- write back out the environments.
        # TODO: Not great that this is manual
        self._conda.write_out_environments()

        # Delegate to whatever the base environment needs to do.
        self.base_env.init_environment(echo)

    def validate_environment(
        self, echo: Callable[..., Any], datastore_type: str
    ) -> None:
        self._local_root = cast(str, LocalStorage.get_datastore_root_from_config(echo))
        self._datastore_type = datastore_type
        self._conda = Conda(echo, datastore_type)

        return self.base_env.validate_environment(echo, datastore_type)

    def decospecs(self) -> Tuple[str, ...]:
        # Apply conda decorator and base environment's decorators to all steps
        return ("conda",) + self.base_env.decospecs()

    def _get_env_id(self, step_name: str) -> Optional[EnvID]:
        conda_decorator = get_conda_decorator(self._flow, step_name)
        if conda_decorator.is_enabled():
            resolved_env = cast(Conda, self._conda).environment(conda_decorator.env_id)
            if resolved_env:
                return resolved_env.env_id
        return None

    def _get_executable(self, step_name: str) -> Optional[str]:
        env_id = self._get_env_id(step_name)
        if env_id is not None:
            # The create method in Conda() sets up this symlink when creating the
            # environment.
            return os.path.join(".", "__conda_python")
        return None

    def bootstrap_commands(self, step_name: str, datastore_type: str) -> List[str]:
        # Bootstrap conda and execution environment for step
        env_id = self._get_env_id(step_name)
        if env_id is not None:
            return [
                "export CONDA_START=$(date +%s)",
                "echo 'Bootstrapping environment ...'",
                'python -m %s.remote_bootstrap "%s" "%s" %s %s %s'
                % (
                    "metaflow_extensions.netflix_ext.plugins.conda",
                    self._flow.name,
                    step_name,
                    env_id.req_id,
                    env_id.full_id,
                    datastore_type,
                ),
                "export _METAFLOW_CONDA_ENV='%s'"
                % json.dumps(env_id).replace('"', '\\"'),
                "echo 'Environment bootstrapped.'",
                "export CONDA_END=$(date +%s)",
            ]
            # TODO: Add the PATH part (need conda directory)
        return []

    def add_to_package(self) -> List[Tuple[str, str]]:
        # TODO: Improve this to only extract the environments that we care about
        # The issue is that we need to pass a path which is a bit annoying since
        # we then can't clean it up easily.
        files = self.base_env.add_to_package()
        # Add conda manifest file to job package at the top level.

        # In the case of a resume, we still "package" the code but don't send it
        # anywhere -- if we are resuming on a remote node, this file may not exist
        # so we skip
        path = get_conda_manifest_path(self._local_root)
        if path and os.path.exists(path):
            files.append((path, os.path.basename(path)))
        return files

    def pylint_config(self) -> List[str]:
        config = self.base_env.pylint_config()
        # Disable (import-error) in pylint
        config.append("--disable=F0401")
        return config

    def executable(self, step_name: str) -> str:
        # Get relevant python interpreter for step
        executable = self._get_executable(step_name)
        if executable is not None:
            return executable
        return self.base_env.executable(step_name)

    @classmethod
    def get_client_info(
        cls, flow_name: str, metadata: Dict[str, str]
    ) -> Dict[str, Any]:
        # TODO: FIX THIS: this doesn't work with the new format.
        if cls._filecache is None:
            from metaflow.client.filecache import FileCache

            cls._filecache = FileCache()
        info = metadata.get("code-package")
        env_id = json.loads(metadata.get("conda_env_id", "[]"))
        if info is None or not env_id:
            return {"type": "conda"}
        info = json.loads(info)
        _, blobdata = cls._filecache.get_data(
            info["ds_type"], flow_name, info["location"], info["sha"]
        )
        with tarfile.open(fileobj=BytesIO(blobdata), mode="r:gz") as tar:
            conda_file = tar.extractfile(CONDA_MAGIC_FILE_V2)
        if conda_file is None:
            return {"type": "conda"}
        cached_env_info = CachedEnvironmentInfo.from_dict(
            json.loads(conda_file.read().decode("utf-8"))
        )
        resolved_env = cached_env_info.env_for(env_id[0], env_id[1])
        if resolved_env:
            new_info = {
                "type": "conda",
                "req_id": env_id[0],
                "full_id": env_id[1],
                "user_deps": "; ".join(map(str, resolved_env.deps)),
                "all_packages": "; ".join([p.filename for p in resolved_env.packages]),
            }
            return new_info
        return {"type": "conda"}

    def get_package_commands(self, code_package_url: str, datastore_type: str):
        return self.base_env.get_package_commands(code_package_url, datastore_type)

    def get_environment_info(self, include_ext_info=False):
        return self.base_env.get_environment_info(include_ext_info)
