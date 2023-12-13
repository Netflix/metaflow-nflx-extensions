# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

import json
import os
import platform
import re
import tarfile

from io import BytesIO
from itertools import chain
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

from metaflow.debug import debug

from metaflow.plugins.datastores.local_storage import LocalStorage
from metaflow.flowspec import FlowSpec

from metaflow.exception import MetaflowException

from metaflow.metaflow_config import (
    CONDA_MAGIC_FILE_V2,
    CONDA_REMOTE_COMMANDS,
    get_pinned_conda_libs,
)

from metaflow.metaflow_environment import (
    InvalidEnvironmentException,
    MetaflowEnvironment,
)

from metaflow.unbounded_foreach import UBF_TASK

from metaflow._vendor.packaging.utils import canonicalize_version

from .envsresolver import EnvsResolver
from .utils import (
    arch_id,
    channel_or_url,
    conda_deps_to_pypi_deps,
    get_conda_manifest_path,
    get_sys_packages,
    merge_dep_dicts,
    resolve_env_alias,
)

from .env_descr import (
    AliasType,
    CachedEnvironmentInfo,
    EnvID,
    EnvType,
    ResolvedEnvironment,
)
from .conda import Conda

from .conda_common_decorator import StepRequirement, StepRequirementMixin


class CondaEnvironment(MetaflowEnvironment):
    TYPE = "conda"
    _filecache = None
    _conda = None
    _flow_req = None  # type: Optional[StepRequirement]
    _result_for_step = (
        {}
    )  # type: Dict[str, Tuple[str, StepRequirement, Optional[Tuple[EnvID, EnvType, Optional[ResolvedEnvironment]]]]]

    def __init__(self, flow: FlowSpec):
        self._flow = flow

        self.conda = None  # type: Optional[Conda]

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

        self.conda = cast(Conda, self.conda)
        resolver = EnvsResolver(self.conda)

        for step in self._flow:
            # Figure out the environments that we need to resolve for all steps
            # We will resolve all unique environments in parallel
            env_type, arch, req, base_env = self.extract_merged_reqs_for_step(
                self.conda, self._flow, self._datastore_type, step
            )

            if req.is_disabled or req.is_fetch_at_exec:
                continue

            if req.from_env_name and base_env is None:
                raise InvalidEnvironmentException(
                    "Base environment '%s' was not found for architecture '%s'"
                    % (req.from_env_name, arch)
                )
            # Determine if the base is a full-id -- we don't currently allow this so
            # this should always be false but keeping to not forget later
            base_from_full_id = False
            if req.from_env_name:
                base_from_full_id = (
                    resolve_env_alias(req.from_env_name)[0] == AliasType.FULL_ID
                )

            resolver.add_environment(
                arch,
                req.packages_as_str,
                req.sources,
                {},
                step.name,
                base_env,
                base_from_full_id=base_from_full_id,
            )

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

            self.conda.cache_environments(update_envs, {"conda": list(formats)})
        else:
            update_envs = [
                resolved_env for _, resolved_env, _ in resolver.new_environments()
            ]

        self.conda.add_environments(update_envs)

        # Update the default environment
        for env_id, resolved_env, _ in resolver.resolved_environments():
            if resolved_env and env_id.full_id == "_default":
                self.conda.set_default_environment(resolved_env.env_id)

        # We are done -- write back out the environments.
        # TODO: Not great that this is manual
        self.conda.write_out_environments()

        # Delegate to whatever the base environment needs to do.
        self.base_env.init_environment(echo)

    def validate_environment(
        self, echo: Callable[..., Any], datastore_type: str
    ) -> None:
        self._local_root = cast(str, LocalStorage.get_datastore_root_from_config(echo))
        self._datastore_type = datastore_type
        self.conda = Conda(echo, datastore_type)

        return self.base_env.validate_environment(echo, datastore_type)

    def decospecs(self) -> Tuple[str, ...]:
        return ("conda_env_internal",) + self.base_env.decospecs()

    def bootstrap_commands(self, step_name: str, datastore_type: str) -> List[str]:
        # Bootstrap conda and execution environment for step
        env_id = self.get_env_id_noconda(step_name)
        if env_id is not None:
            if isinstance(env_id, EnvID):
                arg1 = env_id.req_id
                arg2 = env_id.full_id
            else:
                arg1 = env_id
                arg2 = "_fetch_exec"
            return [
                "export CONDA_START=$(date +%s)",
                "echo 'Bootstrapping environment ...'",
                'python -m %s.remote_bootstrap "%s" "%s" %s %s %s'
                % (
                    "metaflow_extensions.netflix_ext.plugins.conda",
                    self._flow.name,
                    step_name,
                    arg1,
                    arg2,
                    datastore_type,
                ),
                "export _METAFLOW_CONDA_ENV=$(cat _env_id)",
                "export PYTHONPATH=$(pwd)/_escape_trampolines:$(printenv PYTHONPATH)",
                # NOTE: Assumes here that remote nodes are Linux
                "if [[ -n $(printenv LD_LIBRARY_PATH) ]]; then "
                "export MF_ORIG_LD_LIBRARY_PATH=$(printenv LD_LIBRARY_PATH); "
                "export LD_LIBRARY_PATH=$(cat _env_path)/lib:$(printenv LD_LIBRARY_PATH); fi",
                "echo 'Environment bootstrapped.'",
                "export CONDA_END=$(date +%s)",
            ]
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
                "all_packages": "; ".join(
                    [
                        p.filename
                        for p in sorted(resolved_env.packages, key=lambda p: p.filename)
                    ]
                ),
            }
            return new_info
        return {"type": "conda"}

    def get_package_commands(self, code_package_url: str, datastore_type: str):
        return self.base_env.get_package_commands(code_package_url, datastore_type)

    def get_environment_info(self, include_ext_info=False):
        return self.base_env.get_environment_info(include_ext_info)

    def executable(self, step_name: str, default: Optional[str] = None) -> str:
        # Get relevant python interpreter for step
        executable = self._get_executable(step_name)
        if executable is not None:
            return executable
        return self.base_env.executable(step_name, default)

    # The below methods are specific to this class (not from the base class)
    def get_env_id_noconda(self, step_name: str) -> Optional[Union[str, EnvID]]:
        return self.get_env_id(cast(Conda, self.conda), step_name)

    def resolve_fetch_at_exec_env(
        self,
        step_name: str,
        envvars: Optional[Dict[str, Union[str, Callable[[], str]]]] = None,
    ) -> Optional[EnvID]:
        step_result = self._result_for_step.get(step_name)
        if step_result is None or step_result[1].from_env_name is None:
            return None
        resolved_name = self.sub_envvars_in_envname(
            step_result[1].from_env_name, envvars
        )

        resolved_env_id = cast(Conda, self.conda).env_id_from_alias(
            resolved_name, arch=step_result[0]
        )
        if resolved_env_id is None:
            raise RuntimeError(
                "Cannot find environment '%s' (from '%s') for arch '%s'"
                % (resolved_name, step_result[1].from_env_name, step_result[0])
            )
        return resolved_env_id

    @classmethod
    def get_env_id(cls, conda: Conda, step_name: str) -> Optional[Union[str, EnvID]]:
        # _METAFLOW_CONDA_ENV is set:
        #  - by remote_bootstrap:
        #    - if executing on a scheduler (no runtime), this will set it to
        #      the fully resolved ID even in the case of fetch_at_exec
        #    - if executing on a remote node with the runtime, it will also set it but
        #      it is unused
        #  - in runtime_step_cli:
        #    - if executing locally, we can use this in our actual execution to create
        #      the environment in runtime_step_cli
        #    - if executing remotely, we use this to determine the executable and
        #      it is used in bootstrap_commands to build the command line
        step_info = cls._result_for_step.get(step_name)
        if step_info is None:
            # _METAFLOW_CONDA_ENV is set:
            #  - by remote_bootstrap:
            #    - if executing on a scheduler (no runtime), this will set it to
            #      the fully resolved ID even in the case of fetch_at_exec
            #    - if executing on a remote node with the runtime, it will also set it but
            #      it is unused
            #  - in runtime_step_cli:
            #    - if executing locally, we can use this in our actual execution to create
            #      the environment in runtime_step_cli
            #    - if executing remotely, we use this to determine the executable and
            #      it is used in bootstrap_commands to build the command line
            resolved_env_id = None
            t = os.environ.get("_METAFLOW_CONDA_ENV")
            if t:
                resolved_env_id = EnvID(*json.loads(t))
            return resolved_env_id
        elif step_info[1].is_disabled:
            # Another case for the use of this function is the "show" command so
            # in that case we do not have _METAFLOW_CONDA_ENV but we do have the
            # information in step_info[1]
            return None
        elif step_info[1].is_fetch_at_exec:
            return step_info[1].from_env_name
        elif step_info[2]:
            # In this case, we should know about the environment -- it will have
            # _default flag at this time
            resolved_env = conda.environment(step_info[2][0], local_only=True)
            if resolved_env:
                return resolved_env.env_id
            raise RuntimeError(
                "Cannot find environment for step '%s' -- this is a bug" % step_name
            )

    @classmethod
    def enabled_for_step(cls, step_name: str, ubf_context: str) -> bool:
        step_info = cls._result_for_step.get(step_name)
        if step_info:
            if step_info[1].is_disabled is None:
                if step_info[1].default_disabled(ubf_context) is None:
                    return ubf_context == UBF_TASK
                return not step_info[1].default_disabled(ubf_context)
            return not step_info[1].is_disabled
        return os.environ.get("_METAFLOW_CONDA_ENV") is not None

    @classmethod
    def fetch_at_exec_for_step(cls, step_name: str) -> bool:
        # NOTE: This method does not work after runtime_step_cli
        # The good thing is we don't need it :)
        step_info = cls._result_for_step.get(step_name)
        if step_info:
            return step_info[1].is_fetch_at_exec or False
        return False

    @classmethod
    def unresolved_from_name_for_step(cls, step_name: str) -> Optional[str]:
        step_info = cls._result_for_step.get(step_name)
        if step_info:
            return step_info[1].from_env_name
        return None

    @classmethod
    def extract_reqs_for_flow(cls, flow: FlowSpec) -> StepRequirement:
        if cls._flow_req is not None:
            return cls._flow_req.copy()

        cls._flow_req = StepRequirement()
        for decos in flow._flow_decorators.values():
            for deco in decos:
                if isinstance(deco, StepRequirementMixin):
                    cls._flow_req.merge_update(deco)
        return cls._flow_req.copy()

    @classmethod
    def extract_reqs_for_step(cls, step: Any) -> StepRequirement:
        req = StepRequirement()
        for step_deco in step.decorators:
            if isinstance(step_deco, StepRequirementMixin):
                req.merge_update(step_deco)
        return req

    @classmethod
    def extract_merged_reqs_for_step(
        cls,
        conda: Conda,
        flow: FlowSpec,
        datastore_type: str,
        step: Any,
        override_arch: Optional[str] = None,
        local_only: bool = False,
    ) -> Tuple[EnvType, str, StepRequirement, Optional[ResolvedEnvironment]]:
        computed_result = cls._result_for_step.get(step.name)
        if computed_result:
            if computed_result[2] is None:
                # Disabled or fetch_at_exec
                return EnvType.CONDA_ONLY, computed_result[0], computed_result[1], None
            return (
                computed_result[2][1],
                computed_result[0],
                computed_result[1],
                computed_result[2][2],
            )

        # First figure out the architecture for this step as well as if a GPU
        # is needed (to inject the proper __cuda dependency)
        resources_deco = [
            deco
            for deco in step.decorators
            if deco.name in ("resources", *CONDA_REMOTE_COMMANDS)
        ]
        step_is_remote = False
        step_gpu_requested = False
        for deco in resources_deco:
            if deco.name in CONDA_REMOTE_COMMANDS:
                step_is_remote = True
            if deco.attributes.get("gpu") not in (None, 0, "0"):
                step_gpu_requested = True

        if override_arch:
            step_arch = override_arch
        else:
            step_arch = "linux-64" if step_is_remote else arch_id()

        final_req = cls.extract_reqs_for_flow(flow)
        step_req = cls.extract_reqs_for_step(step)

        debug.conda_exec(
            "For step %s, got flow req: %s; step req: %s"
            % (step.name, repr(final_req), repr(step_req))
        )

        final_req.override_update(step_req)

        debug.conda_exec(
            "For step %s, merged requirement: %s" % (step.name, repr(final_req))
        )

        if final_req.is_disabled:
            cls._result_for_step[step.name] = (step_arch, final_req, None)
            return (
                EnvType.CONDA_ONLY,
                step_arch,
                final_req,
                None,
            )  # No point going further -- it is disabled
        if final_req.is_fetch_at_exec:
            # In this case, we check things are valid
            if final_req.from_env_name is None:
                raise InvalidEnvironmentException(
                    "In step '%s', a 'fetch-at-exec' environment needs to have an "
                    "environment specified with '@named_env'" % step.name
                )
            if (
                any([v for v in final_req.sources.values()])
                or any([v for v in final_req.packages.values()])
                or final_req.python
            ):
                raise InvalidEnvironmentException(
                    "In step '%s', a 'fetch_at_exec' environment cannot have "
                    "any additional requirements (packages, sources, or python)"
                    % step.name
                )
            cls._result_for_step[step.name] = (step_arch, final_req, None)
            return (
                EnvType.CONDA_ONLY,
                step_arch,
                final_req,
                None,  # No point going further -- we won't be able to get all info
            )

        all_packages = final_req.packages

        has_conda = len(all_packages.get("conda", {})) > 0
        has_pypi = len(all_packages.get("pypi", {})) > 0

        # Figure out the from_env information
        from_env_name = final_req.from_env_name

        from_env = None
        if from_env_name:
            from_env_id = conda.env_id_from_alias(
                from_env_name, step_arch, local_only=local_only
            )
            if from_env_id is not None:
                from_env = conda.environment(from_env_id, local_only=local_only)
                if from_env is None:
                    raise InvalidEnvironmentException(
                        "Base environment '%s' exists but is not available for architecture '%s'"
                        % (from_env_name, step_arch)
                    )
            else:
                raise InvalidEnvironmentException(
                    "Base environment '%s' not found" % from_env_name
                )

        env_type = EnvType.CONDA_ONLY
        if from_env is not None:
            # We keep the same environment if possible
            if (from_env.env_type == EnvType.PYPI_ONLY and has_conda) or (
                from_env.env_type == EnvType.CONDA_ONLY and has_pypi
            ):
                env_type = EnvType.MIXED
            else:
                env_type = from_env.env_type
            # Extract python version from the environment
            for p in from_env.packages:
                if p.package_name == "python":
                    final_req.python = p.package_version
                    break
            else:
                raise InvalidEnvironmentException(
                    "Cannot determine Python version from the base environment"
                )
        else:
            if has_conda and has_pypi:
                env_type = EnvType.MIXED
            elif has_pypi:
                env_type = EnvType.PYPI_ONLY

            if final_req.python is None:
                final_req.python = platform.python_version()
            all_packages.setdefault("conda", {})["python"] = canonicalize_version(
                final_req.python
            )

        # Add pinned dependencies based on env-type; we prefer conda dependencies if
        # env-type is mixed
        if env_type == EnvType.PYPI_ONLY:
            all_packages["pypi"] = merge_dep_dicts(
                all_packages.get("pypi", {}),
                conda_deps_to_pypi_deps(
                    get_pinned_conda_libs(final_req.python, datastore_type)
                ),
            )
        else:
            all_packages["conda"] = merge_dep_dicts(
                all_packages.get("conda", {}),
                get_pinned_conda_libs(final_req.python, datastore_type),
            )

        final_req.packages = all_packages

        # Add the system requirements and default channels.
        # The default channels go into the computation of the req ID so it is important
        # to have them at this time.

        sys_pkgs = get_sys_packages(
            conda.virtual_packages, step_arch, step_gpu_requested
        )

        # The user can specify whatever they want but we inject things they don't
        # specify
        final_req_sys = final_req.packages.setdefault("sys", {})
        for p, v in sys_pkgs.items():
            if p not in final_req_sys:
                final_req_sys[p] = v

        # Update sources -- here the order is important so we explicitly set it
        # This code will put:
        #  - unique conda sources using user sources *first*
        #  - conda sources will be just channels if possible
        #  - pypi sources: default ones first followed by users who basically adds
        #    only extra-indices
        final_sources = {
            "conda": list(
                dict.fromkeys(
                    map(
                        channel_or_url,
                        chain(
                            final_req.sources.get("conda", []),
                            conda.default_conda_channels,
                        ),
                    )
                )
            )
        }

        if env_type != EnvType.CONDA_ONLY:
            final_sources["pypi"] = list(
                dict.fromkeys(
                    chain(conda.default_pypi_sources, final_req.sources.get("pypi", []))
                )
            )

        final_req.sources = final_sources

        debug.conda_exec("For step %s, final req: %s" % (step.name, repr(final_req)))
        # TODO: This is a bit wasteful because we will do this twice but we need
        # to store the env_id here in case this is called from the CLI (ie: we need
        # to be able to get the req_id from steps even if init_environment is not called)
        # We could improve this though it is likely not a huge overhead.
        if from_env:
            _, env_id, _, _, _, _ = EnvsResolver.extract_info_from_base(
                conda,
                from_env,
                final_req.packages_as_str,
                final_req.sources,
                {},
                step_arch,
            )
        else:
            env_id = EnvID(
                ResolvedEnvironment.get_req_id(
                    final_req.packages_as_str, final_req.sources, {}
                ),
                "_default",
                step_arch,
            )
        cls._result_for_step[step.name] = (
            step_arch,
            final_req,
            (env_id, env_type, from_env),
        )
        return env_type, step_arch, final_req, from_env

    @staticmethod
    def sub_envvars_in_envname(
        name: str, addl_env: Optional[Dict[str, Union[str, Callable[[], str]]]] = None
    ) -> str:
        if addl_env is None:
            addl_env = {}
        envvars_to_sub = re.findall(r"\@{(\w+)}", name)
        for envvar in set(envvars_to_sub):
            replacement = os.environ.get(envvar, addl_env.get(envvar))
            if callable(replacement):
                replacement = replacement()
            if replacement is not None:
                name = name.replace("@{%s}" % envvar, replacement)
            else:
                raise InvalidEnvironmentException(
                    "Could not find '%s' in the environment -- needed to resolve '%s'"
                    % (envvar, name)
                )
        return name

    def _get_executable(self, step_name: str) -> Optional[str]:
        env_id = self.get_env_id_noconda(step_name)
        if env_id is not None:
            # The create method in Conda() sets up this symlink when creating the
            # environment.
            return os.path.join(".", "__conda_python")
        return None
