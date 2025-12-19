# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

import json
import os
import sys
import tempfile

from metaflow._vendor.packaging.utils import canonicalize_version

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,  # noqa
    Set,  # noqa
    Union,  # noqa
    cast,
)

from metaflow.datastore.flow_datastore import FlowDataStore
from metaflow.datastore.task_datastore import TaskDataStore
from metaflow.decorators import StepDecorator
from metaflow.flowspec import FlowSpec
from metaflow.graph import FlowGraph
from metaflow.metadata_provider import MetaDatum, MetadataProvider
from metaflow.metaflow_config import CONDA_REMOTE_COMMANDS
from metaflow.metaflow_environment import (
    InvalidEnvironmentException,
    MetaflowEnvironment,
)

from metaflow.packaging_sys import ContentType
from metaflow.plugins.env_escape import generate_trampolines
from metaflow.unbounded_foreach import UBF_TASK

from .conda_environment import CondaEnvironment
from .env_descr import EnvID
from .utils import arch_id

from .conda_common_decorator import (
    CondaRequirementDecoratorMixin,
    NamedEnvRequirementDecoratorMixin,
    PypiRequirementDecoratorMixin,
    SysPackagesRequirementDecoratorMixin,
    StepRequirementMixin,
)
from .conda import Conda


class PackageRequirementStepDecorator(StepDecorator):
    name = "step_package_req"

    allow_multiple = True

    def step_init(
        self,
        flow: FlowSpec,
        graph: FlowGraph,
        step_name: str,
        decorators: List[StepDecorator],
        environment: MetaflowEnvironment,
        flow_datastore: FlowDataStore,
        logger: Callable[..., None],
    ):
        if environment.TYPE != "conda":
            raise InvalidEnvironmentException(
                "The *@%s* decorator requires " "--environment=conda" % self.name
            )


class CondaRequirementStepDecorator(
    CondaRequirementDecoratorMixin, PackageRequirementStepDecorator
):
    """
    Specifies the Conda packages for the step.

    Information in this decorator will augment any
    attributes set in the `@conda_base`, `@pypi_base` or `@named_env_base`
    flow-level decorator. Hence you can use the flow decorators to set common libraries
    required by all steps and use `@conda`, `@pypi` to specify step-specific additions
    or replacements.
    Information specified in this decorator will augment the information in the base
    decorator and, in case of a conflict (for example the same library specified in
    both the base decorator and the step decorator), the step decorator's information
    will prevail.

    Parameters
    ----------
    libraries : Dict[str, str], default {}
        Libraries to use for this step. The key is the name of the package
        and the value is the version to use. Note that versions can
        be specified either as a specific version or as a comma separated string
        of constraints like "<2.0,>=1.5".
    channels : List[str], default []
        Additional channels to search
    python : str, optional, default None
        Version of Python to use, e.g. '3.7.4'. If not specified, the current version
        will be used.
    disabled : bool, default False
        If set to True, uses the external environment.
    name : str, optional, default None
        DEPRECATED -- use `@named_env(name=)` instead.
        If specified, can refer to a named environment. The environment referred to
        here will be the one used for this step. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    pathspec : str, optional, default None
        DEPRECATED -- use `@named_env(pathspec=)` instead.
        If specified, can refer to the pathspec of an existing step. The environment
        of this referred step will be used here. If specified, nothing else can be
        specified in this decorator. In the pathspec, you can use `@{}` values and
        environment variables will be used to substitute.
    pip_packages : Dict[str, str], default {}
        DEPRECATED -- use `@pypi(packages=)` instead.
        Same as libraries but for pip packages.
    pip_sources : List[str], default []
        DEPRECATED -- use `@pypi(extra_indices=)` instead.
        Same as channels but for pip sources.
    fetch_at_exec : bool, default False
        DEPRECATED -- use `@named_env(fetch_at_exec=)` instead.
        If set to True, the environment will be fetched when the task is
        executing as opposed to at the beginning of the flow (or at deploy time if
        deploying to a scheduler). This option requires name or pathspec to be
        specified. This is useful, for example, if you want this step to always use
        the latest named environment when it runs as opposed to the latest when it
        is deployed.
    """

    name = "conda"

    def step_init(
        self,
        flow: FlowSpec,
        graph: FlowGraph,
        step_name: str,
        decorators: List[StepDecorator],
        environment: MetaflowEnvironment,
        flow_datastore: FlowDataStore,
        logger: Callable[..., None],
    ):
        deprecated_keys = set(
            ("pip_packages", "pip_sources", "fetch_at_exec", "name", "pathspec")
        ).intersection((k for k, v in self.attributes.items() if v))
        if deprecated_keys:
            logger(
                "*DEPRECATED*: Using '%s' in '@%s' is deprecated. Please use '@pypi' or "
                "'@named_env' instead. " % (", ".join(deprecated_keys), self.name)
            )
        return super().step_init(
            flow, graph, step_name, decorators, environment, flow_datastore, logger
        )


class PypiRequirementStepDecorator(
    PypiRequirementDecoratorMixin, PackageRequirementStepDecorator
):
    """
    Specifies the Pypi packages for the step.

    Information in this decorator will augment any
    attributes set in the `@conda_base`, `@pypi_base` or `@named_env_base`
    flow-level decorator. Hence you can use the flow decorators to set common libraries
    required by all steps and use `@conda`, `@pypi` to specify step-specific additions
    or replacements.
    Information specified in this decorator will augment the information in the base
    decorator and, in case of a conflict (for example the same library specified in
    both the base decorator and the step decorator), the step decorator's information
    will prevail.

    Parameters
    ----------
    packages : Dict[str, str], default {}
        Packages to use for this step. The key is the name of the package
        and the value is the version to use (default `{}`).
    conda_only: Dict[str, str], default {}
        Packages that are only available in conda. These packages should not depend on
        any python package (example ffmpeg). Note that this is different than using
        the `@conda_base` decorator as using `conda_only` will still build a Pypi
        environment instead of a mixed one.
    extra_indices : List[str], default []
        Additional sources to search for
    extras: List[str], default []
        Extra arguments to pass to the resolver. For example "pre" to include
        pre-release packages.
    python : str, optional, default None
        Version of Python to use, e.g. '3.7.4'. If not specified, the current python
        version will be used.
    disabled : bool, default False
        If set to True, uses the external environment.
    name : str, optional, default None
        DEPRECATED -- use `@named_env(name=)` instead.
        If specified, can refer to a named environment. The environment referred to
        here will be the one used for this step. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    pathspec : str, optional, default None
        DEPRECATED -- use `@named_env(name=)` instead.
        If specified, can refer to the pathspec of an existing step. The environment
        of this referred step will be used here. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    fetch_at_exec : bool, default False
        DEPRECATED -- use `@named_env(name=)` instead.
        If set to True, the environment will be fetched when the task is
        executing as opposed to at the beginning of the flow (or at deploy time if
        deploying to a scheduler). This option requires name or pathspec to be
        specified. This is useful, for example, if you want this step to always use
        the latest named environment when it runs as opposed to the latest when it
        is deployed.
    """

    name = "pypi"

    def step_init(
        self,
        flow: FlowSpec,
        graph: FlowGraph,
        step_name: str,
        decorators: List[StepDecorator],
        environment: MetaflowEnvironment,
        flow_datastore: FlowDataStore,
        logger: Callable[..., None],
    ):
        deprecated_keys = set(
            ("sources", "fetch_at_exec", "name", "pathspec")
        ).intersection((k for k, v in self.attributes.items() if v))

        if deprecated_keys:
            logger(
                "*DEPRECATED*: Using '%s' in '@%s' is deprecated. Please use "
                "'@named_env' instead. " % (", ".join(deprecated_keys), self.name)
            )
        return super().step_init(
            flow, graph, step_name, decorators, environment, flow_datastore, logger
        )


class NamedEnvRequirementStepDecorator(
    NamedEnvRequirementDecoratorMixin, PackageRequirementStepDecorator
):
    """
    Specifies a named environment to extract the environment from

    Information in this decorator will augment any
    attributes set in the `@conda_base`, `@pypi_base` or `@named_env_base`
    flow-level decorator. Hence you can use the flow decorators to set common libraries
    required by all steps and use `@conda`, `@pypi` to specify step-specific additions
    or replacements.
    Information specified in this decorator will augment the information in the base
    decorator and, in case of a conflict (for example the same library specified in
    both the base decorator and the step decorator), the step decorator's information
    will prevail.

    Parameters
    ----------
    name : str, optional, default None
        If specified, can refer to a named environment. The environment referred to
        here will be the one used for this step. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    pathspec : str, optional, default None
        If specified, can refer to the pathspec of an existing step. The environment
        of this referred step will be used here. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    fetch_at_exec : bool, default False
        If set to True, the environment will be fetched when the task is
        executing as opposed to at the beginning of the flow (or at deploy time if
        deploying to a scheduler). This option requires name or pathspec to be
        specified. This is useful, for example, if you want this step to always use
        the latest named environment when it runs as opposed to the latest when it
        is deployed.
    disabled : bool, default False
        If set to True, uses the external environment.
    """

    name = "named_env"


class SysPackagesRequirementStepDecorator(
    SysPackagesRequirementDecoratorMixin, PackageRequirementStepDecorator
):
    """
    Specifies system virtual packages for this step.

    This is an advanced usage decorator allowing you to override the __glibc and
    __cuda virtual packages that are used when resolving your environment.

    Parameters
    ----------
    packages : Dict[str, str], default {}
        System virtual packages to use for this flow. Supported keys are "__cuda" and
        "__glibc".
    disabled : bool, default False
        If set to True, uses the external environment.
    """

    name = "sys_packages"


# Here for legacy reason -- use @pypi instead
class PipRequirementStepDecorator(PypiRequirementStepDecorator):
    """
    Specifies the Pypi packages for the step.

    DEPRECATED: please use `@pypi` instead.

    Parameters
    ----------
    name : str, optional, default None
        DEPRECATED -- use `@named_env(name=)` instead.
        If specified, can refer to a named environment. The environment referred to
        here will be the one used for this step. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    pathspec : str, optional, default None
        DEPRECATED -- use `@named_env(name=)` instead.
        If specified, can refer to the pathspec of an existing step. The environment
        of this referred step will be used here. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    packages : Dict[str, str], default {}
        Packages to use for this step. The key is the name of the package
        and the value is the version to use (default `{}`).
    extra_indices : List[str], default []
        Additional sources to search for
    python : str, optional, default None
        Version of Python to use, e.g. '3.7.4'. If not specified, the current python
        version will be used.
    fetch_at_exec : bool, default False
        DEPRECATED -- use `@named_env(name=)` instead.
        If set to True, the environment will be fetched when the task is
        executing as opposed to at the beginning of the flow (or at deploy time if
        deploying to a scheduler). This option requires name or pathspec to be
        specified. This is useful, for example, if you want this step to always use
        the latest named environment when it runs as opposed to the latest when it
        is deployed.
    disabled : bool, default False
        If set to True, uses the external environment.
    """

    name = "pip"

    def step_init(
        self,
        flow: FlowSpec,
        graph: FlowGraph,
        step_name: str,
        decorators: List[StepDecorator],
        environment: MetaflowEnvironment,
        flow_datastore: FlowDataStore,
        logger: Callable[..., None],
    ):
        logger("*DEPRECATED*: Use '@pypi' instead of '@%s'." % self.name)
        return super().step_init(
            flow, graph, step_name, decorators, environment, flow_datastore, logger
        )


class PylockTomlInternalDecorator(StepRequirementMixin, StepDecorator):
    defaults = {
        "path": None,
        "disabled": None,
        # Context for user_deps_for_hash attribute:
        #
        # * For reusing hydrated environments, we were using
        #   env_descr.py::get_req_id() to calculate the id (actually hash) for
        #   each step's environment
        # * In this calculation, three dictionaries: deps (user_deps), sources,
        #   extras, are considered.
        #
        # And why we have this attribute here:
        #
        # * We chose not to add the hash of file_paths to the environment hash
        #   (req_id).
        # * We added a user_deps_for_hash dict parameter for the
        #   pylock_toml_internal decorator.
        # * We deliberately named this parameter not to be user_dep to hint this
        #   passed in dep is only used for hash calculation, not for actual
        #   dependencies resolution.
        # * Only the content in passed pylock_toml file is respected to
        #   determine the actual packages to be resolved.
        # * Caveat: if there are inconsistencies between the user_dep and
        #   pylock_toml, such as a user package is in the resulting pylock.toml
        #   but not specified in user_dep, we could end up reusing the wrong
        #   hydrated environment. And this kind of bug is very hard to locate.
        #
        # Mitigations: 1) we will carefully use this internal decorator with this
        # knowledge, only for the seed users. 2) We will have robust test on the
        # upperstream @uv decorator, which will assign the user_deps_for_hash.
        "user_deps_for_hash": {},
        # The purpose is similar to user_deps_for_hash.
        "user_sources_for_hash": [],
    }

    name = "pylock_toml_internal"

    def step_init(
        self,
        flow: FlowSpec,
        graph: FlowGraph,
        step_name: str,
        decorators: List[StepDecorator],
        environment: MetaflowEnvironment,
        flow_datastore: FlowDataStore,
        logger: Callable[..., None],
    ):
        return super().step_init(
            flow, graph, step_name, decorators, environment, flow_datastore, logger
        )

    @property
    def file_paths(self) -> Dict[str, List[str]]:
        return {
            # TODO
            # more robust handling of a missing pylock_toml key in the requirements path.
            "pylock_toml": [self.attributes["path"]],
        }

    @property
    # See the notes at the definition of user_deps_for_hash for more contexts of this.
    def packages(self) -> Dict[str, Dict[str, str]]:
        return {
            "pypi": {
                k: canonicalize_version(v)
                for k, v in cast(
                    Dict[str, str], self.attributes["user_deps_for_hash"]
                ).items()
            },
        }

    @property
    def sources(self) -> Dict[str, List[str]]:
        return {
            "pypi": self.attributes["user_sources_for_hash"],
        }

    @property
    def python(self) -> Optional[str]:
        return self.attributes["user_deps_for_hash"].get("python")


class CondaEnvInternalDecorator(StepDecorator):
    name = "conda_env_internal"
    TYPE = "conda"

    conda = None  # type: Optional[Conda]
    _local_root = None  # type: Optional[str]

    _metaflow_home = None  # type: Optional[str]
    _addl_env_vars = None  # type: Optional[Dict[str, str]]

    def step_init(
        self,
        flow: FlowSpec,
        graph: FlowGraph,
        step_name: str,
        decorators: List[StepDecorator],
        environment: MetaflowEnvironment,
        flow_datastore: FlowDataStore,
        logger: Callable[..., None],
    ):
        self._echo = logger
        self._env = cast(CondaEnvironment, environment)
        self._flow = flow
        self._step_name = step_name
        self._flow_datastore = flow_datastore

        # Environment variables used in resolving at fetch time pathspec/name
        self._env_for_fetch = {}  # type: Dict[str, Union[str, Callable[[], str]]]

        self._env_id = None  # type: Optional[EnvID]

        self._is_remote = any([d.name in CONDA_REMOTE_COMMANDS for d in decorators])

        os.environ["PYTHONNOUSERSITE"] = "1"

    def runtime_init(self, flow: FlowSpec, graph: FlowGraph, package: Any, run_id: str):

        if self.__class__._metaflow_home is None:
            # Do this ONCE per flow (thus class variable)
            # Create a symlink to installed version of metaflow to execute user code against
            self.__class__._metaflow_home = tempfile.mkdtemp(dir="/tmp")
            package.extract_into(
                self.__class__._metaflow_home,
                ContentType.CODE_CONTENT.value
                | ContentType.MODULE_CONTENT.value
                | ContentType.OTHER_CONTENT.value,
            )

            self.__class__._addl_env_vars = package.get_post_extract_env_vars(
                package.package_metadata, self.__class__._metaflow_home
            )

            # Also install any environment escape overrides directly here to enable
            # the escape to work even in non metaflow-created subprocesses
            generate_trampolines(self.__class__._metaflow_home)

        # If we need to fetch the environment on exec, save the information we need
        # so that we can resolve it using information such as run id, step name, task
        # id and parameter values
        if self._is_enabled() and self._is_fetch_at_exec():
            self._env_for_fetch["METAFLOW_RUN_ID"] = run_id
            self._env_for_fetch["METAFLOW_RUN_ID_BASE"] = run_id
            self._env_for_fetch["METAFLOW_STEP_NAME"] = self._step_name

    def runtime_task_created(
        self,
        task_datastore: TaskDataStore,
        task_id: str,
        split_index: int,
        input_paths: List[str],
        is_cloned: bool,
        ubf_context: str,
    ):
        if self._is_enabled(ubf_context):
            if self._is_fetch_at_exec():
                # We need to ensure we can properly find the environment we are
                # going to run in
                run_id, step_name, task_id = input_paths[0].split("/")
                parent_ds = self._flow_datastore.get_task_datastore(
                    run_id, step_name, task_id
                )
                for var, param in self._flow._get_parameters():
                    if param.IS_CONFIG_PARAMETER:
                        continue
                    self._env_for_fetch[
                        "METAFLOW_INIT_%s" % var.upper().replace("-", "_")
                    ] = lambda _param=getattr(  # type: ignore[misc]
                        self._flow, var
                    ), _var=var, _ds=parent_ds: str(
                        _param.load_parameter(_ds[_var])
                    )
                self._env_for_fetch["METAFLOW_TASK_ID"] = task_id

                self._env_id = self._env.resolve_fetch_at_exec_env(
                    self._step_name, self._env_for_fetch
                )
                if self._env_id is None:
                    raise InvalidEnvironmentException(
                        "Cannot find the environment ID for a fetch-at-exec step"
                    )
            else:
                t = self._env.get_env_id_noconda(self._step_name)
                if isinstance(t, EnvID):
                    self._env_id = t
                else:
                    raise InvalidEnvironmentException(
                        "Unexpected ID for the Conda environment for step '%s': '%s'"
                        % (self._step_name, str(t))
                    )

    def runtime_step_cli(
        self,
        cli_args: Any,  # Importing CLIArgs causes an issue so ignore for now
        retry_count: int,
        max_user_code_retries: int,
        ubf_context: str,
    ):
        # Check if there is a conda environment to use. We check for UBF_TASK (instead of
        # ubf_context) because, even if we don't create the environment for a control task,
        # we want to pass down the environment variable for the mapper tasks.
        if self._is_enabled(UBF_TASK):
            cli_args.env["_METAFLOW_CONDA_ENV"] = json.dumps(self._env_id)
            if self._is_remote or not self._is_enabled(ubf_context):
                # If we are executing remotely, we don't need to actually create the
                # environment at all. We also don't create it if we are local and
                # this is not enabled for the current UBF context.
                return
        else:
            # In this case, there is no need for a conda environment. Since we can
            # be running with a runner *inside* another Metaflow step that *does* have
            # a conda environment, we make sure to clear _METAFLOW_CONDA_ENV so that
            # the outside environment doesn't "leak" (it could have been set for the external
            # process in which case it would get passed down).
            cli_args.env["_METAFLOW_CONDA_ENV"] = ""
            return

        # Create the environment only if executing locally (ie: not self._is_remote)
        conda = cast(Conda, self._env.conda)
        assert self._env_id
        entrypoint = None  # type: Optional[str]
        # Create the environment we are going to use
        existing_env_info = conda.created_environment(self._env_id)
        if existing_env_info:
            self._echo(
                "Using existing Conda environment %s (%s)"
                % (self._env_id.req_id, self._env_id.full_id)
            )
            entrypoint = os.path.join(existing_env_info[1], "bin", "python")
        else:
            # Otherwise, we read the conda file and create the environment locally
            self._echo(
                "Creating Conda environment %s (%s)..."
                % (self._env_id.req_id, self._env_id.full_id)
            )
            resolved_env = conda.environment(self._env_id)
            if resolved_env:
                entrypoint = os.path.join(
                    conda.create_for_step(self._step_name, resolved_env),
                    "bin",
                    "python",
                )
            else:
                raise InvalidEnvironmentException("Cannot create environment")

        # Actually set it up.
        python_path = self.__class__._metaflow_home
        addl_env_vars = {}
        if self.__class__._addl_env_vars:
            for key, value in self.__class__._addl_env_vars.items():
                if key.endswith(":"):
                    addl_env_vars[key[:-1]] = value
                elif key == "PYTHONPATH":
                    path_components = [value]
                    if python_path is not None:
                        path_components.append(python_path)
                    addl_env_vars[key] = os.pathsep.join(path_components)
                else:
                    addl_env_vars[key] = value
        cli_args.env.update(addl_env_vars)

        if entrypoint is None:
            # This should never happen -- it means the environment was not
            # created somehow
            raise InvalidEnvironmentException("No executable found for environment")

        if arch_id().startswith("linux"):
            # No need for MacOS -- with SIP, DYLD_LIBRARY_PATH is ignored anyways
            old_ld_path = os.environ.get("LD_LIBRARY_PATH")
            if old_ld_path is not None:
                cli_args.env["MF_ORIG_LD_LIBRARY_PATH"] = old_ld_path
                cli_args.env["LD_LIBRARY_PATH"] = ":".join(
                    [
                        os.path.join(
                            os.path.dirname(os.path.dirname(entrypoint)), "lib"
                        ),
                        old_ld_path,
                    ]
                )
        cli_args.entrypoint[0] = entrypoint

    def task_pre_step(
        self,
        step_name: str,
        task_datastore: TaskDataStore,
        metadata: MetadataProvider,
        run_id: str,
        task_id: str,
        flow: FlowSpec,
        graph: FlowGraph,
        retry_count: int,
        max_user_code_retries: int,
        ubf_context: str,
        inputs: List[str],
    ):
        if self._is_enabled(ubf_context):
            # Add the Python interpreter's parent to the path. This is to
            # ensure that any non-pythonic dependencies introduced by the conda
            # environment are visible to the user code.
            env_path = os.path.dirname(os.path.realpath(sys.executable))
            if os.environ.get("PATH") is not None:
                env_path = os.pathsep.join([env_path, os.environ["PATH"]])
            os.environ["PATH"] = env_path

            metadata.register_metadata(
                run_id,
                step_name,
                task_id,
                [
                    MetaDatum(
                        field="conda_env_id",
                        value=os.environ["_METAFLOW_CONDA_ENV"],
                        type="conda_env_id",
                        tags=["attempt_id:{0}".format(retry_count)],
                    )
                ],
            )

    def runtime_finished(self, exception: Exception):
        pass
        # if self.__class__._metaflow_home is not None:
        #     shutil.rmtree(self.__class__._metaflow_home)
        #     self.__class__._metaflow_home = None

    def _is_enabled(self, ubf_context: str = UBF_TASK) -> bool:
        return CondaEnvironment.enabled_for_step(self._step_name, ubf_context)

    def _is_fetch_at_exec(self) -> bool:
        return CondaEnvironment.fetch_at_exec_for_step(self._step_name)
