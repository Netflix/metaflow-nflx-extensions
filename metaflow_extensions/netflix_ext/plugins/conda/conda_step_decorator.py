# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

import importlib
import json
import os
import shutil
import sys
import tempfile

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Union,
    cast,
)

from metaflow.datastore.flow_datastore import FlowDataStore
from metaflow.datastore.task_datastore import TaskDataStore
from metaflow.debug import debug
from metaflow.decorators import StepDecorator
from metaflow.extension_support import EXT_PKG
from metaflow.flowspec import FlowSpec
from metaflow.graph import FlowGraph
from metaflow.metadata import MetaDatum
from metaflow.metadata.metadata import MetadataProvider
from metaflow.metaflow_config import CONDA_REMOTE_COMMANDS
from metaflow.metaflow_environment import (
    InvalidEnvironmentException,
    MetaflowEnvironment,
)
from metaflow.plugins.env_escape import generate_trampolines
from metaflow.unbounded_foreach import UBF_CONTROL, UBF_TASK
from metaflow.util import get_metaflow_root

from .conda_environment import CondaEnvironment
from .env_descr import EnvID
from .utils import arch_id


from .conda_common_decorator import (
    CondaRequirementDecoratorMixin,
    NamedEnvRequirementDecoratorMixin,
    PypiRequirementDecoratorMixin,
    SysPackagesRequirementDecoratorMixin,
)
from .conda import Conda


class PackageRequirementStepDecorator(StepDecorator):
    name = "step_package_req"

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
    libraries : Dict[str, str], default {}
        Libraries to use for this step. The key is the name of the package
        and the value is the version to use. Note that versions can
        be specified either as a specific version or as a comma separated string
        of constraints like "<2.0,>=1.5".
    channels : List[str], default []
        Additional channels to search
    pip_packages : Dict[str, str], default {}
        DEPRECATED -- use `@pypi(packages=)` instead.
        Same as libraries but for pip packages.
    pip_sources : List[str], default []
        DEPRECATED -- use `@pypi(extra_indices=)` instead.
        Same as channels but for pip sources.
    python : str, optional, default None
        Version of Python to use, e.g. '3.7.4'. If not specified, the current version
        will be used.
    fetch_at_exec : bool, default False
        DEPRECATED -- use `@named_env(fetch_at_exec=)` instead.
        If set to True, the environment will be fetched when the task is
        executing as opposed to at the beginning of the flow (or at deploy time if
        deploying to a scheduler). This option requires name or pathspec to be
        specified. This is useful, for example, if you want this step to always use
        the latest named environment when it runs as opposed to the latest when it
        is deployed.
    disabled : bool, default False
        If set to True, uses the external environment.
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
    name = "pip"
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


class CondaEnvInternalDecorator(StepDecorator):
    name = "conda_env_internal"
    TYPE = "conda"

    conda = None  # type: Optional[Conda]
    _local_root = None  # type: Optional[str]

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
        # Create a symlink to installed version of metaflow to execute user code against
        path_to_metaflow = os.path.join(get_metaflow_root(), "metaflow")
        path_to_info = os.path.join(get_metaflow_root(), "INFO")
        self._metaflow_home = tempfile.mkdtemp(dir="/tmp")
        self._addl_paths = None  # type: Optional[List[str]]
        os.symlink(path_to_metaflow, os.path.join(self._metaflow_home, "metaflow"))

        # Symlink the INFO file as well to properly propagate down the Metaflow version
        # if launching on AWS Batch for example
        if os.path.isfile(path_to_info):
            os.symlink(path_to_info, os.path.join(self._metaflow_home, "INFO"))
        else:
            # If there is no "INFO" file, we will actually create one in this new
            # place because we won't be able to properly resolve the EXT_PKG extensions
            # the same way as outside conda (looking at distributions, etc). In a
            # Conda environment, as shown below (where we set self._addl_paths), all
            # EXT_PKG extensions are PYTHONPATH extensions. Instead of re-resolving,
            # we use the resolved information that is written out to the INFO file.
            with open(
                os.path.join(self._metaflow_home, "INFO"), mode="wt", encoding="utf-8"
            ) as f:
                f.write(
                    json.dumps(self._env.get_environment_info(include_ext_info=True))
                )

        # Do the same for EXT_PKG
        try:
            m = importlib.import_module(EXT_PKG)
        except ImportError:
            # No additional check needed because if we are here, we already checked
            # for other issues when loading at the toplevel
            pass
        else:
            custom_paths = list(set(m.__path__))  # For some reason, at times, unique
            # paths appear multiple times. We simplify
            # to avoid un-necessary links

            if len(custom_paths) == 1:
                # Regular package; we take a quick shortcut here
                os.symlink(
                    custom_paths[0],
                    os.path.join(self._metaflow_home, EXT_PKG),
                )
            else:
                # This is a namespace package, we therefore create a bunch of directories
                # so we can symlink in those separately and we will add those paths
                # to the PYTHONPATH for the interpreter. Note that we don't symlink
                # to the parent of the package because that could end up including
                # more stuff we don't want
                self._addl_paths = []
                for p in custom_paths:
                    temp_dir = tempfile.mkdtemp(dir=self._metaflow_home)
                    os.symlink(p, os.path.join(temp_dir, EXT_PKG))
                    self._addl_paths.append(temp_dir)

        # Also install any environment escape overrides directly here to enable
        # the escape to work even in non metaflow-created subprocesses
        generate_trampolines(self._metaflow_home)

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
                for var, _ in self._flow._get_parameters():
                    self._env_for_fetch[
                        "METAFLOW_INIT_%s" % var.upper().replace("-", "_")
                    ] = lambda _param=getattr(
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
        # We also set the env var in remote case for is_fetch_at_exec
        # so that it can be used to fill out the bootstrap command with
        # the proper environment
        if self._is_enabled(UBF_TASK) or self._is_fetch_at_exec():
            # Export this for local runs, we will use it to read the "resolved"
            # environment ID in task_pre_step as well as in get_env_id in
            # conda_environment.py. This makes it compatible with the remote
            # bootstrap which also exports it. We do this even for UBF control tasks as
            # this environment variable is then passed to the actual tasks. We don't
            # always create the environment for the control task. Note that this is
            # determined by _is_enabled

            # Note that in the case of a fetch_at_exec, self._env_id is fully resolved
            # (it was resolved in runtime_task_created) so this is the env_id we need
            # to use for our task.
            cli_args.env["_METAFLOW_CONDA_ENV"] = json.dumps(self._env_id)

            # If we are executing remotely, we now have _METAFLOW_CONDA_ENV set-up
            # properly so we will be able to use it in _get_env_id in conda_environment
            # to figure out what environment we need to execute remotely
            if self._is_remote or not self._is_enabled(ubf_context):
                return
        else:
            return

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
        python_path = self._metaflow_home
        if self._addl_paths is not None:
            addl_paths = os.pathsep.join(self._addl_paths)
            python_path = os.pathsep.join([addl_paths, python_path])

        cli_args.env["PYTHONPATH"] = python_path

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
        shutil.rmtree(self._metaflow_home)

    def _is_enabled(self, ubf_context: str = UBF_TASK) -> bool:
        return CondaEnvironment.enabled_for_step(self._step_name, ubf_context)

    def _is_fetch_at_exec(self) -> bool:
        return CondaEnvironment.fetch_at_exec_for_step(self._step_name)
