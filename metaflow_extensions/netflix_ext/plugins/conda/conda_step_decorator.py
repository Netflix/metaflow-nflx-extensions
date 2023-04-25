# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

import importlib
import json
import os
import platform
import shutil
import sys
import tempfile

from itertools import chain

from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    cast,
)

from metaflow.plugins.datastores.local_storage import LocalStorage
from metaflow.datastore.flow_datastore import FlowDataStore
from metaflow.datastore.task_datastore import TaskDataStore
from metaflow.debug import debug
from metaflow.decorators import Decorator, StepDecorator
from metaflow.extension_support import EXT_PKG
from metaflow.flowspec import FlowSpec
from metaflow.graph import FlowGraph
from metaflow.metadata import MetaDatum
from metaflow.metadata.metadata import MetadataProvider
from metaflow.metaflow_config import (
    CONDA_REMOTE_COMMANDS,
    get_pinned_conda_libs,
)
from metaflow.metaflow_environment import (
    InvalidEnvironmentException,
    MetaflowEnvironment,
)
from .env_descr import (
    TStr,
    EnvID,
    ResolvedEnvironment,
)
from metaflow.plugins.env_escape import generate_trampolines
from metaflow.unbounded_foreach import UBF_CONTROL
from metaflow.util import get_metaflow_root

from .utils import AliasType, arch_id, resolve_env_alias
from .conda import Conda


class CondaStepDecorator(StepDecorator):
    """
    Specifies the Conda environment for the step.

    Information in this decorator will augment any
    attributes set in the `@conda_base` flow-level decorator. Hence
    you can use `@conda_base` to set common libraries required by all
    steps and use `@conda` to specify step-specific additions.

    Parameters
    ----------
    name : Optional[str]
        If specified, can refer to a named environment. The environment referred to
        here will be the one used for this step. If specified, nothing else can be
        specified in this decorator
    pathspec : Optional[str]
        If specified, can refer to the pathspec of an existing step. The environment
        of this referred step will be used here. If specified, nothing else can be
        specified in this decorator.
    libraries : Dict[str, str]
        Libraries to use for this step. The key is the name of the package
        and the value is the version to use (default: `{}`). Note that versions can
        be specified either as a specific version or as a comma separated string
        of constraints like "<2.0,>=1.5".
    channels : List[str]
        Additional channels to search
    pip_packages : Dict[str, str]
        Same as libraries but for pip packages
    pip_sources : List[str]
        Same as channels but for pip sources
    python : str
        Version of Python to use, e.g. '3.7.4'
        (default: None, i.e. the current Python version).
    disabled : bool
        If set to True, disables Conda (default: False).
    """

    name = "conda"
    defaults = {
        "name": None,
        "pathspec": None,
        "libraries": {},
        "channels": [],
        "pip_packages": {},
        "pip_sources": [],
        "python": None,
        "disabled": None,
    }  # type: Dict[str, Any]

    conda = None  # type: Optional[Conda]
    _local_root = None  # type: Optional[str]

    def is_enabled(self, ubf_context: Optional[str] = None) -> bool:
        if ubf_context == UBF_CONTROL:
            return False
        return not next(
            x
            for x in [
                self.attributes["disabled"],
                self._base_attributes["disabled"],
                False,
            ]
            if x is not None
        )

    @property
    def env_ids(self) -> List[EnvID]:
        # Note this returns a list because initially we had support to specify
        # architectures in the decorator -- keeping for now as this is still valid code
        debug.conda_exec(
            "Requested for step %s: deps: %s; sources: %s"
            % (self._step_name, str(self.step_deps), str(self.source_deps))
        )
        return [
            EnvID(
                req_id=ResolvedEnvironment.get_req_id(
                    self.step_deps,
                    self.source_deps,
                ),
                full_id="_default",
                arch=self._arch,
            )
        ]

    @property
    def env_id(self) -> EnvID:
        arch = self.requested_arch
        my_arch_env = [i for i in self.env_ids if i.arch == arch]
        if my_arch_env:
            return my_arch_env[0]

        raise InvalidEnvironmentException(
            "Architecture '%s' not requested for step" % arch
        )

    @property
    def source_deps(self) -> Sequence[TStr]:
        sources = list(
            map(
                lambda x: TStr("conda", x),
                self._conda_channels(),
            )
        )

        sources.extend(
            map(
                lambda x: TStr("pip", x),
                self._pip_sources(),
            )
        )
        return sources

    @property
    def step_deps(self) -> Sequence[TStr]:
        py_version = self._python_version()
        if py_version == self._from_env_python():
            deps = []
        else:
            deps = [TStr("conda", "python==%s" % self._python_version())]
        # Empty version will just be "I want this package with no version constraints"
        deps.extend(
            TStr("conda", "%s==%s" % (name, ver) if ver else name)
            for name, ver in self._conda_deps().items()
        )
        deps.extend(
            TStr("npconda", "%s==%s" % (name, ver) if ver else name)
            for name, ver in self._np_conda_deps().items()
        )
        # If we have an empty version, we consider that the name is a direct
        # link to a package like a URL
        deps.extend(
            TStr("pip", "%s==%s" % (name, ver) if ver else name)
            for name, ver in self._pip_deps().items()
        )
        return deps

    @property
    def requested_arch(self) -> str:
        return self._arch

    @property
    def local_root(self) -> Optional[str]:
        return self._local_root

    @property
    def from_env_name(self) -> Optional[str]:
        return self._from()

    @property
    def from_env(self) -> Optional[ResolvedEnvironment]:
        from_alias = self._from()
        if from_alias is not None:
            if self._from_env:
                return self._from_env
            # Else, we need to resolve it
            self._get_conda(self._echo, self._flow_datastore_type)
            assert self.conda
            from_env_id = self.conda.env_id_from_alias(from_alias, self._arch)
            if not from_env_id:
                raise InvalidEnvironmentException(
                    "'%s' does not refer to a known Conda environment" % from_alias
                )
            # Here we have a valid env_id so we can now get it for this architecture
            self._from_env = self.conda.environment(from_env_id)
            if self._from_env is None:
                raise InvalidEnvironmentException(
                    "'%s' is a valid Conda environment but does not exist for %s"
                    % (from_alias, self._arch)
                )
        return self._from_env

    def step_init(
        self,
        flow: FlowSpec,
        graph: FlowGraph,
        step_name: str,
        decorators: Sequence[StepDecorator],
        environment: MetaflowEnvironment,
        flow_datastore: FlowDataStore,
        logger: Callable[..., None],
    ):
        if environment.TYPE != "conda":
            raise InvalidEnvironmentException(
                "The *@%s* decorator requires " "--environment=conda" % self.name
            )

        self._echo = logger
        self._env = environment
        self._flow = flow
        self._step_name = step_name
        self._flow_datastore_type = flow_datastore.TYPE  # type: str
        self._base_attributes = self._get_base_attributes()

        self._is_remote = any(
            [deco.name in CONDA_REMOTE_COMMANDS for deco in decorators]
        )
        if self._is_remote:
            self._arch = "linux-64"
        else:
            self._arch = arch_id()

        self.__class__._local_root = LocalStorage.get_datastore_root_from_config(
            self._echo
        )  # type: str

        # Information about the environment this environment is built from
        self._from_env = None  # type: Optional[ResolvedEnvironment]
        self._from_env_conda_deps = None  # type: Optional[Dict[str, str]]
        self._from_env_np_conda_deps = None  # type: Optional[Dict[str, str]]
        self._from_env_conda_channels = None  # type: Optional[List[str]]
        self._from_env_pip_deps = None  # type: Optional[Dict[str, str]]
        self._from_env_pip_sources = None  # type: Optional[List[str]]

        if (self.attributes["name"] or self.attributes["pathspec"]) and len(
            [
                k
                for k, v in self.attributes.items()
                if v and k not in ("name", "pathspec")
            ]
        ):
            raise InvalidEnvironmentException(
                "You cannot specify `name` or `pathspec` along with other attributes in @%s"
                % self.name
            )

        os.environ["PYTHONNOUSERSITE"] = "1"

    def runtime_init(self, flow: FlowSpec, graph: FlowGraph, package: Any, run_id: str):
        # Create a symlink to installed version of metaflow to execute user code against
        path_to_metaflow = os.path.join(get_metaflow_root(), "metaflow")
        path_to_info = os.path.join(get_metaflow_root(), "INFO")
        self._metaflow_home = tempfile.mkdtemp(dir="/tmp")
        self._addl_paths = None
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
                self._addl_paths = []  # type: List[str]
                for p in custom_paths:
                    temp_dir = tempfile.mkdtemp(dir=self._metaflow_home)
                    os.symlink(p, os.path.join(temp_dir, EXT_PKG))
                    self._addl_paths.append(temp_dir)

        # Also install any environment escape overrides directly here to enable
        # the escape to work even in non metaflow-created subprocesses
        generate_trampolines(self._metaflow_home)

    def runtime_step_cli(
        self,
        cli_args: Any,  # Importing CLIArgs causes an issue so ignore for now
        retry_count: int,
        max_user_code_retries: int,
        ubf_context: str,
    ):
        # If remote -- we don't do anything
        if self._is_remote:
            return

        self._get_conda(self._echo, self._flow_datastore_type)
        assert self.conda
        resolved_env = cast(ResolvedEnvironment, self.conda.environment(self.env_id))
        my_env_id = resolved_env.env_id
        # Export this for local runs, we will use it to read the "resolved"
        # environment ID in task_pre_step; this makes it compatible with the remote
        # bootstrap which also exports it. We do this even for UBF control tasks as
        # this environment variable is then passed to the actual tasks. We don't create
        # the environment for the control task -- just for the actual tasks.
        cli_args.env["_METAFLOW_CONDA_ENV"] = json.dumps(my_env_id)

        if self.is_enabled(ubf_context):
            # Create the environment we are going to use
            if self.conda.created_environment(my_env_id):
                self._echo(
                    "Using existing Conda environment %s (%s)"
                    % (my_env_id.req_id, my_env_id.full_id)
                )
            else:
                # Otherwise, we read the conda file and create the environment locally
                self._echo(
                    "Creating Conda environment %s (%s)..."
                    % (my_env_id.req_id, my_env_id.full_id)
                )
                self.conda.create_for_step(self._step_name, resolved_env)

            # Actually set it up.
            python_path = self._metaflow_home
            if self._addl_paths is not None:
                addl_paths = os.pathsep.join(self._addl_paths)
                python_path = os.pathsep.join([addl_paths, python_path])

            cli_args.env["PYTHONPATH"] = python_path
            entrypoint = self.conda.python(my_env_id)
            if entrypoint is None:
                # This should never happen -- it means the environment was not
                # created somehow
                raise InvalidEnvironmentException("No executable found for environment")
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
        if self.is_enabled(ubf_context):
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

    def _get_base_attributes(self) -> Dict[str, Any]:
        if "pip_base" in self._flow._flow_decorators:
            raise InvalidEnvironmentException(
                "@conda decorator is not compatible with @pip_base decorator."
            )
        if "conda_base" in self._flow._flow_decorators:
            return self._flow._flow_decorators["conda_base"][0].attributes
        return self.defaults

    def _python_version(self) -> str:
        return next(
            x
            for x in [
                self.attributes["python"],
                self._base_attributes["python"],
                self._from_env_python(),
                platform.python_version(),
            ]
            if x is not None
        )

    def _from(self) -> Optional[str]:
        return (
            next(
                x
                for x in [
                    self.attributes["name"],
                    "step:%s" % self.attributes["pathspec"]
                    if self.attributes["pathspec"]
                    else None,
                    self._base_attributes["name"],
                    "step:%s" % self._base_attributes["pathspec"]
                    if self._base_attributes["pathspec"]
                    else None,
                    "",
                ]
                if x is not None
            )
            or None
        )

    def _compute_from_env(self):
        base = self.from_env
        if base and self._from_env_conda_deps is None:
            # We either compute all or nothing so it means we computed none
            self._from_env_conda_deps = {}
            self._from_env_pip_deps = {}
            self._from_env_np_conda_deps = {}
            self._from_env_conda_channels = []
            self._from_env_pip_sources = []

            # Take care of dependencies first
            all_deps = base.deps
            for d in all_deps:
                vals = d.value.split("==")
                if len(vals) == 1:
                    vals.append("")
                if d.category == "pip":
                    self._from_env_pip_deps[vals[0]] = vals[1]
                elif d.category == "conda":
                    self._from_env_conda_deps[vals[0]] = vals[1]
                elif d.category == "npconda":
                    self._from_env_np_conda_deps[vals[0]] = vals[1]

            # Now of channels/sources
            all_sources = base.sources
            self._from_env_conda_channels = [
                s.value for s in all_sources if s.category == "conda"
            ]
            self._from_env_pip_sources = [
                s.value for s in all_sources if s.category == "pip"
            ]

    def _from_conda_deps(self) -> Optional[Dict[str, str]]:
        self._compute_from_env()
        return self._from_env_conda_deps

    def _from_np_conda_deps(self) -> Optional[Dict[str, str]]:
        self._compute_from_env()
        return self._from_env_np_conda_deps

    def _from_pip_deps(self) -> Optional[Dict[str, str]]:
        self._compute_from_env()
        return self._from_env_pip_deps

    def _from_conda_channels(self) -> Optional[List[str]]:
        self._compute_from_env()
        return self._from_env_conda_channels

    def _from_pip_sources(self) -> Optional[List[str]]:
        self._compute_from_env()
        return self._from_env_pip_sources

    def _from_env_python(self) -> Optional[str]:
        self._compute_from_env()
        conda_deps = self._from_conda_deps()
        if conda_deps:
            return conda_deps["python"]
        return None

    def _np_conda_deps(self) -> Dict[str, str]:
        if self.from_env:
            return dict(cast(Dict[str, str], self._from_np_conda_deps()))
        return {}

    def _conda_deps(self) -> Dict[str, str]:
        deps = get_pinned_conda_libs(self._python_version(), self._flow_datastore_type)

        if self.from_env:
            deps.update(cast(Dict[str, str], self._from_conda_deps()))

        deps.update(self._base_attributes["libraries"])
        deps.update(self.attributes["libraries"])

        return deps

    def _conda_channels(self) -> List[str]:
        if self.from_env:
            from_channels = cast(List[str], self._from_conda_channels())
        else:
            from_channels = []

        seen = set()  # type: Set[str]
        result = []  # type: List[str]
        for c in chain(
            from_channels,
            self.attributes["channels"],
            self._base_attributes["channels"],
        ):
            if c in seen:
                continue
            seen.add(c)
            result.append(c)
        return result

    def _pip_deps(self) -> Dict[str, str]:
        if self.from_env:
            deps = cast(Dict[str, str], self._from_pip_deps())
        else:
            deps = {}

        deps.update(self._base_attributes["pip_packages"])
        deps.update(self.attributes["pip_packages"])

        return deps

    def _pip_sources(self) -> List[str]:
        if self.from_env:
            from_sources = cast(List[str], self._from_pip_sources())
        else:
            from_sources = []
        seen = set()  # type: Set[str]
        result = []  # type: List[str]
        for c in chain(
            from_sources,
            self.attributes["pip_sources"],
            self._base_attributes["pip_sources"],
        ):
            if c in seen:
                continue
            seen.add(c)
            result.append(c)
        return result

    @classmethod
    def _get_conda(cls, echo: Callable[..., None], datastore_type: str) -> None:
        if cls.conda is None:
            cls.conda = Conda(echo, datastore_type)


class PipStepDecorator(CondaStepDecorator):
    """
    Specifies the Pip environment for the step.

    Information in this decorator will augment any
    attributes set in the `@pip_base` flow-level decorator. Hence
    you can use `@pip_base` to set common libraries required by all
    steps and use `@pip` to specify step-specific additions.

    Parameters
    ----------
    name : Optional[str]
        If specified, can refer to a named environment. The environment referred to
        here will be the one used for this step. If specified, nothing else can be
        specified in this decorator
    pathspec : Optional[str]
        If specified, can refer to the pathspec of an existing step. The environment
        of this referred step will be used here. If specified, nothing else can be
        specified in this decorator.
    packages : Dict[str, str]
        Packages to use for this step. The key is the name of the package
        and the value is the version to use (default: `{}`).
    sources : List[str]
        Additional channels to search for
    python : str
        Version of Python to use, e.g. '3.7.4'
        (default: None, i.e. the current Python version).
    disabled : bool
        If set to True, disables Pip (default: False).
    """

    name = "pip"

    defaults = {
        "name": None,
        "pathspec": None,
        "packages": {},
        "sources": [],
        "python": None,
        "disabled": None,
    }

    def _conda_deps(self) -> Dict[str, str]:
        if self.from_env:
            return cast(Dict[str, str], self._from_conda_deps())
        return {}

    def _conda_channels(self) -> List[str]:
        if self.from_env:
            return cast(List[str], self._from_conda_channels())
        return []

    def _pip_deps(self) -> Dict[str, str]:
        deps = get_pinned_conda_libs(self._python_version(), self._flow_datastore_type)

        if self.from_env:
            deps.update(cast(Dict[str, str], self._from_pip_deps()))

        deps.update(self._base_attributes["packages"])
        deps.update(self.attributes["packages"])

        return deps

    def _pip_sources(self) -> List[str]:
        if self.from_env:
            from_sources = cast(List[str], self._from_pip_sources())
        else:
            from_sources = []
        seen = set()  # type: Set[str]
        result = []  # type: List[str]
        for c in chain(
            from_sources,
            self.attributes["sources"],
            self._base_attributes["sources"],
        ):
            if c in seen:
                continue
            seen.add(c)
            result.append(c)
        return result

    def _get_base_attributes(self) -> Dict[str, Any]:
        if "conda_base" in self._flow._flow_decorators:
            raise InvalidEnvironmentException(
                "@pip decorator is not compatible with @conda_base decorator."
            )
        if "pip_base" in self._flow._flow_decorators:
            return self._flow._flow_decorators["pip_base"].attributes
        return self.defaults

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        if environment.TYPE != "conda":
            raise InvalidEnvironmentException(
                "@%s decorator requires --environment=conda" % self.name
            )

        for idx, deco in enumerate(decorators):
            if deco.name == "conda":
                if deco.statically_defined:
                    raise InvalidEnvironmentException(
                        "@pip decorator is not compatible with @conda. "
                        "If you need both pip and conda dependencies, use @conda and "
                        "pass in the pip dependencies as `pip_packages` and the "
                        "sources as `pip_sources`"
                    )
                else:
                    # Auto inserted decorator -- we remove since we replace it
                    del decorators[idx]
                    break
        return super().step_init(
            flow, graph, step_name, decorators, environment, flow_datastore, logger
        )


def get_conda_decorator(flow: FlowSpec, step_name: str) -> CondaStepDecorator:
    step = next(step for step in flow if step.name == step_name)

    decorator = next(
        deco for deco in step.decorators if isinstance(deco, CondaStepDecorator)
    )
    # Guaranteed to have a conda decorator because of env.decospecs()
    return decorator
