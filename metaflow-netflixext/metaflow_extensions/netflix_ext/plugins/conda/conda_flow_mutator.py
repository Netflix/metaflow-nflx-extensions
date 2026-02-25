import os
import tempfile

from pathlib import Path
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from metaflow import FlowMutator

from .conda_common_decorator import StepRequirementMixin
from .conda_flow_decorator import PackageRequirementFlowDecorator
from .parsers import req_parser, toml_parser
from .utils import (
    call_binary,
    determine_uv_env_python_version,
    compute_file_hash,
)


if TYPE_CHECKING:
    import metaflow


# ## resolved_uv, resolved_req, and resolved_env flow decorators.

# ### 1. TLDR:
# * resolved_uv is a convenient way to allow users to use their uv managed environment,
#   reflected by uv.lock and pyproject.toml files, in Metaflow flows.
# * resolved_req similarly, is a convenient way to allow users to use their *resolved*
#   requirements.txt
# * resolved_env is an both a base class for the previous one, and later to be an umbrella
#   decorator to automatically determine which scenario (uv.lock based or requirements.txt based)
#   the user is using.

# ### 2. Backgrounds

# #### 2.1 the uv movement
# This whole initiative started with uv, and evolved into a more general support for "resolved_env".

# uv is gaining traction as a fast dependency management tool.
# When developing, there are several major ways to use uv in the command.

# Our customers often use uv in the following ways:

# 1. "uv init" in a project folder to create a pyproject.toml file
# 2. "uv add dep" to add more dependencies
# 3. As they add, pyproject.toml is updated to contain the user specified
#     dependencies, and uv.lock is updated to contain the fully resolved
#     (derived) dependencies.
# 4. "uv sync" to sync (install and uninstall) the dependencies
# 5. "uv run" to run a python file with current project defined dependencies.

# And we decided to allow metaflow to directly consume uv's "state" (uv.lock)
# to create metaflow's flow running environment.

# #### 2.2. From uv to resolved_env
# As we progress on the uv initiative, we realized that a new pattern to support
# is emerging: "resolved environments". Contrary to unresolved environments where
# a resolution step is needed (e.g. conda/pip compiling a list of user dependencies
# into a locked expanded set of packages to install), resolved environments
# are already a locked set of packages to install.

# Another example is resolved requirements.txt files, which are often generated
# by from requirements.in, which is smaller set of user-defined dependencies.

# As a result, we decided to create resolved_env as a base class for both
# resolved_uv and resolved_req decorators, and later to be an umbrella decorator
# to automatically determine which scenario the user is using.

# ### 3. Usage (resolved_uv)

#     Users can simply do this:

#     ```
#     @resolved_uv
#     class MyFlow(FlowSpec):
#         ...
#     ```

#     And this uv decorator will locate uv.lock and pyproject.toml files
#     (searching from the flow code directory to its parent directories),
#     and create environment for a whole flow.


# ### 3. Usage (resolved_req)

#     Similarly, users can simply do this:

#     ```
#     @resolved_req
#     class MyFlow(FlowSpec):
#         ...
#     ```

#     And this decorator will
#     1. locate requirements.txt and requirements.in files, searching from the
#        flow code directory to its parent directories
#     2. confirm requirements.txt is resolved.
#     3. create environment for a whole flow

# ### 4. Caveats
#     1. resolved_uv and resolved_req decorators cannot be used together with
#        conda_base, pip_base, pypi_base, named_env_base, sys_packages_base
#        decorators on the same flow.
#     2. Steps in flows decorated by resolved_uv or resolved_req decorators
#        cannot use other environment decorators (for now -- this could be later
#        relaxed to support npconda decorator if we add something like this).
#     3. groups and optional dependencies in pyproject.toml are not supported yet.


class ResolvedEnvironmentBaseFlowMutator(FlowMutator):
    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)

        # The actual scenario computation (finding requirements.txt, uv.lock etc.) is done
        # at mutate() stage, not in init(), because mutable_flow object is not available
        # at init() stage. And we need mutable_flow to find the flow code path
        self.kwargs = kwargs

        # Get cwd at the init stage in case it later changes
        # at the mutate() stage, use_flow_dir parameter will be checked
        # and if the parameter is False, cwd rather than the flow code dir
        # will be used to start search for uv.lock/pyproject.toml etc.
        self.cwd = os.getcwd()

    def mutate(self, mutable_flow):
        if not self._do_mutate():
            return

        ResolvedEnvironmentBaseFlowMutator._verify_no_conflict_flow_decorators(
            mutable_flow
        )

    @staticmethod
    def _locate_first_folder_contains_any_file_from_list(
        file_name_no_path_list: List[str],
        search_start_path: str,
        # The first dir found containing any of the files will need
        # to contain all files if this flag is set to True.
        validate_all_files_in_same_dir: bool = True,
    ) -> Optional[str]:
        for p in [Path(search_start_path).absolute()] + list(
            Path(search_start_path).absolute().parents
        ):
            count = 0
            for file_name_no_path in file_name_no_path_list:
                if (p / file_name_no_path).exists():
                    count += 1
                    if not validate_all_files_in_same_dir:
                        return str(p)

            if count > 0:
                if validate_all_files_in_same_dir and count < len(
                    file_name_no_path_list
                ):
                    raise FileNotFoundError(
                        f"Found only partial files in the same directory {p}. "
                        + f"Expected files: {file_name_no_path_list}"
                    )
                return str(p)

        return None

    @staticmethod
    def _do_mutate() -> bool:
        # Returns true if this decorator should apply and false otherwise
        # It will return false when, for example, we already have the
        # environment setup and we don't really need to do anything else
        return os.environ.get("_METAFLOW_CONDA_ENV") is None

    @staticmethod
    def _mutate_step(
        step_name: str,
        step: "metaflow.user_decorators.mutable_step.MutableStep",
        pylock_toml_path: str,
        # See _parse_requirements_txt() for the format of this dict.
        user_deps: Dict[str, str],
        user_sources: List[str],
        # takes a full_id_unique_keys dict rather than uv_content_hash
        # to allow more flexibility in the future.
        full_id_unique_keys: Dict[str, str],
    ):
        ResolvedEnvironmentBaseFlowMutator._verify_no_conflict_step_decorators(
            step_name, step
        )
        step.add_decorator(
            "pylock_toml_internal",
            deco_kwargs={
                "path": pylock_toml_path,
                "user_deps_for_hash": user_deps,
                "user_sources_for_hash": user_sources,
                "full_id_unique_keys": full_id_unique_keys,
            },
        )

    @staticmethod
    def _verify_no_conflict_flow_decorators(mutable_flow):
        package_req_flow_deco_derived_classnames = (
            PackageRequirementFlowDecorator.get_derived_classes_fullnames()
        )

        conflicting_decorators = []
        for t in mutable_flow.decorator_specs:
            if t[1] in package_req_flow_deco_derived_classnames:
                conflicting_decorators.append(
                    package_req_flow_deco_derived_classnames[t[1]]
                )
        if conflicting_decorators:
            raise ValueError(
                "Conflicting flow decorators detected: "
                f"found {', '.join(conflicting_decorators)}. "
                "resolved_* decorators are not compatible with the following "
                "decorators: "
                f"{', '.join(package_req_flow_deco_derived_classnames.values())}. "
                "Please use only one of the resolved_* decorators per flow."
            )

    @staticmethod
    def _verify_no_conflict_step_decorators(
        step_name: str,
        step: "metaflow.user_decorators.mutable_step.MutableStep",
    ):

        req_mixin_derived_classes = StepRequirementMixin.get_derived_classes_fullnames()
        conflicting_decorators = []
        for t in step.decorator_specs:
            if t[1] in req_mixin_derived_classes:
                conflicting_decorators.append(req_mixin_derived_classes[t[1]])
        if conflicting_decorators:
            raise ValueError(
                f"Conflicting decorators detected on step {step_name}: "
                f"found {', '.join(conflicting_decorators)}. "
                "resolved_* decorators are not compatible with the following "
                "decorators: "
                f"{', '.join(req_mixin_derived_classes.values())}. "
                "Please use only one of the resolved_* decorators per flow."
            )


class ResolvedUVEnvFlowDecorator(ResolvedEnvironmentBaseFlowMutator):
    ENV_FILE_LIST = [
        "uv.lock",
        "pyproject.toml",
    ]
    PARAM_KEYS = [
        "use_flow_dir",
        "path",
    ]

    def init(self, *args, **kwargs):
        # Need to keep this method since the mutator needs to have its own one to be
        # called.
        super().init(*args, **kwargs)

    def mutate(self, mutable_flow):
        if not self._do_mutate():
            return

        super().mutate(mutable_flow)

        filtered_kwargs = {
            k: v
            for k, v in self.kwargs.items()
            if k in ResolvedUVEnvFlowDecorator.PARAM_KEYS and v is not None
        }
        if "path" in filtered_kwargs and "use_flow_dir" in filtered_kwargs:
            raise ValueError("Cannot set both `path` and `use_flow_dir` in resolved_uv")

        if "path" in filtered_kwargs:
            proj_config_dir = filtered_kwargs["path"]
        else:
            cwd = self.cwd
            if filtered_kwargs.get("use_flow_dir", True):
                import inspect

                cwd = os.path.dirname(inspect.getfile(mutable_flow._flow_cls))

            proj_config_dir = self._locate_first_folder_contains_any_file_from_list(
                self.ENV_FILE_LIST, cwd
            )

        # Ideally we should run this check at init() stage not at mutate() stage,
        # but mutable_flow is not available at init() stage.
        if not proj_config_dir:
            raise ValueError(
                "@resolved_uv used outside of a uv directory. "
                "Please specify a directory using the `path` argument "
                f"containing the files: {', '.join(self.ENV_FILE_LIST)}"
            )

        # We need to check if the files exist properly in case the user specifies
        # a specific path. In the other case, we have already located these files
        if any(
            not os.path.exists(os.path.join(proj_config_dir, filename))
            for filename in self.ENV_FILE_LIST
        ):
            raise FileNotFoundError(
                f"Cannot find all required files. Files {', '.join(self.ENV_FILE_LIST)} "
                f"must exist in the specified path {proj_config_dir}"
            )

        temp_toml_file_path, user_deps, user_sources, uv_lock_file_content_hash = (
            self._handle_uv_lock_scenario(
                os.path.join(proj_config_dir, "uv.lock"),
                os.path.join(proj_config_dir, "pyproject.toml"),
            )
        )

        for step_name, step in mutable_flow.steps:
            ResolvedEnvironmentBaseFlowMutator._mutate_step(
                step_name,
                step,
                temp_toml_file_path,
                user_deps,
                user_sources,
                full_id_unique_keys={
                    "uv_lock_content_hash": uv_lock_file_content_hash,
                },
            )

    # @staticmethod
    # def _handle_req_txt_scenario(
    #     requirements_txt_path: str, requirements_in_path: str
    # ) -> tuple[Optional[str], Dict[str, str]]:
    #     raise NotImplementedError("This will be supported in a stacked PR soon.")

    # Returns: (temp_pylock_toml_path, user_deps_dict, user_sources_list, uv_lock_content_hash)
    @staticmethod
    def _handle_uv_lock_scenario(
        uv_lock_path: str, pyproject_toml_path: str
    ) -> Tuple[str, Dict[str, str], List[str], str]:
        # 1. read pyproject.toml
        user_deps, user_srcs = (
            ResolvedUVEnvFlowDecorator._parse_pyproject_toml_to_user_deps_dict(
                pyproject_toml_path
            )
        )

        uv_env_python_version = determine_uv_env_python_version(
            str(Path(pyproject_toml_path).parent)
        )
        user_deps["python"] = uv_env_python_version

        # 2. call uv to generate a temp pylock.toml from uv.lock
        #  Note that this is just a translation, *not* a resolution.

        temp_dir = tempfile.mkdtemp()
        temp_path = str(Path(temp_dir) / "pylock.toml")
        cwd = os.path.dirname(uv_lock_path)

        # "uv export" command does not take an input uv.lock path,
        # it assumes uv.lock is in the current working directory.
        call_binary(
            ["export", "--frozen", "-o", temp_path],
            "uv",
            cwd=cwd,
        )

        # Calculate uv lock file content hash, to be passed to PylockToml Resolver,
        # and eventually ResolvedEnvironment, to be a component of full_id hash calculation,
        # and will be used to invalidate the cache if pyproject.toml is the same but uv.lock changes.
        uv_lock_content_hash = compute_file_hash(uv_lock_path)

        return temp_path, user_deps, user_srcs, uv_lock_content_hash

    @staticmethod
    def _parse_pyproject_toml_to_user_deps_dict(
        pyproject_toml_path: str,
    ) -> Tuple[Dict[str, str], List[str]]:
        with open(pyproject_toml_path, "r", encoding="utf-8") as fd:
            result = toml_parser(fd.read())

        # Eventually python will be one of the packages, and gets checked by
        # EnvsResolver._resolve_environments(), and built in to builder environment
        # packages. And python will be installed by conda, with the specified
        # python version.
        return result["packages"], result.get("extra_indices", [])


class ResolvedReqFlowDecorator(ResolvedEnvironmentBaseFlowMutator):
    """
    This is a placeholder for future support of resolved requirements.txt based flows.

    See ResolvedUVEnvFlowDecorator for more details about the rationale, usage,
    and under the hood design.
    """

    PARAM_KEYS = [
        "use_flow_dir",
        "path",
    ]
    ENV_FILE_LIST = [
        "requirements.txt",
        "requirements.in",
    ]

    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        raise NotImplementedError("This will be supported soon.")

    def mutate(self, mutable_flow):
        if not self._do_mutate():
            return
        # Step 1: find requirements.txt and requirements.in
        # Step 2: confirm requirements.txt is resolved
        # Step 3: parse requirements.in to get user_deps dict
        # Step 4: call uv to translate requirements.txt to pylock.toml
        # Step 5: add pylock_toml_internal decorator to each step
        raise NotImplementedError("This will be supported soon.")

    # Sample return value:
    # {
    #     'pydub': '==0.25.1', 'numpy': '>=1.21.0', 'pandas': '>=1.3.0',
    #     "wrapt": "==1.17.3",
    #     "bdp-gandalf-policy-manager": "==~=0.1.9",
    #     "astroid": "==<=3.3.11",
    #     "blinker": "==<=2.0.0,>=1.9.0",
    # }
    #
    # Refer to test_parsing_requirements() in uv_decorator_test.py for more details.
    @staticmethod
    def _parse_requirements_txt(
        requirements_txt_path: str,
    ) -> Tuple[Dict[str, str], List[str]]:
        with open(requirements_txt_path, "r", encoding="utf-8") as fd:
            result = req_parser(fd.read())
        result["packages"]["python"] = result["python"]
        return result["packages"], result.get("extra_indices", [])
