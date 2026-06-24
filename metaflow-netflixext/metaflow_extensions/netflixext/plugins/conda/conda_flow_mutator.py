from itertools import islice
import hashlib
import os
import re
import tempfile

from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from metaflow import FlowMutator
from metaflow._vendor.packaging.requirements import InvalidRequirement, Requirement
from metaflow._vendor.packaging.utils import canonicalize_name

from .conda_common_decorator import StepRequirementMixin
from .conda_flow_decorator import PackageRequirementFlowDecorator
from .parsers import req_parser, toml_parser
from .utils import (
    call_binary,
    determine_uv_env_python_version,
    compute_file_hash,
    CondaException,
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
    _BAZEL_PIP_REPO_RE = re.compile(r"rules_python\+\+pip\+[^/ ]+")
    _DIST_INFO_METADATA_RE = re.compile(r"/site-packages/[^/ ]+\.dist-info/METADATA$")

    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)

    def mutate(self, mutable_flow):
        if not self._do_mutate():
            return
        super().mutate(mutable_flow)

        filtered_kwargs = {
            k: v
            for k, v in self.kwargs.items()
            if k in ResolvedReqFlowDecorator.PARAM_KEYS and v is not None
        }
        if "path" in filtered_kwargs and "use_flow_dir" in filtered_kwargs:
            raise ValueError(
                "Cannot set both `path` and `use_flow_dir` in resolved_req"
            )

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

        # Step 1: find requirements.txt and requirements.in
        req_in = os.path.join(proj_config_dir, "requirements.in")
        resolved_req = os.path.join(proj_config_dir, "requirements.txt")
        if any(not os.path.exists(f) for f in [req_in, resolved_req]):
            raise FileNotFoundError(
                f"Cannot find all required files. Files {', '.join(self.ENV_FILE_LIST)} "
                f"must exist in the specified path {proj_config_dir}"
            )

        # Step 2: confirm requirements.txt is resolved
        if not ResolvedReqFlowDecorator._contains_autogen_hint_in_requirements_txt(
            resolved_req
        ):
            raise CondaException(
                "requirements.txt does not contain hints that it is autogenerated. "
                + "Please ensure the requirements.txt is generated, or explicitly provide "
                + "uv.lock and pyproject.toml to use uv.lock."
            )

        selected_package_names = None
        if ResolvedReqFlowDecorator._should_use_bazel_subset():
            import inspect

            selected_package_names = (
                ResolvedReqFlowDecorator._bazel_runfiles_package_names(
                    inspect.getfile(mutable_flow._flow_cls)
                )
            )

        # Step 3: call uv to translate requirements.txt to pylock.toml
        temp_toml_file_path, user_deps, user_srcs, req_txt_content_hash = (
            ResolvedReqFlowDecorator._handle_req_txt_scenario(
                resolved_req,
                req_in,
                selected_package_names=selected_package_names,
            )
        )

        # Step 4: add pylock_toml_internal decorator to each step
        for step_name, step in mutable_flow.steps:
            ResolvedEnvironmentBaseFlowMutator._mutate_step(
                step_name,
                step,
                temp_toml_file_path,
                user_deps,
                user_srcs,
                full_id_unique_keys={
                    "requirements_txt_content_hash": req_txt_content_hash
                },
            )

    @staticmethod
    def _contains_autogen_hint_in_requirements_txt(requirements_txt_path: str) -> bool:
        with open(requirements_txt_path, "r") as f:
            first_n_lines = "\n".join(islice(f, 5))
            return "autogenerated" in first_n_lines

    # Sample return value:
    # {
    #     'pydub': '==0.25.1', 'numpy': '>=1.21.0', 'pandas': '>=1.3.0',
    #     "wrapt": "==1.17.3",
    #     "astroid": "==<=3.3.11",
    #     "blinker": "==<=2.0.0,>=1.9.0",
    # }
    #
    # Refer to test_parsing_requirements() in uv_decorator_test.py for more details.
    @staticmethod
    def _parse_requirements_in(
        requirements_in_path: str,
    ) -> Tuple[Dict[str, str], List[str]]:
        with open(requirements_in_path, "r", encoding="utf-8") as fd:
            result = req_parser(fd.read())

        # If python is specified in requirements.in, the task environment
        # will be built with this python version.
        # By assigning the python version to result["packages"],
        # this value goes to user_deps, which goes to decorator's
        # ["user_deps_for_hash"], which will be used as the environment's
        # python version.
        if "python" in result:
            result["packages"]["python"] = result["python"]
        return result["packages"], result.get("extra_indices", [])

    @staticmethod
    def _should_use_bazel_subset() -> bool:
        return os.environ.get(
            "METAFLOW_CONDA_RESOLVED_REQ_BAZEL_SUBSET", "1"
        ).lower() not in (
            "0",
            "false",
            "no",
            "off",
        )

    @staticmethod
    def _content_hash(content: str) -> str:
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    @staticmethod
    def _strip_requirement_comment(line: str) -> str:
        comment_idx = line.find("#")
        if comment_idx >= 0:
            before = line[:comment_idx]
            if not before or before[-1] in (" ", "\t"):
                return before.strip()
        return line.strip()

    @staticmethod
    def _requirement_line_name(line: str) -> Optional[str]:
        stripped = ResolvedReqFlowDecorator._strip_requirement_comment(line)
        if not stripped or stripped.startswith("-"):
            return None
        try:
            return canonicalize_name(Requirement(stripped).name)
        except InvalidRequirement:
            return None

    @staticmethod
    def _filter_requirements_txt(
        requirements_txt_path: str, selected_package_names: Set[str]
    ) -> Tuple[str, Set[str]]:
        selected = {canonicalize_name(name) for name in selected_package_names}
        kept_names: Set[str] = set()
        output_lines: List[str] = []

        with open(requirements_txt_path, "r", encoding="utf-8") as fd:
            for raw_line in fd.read().splitlines():
                stripped = raw_line.strip()
                req_name = ResolvedReqFlowDecorator._requirement_line_name(raw_line)
                if req_name is None:
                    # Preserve index/find-links/options and header comments.
                    if (
                        not stripped
                        or stripped.startswith("#")
                        or stripped.startswith("-")
                    ):
                        output_lines.append(raw_line)
                    continue
                if req_name in selected:
                    output_lines.append(raw_line)
                    kept_names.add(req_name)

        if not kept_names:
            raise CondaException(
                "Bazel runfiles package filtering found no packages from %s. "
                "Set METAFLOW_CONDA_RESOLVED_REQ_BAZEL_SUBSET=0 to use the full "
                "requirements.txt file." % requirements_txt_path
            )

        return "\n".join(output_lines) + "\n", kept_names

    @staticmethod
    def _parse_user_deps_for_filtered_lock(
        filtered_requirements_content: str, requirements_in_path: str
    ) -> Tuple[Dict[str, str], List[str]]:
        result = req_parser(filtered_requirements_content)
        user_deps = result["packages"]
        user_sources = result.get("extra_indices", [])

        original_user_deps, _ = ResolvedReqFlowDecorator._parse_requirements_in(
            requirements_in_path
        )
        if "python" in original_user_deps:
            user_deps["python"] = original_user_deps["python"]
        return user_deps, user_sources

    @staticmethod
    def _handle_req_txt_scenario(
        requirements_txt_path: str,
        requirements_in_path: str,
        selected_package_names: Optional[Set[str]] = None,
    ) -> Tuple[str, Dict[str, str], List[str], str]:
        requirements_for_compile = requirements_txt_path
        needs_cleanup = False

        if selected_package_names:
            filtered_content, _ = ResolvedReqFlowDecorator._filter_requirements_txt(
                requirements_txt_path, selected_package_names
            )
            user_deps, user_sources = (
                ResolvedReqFlowDecorator._parse_user_deps_for_filtered_lock(
                    filtered_content, requirements_in_path
                )
            )
            temp_requirements = tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix="-requirements.txt",
                delete=False,
            )
            with temp_requirements:
                temp_requirements.write(filtered_content)
            requirements_for_compile = temp_requirements.name
            needs_cleanup = True
            req_txt_content_hash = ResolvedReqFlowDecorator._content_hash(
                filtered_content
            )
        else:
            # 1. Parse requirements.in for user_deps
            user_deps, user_sources = ResolvedReqFlowDecorator._parse_requirements_in(
                requirements_in_path
            )
            req_txt_content_hash = compute_file_hash(requirements_txt_path)

        # 2. Use uv pip compile on requirements.in to generate resolved output
        #    This is similar to how uv.lock scenario works - we compile/export from source
        temp_dir = tempfile.mkdtemp()
        temp_path = str(Path(temp_dir) / "pylock.toml")

        cwd = os.path.dirname(requirements_txt_path)

        # Why we call `pip compile` on requirements.txt, which involves resolving the resolved
        # file again:
        #
        # * We don’t have a uv command to translate (not resolve) requirements.txt to pylock.toml.
        # * If we resolve requirements.in instead, due to lack of pins in requirements.txt
        #   generated by previous pip resolution, we will get a very different pylock.toml
        #
        # Use uv pip compile vs pip lock to generate pylock.toml from requirements.txt:
        #
        # * In an ideal case, we should have a more complete support for pylock.toml format.
        #   But right now we are only supporting a subset of this standard, which causes our
        #   pylock.toml parser not able to read pylock.toml from pip lock.
        try:
            call_binary(
                [
                    "pip",
                    "compile",
                    requirements_for_compile,
                    "--index-strategy",
                    # With the default "first-index" strategy, uv stops at the first index
                    # that has *any* version of a package, ignoring better matches on later
                    # indices. "unsafe-best-match" checks all indices and picks the best
                    # version across all of them — required when a private index provides
                    # a newer or custom build of a package that also exists on PyPI.
                    "unsafe-best-match",
                    "--output-file",
                    temp_path,
                ],
                "uv",
                cwd=cwd,
            )
        finally:
            if needs_cleanup:
                try:
                    os.unlink(requirements_for_compile)
                except OSError:
                    pass

        return temp_path, user_deps, user_sources, req_txt_content_hash

    @staticmethod
    def _runfiles_roots_for_flow_file(flow_file: str) -> List[Path]:
        roots: List[Path] = []
        for parent in Path(flow_file).parents:
            if parent.name.endswith(".runfiles"):
                roots.append(parent)

        runfiles_dir = os.environ.get("RUNFILES_DIR")
        if runfiles_dir:
            roots.append(Path(runfiles_dir))

        seen: Set[str] = set()
        unique_roots: List[Path] = []
        for root in roots:
            root_str = str(root)
            if root_str not in seen:
                seen.add(root_str)
                unique_roots.append(root)
        return unique_roots

    @staticmethod
    def _runfiles_manifest_candidates(flow_file: str) -> List[Path]:
        candidates: List[Path] = []

        manifest = os.environ.get("RUNFILES_MANIFEST_FILE")
        if manifest:
            candidates.append(Path(manifest))

        for root in ResolvedReqFlowDecorator._runfiles_roots_for_flow_file(flow_file):
            root_str = str(root)
            if root_str.endswith(".runfiles"):
                candidates.append(
                    Path(root_str[: -len(".runfiles")] + ".runfiles_manifest")
                )
                candidates.append(Path(root_str + "_manifest"))

        seen: Set[str] = set()
        unique_candidates: List[Path] = []
        for candidate in candidates:
            candidate_str = str(candidate)
            if candidate_str not in seen:
                seen.add(candidate_str)
                unique_candidates.append(candidate)
        return unique_candidates

    @staticmethod
    def _read_package_name_from_metadata(metadata_path: str) -> Optional[str]:
        try:
            with open(metadata_path, "r", encoding="utf-8", errors="replace") as fd:
                for line in fd:
                    if line.startswith("Name:"):
                        return canonicalize_name(line.split(":", 1)[1].strip())
        except OSError:
            return None
        return None

    @staticmethod
    def _package_name_from_dist_info_path(path: str) -> Optional[str]:
        metadata_path = Path(path)
        dist_info = metadata_path.parent.name
        if not dist_info.endswith(".dist-info"):
            return None
        dist_stem = dist_info[: -len(".dist-info")]
        match = re.match(r"(.+)-[0-9][^-]*$", dist_stem)
        if not match:
            return None
        return canonicalize_name(match.group(1))

    @staticmethod
    def _package_names_from_runfiles_manifest(manifest_path: str) -> Set[str]:
        names: Set[str] = set()
        with open(manifest_path, "r", encoding="utf-8", errors="replace") as fd:
            for line in fd:
                parts = line.rstrip("\n").split(" ", 1)
                if len(parts) != 2:
                    continue
                logical_path, real_path = parts
                if ResolvedReqFlowDecorator._BAZEL_PIP_REPO_RE.search(
                    logical_path
                ) and ResolvedReqFlowDecorator._DIST_INFO_METADATA_RE.search(
                    logical_path
                ):
                    name = ResolvedReqFlowDecorator._read_package_name_from_metadata(
                        real_path
                    )
                    if not name:
                        name = (
                            ResolvedReqFlowDecorator._package_name_from_dist_info_path(
                                logical_path
                            )
                        )
                    if name:
                        names.add(name)
        return names

    @staticmethod
    def _package_names_from_runfiles_dir(runfiles_root: str) -> Set[str]:
        names: Set[str] = set()
        root = Path(runfiles_root)
        for metadata_path in root.glob(
            "rules_python++pip+*/site-packages/*.dist-info/METADATA"
        ):
            name = ResolvedReqFlowDecorator._read_package_name_from_metadata(
                str(metadata_path)
            )
            if name:
                names.add(name)
        return names

    @staticmethod
    def _bazel_runfiles_package_names(flow_file: str) -> Optional[Set[str]]:
        for manifest_path in ResolvedReqFlowDecorator._runfiles_manifest_candidates(
            flow_file
        ):
            if manifest_path.is_file():
                names = ResolvedReqFlowDecorator._package_names_from_runfiles_manifest(
                    str(manifest_path)
                )
                if names:
                    return names

        for runfiles_root in ResolvedReqFlowDecorator._runfiles_roots_for_flow_file(
            flow_file
        ):
            if runfiles_root.is_dir():
                names = ResolvedReqFlowDecorator._package_names_from_runfiles_dir(
                    str(runfiles_root)
                )
                if names:
                    return names

        return None
