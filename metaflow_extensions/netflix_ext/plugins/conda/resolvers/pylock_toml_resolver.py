from ..env_descr import (
    PackageSpecification,
    PypiPackageSpecification,
    ResolvedEnvironment,
    EnvType,
)

from ..utils import (
    filter_packages_by_markers,
    get_best_compatible_packages,
    get_maximum_glibc_version,
    get_python_full_version_from_builder_envs,
    parse_explicit_url_pypi_no_hash,
    pypi_tags_from_arch,
)

from metaflow._vendor.packaging.tags import (
    Tag,
)

from metaflow_extensions.netflix_ext.plugins.conda.conda import CondaException

from . import Resolver
from typing import Callable, Dict, Iterable, List, Any, Optional, Tuple
import os
import itertools
from itertools import chain


class PylockTomlResolver(Resolver):
    TYPES = ["pylock_toml"]
    REQUIRES_BUILDER_ENV = True

    """
    Resolve the environment based on the provided pylock.toml file.

    The pylock.toml path will be passed in file_paths['pylock_toml']
    """

    def resolve(
        self,
        env_type: EnvType,
        python_version_requested: str,
        deps: Dict[str, List[str]],
        sources: Dict[str, List[str]],
        extras: Dict[str, List[str]],
        architecture: str,
        builder_envs: Optional[List[ResolvedEnvironment]] = None,
        base_env: Optional[ResolvedEnvironment] = None,
        file_paths: Dict[str, List[str]] = {},
    ) -> Tuple[ResolvedEnvironment, Optional[List[ResolvedEnvironment]]]:
        # Checks on len(tom_path_list) >= 1 skiped due to get_resolver_cls()
        # only triggers this class's resolve() call when its len >= 1
        # builder_envs check also skipped.

        toml_path_list = file_paths["pylock_toml"]

        toml_path = toml_path_list[0]
        if not (
            toml_path is not None
            and isinstance(toml_path, str)
            and toml_path.strip() != ""
        ):
            raise ValueError("pylock_toml_path must be a non-empty string")

        if not os.path.isfile(toml_path):
            raise ValueError(f"TOML file does not exist: {toml_path}")

        toml_root_obj = self._read_toml(toml_path)

        if not builder_envs:
            raise CondaException(
                "PylockTomlResolver requires a non-empty builder_envs. Please contact metaflow team."
            )
        base_conda_envs = [r for r in builder_envs if r.env_id.arch == architecture]
        if not base_conda_envs:
            raise CondaException(
                "Could not find a builder environment for architecture {architecture}. "
                + "Please contact the Metaflow team."
            )

        base_conda_env = base_conda_envs[0]

        glibc_version = get_maximum_glibc_version(architecture, deps)
        python_full_version = get_python_full_version_from_builder_envs(
            builder_envs, architecture
        )
        if python_full_version is None:
            raise CondaException(
                "Could not determine version of Python from conda packages"
            )

        supported_tags = pypi_tags_from_arch(
            python_full_version,
            architecture,
            glibc_version,
        )

        def filter_func(
            package_dict: Dict[str, List[PackageSpecification]]
        ) -> Dict[str, List[PackageSpecification]]:
            return PylockTomlResolver._filter_packages(
                package_dict,
                python_full_version,
                architecture,
            )

        resolved_env = self._translate_pylock_toml_to_resolved_env(
            toml_root_obj,
            env_type,
            deps,
            list(base_conda_env.packages),
            sources,
            extras,
            architecture,
            supported_tags,
            filter_func,
        )

        return (resolved_env, builder_envs)

    """
    At this time, we will only support the wheels format, and explicitly reject vcs, archive, directory
    formats.

    CONTEXT:

    The formats of packages in pylock.toml (https://peps.python.org/pep-0751/) are:
    vcs, archive, sdist+wheels, directory.
    1. The formats are specified by the existence of one of the four inline-tables (subtables): [packages.vcs], [packages.directory],
        [packages.sdist] within a [[package]] table. These four subtables are mutually exclusive
        within the same package. This design ensures that a package can be specified with only one format.
    2. [[packages.Wheels]] is a nested array table mutually exclusive with other formats except [packages.sdist], which makes
       it a nested array only available within a [packages.sdist].
        (More details can be found here: https://peps.python.org/pep-0751/#packages)

    RATIONALE for supporting wheels only in this version:

    1. Despite the user passions in adoption of uv, we want a gradual rollout of support for other formats,
       to ensure our thorough testing.
    2. We do not want our customers to be the first ones to encounter issues with them.
       We will officially support those only after a thorough testing, with abundant real-world
       pylock.py examples with those formats (which we don't have).
    3. We don't want to skip parsing of a particular package when it's vcs/archive/directory,
       a practice which makes the silent failures much harder to debug.
    """
    UNSUPPORTED_PACKAGE_SUBTYPES = {"vcs", "archive", "directory"}

    @staticmethod
    def _read_toml(path: str) -> Dict[str, Any]:
        try:
            import tomli
        except ImportError:
            raise RuntimeError(
                "Could not import the tomli library to use pylock_toml_resolver (or uv decorator). "
                + "We intentionally did not include this as a Metaflow's required dependency. Please "
                + "manually install it when you see this message."
            )

        with open(path, "rb") as f:
            return tomli.load(f)

    @staticmethod
    def _filter_packages(
        packages_dict: Dict[str, List[PackageSpecification]],
        python_full_version: str,
        architecture: str,
        target_env: Optional[Dict[str, str]] = None,
    ) -> Dict[str, List[PackageSpecification]]:

        for key, pkgs in packages_dict.items():
            for pkg in pkgs:
                if key != pkg._package_name:
                    raise CondaException(
                        "Package dict's key should equal to each package's _package_name attribute."
                        f"key={key}, pkg._package_name={pkg._package_name}. Please contact Metaflow team."
                    )

        flattened_packages = list(chain.from_iterable(packages_dict.values()))
        filtered_packages = filter_packages_by_markers(
            flattened_packages,
            python_full_version,
            architecture,
            target_env,
        )

        # TODO: check this name is equal to the previous package dict name.
        filtered_packages.sort(key=lambda pkg: pkg._package_name)
        out_dict = dict(
            (k, list(v))
            for k, v in itertools.groupby(
                filtered_packages, key=lambda pkg: pkg._package_name
            )
        )
        return out_dict

    @staticmethod
    def _translate_pylock_toml_to_resolved_env(
        toml_root_obj: Dict[str, Any],
        env_type: EnvType,
        deps: Dict[str, List[str]],
        base_packages: Iterable[PackageSpecification],
        sources: Dict[str, List[str]],
        extras: Dict[str, List[str]],
        architecture: str,
        supported_tags: List[Tag],
        # We take a Callable here rather than do the filtering directly
        # in this function due to:
        # 1. The filtering call requires knowledge of architecture, python_version,
        #    target_env, which are not directly available in this function,
        #    the bringing in of which complicates this function.
        # 2. Possibility of a centralized filtering logic for all file formats.
        # 3. Easier for unit testing and disabling of filtering.
        package_filter_func: Optional[
            Callable[
                [Dict[str, List[PackageSpecification]]],
                Dict[str, List[PackageSpecification]],
            ]
        ] = None,
    ) -> ResolvedEnvironment:

        packages_dict = PylockTomlResolver._pylock_toml_root_obj_to_packages(
            toml_root_obj
        )

        if package_filter_func:
            packages_dict = package_filter_func(packages_dict)

        best_packages = get_best_compatible_packages(
            packages_dict,
            supported_tags,
        )

        packages: List[PackageSpecification] = list(best_packages.values())
        packages.extend(base_packages)

        resolved_env = PylockTomlResolver._packages_to_resolved_env(
            packages,
            env_type,
            deps,
            sources,
            extras,
            architecture,
        )

        return resolved_env

    @staticmethod
    def _toml_package_to_package_specs(
        toml_package_dict: Dict[str, Any],
    ) -> List[PackageSpecification]:
        """
        Convert a TOML package item to a PackageSpecification object.

        Sample package item from pylock.toml:
        """

        if PylockTomlResolver.UNSUPPORTED_PACKAGE_SUBTYPES & set(
            toml_package_dict.keys()
        ):
            raise CondaException(
                "Currently we only support wheel packages. Please contact the Metaflow team "
                + "if you need us to support more general cases.\n"
                + "And please mention the following information to the Metaflow team:\n"
                f"toml_package_dict.keys()={toml_package_dict.keys()}"
            )

        wheels = toml_package_dict.get("wheels", [])
        if not wheels:
            raise CondaException(
                "We just encountered a package definition that contains no wheels.\n"
                + "Please help us diagnose the issue by mentioning the following information to the Metaflow team:\n"
                f"toml_package_dict={toml_package_dict}"
            )

        packages: List[PackageSpecification] = []

        # This set of attributes will be applied to all wheels within this package.
        package_attributes = {
            k: toml_package_dict.get(k)
            for k in ["name", "version", "marker"]
            if k in toml_package_dict
        }

        for wheel in wheels:
            url = wheel.get("url", "")
            file_hashes = wheel.get("hashes", {})

            if url and file_hashes:
                parse_result = parse_explicit_url_pypi_no_hash(url)
                package = PypiPackageSpecification(
                    filename=parse_result.filename,
                    url=url,
                    hashes=file_hashes,
                    # Confirmed marker will only be a string in pylock.toml
                    environment_marker=package_attributes.get("marker"),
                )

                packages.append(package)
            else:
                # Despite we can continue the loop ignoring this particular wheel,
                # we should raise an exception to make us be aware of this issue in case
                # it's hiding other issues.
                raise CondaException(
                    "We encountered a wheel package that's missing an url or a hash. "
                    + f"Please escalate this case to the Metaflow team. url={url}, file_hashes={file_hashes}"
                )

        return packages

    @staticmethod
    def _pylock_toml_root_obj_to_packages(
        toml_root_obj: Dict[str, Any],
    ) -> Dict[str, List[PackageSpecification]]:
        # Despite pylock.toml being a standard format, let's refrain the scope of
        # of supported scenarios to reduce risks.
        if toml_root_obj.get("created-by", "") != "uv":
            raise CondaException(
                "Currently we only support pylock.toml file generated by uv. Please contact "
                "the Metaflow team if you need us to support more general cases."
            )

        packages = toml_root_obj.get("packages", [])
        out_dict: Dict[str, List[PackageSpecification]] = {}

        for package in packages:
            if not isinstance(package, dict):
                raise CondaException(
                    "Each package in 'packages' should be a dictionary."
                )
            name = package.get("name", "")
            if not name:
                # https://peps.python.org/pep-0751/#packages-name: name is required.
                raise CondaException("Each package in 'packages' must have a name.")

            # Packages of the same name can appear multiple times with different markers.
            # We will apply filtering of them later based on the environment markers.
            out_dict.setdefault(name, []).extend(
                PylockTomlResolver._toml_package_to_package_specs(package)
            )

        return out_dict

    @staticmethod
    def _packages_to_resolved_env(
        packages: List[PackageSpecification],
        env_type: EnvType,
        deps: Dict[str, List[str]],
        sources: Optional[Dict[str, List[str]]],
        extras: Optional[Dict[str, List[str]]],
        architecture: Optional[str] = None,
    ):
        resolved_env = ResolvedEnvironment(
            deps,
            sources,
            extras,
            architecture,
            all_packages=packages,
            env_type=env_type,
        )

        return resolved_env
