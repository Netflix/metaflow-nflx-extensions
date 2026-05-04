from ..env_descr import (
    PackageSpecification,
    PypiCachePackage,
    PypiPackageSpecification,
    ResolvedEnvironment,
    EnvType,
)

from ..pypi_package_builder import PackageToBuild, build_pypi_packages

from ..utils import (
    correct_splitext,
    filter_packages_by_markers,
    get_best_compatible_packages,
    get_maximum_glibc_version,
    get_python_full_version_from_builder_envs,
    parse_explicit_path_pypi,
    parse_explicit_url_pypi_no_hash,
    pypi_tags_from_arch,
)

from metaflow._vendor.packaging.tags import (
    Tag,
)

from metaflow.debug import debug
from ..utils import CondaException

from . import Resolver
from typing import Callable, Dict, Iterable, List, Any, Optional, Tuple
import os
import itertools
import tempfile
from itertools import chain

_FAKE_WHEEL = "_fake-1.0-py3-none-any.whl"


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
        full_id_unique_keys: Optional[Dict[str, str]] = None,
    ) -> Tuple[ResolvedEnvironment, Optional[List[ResolvedEnvironment]]]:
        # Checks on len(tom_path_list) >= 1 skiped due to get_resolver_cls()
        # only triggers this class's resolve() call when its len >= 1
        # builder_envs check also skipped.

        toml_path_list = file_paths["pylock_toml"]
        debug.conda_exec(
            "PylockTomlResolver: resolving for architecture %s" % architecture
        )

        toml_path = toml_path_list[0]
        if not (
            toml_path is not None
            and isinstance(toml_path, str)
            and toml_path.strip() != ""
        ):
            raise ValueError("pylock_toml_path must be a non-empty string")

        if not os.path.isfile(toml_path):
            raise ValueError(f"TOML file does not exist: {toml_path}")

        debug.conda_exec("Reading pylock.toml from %s" % toml_path)
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

        debug.conda_exec(
            "Python version: %s, glibc: %s" % (python_full_version, glibc_version)
        )
        supported_tags = pypi_tags_from_arch(
            python_full_version,
            architecture,
            glibc_version,
        )
        debug.conda_exec("Generated %d supported tags" % len(supported_tags))

        def filter_func(
            package_dict: Dict[str, List[PackageSpecification]],
        ) -> Dict[str, List[PackageSpecification]]:
            return PylockTomlResolver._filter_packages(
                package_dict,
                python_full_version,
                architecture,
            )

        toml_dir = os.path.dirname(os.path.abspath(toml_path))
        resolved_env, to_build_pkg_info = self._translate_pylock_toml_to_resolved_env(
            toml_root_obj,
            env_type,
            deps,
            list(base_conda_env.packages),
            sources,
            extras,
            architecture,
            supported_tags,
            toml_dir,
            filter_func,
            full_id_unique_keys,
        )

        if to_build_pkg_info:
            debug.conda_exec(
                "Need to build %d source packages: %s"
                % (
                    len(to_build_pkg_info),
                    ", ".join(
                        "%s @ %s" % (v.spec, k) for k, v in to_build_pkg_info.items()
                    ),
                )
            )
            if not self._conda.storage:
                raise CondaException(
                    "Cannot create a relocatable environment as it depends on "
                    "source packages and no storage backend is defined: %s"
                    % ", ".join([v.url for v in to_build_pkg_info.values()])
                )
            with tempfile.TemporaryDirectory() as build_dir:
                built_packages, builder_envs = build_pypi_packages(
                    self._conda,
                    self._conda.storage,
                    python_full_version,
                    to_build_pkg_info,
                    builder_envs,
                    build_dir,
                    architecture,
                    supported_tags,
                    sources.get("pypi", []),
                )
            # Reconstruct resolved env with built packages included
            all_packages = list(resolved_env.packages) + built_packages
            resolved_env = PylockTomlResolver._packages_to_resolved_env(
                all_packages,
                env_type,
                deps,
                sources,
                extras,
                architecture,
                full_id_unique_keys=full_id_unique_keys,
            )

        debug.conda_exec(
            "Resolved environment with %d packages" % len(list(resolved_env.packages))
        )
        return (resolved_env, builder_envs)

    """
    Supported package formats in pylock.toml (https://peps.python.org/pep-0751/):
    - wheels: used directly
    - sdist: built into wheels via build_pypi_packages
    - directory: local path packages, built into wheels
    - vcs: git/hg/etc repositories, built into wheels
    - archive: remote tarballs/zips, built into wheels

    Non-wheel formats are built using the same mechanism as PipResolver
    (via pypi_package_builder.build_pypi_packages).
    """

    # TODO(https://netflix.atlassian.net/browse/MLPMF-502): wait for dependency of pylock.py to resolved, so that we could use
    # the pylock.py to read it.
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
        toml_dir: Optional[str] = None,
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
        full_id_unique_keys: Optional[Dict[str, str]] = None,
    ) -> Tuple[ResolvedEnvironment, Dict[str, PackageToBuild]]:

        packages_dict, to_build = PylockTomlResolver._pylock_toml_root_obj_to_packages(
            toml_root_obj, toml_dir
        )

        if package_filter_func:
            pre_filter_count = len(packages_dict)
            packages_dict = package_filter_func(packages_dict)
            debug.conda_exec(
                "Marker filtering: %d -> %d package names"
                % (pre_filter_count, len(packages_dict))
            )

        best_packages = get_best_compatible_packages(
            packages_dict,
            supported_tags,
        )
        debug.conda_exec("Selected %d best compatible wheel(s)" % len(best_packages))

        packages: List[PackageSpecification] = list(best_packages.values())
        packages.extend(base_packages)

        resolved_env = PylockTomlResolver._packages_to_resolved_env(
            packages,
            env_type,
            deps,
            sources,
            extras,
            architecture,
            full_id_unique_keys=full_id_unique_keys,
        )

        return resolved_env, to_build

    @staticmethod
    # TODO(https://netflix.atlassian.net/browse/MLPMF-502): use the pylock.py object instead.
    def _toml_package_to_package_specs(
        toml_package_dict: Dict[str, Any],
        toml_dir: Optional[str] = None,
    ) -> Tuple[List[PackageSpecification], List[Tuple[str, PackageToBuild]]]:
        """
        Convert a TOML package item to PackageSpecification objects and/or
        PackageToBuild entries for packages that need to be built from source.

        Returns a tuple of:
          - List of PackageSpecification (ready-to-use wheel specs)
          - List of (cache_key, PackageToBuild) for packages needing a build

        Sample package item from pylock.toml:

        [[packages]]
        name = "certifi"
        version = "2025.8.3"
        sdist = { url = "https://pypi.netflix.net/packages/18933835518/certifi-2025.8.3.tar.gz", upload-time = 2025-08-03T03:07:47Z, size = 162386, hashes = { sha256 = "e564105f78ded564e3ae7c923924435e1daa7463faeab5bb932bc53ffae63407" } }
        wheels = [{ url = "https://pypi.netflix.net/packages/18933835517/certifi-2025.8.3-py3-none-any.whl", upload-time = 2025-08-03T03:07:45Z, size = 161216, hashes = { sha256 = "f6c12493cfb1b06ba2ff328595af9350c65d6644968e5d3a2ffd78699af217a5" } }]
        """

        packages: List[PackageSpecification] = []
        to_build: List[Tuple[str, PackageToBuild]] = []
        pkg_name = toml_package_dict.get("name", "unknown")
        pkg_version = toml_package_dict.get("version", "")

        # Handle directory packages (local path)
        if "directory" in toml_package_dict:
            dir_info = toml_package_dict["directory"]
            local_path = dir_info.get("path", "")
            if not local_path:
                raise CondaException(
                    "Directory package is missing 'path': %s" % toml_package_dict
                )
            # Resolve relative paths against the pylock.toml directory
            if not os.path.isabs(local_path) and toml_dir:
                local_path = os.path.normpath(os.path.join(toml_dir, local_path))
            if dir_info.get("editable", False):
                raise CondaException(
                    "Cannot include an editable package: '%s'" % local_path
                )
            base_pkg_url = "file://%s/%s-%s.whl" % (local_path, pkg_name, pkg_version)
            cache_key = PypiCachePackage.make_partial_cache_url(
                base_pkg_url, is_real_url=False
            )
            to_build.append(
                (
                    cache_key,
                    PackageToBuild(
                        "file://" + local_path,
                        PypiPackageSpecification(
                            _FAKE_WHEEL,
                            base_pkg_url,
                            is_real_url=False,
                            url_format=".whl",
                        ),
                    ),
                )
            )
            return packages, to_build

        # Handle VCS packages
        if "vcs" in toml_package_dict:
            vcs_info = toml_package_dict["vcs"]
            vcs_type = vcs_info.get("type", "git")
            vcs_url = vcs_info.get("url", "")
            commit = vcs_info.get("commit", "")
            if not vcs_url or not commit:
                raise CondaException(
                    "VCS package is missing 'url' or 'commit': %s" % toml_package_dict
                )
            build_url = "%s+%s@%s" % (vcs_type, vcs_url, commit)
            base_pkg_url = "%s/%s" % (vcs_url, commit)
            if "subdirectory" in vcs_info:
                build_url += "#subdirectory=%s" % vcs_info["subdirectory"]
                base_pkg_url += "/%s" % vcs_info["subdirectory"]
            cache_key = PypiCachePackage.make_partial_cache_url(
                base_pkg_url, is_real_url=False
            )
            to_build.append(
                (
                    cache_key,
                    PackageToBuild(
                        build_url,
                        PypiPackageSpecification(
                            _FAKE_WHEEL,
                            base_pkg_url,
                            is_real_url=False,
                            url_format=".whl",
                        ),
                    ),
                )
            )
            return packages, to_build

        # Handle archive packages (remote URL or local path)
        if "archive" in toml_package_dict:
            archive_info = toml_package_dict["archive"]
            url = archive_info.get("url", "")
            local_path = archive_info.get("path", "")
            file_hashes = archive_info.get("hashes", {})

            if local_path:
                # Resolve relative paths against the pylock.toml directory
                if not os.path.isabs(local_path) and toml_dir:
                    local_path = os.path.normpath(os.path.join(toml_dir, local_path))
                file_url = "file://" + local_path
                parse_result = parse_explicit_path_pypi(file_url)
                _, url_format = correct_splitext(os.path.basename(local_path))
                if url_format == ".whl":
                    # Local wheel — use directly, no build needed
                    spec = PypiPackageSpecification(
                        parse_result.filename,
                        parse_result.url,
                        is_real_url=False,
                        url_format=parse_result.url_format,
                        hashes=file_hashes if file_hashes else None,
                    )
                    spec.add_local_file(parse_result.url_format, local_path)
                    packages.append(spec)
                else:
                    # Local source archive — needs building
                    cache_key = PypiCachePackage.make_partial_cache_url(
                        parse_result.url, is_real_url=False
                    )
                    spec = PypiPackageSpecification(
                        parse_result.filename,
                        parse_result.url,
                        is_real_url=False,
                        url_format=parse_result.url_format,
                        hashes=file_hashes if file_hashes else None,
                    )
                    spec.add_local_file(parse_result.url_format, local_path)
                    to_build.append(
                        (
                            cache_key,
                            PackageToBuild(file_url, spec, [parse_result.url_format]),
                        )
                    )
            elif url:
                parse_result = parse_explicit_url_pypi_no_hash(url)
                cache_key = PypiCachePackage.make_partial_cache_url(
                    parse_result.url, is_real_url=True
                )
                spec = PypiPackageSpecification(
                    parse_result.filename,
                    parse_result.url,
                    url_format=parse_result.url_format,
                    hashes=file_hashes if file_hashes else None,
                )
                to_build.append((cache_key, PackageToBuild(url, spec)))
            else:
                raise CondaException(
                    "Archive package is missing both 'url' and 'path': %s"
                    % toml_package_dict
                )
            return packages, to_build

        # Handle wheels (and sdist fallback)
        wheels = toml_package_dict.get("wheels", [])

        if wheels:
            # This set of attributes will be applied to all wheels within this package.
            package_attributes = {
                k: toml_package_dict.get(k)
                for k in ["name", "version", "marker"]
                if k in toml_package_dict
            }

            for wheel in wheels:
                url = wheel.get("url", "")
                file_hashes = wheel.get("hashes", {})

                if not url:
                    raise CondaException(
                        "Wheel entry is missing a url. "
                        + f"Please escalate this to the Metaflow team. wheel={wheel}"
                    )

                parse_result = parse_explicit_url_pypi_no_hash(url)
                package = PypiPackageSpecification(
                    filename=parse_result.filename,
                    url=url,
                    hashes=file_hashes if file_hashes else None,
                    # Confirmed marker will only be a string in pylock.toml
                    # Thread: https://netflix.slack.com/archives/C0D3Z78F4/p1763265175272329
                    environment_marker=package_attributes.get("marker"),
                )

                packages.append(package)
        elif "sdist" in toml_package_dict:
            # No wheels available, fall back to building from sdist
            sdist_info = toml_package_dict["sdist"]
            url = sdist_info.get("url", "")
            file_hashes = sdist_info.get("hashes", {})
            if not url:
                raise CondaException(
                    "sdist package is missing 'url': %s" % toml_package_dict
                )
            parse_result = parse_explicit_url_pypi_no_hash(url)
            cache_key = PypiCachePackage.make_partial_cache_url(
                parse_result.url, is_real_url=True
            )
            spec = PypiPackageSpecification(
                parse_result.filename,
                parse_result.url,
                url_format=parse_result.url_format,
                hashes=file_hashes if file_hashes else None,
            )
            to_build.append((cache_key, PackageToBuild(url, spec)))
        else:
            raise CondaException(
                "Package has no wheels, sdist, or other recognized format: %s"
                % toml_package_dict
            )

        return packages, to_build

    # TODO(https://netflix.atlassian.net/browse/MLPMF-502): use the open source PyLock
    # code to parse the toml file instead of parsing it to a dict as in current code.
    @staticmethod
    def _pylock_toml_root_obj_to_packages(
        toml_root_obj: Dict[str, Any],
        toml_dir: Optional[str] = None,
    ) -> Tuple[Dict[str, List[PackageSpecification]], Dict[str, PackageToBuild]]:
        # Despite pylock.toml being a standard format, let's refrain the scope of
        # of supported scenarios to reduce risks.
        if toml_root_obj.get("created-by", "") != "uv":
            raise CondaException(
                "Currently we only support pylock.toml file generated by uv. Please contact "
                "the Metaflow team if you need us to support more general cases."
            )

        packages = toml_root_obj.get("packages", [])
        out_dict: Dict[str, List[PackageSpecification]] = {}
        to_build: Dict[str, PackageToBuild] = {}

        # Track package types for summary
        pkg_types: Dict[str, List[str]] = {
            "wheels": [],
            "sdist": [],
            "directory": [],
            "vcs": [],
            "archive_local": [],
            "archive_remote": [],
        }

        for package in packages:
            if not isinstance(package, dict):
                raise CondaException(
                    "Each package in 'packages' should be a dictionary."
                )
            name = package.get("name", "")
            if not name:
                # https://peps.python.org/pep-0751/#packages-name: name is required.
                raise CondaException("Each package in 'packages' must have a name.")

            version = package.get("version", "")
            label = "%s==%s" % (name, version) if version else name

            # Categorize for summary
            if "directory" in package:
                pkg_types["directory"].append(label)
            elif "vcs" in package:
                pkg_types["vcs"].append(label)
            elif "archive" in package:
                if package["archive"].get("path"):
                    pkg_types["archive_local"].append(label)
                else:
                    pkg_types["archive_remote"].append(label)
            elif package.get("wheels"):
                pkg_types["wheels"].append(label)
            elif "sdist" in package:
                pkg_types["sdist"].append(label)

            # Packages of the same name can appear multiple times with different markers.
            # We will apply filtering of them later based on the environment markers.
            # See https://netflix.atlassian.net/browse/MLPMF-545.
            wheel_specs, build_entries = (
                PylockTomlResolver._toml_package_to_package_specs(package, toml_dir)
            )
            out_dict.setdefault(name, []).extend(wheel_specs)
            for cache_key, pkg_to_build in build_entries:
                to_build[cache_key] = pkg_to_build

        # Log a single summary of the pylock.toml content
        total = sum(len(v) for v in pkg_types.values())
        summary_parts = []
        if pkg_types["wheels"]:
            summary_parts.append("%d wheels" % len(pkg_types["wheels"]))
        if pkg_types["sdist"]:
            summary_parts.append(
                "%d sdist-only (need build): %s"
                % (len(pkg_types["sdist"]), ", ".join(pkg_types["sdist"]))
            )
        if pkg_types["directory"]:
            summary_parts.append(
                "%d directory (need build): %s"
                % (len(pkg_types["directory"]), ", ".join(pkg_types["directory"]))
            )
        if pkg_types["vcs"]:
            summary_parts.append(
                "%d vcs (need build): %s"
                % (len(pkg_types["vcs"]), ", ".join(pkg_types["vcs"]))
            )
        if pkg_types["archive_local"]:
            summary_parts.append(
                "%d local archive: %s"
                % (
                    len(pkg_types["archive_local"]),
                    ", ".join(pkg_types["archive_local"]),
                )
            )
        if pkg_types["archive_remote"]:
            summary_parts.append(
                "%d remote archive (need build): %s"
                % (
                    len(pkg_types["archive_remote"]),
                    ", ".join(pkg_types["archive_remote"]),
                )
            )
        debug.conda_exec(
            "pylock.toml: %d packages — %s" % (total, "; ".join(summary_parts))
        )

        return out_dict, to_build

    @staticmethod
    def _packages_to_resolved_env(
        packages: List[PackageSpecification],
        env_type: EnvType,
        deps: Dict[str, List[str]],
        sources: Optional[Dict[str, List[str]]],
        extras: Optional[Dict[str, List[str]]],
        architecture: Optional[str] = None,
        full_id_unique_keys: Optional[Dict[str, str]] = None,
    ):
        resolved_env = ResolvedEnvironment(
            deps,
            sources,
            extras,
            architecture,
            all_packages=packages,
            env_type=env_type,
            full_id_unique_keys=full_id_unique_keys,
        )

        return resolved_env

    """
    Get the best compatible packages for the given supported tags.

    STEP 1 (Context):
    
    The full list of available packages are generated by:
    1. Filtering out packages that don't satisfy the user's conditions (such as by version specifiers)
       For example, "numpy<2.0"
    2. Group all packages by their names.

    For example, for package name = "charset-normalizer", we could the following potential packages:
    { url = "https://pypi.netflix.net/packages/18953191400/charset_normalizer-3.4.3-cp310-cp310-macosx_10_9_universal2.whl", upload-time = 2025-08-09T07:55:36Z, size = 207695, hashes = { sha256 = "fb7f67a1bfa6e40b438170ebdc8158b78dc465a5a67b6dde178a46987b244a72" } },
    { url = "https://pypi.netflix.net/packages/18953191401/charset_normalizer-3.4.3-cp310-cp310-manylinux2014_aarch64.manylinux_2_17_aarch64.manylinux_2_28_aarch64.whl", upload-time = 2025-08-09T07:55:38Z, size = 147153, hashes = { sha256 = "cc9370a2da1ac13f0153780040f465839e6cccb4a1e44810124b4e22483c93fe" } },
    { url = "https://pypi.netflix.net/packages/18953191402/charset_normalizer-3.4.3-cp310-cp310-manylinux2014_ppc64le.manylinux_2_17_ppc64le.manylinux_2_28_ppc64le.whl", upload-time = 2025-08-09T07:55:40Z, size = 160428, hashes = { sha256 = "07a0eae9e2787b586e129fdcbe1af6997f8d0e5abaa0bc98c0e20e124d67e601" } },
    { url = "https://pypi.netflix.net/packages/18953191403/charset_normalizer-3.4.3-cp310-cp310-manylinux2014_s390x.manylinux_2_17_s390x.manylinux_2_28_s390x.whl", upload-time = 2025-08-09T07:55:41Z, size = 157627, hashes = { sha256 = "74d77e25adda8581ffc1c720f1c81ca082921329452eba58b16233ab1842141c" } },
    { url = "https://pypi.netflix.net/packages/18953191404/charset_normalizer-3.4.3-cp310-cp310-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl", upload-time = 2025-08-09T07:55:43Z, size = 152388, hashes = { sha256 = "d0e909868420b7049dafd3a31d45125b31143eec59235311fc4c57ea26a4acd2" } },
    { url = "https://pypi.netflix.net/packages/18953191405/charset_normalizer-3.4.3-cp310-cp310-musllinux_1_2_aarch64.whl", upload-time = 2025-08-09T07:55:44Z, size = 150077, hashes = { sha256 = "c6f162aabe9a91a309510d74eeb6507fab5fff92337a15acbe77753d88d9dcf0" } },
    { url = "https://pypi.netflix.net/packages/18953191406/charset_normalizer-3.4.3-cp310-cp310-musllinux_1_2_ppc64le.whl", upload-time = 2025-08-09T07:55:46Z, size = 161631, hashes = { sha256 = "4ca4c094de7771a98d7fbd67d9e5dbf1eb73efa4f744a730437d8a3a5cf994f0" } },
    { url = "https://pypi.netflix.net/packages/18953191407/charset_normalizer-3.4.3-cp310-cp310-musllinux_1_2_s390x.whl", upload-time = 2025-08-09T07:55:47Z, size = 159210, hashes = { sha256 = "02425242e96bcf29a49711b0ca9f37e451da7c70562bc10e8ed992a5a7a25cc0" } },

    STEP 2 (Context):

    Then we calculated the supported_tags by calling pypi_tags_from_arch(), which generates
    a list of tags that are compatible with the current architecture.
    An example of the supported tags:
    https://github.netflix.net/gist/shuishiy/af1f3287731931b443fddecb005fb7a6

    STEP 3 (this method):
    
    This method iterates through all the supported tags by the current architecture,
    and iterates all the packages in its original sorted order. For each package, the
    architecture is parsed through its package url or file name. And the first package
    that satisfies the current architecture is returned. There is an underlying assumption
    that the package is already ordered in a desired way.
    """
