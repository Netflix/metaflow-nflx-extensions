# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
from __future__ import annotations

import os
import shutil

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Set, Tuple, cast

if TYPE_CHECKING:
    from metaflow.datastore.datastore_storage import DataStoreStorage
    from .conda import Conda

from metaflow.debug import debug

from metaflow._vendor.packaging.tags import Tag
from metaflow._vendor.packaging.utils import parse_wheel_filename

from .env_descr import (
    EnvType,
    PackageSpecification,
    PypiCachePackage,
    PypiPackageSpecification,
    ResolvedEnvironment,
)

from .resolvers.builder_envs_resolver import BuilderEnvsResolver

from .utils import (
    CondaException,
    arch_id,
    auth_from_urls,
    change_pypi_package_version,
    correct_splitext,
    parse_explicit_path_pypi,
)

_DEV_TRANS = str.maketrans("abcdef", "123456")


# This is a dataclass -- can move to that when we only support 3.7+
class PackageToBuild:
    def __init__(
        self,
        url: str,
        spec: Optional[PackageSpecification] = None,
        have_formats: Optional[List[str]] = None,
    ):
        self.url = url
        self.spec = spec
        self.have_formats = have_formats or []


def build_pypi_packages(
    conda: Conda,
    storage: DataStoreStorage,
    python_version: str,
    to_build_pkg_info: Dict[str, PackageToBuild],
    builder_envs: Optional[List[ResolvedEnvironment]],
    build_dir: str,
    architecture: str,
    supported_tags: List[Tag],
    pypi_sources: List[str],
) -> Tuple[List[PackageSpecification], Optional[List[ResolvedEnvironment]]]:
    # We check in the cache -- we don't actually have the filename or
    # hash so we check things starting with the partial URL.
    # The URL in cache will be:
    #  - <base url>/<filename>/<hash>/<filename>

    debug.conda_exec(
        "Checking for pre-built packages: %s"
        % ", ".join(["%s @ %s" % (v.spec, k) for k, v in to_build_pkg_info.items()])
    )
    found_files = cast(
        Sequence[Tuple[str, bool]], storage.list_content(to_build_pkg_info.keys())
    )

    keys_to_check = set()  # type: Set[str]

    # Key: key in to_build_pkg_info
    # Value: list of possible cache paths
    possible_wheels = {}  # type: Dict[str, List[str]]
    for cache_path, is_file in found_files:
        cache_path = cache_path.rstrip("/")
        if is_file:
            raise CondaException("Invalid cache content at '%s'" % cache_path)
        base_cache_path, cache_filename_with_ext = os.path.split(cache_path)
        cache_format = correct_splitext(cache_filename_with_ext)[1]
        if cache_format != ".whl":
            # This is a source format -- we add it to the keys_to_check so we can
            keys_to_check.add(cache_path)
            debug.conda_exec("Found source package at '%s'" % cache_path)
        else:
            # There may be multiple wheel files so we want to pick the best one
            # so we record for now and then we will pick the best one.
            possible_wheels.setdefault(base_cache_path, []).append(cache_path)
            debug.conda_exec("Found potential pre-built package at '%s'" % cache_path)

    # We now check and pick the best wheel if one is compatible and then we will
    # check it further
    for key, wheel_potentials in possible_wheels.items():
        for t in supported_tags:
            # Tags are ordered from most-preferred to least preferred
            for p in wheel_potentials:
                # Potentials are in no particular order but we will
                # effectively get a package with the most preferred tag
                # if one exists
                wheel_name = os.path.split(p)[1]
                _, _, _, tags = parse_wheel_filename(wheel_name)
                if t in tags:
                    keys_to_check.add(p)
                    debug.conda_exec("%s: matching package @ %s" % (key, p))
                    break
            else:
                # If we don't find a match, continue to next tag (and
                # skip break of outer loop on next line)
                continue
            break

    # We now check for hashes for those packages we did find (it's the
    # next level down in the cache)
    found_files = cast(Sequence[Tuple[str, bool]], storage.list_content(keys_to_check))
    for cache_path, is_file in found_files:
        cache_path = cache_path.rstrip("/")
        if is_file:
            raise CondaException("Invalid cache content at '%s'" % cache_path)
        head, _ = os.path.split(cache_path)
        base_cache_path, cache_filename_with_ext = os.path.split(head)
        cache_filename, cache_format = correct_splitext(cache_filename_with_ext)

        pkg_info = to_build_pkg_info[base_cache_path]
        pkg_spec = cast(PypiPackageSpecification, pkg_info.spec)
        pkg_info.have_formats.append(cache_format)
        if cache_format == ".whl":
            # In some cases, we don't know the filename so we change it here (or
            # we need to update it since a tarball has a generic name without
            # ABI, etc but a wheel name has more information)
            if pkg_spec.filename != cache_filename:
                pkg_spec = pkg_spec.clone_with_filename(cache_filename)
                pkg_info.spec = pkg_spec
        debug.conda_exec(
            "%s:%s adding cache file %s"
            % (
                pkg_spec.filename,
                cache_format,
                os.path.join(cache_path, cache_filename_with_ext),
            )
        )
        pkg_spec.add_cached_version(
            cache_format,
            PypiCachePackage(os.path.join(cache_path, cache_filename_with_ext)),
        )

    if arch_id() != architecture:
        # We can't build here so we make sure we have at least something for each
        # pypi package; either we have something in cache (a source or wheel or both)
        # or we have an actual URL pointing to a source tarball.
        not_in_cache_or_local = [
            k for k, v in to_build_pkg_info.items() if not v.have_formats
        ]

        not_downloadable = [
            k
            for k, v in to_build_pkg_info.items()
            if not cast(PackageSpecification, v.spec).is_downloadable_url()
        ]
        no_info = set(not_in_cache_or_local).intersection(not_downloadable)
        if no_info:
            raise CondaException(
                "Cannot build PYPI packages across architectures. "
                "Requirements would have us build: %s. "
                "This may be because you are specifying non wheel dependencies or "
                "no wheel dependencies exist." % ", ".join(no_info)
            )
        return [v.spec for v in to_build_pkg_info.values() if v.spec], builder_envs

    # Determine what we need to build -- all non wheels
    keys_to_build = [
        k for k, v in to_build_pkg_info.items() if ".whl" not in v.have_formats
    ]

    if not keys_to_build:
        return [v.spec for v in to_build_pkg_info.values() if v.spec], builder_envs

    debug.conda_exec(
        "Going to build packages %s"
        % ", ".join([to_build_pkg_info[k].spec.filename for k in keys_to_build])
    )
    # Here we are the same architecture so we can go ahead and build the wheel and
    # add it.
    conda.echo(" (building PYPI packages from repositories)", nl=False)
    debug.conda_exec("Creating builder environment to build PYPI packages")

    # Create the environment in which we will call pip
    # We look for a builder environment for the architecture we are building on
    # (we never build cross arch)
    builder_env = None  # type: Optional[ResolvedEnvironment]
    if builder_envs is not None:
        t = [r for r in builder_envs if r.env_id.arch == arch_id()]
        if t:
            builder_env = t[0]

    if not builder_env:
        builder_env, builder_envs = BuilderEnvsResolver(conda).resolve(
            EnvType.CONDA_ONLY,
            {"conda": ["python==%s" % python_version]},
            {},
            {},
            arch_id(),
            builder_envs,
            None,
        )

        if builder_envs:
            builder_envs.append(builder_env)
        else:
            builder_envs = [builder_env]

    builder_python = os.path.join(
        conda.create_builder_env(builder_env), "bin", "python"
    )

    # Download any source either from cache or the web. We can use our typical
    # lazy fetch to do this. We just make sure that we only pass it packages that
    # it has something to fetch
    target_directory = conda.package_dir("pypi")
    os.makedirs(target_directory, exist_ok=True)
    pkgs_to_fetch = cast(
        List[PypiPackageSpecification],
        [to_build_pkg_info[k].spec for k in keys_to_build],
    )
    pkgs_to_fetch = list(
        filter(
            lambda x: x.is_downloadable_url() or x.cached_version(x.url_format),
            pkgs_to_fetch,
        )
    )
    debug.conda_exec(
        "Going to fetch sources for %s" % ", ".join([p.filename for p in pkgs_to_fetch])
    )
    if pkgs_to_fetch:
        conda.lazy_fetch_packages(
            pkgs_to_fetch, auth_from_urls(pypi_sources), target_directory
        )

    with ThreadPoolExecutor() as executor:
        build_result = [
            executor.submit(
                _build_with_pip,
                conda,
                builder_python,
                os.path.join(build_dir, "build_%d" % idx),
                key,
                cast(PypiPackageSpecification, to_build_pkg_info[key].spec),
                to_build_pkg_info[key].url,
            )
            for idx, key in enumerate(keys_to_build)
        ]
        for f in as_completed(build_result):
            key, build_dir = f.result()
            wheel_files = [
                f
                for f in os.listdir(build_dir)
                if os.path.isfile(os.path.join(build_dir, f)) and f.endswith(".whl")
            ]
            if len(wheel_files) != 1:
                raise CondaException(
                    "Could not build '%s' -- found built packages: %s"
                    % (key, wheel_files)
                )

            pkg_spec = cast(PypiPackageSpecification, to_build_pkg_info[key].spec)
            wheel_file = os.path.join(build_dir, wheel_files[0])
            # Move the built wheel to a less temporary location
            wheel_file = shutil.copy(wheel_file, target_directory)

            parse_result = parse_explicit_path_pypi("file://%s" % wheel_file)
            # If the source is not an actual URL, we are going to change the name
            # of the package to avoid any potential conflict. We consider that
            # packages derived from internet URLs (so likely a source package)
            # do not need name changes
            if not pkg_spec.is_downloadable_url():
                pkg_version = parse_wheel_filename(parse_result.filename + ".whl")[1]

                pkg_version_str = str(pkg_version)
                if not pkg_version.dev:
                    wheel_hash = PypiPackageSpecification.hash_pkg(wheel_file)
                    pkg_version_str += ".dev" + wheel_hash[:8].translate(_DEV_TRANS)
                pkg_version_str += "+mfbuild"
                wheel_file = change_pypi_package_version(
                    builder_python, wheel_file, pkg_version_str
                )
                parse_result = parse_explicit_path_pypi("file://%s" % wheel_file)

            debug.conda_exec("Package for '%s' built in '%s'" % (key, wheel_file))

            # We update because we need to change the filename mostly so that it
            # now reflects the abi, etc and all that goes in a wheel filename.
            pkg_spec = pkg_spec.clone_with_filename(parse_result.filename)
            to_build_pkg_info[key].spec = pkg_spec
            pkg_spec.add_local_file(".whl", wheel_file)

    return [v.spec for v in to_build_pkg_info.values() if v.spec], builder_envs


def _build_with_pip(
    conda: Conda,
    binary: str,
    dest_path: str,
    key: str,
    spec: PypiPackageSpecification,
    build_url: str,
):
    src = spec.local_file(spec.url_format) or build_url
    debug.conda_exec("%s: building from '%s' in '%s'" % (key, src, dest_path))

    conda.call_binary(
        [
            "-m",
            "pip",
            "--isolated",
            "wheel",
            "--no-deps",
            "--progress-bar",
            "off",
            "-w",
            dest_path,
            src,
        ],
        binary=binary,
    )
    return key, dest_path
