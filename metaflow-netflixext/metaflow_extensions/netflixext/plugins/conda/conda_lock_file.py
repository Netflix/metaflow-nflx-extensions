"""
Turn a conda-lock lockfile into ResolvedEnvironments.

A lockfile produced by `conda-lock lock` is *already resolved*: every package carries an
exact URL and hash, for every platform the lockfile was solved for. There is therefore
nothing for a Resolver to do -- the environment is translated straight into
ResolvedEnvironment/PackageSpecification objects, which is what lets the lockfile a team
validated be the exact thing that runs, rather than re-solving an environment.yml and
hoping the result matches.

See https://github.com/Netflix/metaflow-nflx-extensions/issues/34.
"""

from typing import Dict, List, Optional, Sequence

from metaflow.metaflow_environment import InvalidEnvironmentException

from .env_descr import (
    CondaPackageSpecification,
    EnvType,
    PackageSpecification,
    PypiPackageSpecification,
    ResolvedEnvironment,
)
from .parsers import CondaLockFile, CondaLockPackage, parse_conda_lock_yml
from .utils import parse_explicit_url_conda, parse_explicit_url_pypi


def resolved_environments_from_conda_lock(
    file_content: str,
    platforms: Optional[Sequence[str]] = None,
    include_dev: bool = False,
) -> Dict[str, ResolvedEnvironment]:
    """
    Build one ResolvedEnvironment per platform from the content of a conda-lock lockfile.

    Parameters
    ----------
    file_content : str
        Content of the `conda-lock.yml` file.
    platforms : Optional[Sequence[str]]
        Restrict to these platforms. Defaults to every platform in the lockfile.
    include_dev : bool
        Include packages in the `dev` category. Defaults to False, matching
        `conda-lock install`, which installs the `main` category only.

    Returns
    -------
    Dict[str, ResolvedEnvironment]
        The resolved environment for each platform, keyed by platform.
    """
    return conda_lock_to_resolved_environments(
        parse_conda_lock_yml(file_content),
        platforms=platforms,
        include_dev=include_dev,
    )


def conda_lock_to_resolved_environments(
    lock: CondaLockFile,
    platforms: Optional[Sequence[str]] = None,
    include_dev: bool = False,
) -> Dict[str, ResolvedEnvironment]:
    """
    Build one ResolvedEnvironment per platform from an already parsed lockfile.

    All the environments share the same req_id -- they come from one lockfile and are
    co-resolved by construction -- and, when there is more than one, the same full_id, so
    that they behave like any other set of co-resolved environments.

    Note that the environments are identified by the package set they pin, not by the
    bytes of the lockfile: two lockfiles that pin exactly the same packages give the same
    environment and reuse the same cache entry, which is the intended behaviour.
    """
    requested = list(platforms) if platforms else list(lock.platforms)
    missing = [p for p in requested if p not in lock.platforms]
    if missing:
        raise InvalidEnvironmentException(
            "The conda-lock lockfile does not contain platform(s) %s. It was locked "
            "for: %s. Re-run 'conda-lock lock' with '-p %s' to add it."
            % (", ".join(missing), ", ".join(lock.platforms), " -p ".join(missing))
        )

    # The user dependencies are computed once from the whole lockfile so that every
    # platform yields the same req_id. They are the lockfile's root packages: the ones no
    # other package depends on, which is as close as a lockfile gets to recording what
    # was originally asked for (it does not keep the source environment.yml).
    deps = _root_dependencies(lock, requested, include_dev)

    sources = {}  # type: Dict[str, List[str]]
    if lock.channels:
        sources["conda"] = list(lock.channels)

    envs = {}  # type: Dict[str, ResolvedEnvironment]
    for platform in requested:
        lock_packages = lock.packages_for(platform, include_dev=include_dev)
        if not lock_packages:
            raise InvalidEnvironmentException(
                "The conda-lock lockfile has no packages for platform '%s'" % platform
            )
        packages = [_to_package_specification(p) for p in lock_packages]
        envs[platform] = ResolvedEnvironment(
            deps,
            sources,
            {},
            platform,
            all_packages=packages,
            env_type=_env_type(lock_packages),
        )

    if len(envs) > 1:
        # Give every platform the same full_id, as for any other co-resolved set.
        ResolvedEnvironment.set_coresolved_full_id(list(envs.values()))

    return envs


def _env_type(packages: Sequence[CondaLockPackage]) -> EnvType:
    managers = {p.manager for p in packages}
    if managers == {"pypi"}:
        return EnvType.PYPI_ONLY
    if "pypi" in managers:
        return EnvType.MIXED
    return EnvType.CONDA_ONLY


def _root_dependencies(
    lock: CondaLockFile, platforms: Sequence[str], include_dev: bool
) -> Dict[str, List[str]]:
    """
    The packages of the lockfile that nothing else depends on, per manager.

    Computed over all the requested platforms at once so the result -- and therefore the
    req_id -- does not depend on which platform is being built.
    """
    deps = {"conda": set(), "pypi": set()}  # type: Dict[str, set]
    for platform in platforms:
        packages = lock.packages_for(platform, include_dev=include_dev)
        depended_on = {
            dep_name for pkg in packages for dep_name in pkg.dependencies.keys()
        }
        for pkg in packages:
            if pkg.name not in depended_on:
                deps[pkg.manager].add("%s==%s" % (pkg.name, pkg.version))
    return {manager: sorted(names) for manager, names in deps.items() if names}


def _to_package_specification(pkg: CondaLockPackage) -> PackageSpecification:
    if pkg.manager == "conda":
        return _to_conda_package_specification(pkg)
    return _to_pypi_package_specification(pkg)


def _to_conda_package_specification(pkg: CondaLockPackage) -> CondaPackageSpecification:
    # Conda packages are addressed by md5 everywhere else in this codebase, and
    # conda-lock always records one.
    pkg_hash = pkg.hashes.get("md5")
    if not pkg_hash:
        raise InvalidEnvironmentException(
            "Conda package '%s' of the conda-lock lockfile has no md5 hash" % pkg.name
        )
    try:
        parsed = parse_explicit_url_conda("%s#%s" % (pkg.url, pkg_hash))
    except Exception as e:
        raise InvalidEnvironmentException(
            "Could not use the URL of conda package '%s' from the conda-lock "
            "lockfile: %s" % (pkg.name, str(e))
        ) from e
    return CondaPackageSpecification(
        parsed.filename,
        parsed.url,
        url_format=parsed.url_format,
        hashes={parsed.url_format: pkg_hash},
    )


def _to_pypi_package_specification(pkg: CondaLockPackage) -> PypiPackageSpecification:
    pkg_hash = pkg.hashes.get("sha256")
    if not pkg_hash:
        raise InvalidEnvironmentException(
            "Pypi package '%s' of the conda-lock lockfile has no sha256 hash" % pkg.name
        )
    if pkg.url.startswith("git+"):
        raise InvalidEnvironmentException(
            "Pypi package '%s' of the conda-lock lockfile is a VCS dependency (%s). "
            "Those have to be built and are not supported from a lockfile yet -- "
            "resolve from the source environment.yml instead." % (pkg.name, pkg.url)
        )
    try:
        parsed = parse_explicit_url_pypi("%s#sha256=%s" % (pkg.url, pkg_hash))
    except Exception as e:
        raise InvalidEnvironmentException(
            "Could not use the URL of pypi package '%s' from the conda-lock "
            "lockfile: %s" % (pkg.name, str(e))
        ) from e
    if parsed.url_format != ".whl":
        raise InvalidEnvironmentException(
            "Pypi package '%s' of the conda-lock lockfile is a source distribution "
            "(%s). Those have to be built into a wheel, which is not supported from a "
            "lockfile yet -- resolve from the source environment.yml instead."
            % (pkg.name, parsed.filename + parsed.url_format)
        )
    return PypiPackageSpecification(
        parsed.filename,
        parsed.url,
        url_format=parsed.url_format,
        hashes={parsed.url_format: pkg_hash},
    )
