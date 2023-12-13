# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
import json
import os
import shutil
import tempfile

from itertools import chain, product
from typing import Dict, List, Optional, Tuple, cast

from urllib.parse import unquote, urlparse

from metaflow.debug import debug

from ..env_descr import (
    EnvType,
    PackageSpecification,
    PypiCachePackage,
    PypiPackageSpecification,
    ResolvedEnvironment,
)

from ..pypi_package_builder import PackageToBuild, build_pypi_packages

from ..utils import (
    CondaException,
    arch_id,
    correct_splitext,
    get_glibc_version,
    parse_explicit_path_pypi,
    parse_explicit_url_pypi,
    pypi_tags_from_arch,
)
from . import Resolver

_FAKE_WHEEL = "_fake-1.0-py3-none-any.whl"


class PipResolver(Resolver):
    TYPES = ["pip"]

    def resolve(
        self,
        env_type: EnvType,
        deps: Dict[str, List[str]],
        sources: Dict[str, List[str]],
        extras: Dict[str, List[str]],
        architecture: str,
        builder_envs: Optional[List[ResolvedEnvironment]] = None,
        base_env: Optional[ResolvedEnvironment] = None,
    ) -> Tuple[ResolvedEnvironment, Optional[List[ResolvedEnvironment]]]:
        if base_env:
            # For base environments, we may have built packages already so for those
            # packages, we need to make them available again to pip when resolving.
            # We therefore check packages that are:
            #   - not directly downloadable from their URL so, in this case, the URL is
            #     fake (ie: it contains FAKEURL_PATHCOMPONENT -- this is used when
            #     we build a package from a local directory/GIT repo, etc)
            #   - OR derived from a source package. This is the case when we build
            #     a wheel from a source package.
            local_packages = [
                p
                for p in base_env.packages
                if p.TYPE == "pypi" and (not p.is_downloadable_url() or p.is_derived())
            ]
        else:
            local_packages = None
        # Some args may be two actual arguments like "-f <something>" thus the map
        extra_args = list(
            chain.from_iterable(
                map(lambda x: x.split(maxsplit=1), (e for e in extras.get("pypi", [])))
            )
        )

        if builder_envs is None:
            raise CondaException(
                "Cannot build a PYPI only environment without a builder "
                "environment. This is a bug -- please report"
            )
        # These can be the same if building for arch on same arch
        builder_env = [r for r in builder_envs if r.env_id.arch == arch_id()][0]
        base_conda_env = [r for r in builder_envs if r.env_id.arch == architecture][0]

        real_deps = deps.get("pypi", [])
        # We get the python version for this builder env
        python_version = None  # type: Optional[str]
        for p in builder_env.packages:
            if p.filename.startswith("python-"):
                python_version = p.package_version
                break
        if python_version is None:
            raise CondaException(
                "Could not determine version of Python from conda packages"
            )

        # Create the environment in which we will call pip
        debug.conda_exec("Creating builder conda environment")
        builder_python = os.path.join(
            self._conda.create_builder_env(builder_env), "bin", "python"
        )

        packages = []  # type: List[PackageSpecification]
        with tempfile.TemporaryDirectory() as pypi_dir:
            args = [
                "-m",
                "pip",
                "-v",
                "--isolated",
                "install",
                "--ignore-installed",
                "--dry-run",
                "--target",
                pypi_dir,
                "--report",
                os.path.join(pypi_dir, "out.json"),
            ]
            args.extend(extra_args)

            pypi_sources = sources.get("pypi", [])
            # The first source is always the index
            args.extend(["-i", pypi_sources[0]])
            for c in pypi_sources[1:]:
                args.extend(["--extra-index-url", c])

            if arch_id() == "linux-64":
                # Glibc version only relevant on linux
                platform_glibc = get_glibc_version()
                if platform_glibc is None:
                    raise CondaException("Could not determine the system GLIBC version")
            else:
                same_glibc = True

            if architecture == "linux-64":
                # Get the latest supported GLIBC version so we can generate
                # the proper tags
                glibc_version = [
                    d for d in deps.get("sys", []) if d.startswith("__glibc")
                ]
                if len(glibc_version) != 1:
                    raise CondaException("Could not determine maximum GLIBC version")
                # Get version looking like 2.27=0
                glibc_version = glibc_version[0][len("__glibc==") :]
                # Strip =0
                glibc_version = glibc_version.split("=", 1)[0]

                if arch_id() == "linux-64":
                    same_glibc = glibc_version == platform_glibc

                # Replace . with _
                glibc_version = glibc_version.replace(".", "_")
            else:
                # It doesn't matter -- not used so don't compute
                glibc_version = ""
                same_glibc = True

            supported_tags = pypi_tags_from_arch(
                python_version, architecture, glibc_version
            )

            if architecture != arch_id() or not same_glibc:
                implementations = []  # type: List[str]
                abis = []  # type: List[str]
                platforms = []  # type: List[str]
                for tag in supported_tags:
                    implementations.append(tag.interpreter)
                    abis.append(tag.abi)
                    platforms.append(tag.platform)
                implementations = [x.interpreter for x in supported_tags]
                extra_args = (
                    "--only-binary=:all:",
                    # Seems to overly constrain stuff
                    # *(
                    #    chain.from_iterable(
                    #        product(["--implementation"], set(implementations))
                    #    )
                    # ),
                    *(chain.from_iterable(product(["--abi"], set(abis)))),
                    *(chain.from_iterable(product(["--platform"], set(platforms)))),
                )

                args.extend(extra_args)

            # If we have local packages, we download them to a directory and point
            # pip to it using the `--find-links` argument.
            local_packages_dict = {}  # type: Dict[str, PackageSpecification]
            if local_packages:
                # This is a bit convoluted but we try to avoid downloading packages
                # that we already have but we also don't want to point pip to a directory
                # full of packages that we may not want to use so we will create symlinks
                # to packages we already have and download the others

                # This is where we typically download/cache any pypi packages (source)
                base_local_pypi_packages = self._conda.package_dir("pypi")

                # This is where we will create symlinks in.
                tmp_local_pypi_packages = os.path.realpath(
                    os.path.join(pypi_dir, "local_packages", "pypi")
                )

                os.makedirs(tmp_local_pypi_packages)

                # Create this in case we need to move the local packages at some point
                os.makedirs(base_local_pypi_packages, exist_ok=True)

                new_local_packages = []  # type: List[PackageSpecification]
                for p in local_packages:
                    for fmt in PypiPackageSpecification.allowed_formats():
                        filename = "%s%s" % (p.filename, fmt)
                        if os.path.isfile(
                            os.path.join(base_local_pypi_packages, filename)
                        ):
                            os.symlink(
                                os.path.join(base_local_pypi_packages, filename),
                                os.path.join(tmp_local_pypi_packages, filename),
                            )
                            local_packages_dict[
                                os.path.join(tmp_local_pypi_packages, filename)
                            ] = p
                            p.add_local_file(
                                fmt,
                                os.path.join(base_local_pypi_packages, filename),
                                replace=True,
                            )
                            break
                    else:
                        new_local_packages.append(p)
                local_packages = new_local_packages
                # This will not fetch on the web so no need for auth object
                # Note we don't add pypi here because lazy_fetch_packages does that
                # automatically.
                self._conda.lazy_fetch_packages(
                    local_packages, None, os.path.join(pypi_dir, "local_packages")
                )
                args.extend(
                    ["--find-links", os.path.join(pypi_dir, "local_packages", "pypi")]
                )

                for p in local_packages:
                    for _, f in p.local_files:
                        local_packages_dict[os.path.realpath(f)] = p
                debug.conda_exec(
                    "Locally present files: %s" % ", ".join(local_packages_dict)
                )

            # Unfortunately, pip doesn't like things like ==<= so we need to strip
            # the ==
            for d in real_deps:
                splits = d.split("==", 1)
                if len(splits) == 1:
                    args.append(d)
                else:
                    if splits[1][0] in ("=", "<", ">", "!", "~"):
                        # Something originally like pkg==<=ver
                        args.append("".join(splits))
                    else:
                        # Something originally like pkg==ver
                        args.append(d)

            self._conda.call_binary(args, binary=builder_python)

            # We should now have a json blob in out.json
            with open(
                os.path.join(pypi_dir, "out.json"), mode="r", encoding="utf-8"
            ) as f:
                desc = json.load(f)

            to_build_pkg_info = {}  # type: Dict[str, PackageToBuild]
            for package_desc in desc["install"]:
                # Three cases here:
                #  - we have a "dir_info" portion:
                #    - this means we have a directory
                #  - we have a "vcs_info" portion:
                #    - this means we are including a Github (for example) package directly
                #    - if we are the same arch as the target arch, we build the
                #      package and add it
                #  - we have an "archive_info" portion:
                #    - this means we have a direct URL to get the package from
                #    - if this is a wheel package, we add it directly
                #    - if this is not a wheel, we will build the package
                # The spec is given here:
                # https://packaging.python.org/en/latest/specifications/direct-url-data-structure/
                if debug.conda_exec:
                    package_desc = {
                        k: v
                        for k, v in package_desc.items()
                        if k in ("download_info", "vcs_info", "url", "subdirectory")
                    }
                    debug.conda_exec("Need to install %s" % str(package_desc))
                dl_info = package_desc["download_info"]
                url = dl_info["url"]
                if "dir_info" in dl_info or url.startswith("file://"):
                    url = unquote(url)
                    to_build_local_pkg = None  # type: Optional[str]
                    local_path = url[7:]
                    if "dir_info" in dl_info and dl_info["dir_info"].get(
                        "editable", False
                    ):
                        raise CondaException(
                            "Cannot include an editable PYPI package: '%s'" % url
                        )
                    if os.path.isdir(local_path):
                        if os.path.isfile(os.path.join(local_path, "setup.py")):
                            package_name, package_version = (
                                self._conda.call_binary(
                                    [
                                        os.path.join(local_path, "setup.py"),
                                        "-q",
                                        "--name",
                                        "--version",
                                    ],
                                    binary=builder_python,
                                )
                                .decode(encoding="utf-8")
                                .splitlines()
                            )
                        elif os.path.isfile(os.path.join(local_path, "pyproject.toml")):
                            package_name, package_version = (
                                self._conda.call_binary(
                                    [
                                        "-c",
                                        "import tomli; f = open('%s', mode='rb'); "
                                        "d = tomli.load(f); "
                                        "print(d.get('poetry', d['tool']['poetry'])['name']); "
                                        "print(d.get('poetry', d['tool']['poetry'])['version'])"
                                        % os.path.join(local_path, "pyproject.toml"),
                                    ],
                                    binary=builder_python,
                                )
                                .decode(encoding="utf-8")
                                .splitlines()
                            )
                        else:
                            raise CondaException(
                                "Local directory '%s' is not supported as it is "
                                "missing a 'setup.py' or 'pyproject.toml'" % local_path
                            )
                        # Make sure to use the quoted URL here
                        to_build_local_pkg = os.path.join(
                            dl_info["url"],
                            "%s-%s.whl" % (package_name, package_version),
                        )
                    else:
                        # A local wheel or tarball
                        if local_path in local_packages_dict:
                            # We are going to move this file to a less "temporary"
                            # location so that it can be installed if needed

                            pkg_spec = local_packages_dict[local_path]
                            if not os.path.islink(local_path):
                                # If this is not a link, it means we downloaded it
                                debug.conda_exec("Known package -- moving in place")
                                filename = os.path.split(local_path)[1]
                                file_format = correct_splitext(filename)[1]
                                shutil.move(local_path, self._conda.package_dir("pypi"))

                                # Update where the local file resides for this package
                                pkg_spec.add_local_file(
                                    file_format,
                                    os.path.join(
                                        self._conda.package_dir("pypi"), filename
                                    ),
                                    replace=True,
                                )
                            else:
                                debug.conda_exec("Known package already in place")
                            packages.append(pkg_spec)
                        else:
                            parse_result = parse_explicit_path_pypi(url)
                            if parse_result.url_format != ".whl":
                                # This is a source package so we need to build it
                                to_build_local_pkg = dl_info["url"]
                            else:
                                package_spec = PypiPackageSpecification(
                                    parse_result.filename,
                                    parse_result.url,
                                    is_real_url=False,
                                    url_format=parse_result.url_format,
                                )
                                # We extract the actual local file so we can use that for now
                                # Note that the url in PypiPackageSpecication is a fake
                                # one that looks like file://local-file/... which is meant
                                # to act as a key for the package.
                                package_spec.add_local_file(
                                    parse_result.url_format, url[7:]
                                )
                                packages.append(package_spec)
                    if to_build_local_pkg:
                        parse_result = parse_explicit_path_pypi(to_build_local_pkg)
                        cache_base_url = PypiCachePackage.make_partial_cache_url(
                            parse_result.url, is_real_url=False
                        )

                        to_build_pkg_info[cache_base_url] = PackageToBuild(
                            dl_info["url"],
                            # We get the name once we build the package
                            PypiPackageSpecification(
                                _FAKE_WHEEL,
                                parse_result.url,
                                is_real_url=False,
                                url_format=".whl",
                            ),
                        )
                elif "vcs_info" in dl_info:
                    # This is a repository that we can go build

                    # Create the URL
                    base_build_url = "%s+%s@%s" % (
                        dl_info["vcs_info"]["vcs"],
                        dl_info["url"],
                        dl_info["vcs_info"]["commit_id"],
                    )
                    # We form a "fake" URL which will give us a unique key so we can
                    # look up the package build in the cache. Given we have the
                    # commit_id, we assume that for a combination of repo, commit_id
                    # and subdirectory, we have a uniquely built package. This will
                    # give users some consistency as well in the sense that an
                    # environment that uses the same git package reference will
                    # actually use the same package
                    base_pkg_url = "%s/%s" % (
                        dl_info["url"],
                        dl_info["vcs_info"]["commit_id"],
                    )
                    if "subdirectory" in dl_info:
                        base_build_url += "#subdirectory=%s" % dl_info["subdirectory"]
                        base_pkg_url += "/%s" % dl_info["subdirectory"]
                    cache_base_url = PypiCachePackage.make_partial_cache_url(
                        base_pkg_url, is_real_url=False
                    )
                    to_build_pkg_info[cache_base_url] = PackageToBuild(
                        base_build_url,
                        # We get the name once we build the package
                        PypiPackageSpecification(
                            _FAKE_WHEEL,
                            base_pkg_url,
                            is_real_url=False,
                            url_format=".whl",
                        ),
                    )
                else:
                    # We have "archive_info"
                    if "hashes" in dl_info["archive_info"]:
                        hashes = dl_info["archive_info"]["hashes"]
                        for fmt, val in hashes.items():
                            if fmt == PypiPackageSpecification.base_hash_name():
                                hash = "%s=%s" % (fmt, val)
                                break
                        else:
                            raise CondaException(
                                "Cannot find hash '%s' for package at '%s'"
                                % (
                                    PypiPackageSpecification.base_hash_name(),
                                    url,
                                )
                            )
                    else:
                        # Fallback on older "hash" field
                        hash = dl_info["archive_info"]["hash"]

                    parse_result = parse_explicit_url_pypi("%s#%s" % (url, hash))
                    if parse_result.url_format != ".whl":
                        # We need to build non wheel files.
                        url_parse_result = urlparse(cast(str, dl_info["url"]))

                        is_real_url = False
                        if url_parse_result.scheme == "file":
                            # TODO: Not sure if this case happens.
                            parse_result = parse_explicit_path_pypi(dl_info["url"])
                            cache_base_url = PypiCachePackage.make_partial_cache_url(
                                parse_result.url, is_real_url=False
                            )
                        else:
                            # We don't have the hash so we ignore.
                            parse_result = parse_explicit_url_pypi(
                                "%s#" % dl_info["url"]
                            )
                            cache_base_url = PypiCachePackage.make_partial_cache_url(
                                parse_result.url, is_real_url=True
                            )
                            is_real_url = True

                        spec = PypiPackageSpecification(
                            parse_result.filename,
                            parse_result.url,
                            is_real_url=is_real_url,
                            url_format=parse_result.url_format,
                        )
                        to_build_pkg_info[cache_base_url] = PackageToBuild(
                            dl_info["url"],
                            spec,
                        )
                        if not is_real_url:
                            # We have a local file for this tar-ball so we need to
                            # indicate where it is.
                            spec.add_local_file(
                                parse_result.url_format, url_parse_result.path
                            )
                            to_build_pkg_info[cache_base_url].have_formats.append(
                                parse_result.url_format
                            )
                    else:
                        # This is a wheel so we can just add it.
                        packages.append(
                            PypiPackageSpecification(
                                parse_result.filename,
                                parse_result.url,
                                url_format=parse_result.url_format,
                                hashes={parse_result.url_format: parse_result.hash}
                                if parse_result.hash
                                else None,
                            )
                        )
            if to_build_pkg_info:
                if not self._conda.storage:
                    raise CondaException(
                        "Cannot create a relocatable environment as it depends on "
                        "local files or non wheels and no storage backend is defined: %s"
                        % ", ".join([v.url for v in to_build_pkg_info.values()])
                    )

                built_pypi_packages, builder_envs = build_pypi_packages(
                    self._conda,
                    self._conda.storage,
                    python_version,
                    to_build_pkg_info,
                    builder_envs,
                    pypi_dir,
                    architecture,
                    supported_tags,
                    sources.get("pypi", []),
                )

                packages.extend(built_pypi_packages)

        # packages has pypi only packages so order doesn't really matter (we can just
        # directly append))
        packages += list(base_conda_env.packages)
        return (
            ResolvedEnvironment(
                deps,
                sources,
                extras,
                architecture,
                all_packages=packages,
                env_type=env_type,
            ),
            builder_envs,
        )
