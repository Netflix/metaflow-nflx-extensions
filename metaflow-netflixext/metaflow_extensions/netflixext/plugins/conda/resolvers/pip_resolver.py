# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
import json
import os
import shutil
import tempfile

from itertools import chain, product
from typing import Dict, List, Optional, Set, Tuple, cast

from urllib.parse import unquote, urlparse

from metaflow.debug import debug

# Consider re-adding if resolving fails weirdly.
# from metaflow.exception import retry_exp_backoff
from metaflow.metaflow_config import CONDA_SYS_MARKERS

from ..env_descr import (
    EnvType,
    PackageSpecification,  # noqa
    PypiCachePackage,
    PypiPackageSpecification,
    ResolvedEnvironment,
)

from ..pypi_package_builder import PackageToBuild, build_pypi_packages

from ..utils import (
    CondaException,
    arch_id,
    clean_up_double_equal,
    correct_splitext,
    filter_user_reqs_by_markers,
    get_glibc_version,
    parse_explicit_path_pypi,
    parse_explicit_url_pypi,
    pypi_tags_from_arch,
)
from . import Resolver

_FAKE_WHEEL = "_fake-1.0-py3-none-any.whl"


class PipResolver(Resolver):
    TYPES = ["pip"]
    REQUIRES_BUILDER_ENV = True

    def __init__(self, conda):  # type: ignore[override]
        super().__init__(conda)
        # Set to True by EnvsResolver when the caller wants sdist *builds*
        # deferred (e.g. a container image builder will compile them on the
        # target arch). Defaults False -> zero impact on existing callers.
        self.defer_pypi_sdist_build = False

    # @retry_exp_backoff(
    #    (CondaException, OSError),
    #    "Failed to resolve pip environment",
    #    tries=3,
    #    max_delay_per_retry=5,
    # )
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
        full_id_unique_keys: Dict[str, str] = {},
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
            #   - OR packages that have a download URL that is not a pypi URL or belongs to
            #     the sources we have for pypi. For instance pacakage @ github.com/..../package.wheel
            local_packages = [
                p
                for p in base_env.packages
                if p.TYPE == "pypi"
                and (
                    not p.is_downloadable_url()
                    or p.is_derived()
                    or p.is_external_url(sources)  # type: ignore[attr-defined]
                )
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

        real_deps = filter_user_reqs_by_markers(
            {"pypi": deps.get("pypi", [])}, python_version_requested, architecture
        )["pypi"]
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
        # Add the path to the binary dir of the builder environment because
        # things in there may be needed to build/resolve the environment (one that has
        # come up is git-lfs)
        addl_env = {"PATH": os.path.dirname(builder_python) + ":" + os.environ["PATH"]}

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

            # Constrain setuptools<82 so that build isolation environments
            # retain pkg_resources (removed in setuptools 82). Many third-party
            # packages still use pkg_resources in their setup.py.
            constraint_file = os.path.join(pypi_dir, "constraints.txt")
            with open(constraint_file, "w") as cf:
                cf.write("setuptools<82\n")
            args.extend(["--constraint", constraint_file])
            # Also set PIP_CONSTRAINT env var so the constraint propagates to
            # pip's build isolation subprocesses (--constraint only applies to
            # the top-level resolution, not to build dependency installation).
            # The main pip command uses --isolated which ignores env vars, but
            # build isolation subprocesses do not, so they will honor this.
            addl_env["PIP_CONSTRAINT"] = constraint_file

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
                glibc_versions_list = [
                    d for d in deps.get("sys", []) if d.startswith("__glibc")
                ]
                if len(glibc_versions_list) != 1:
                    raise CondaException("Could not determine maximum GLIBC version")
                # Get version looking like 2.27=0
                glibc_version = glibc_versions_list[0][len("__glibc==") :]
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
                extra_args = [
                    # NOTE: cross-arch resolution forces binary-only because
                    # pip's --platform requires --only-binary=:all:, so a
                    # *deferred* sdist-only dep still can't be resolved cross-arch
                    # (e.g. macOS/arm64 -> linux-64). Same-arch deferral (the
                    # common workbench->Titus linux-64 path) is unaffected.
                    # Cross-arch sdist deferral would need arch-independent sdist
                    # metadata extraction — tracked as a follow-up.
                    "--only-binary=:all:",
                    # Seems to overly constrain stuff
                    # *(
                    #    chain.from_iterable(
                    #        product(["--implementation"], set(implementations))
                    #    )
                    # ),
                    *list(chain.from_iterable(product(["--abi"], set(abis)))),
                    *list(chain.from_iterable(product(["--platform"], set(platforms)))),
                ]

                args.extend(extra_args)

                # We will also override certain things when running pip -- particularly
                # platform.machine, etc which are used to compute the default_environment
                # that pip uses to evaluate markers. If we don't do this, resolving
                # an environment that needs torch from a mac for a linux box will fail
                # to include all dependencies because the markers specifying that
                # these additional packages are needed for linux are skipped as
                # pip uses the default_environment of the *current* machine and not the
                # target machine.
                addl_env["PIP_CUSTOMIZE_OVERRIDES"] = json.dumps(
                    CONDA_SYS_MARKERS.get(architecture, {})
                )
                addl_env["PYTHONPATH"] = os.path.join(
                    os.path.dirname(__file__), "..", "pip_customize"
                )
                debug.conda_exec(
                    "Launching pip with PYTHONPATH: %s and PIP_CUSTOMIZE_OVERRIDES: %s"
                    % (addl_env["PYTHONPATH"], addl_env["PIP_CUSTOMIZE_OVERRIDES"])
                )

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
            args.extend(clean_up_double_equal(real_deps))

            self._conda.call_binary(args, binary=builder_python, addl_env=addl_env)

            # We should now have a json blob in out.json
            with open(
                os.path.join(pypi_dir, "out.json"), mode="r", encoding="utf-8"
            ) as json_file:
                desc = json.load(json_file)

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
                # Extract hash if we have it
                # We have "archive_info"
                file_hash = None
                if "archive_info" in dl_info:
                    if "hashes" in dl_info["archive_info"]:
                        hashes = dl_info["archive_info"]["hashes"]
                        for fmt, val in hashes.items():
                            if fmt == PypiPackageSpecification.base_hash_name():
                                file_hash = "%s=%s" % (fmt, val)
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
                        file_hash = dl_info["archive_info"].get("hash")

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
                                    addl_env=addl_env,
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
                                        "print(d.get('project', d.get('poetry', d['tool']['poetry']))['name']); "
                                        "print(d.get('project', d.get('poetry', d['tool']['poetry']))['version'])"
                                        % os.path.join(local_path, "pyproject.toml"),
                                    ],
                                    binary=builder_python,
                                    addl_env=addl_env,
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
                            parse_result = parse_explicit_path_pypi(
                                url,
                                hash_value=(
                                    file_hash.split("=")[1] if file_hash else None
                                ),
                            )
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
                                # one that looks like file://local-<hash>/... which is meant
                                # to act as a key for the package.
                                package_spec.add_local_file(
                                    parse_result.url_format, url[7:]
                                )
                                packages.append(package_spec)
                    if to_build_local_pkg:
                        # Note the hash value here is the hash of the *source* tarball
                        # This is fine as it still uniquely identifies the package
                        # we will be building.
                        parse_result = parse_explicit_path_pypi(
                            to_build_local_pkg,
                            hash_value=file_hash.split("=")[1] if file_hash else None,
                        )
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
                    # Here we have archive_info
                    parse_result = parse_explicit_url_pypi(
                        "%s#%s" % (url, file_hash) if file_hash is not None else url
                    )
                    if parse_result.url_format != ".whl":
                        # We need to build non wheel files.
                        url_parse_result = urlparse(cast(str, dl_info["url"]))

                        is_real_url = False
                        if url_parse_result.scheme == "file":
                            raise CondaException(
                                "URL %s should have been identified as a local file"
                                % dl_info["url"]
                            )
                        else:
                            cache_base_url = PypiCachePackage.make_partial_cache_url(
                                parse_result.url, is_real_url=True
                            )
                            is_real_url = True

                        spec = PypiPackageSpecification(
                            parse_result.filename,
                            parse_result.url,
                            is_real_url=is_real_url,
                            url_format=parse_result.url_format,
                            # Carry the resolved archive hash (as the wheel branch
                            # below does) so a deferred sdist contributes its
                            # sha256 to the env full_id and the build container can
                            # verify the bytes it builds.
                            hashes=(
                                {parse_result.url_format: parse_result.hash}
                                if parse_result.hash
                                else None
                            ),
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
                                hashes=(
                                    {parse_result.url_format: parse_result.hash}
                                    if parse_result.hash
                                    else None
                                ),
                            )
                        )
            # Generic sdist-build deferral. When defer_pypi_sdist_build=True a
            # downstream builder (e.g. the prebuilt image build container) will
            # COMPILE these sdists itself, so we must not build them here. pip
            # has already resolved the full dependency closure (transitive deps
            # are separate resolved packages), so deferring only skips the wheel
            # COMPILE -- the sdist spec stays in the manifest either way.
            #
            # We collect the deferred keys and hand them to build_pypi_packages
            # via `defer_keys` so its cache probe STILL runs for them: an
            # already-built wheel is reused (attached as a cached version, later
            # embedded) instead of needlessly rebuilt, and only sdists with no
            # cached wheel keep their source spec for the downstream builder.
            # build_pypi_packages returns the spec for every entry, so we no
            # longer append/remove them by hand here.
            #
            # Default (defer_pypi_sdist_build=False): defer_keys stays empty and
            # behavior is byte-for-byte identical to before.
            defer_keys = set()  # type: Set[str]
            if self.defer_pypi_sdist_build and to_build_pkg_info:
                for _k, _pkg in to_build_pkg_info.items():
                    _spec = _pkg.spec
                    if _spec is None:
                        continue
                    _spec = cast(PypiPackageSpecification, _spec)
                    # Only defer real-URL sdists; local-file sdists have no
                    # stable URL to fetch in the container, so leave them to
                    # build_pypi_packages as before.
                    if not _spec.is_downloadable_url():
                        continue
                    # NOTE: the sdist's sha256 is carried on the spec URL
                    # (url#hash), so the resolved env full_id stays stable for a
                    # given URL. A *mutable* URL serving different bytes at the
                    # same URL (an anti-pattern; PyPI is immutable) could reuse
                    # the same image identity — out of scope here.
                    defer_keys.add(_k)
                    debug.conda_exec(
                        "Deferring sdist build %s==%s (%s)"
                        % (_spec.package_name, _spec.package_version, _pkg.url)
                    )

            # Without a caching datastore we cannot probe for a pre-built wheel,
            # so fall back to the historical defer path: keep the sdist spec,
            # drop it from to_build_pkg_info, build nothing. (In practice the
            # prebuilt resolve always has a remote datastore, so this only
            # guards the storage-less edge and keeps its behavior unchanged.)
            if defer_keys and not self._conda.storage:
                for _k in defer_keys:
                    packages.append(
                        cast(PypiPackageSpecification, to_build_pkg_info[_k].spec)
                    )
                    del to_build_pkg_info[_k]
                defer_keys = set()

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
                    defer_keys=defer_keys,
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
                # full_id_unique_keys is a uv specific cache invalidation mechanism,
                # and PipResolver shouldn't need it. We still pass this along as {}
                # to make code consistent.
                full_id_unique_keys=full_id_unique_keys,
            ),
            builder_envs,
        )
