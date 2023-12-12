# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
import json
import os
import subprocess
import sys
import tempfile
import uuid

from itertools import chain
from typing import Dict, List, Optional, Set, Tuple, cast

from metaflow.debug import debug
from metaflow.metaflow_config import CONDA_LOCAL_PATH

from metaflow._vendor.packaging.requirements import Requirement

from ..env_descr import (
    CondaPackageSpecification,
    EnvType,
    PackageSpecification,
    PypiCachePackage,
    PypiPackageSpecification,
    ResolvedEnvironment,
)

from ..pypi_package_builder import PackageToBuild, build_pypi_packages

from ..utils import (
    CondaException,
    WithDir,
    arch_id,
    channel_or_url,
    parse_explicit_url_conda,
    parse_explicit_url_pypi,
    pypi_tags_from_arch,
)
from . import Resolver


class CondaLockResolver(Resolver):
    TYPES = ["conda-lock"]

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
        outfile_name = None
        if base_env:
            local_packages = [
                p for p in base_env.packages if not p.is_downloadable_url()
            ]
            if local_packages:
                # We actually only care about things that are not online. Derived packages
                # are OK because we can reconstruct them if needed (or they may even
                # be cached)

                raise CondaException(
                    "Local PYPI packages are not supported in MIXED mode: %s"
                    % ", ".join([p.package_name for p in local_packages])
                )

        # self._start_micromamba_server()

        index_url = self._conda.default_pypi_sources[0]
        # pypi_channels contains everything EXCEPT the pypi default because we don't
        # need to add that to poetry
        pypi_channels = list(
            set(sources.get("pypi", [])).difference(["https://pypi.org/simple"])
        )
        debug.conda_exec("Will add pypi channels: %s" % ", ".join(pypi_channels))

        try:
            # We resolve the environment using conda-lock

            # Write out the TOML file. It's easy enough that we don't use another tool
            # to write it out. We use TOML so that we can disable pypi if needed

            pypi_deps = deps.get("pypi", [])
            conda_deps = list(chain(deps.get("conda", []), deps.get("npconda", [])))

            sys_overrides = {
                k: v for d in deps.get("sys", []) for k, v in [d.split("==")]
            }
            # We only add pip if not present
            if not any([(d == "pip" or d.startswith("pip==")) for d in conda_deps]):
                conda_deps.append("pip")
            toml_lines = [
                "[build-system]\n",
                'requires = ["poetry>=0.12"]\n',
                'build-backend = "poetry.masonry.api"\n',
                "\n",
                "[tool.conda-lock]\n",
            ]
            # Add channels
            have_extra_channels = (
                len(
                    set(sources.get("conda", [])).difference(
                        map(channel_or_url, self._conda.default_conda_channels)
                    )
                )
                > 0
            )

            toml_lines.append(
                "channels = [%s]\n"
                % ", ".join(["'%s'" % c for c in sources.get("conda", [])])
            )

            toml_lines.append(
                "pip-repositories = [%s]\n"
                % ", ".join(["'%s'" % c for c in pypi_channels])
            )
            if index_url != "https://pypi.org/simple":
                toml_lines.append("allow-pypi-requests = false\n")

            toml_lines.append("\n")
            # TODO: Maybe we can make this better and only relax if :: is for channels
            # that don't exist in the list
            if any(["::" in d for d in conda_deps]) or have_extra_channels:
                addl_env = {"CONDA_CHANNEL_PRIORITY": "flexible"}
            else:
                addl_env = {}

            # Add deps
            toml_lines.append("[tool.conda-lock.dependencies]\n")
            seen = set()  # type: Set[str]
            for d in conda_deps:
                splits = d.split("==", 1)
                if len(splits) == 2:
                    if splits[0] in seen:
                        raise CondaException(
                            "Duplicate conda dependency %s" % splits[0]
                        )
                    toml_lines.append('"%s" = "%s"\n' % (splits[0], splits[1]))
                    seen.add(splits[0])
                else:
                    if d in seen:
                        raise CondaException("Duplicate conda dependency %s" % d)
                    toml_lines.append('"%s" = "*"\n' % d)
                    seen.add(d)
            toml_lines.append("\n")
            toml_lines.append("[tool.poetry.dependencies]\n")
            # In some cases (when we build packages), we may actually have the same
            # dependency multiple times. We keep just the URL one in this case
            pypi_dep_lines = {}  # type: Dict[str, Dict[str, str]]
            for d in pypi_deps:
                splits = d.split("==", 1)
                # Here we re-parse the requirement. It will be one of the four options:
                #  - <package_name>
                #  - <package_name>[extras]
                #  - <package_name>@<url>
                #  - <package_name>[extras]@<url>
                parsed_req = Requirement(splits[0])
                if parsed_req.extras:
                    extra_part = "extras = [%s]," % ", ".join(
                        ['"%s"' % e for e in parsed_req.extras]
                    )
                else:
                    extra_part = ""

                version_str = splits[1] if len(splits) == 2 else "*"
                if parsed_req.url:
                    if len(splits) == 2:
                        raise CondaException(
                            "Unexpected version on URL requirement %s" % splits[0]
                        )
                    pypi_dep_lines.setdefault(parsed_req.name, {}).update(
                        {"url": parsed_req.url, "url_extras": extra_part}
                    )
                else:
                    pypi_dep_lines.setdefault(parsed_req.name, {}).update(
                        {"version": version_str, "extras": extra_part}
                    )
            for pypi_name, info in pypi_dep_lines.items():
                if "url" in info:
                    toml_lines.append(
                        '"%s" = {url = "%s", %s source="pypi"}\n'
                        % (pypi_name, info["url"], info["url_extras"])
                    )
                else:
                    toml_lines.append(
                        '"%s" = {version = "%s", %s source="pypi"}\n'
                        % (pypi_name, info["version"], info["extras"])
                    )

            with tempfile.TemporaryDirectory() as conda_lock_dir:
                outfile_name = "/tmp/conda-lock-gen-%s" % os.path.basename(
                    conda_lock_dir
                )

                args = [
                    "lock",
                    "-f",
                    "pyproject.toml",
                    "-p",
                    architecture,
                    "--filename-template",
                    outfile_name,
                    "-k",
                    "explicit",
                    "--conda",
                ]
                #                if "micromamba_server" in self._conda._bins:
                #                    args.extend([self._bins["micromamba_server"], "--micromamba"])
                #                else:
                conda_exec_type = self._conda.conda_executable_type
                if conda_exec_type:
                    args.extend(
                        [
                            cast(str, self._conda.binary(conda_exec_type)),
                            "--%s" % conda_exec_type,
                        ]
                    )
                else:
                    raise CondaException("Could not find conda binary for conda-lock")

                # If arch_id() == architecture, we also use the same virtual packages
                # as the ones that exist on the machine to mimic the current behavior
                # of conda/mamba. If not the same architecture, we pass the system
                # overrides (currently __cuda) down as well.
                if arch_id() == architecture or sys_overrides:
                    lines = ["subdirs:\n", "  %s:\n" % architecture, "    packages:\n"]
                    lines.extend(
                        "      %s: %s\n" % (virt_pkg, virt_build_str)
                        for virt_pkg, virt_build_str in self._conda.virtual_packages.items()
                        if virt_pkg not in sys_overrides
                    )
                    lines.extend(
                        "      %s: %s\n" % (k, v) for k, v in sys_overrides.items()
                    )

                    with open(
                        os.path.join(conda_lock_dir, "virtual_yml.spec"),
                        mode="w",
                        encoding="ascii",
                    ) as virtual_yml:
                        virtual_yml.writelines(lines)
                    args.extend(["--virtual-package-spec", "virtual_yml.spec"])

                debug.conda_exec("Build directory: %s" % conda_lock_dir)
                # conda-lock will only consider a `pyproject.toml` as a TOML file which
                # is somewhat annoying.
                with open(
                    os.path.join(conda_lock_dir, "pyproject.toml"),
                    mode="w",
                    encoding="ascii",
                ) as input_toml:
                    input_toml.writelines(toml_lines)
                    debug.conda_exec("TOML configuration:\n%s" % "".join(toml_lines))
                self._conda.call_binary(
                    args, binary="conda-lock", addl_env=addl_env, cwd=conda_lock_dir
                )
            # At this point, we need to read the explicit dependencies in the file created
            emit = False
            packages = []  # type: List[PackageSpecification]
            packages_to_build = {}  # type: Dict[str, PackageToBuild]
            with open(outfile_name, "r", encoding="utf-8") as out:
                for l in out:
                    if emit:
                        if l.startswith("#"):
                            components = l.split()
                            # Line should be # pip <pkg> @ <url>
                            if len(components) != 5:
                                raise CondaException(
                                    "Unexpected package specification line: %s" % l
                                )
                            parse_result = parse_explicit_url_pypi(components[4])
                            if parse_result.url_format != ".whl":
                                cache_base_url = (
                                    PypiCachePackage.make_partial_cache_url(
                                        parse_result.url, is_real_url=True
                                    )
                                )
                                packages_to_build[cache_base_url] = PackageToBuild(
                                    parse_result.url,
                                    PypiPackageSpecification(
                                        parse_result.filename,
                                        parse_result.url,
                                        is_real_url=True,
                                        url_format=parse_result.url_format,
                                    ),
                                )
                            else:
                                packages.append(
                                    PypiPackageSpecification(
                                        parse_result.filename,
                                        parse_result.url,
                                        url_format=parse_result.url_format,
                                        hashes={
                                            parse_result.url_format: parse_result.hash
                                        }
                                        if parse_result.hash
                                        else None,
                                    )
                                )
                        else:
                            parse_result = parse_explicit_url_conda(l.strip())
                            packages.append(
                                CondaPackageSpecification(
                                    parse_result.filename,
                                    parse_result.url,
                                    url_format=parse_result.url_format,
                                    hashes={
                                        parse_result.url_format: cast(
                                            str, parse_result.hash
                                        )
                                    },
                                )
                            )
                    if not emit and l.strip() == "@EXPLICIT":
                        emit = True
            if packages_to_build:
                with tempfile.TemporaryDirectory() as build_dir:
                    python_version = None  # type: Optional[str]
                    for p in packages:
                        if p.filename.startswith("python-"):
                            python_version = p.package_version
                            break
                    if python_version is None:
                        raise CondaException(
                            "Could not determine version of Python from conda packages"
                        )
                    if architecture == "linux-64":
                        # Get the latest supported GLIBC version so we can generate
                        # the proper tags (only matters on linux)
                        glibc_version = [
                            d for d in deps.get("sys", []) if d.startswith("__glibc")
                        ]
                        if len(glibc_version) != 1:
                            raise CondaException(
                                "Could not determine maximum GLIBC version"
                            )
                        # Get version looking like 2.27=0
                        glibc_version = glibc_version[0][len("__glibc==") :]
                        # Strip =0
                        glibc_version = glibc_version.split("=", 1)[0]
                        # Replace . with _
                        glibc_version = glibc_version.replace(".", "_")
                    else:
                        glibc_version = ""
                    supported_tags = pypi_tags_from_arch(
                        python_version, architecture, glibc_version
                    )
                    if self._conda.storage:
                        built_pypi_packages, builder_envs = build_pypi_packages(
                            self._conda,
                            self._conda.storage,
                            python_version,
                            packages_to_build,
                            builder_envs,
                            build_dir,
                            architecture,
                            supported_tags,
                            sources.get("pypi", []),
                        )
                        packages.extend(built_pypi_packages)
                    else:
                        # Here it was just URLs so we are good
                        packages.extend(
                            [
                                cast(PackageSpecification, v.spec)
                                for v in packages_to_build.values()
                            ]
                        )
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
        finally:
            if outfile_name and os.path.isfile(outfile_name):
                os.unlink(outfile_name)
