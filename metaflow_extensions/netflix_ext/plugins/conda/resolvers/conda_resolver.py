# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
import json
import os
import sys
import tempfile

from itertools import chain
from typing import Dict, List, Optional, Tuple, cast

from ..env_descr import (
    CondaPackageSpecification,
    EnvType,
    PackageSpecification,
    ResolvedEnvironment,
)
from ..utils import CondaException, channel_or_url, parse_explicit_url_conda
from . import Resolver


class CondaResolver(Resolver):
    TYPES = ["conda", "mamba", "micromamba"]

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
            local_packages = [
                p for p in base_env.packages if not p.is_downloadable_url()
            ]
            if local_packages:
                raise CondaException(
                    "Local packages are not allowed in Conda: %s"
                    % ", ".join([p.package_name for p in local_packages])
                )
        sys_overrides = {k: v for d in deps.get("sys", []) for k, v in [d.split("==")]}
        real_deps = list(chain(deps.get("conda", []), deps.get("npconda", [])))
        packages = []  # type: List[PackageSpecification]
        with tempfile.TemporaryDirectory() as mamba_dir:
            args = [
                "create",
                "--prefix",
                os.path.join(mamba_dir, "prefix"),
                "--dry-run",
            ]
            have_channels = False
            for c in set(sources.get("conda", [])).difference(
                map(channel_or_url, self._conda.default_conda_channels)
            ):
                have_channels = True
                args.extend(["-c", c])

            if not have_channels:
                have_channels = any(["::" in d for d in real_deps])
            args.extend(real_deps)

            addl_env = {
                "CONDA_SUBDIR": architecture,
                "CONDA_PKGS_DIRS": mamba_dir,
                "CONDA_ROOT": self._conda.root_prefix,
                "CONDA_UNSATISFIABLE_HINTS_CHECK_DEPTH": "0",
            }
            addl_env.update(
                {
                    "CONDA_OVERRIDE_%s"
                    % pkg_name[2:].upper(): pkg_version.split("=")[0]
                    for pkg_name, pkg_version in sys_overrides.items()
                }
            )
            if have_channels:
                # Add flexible-channel-priority because otherwise if a version
                # is present in the other channels but not in the higher
                # priority channels, it is not found.
                addl_env["CONDA_CHANNEL_PRIORITY"] = "flexible"
            conda_result = json.loads(self._conda.call_conda(args, addl_env=addl_env))

        # This returns a JSON blob with:
        #  - actions:
        #    - FETCH: List of objects to fetch -- this is where we get hash and URL
        #    - LINK: Packages to actually install (in that order)
        # On micromamba, we can just use the LINK blob since it has all information we need
        if not conda_result["success"]:
            print(
                "Pretty-printed Conda create result:\n%s" % conda_result,
                file=sys.stderr,
            )
            raise CondaException(
                "Could not resolve environment -- see above pretty-printed error."
            )

        if self._conda.conda_executable_type == "micromamba":
            for lnk in conda_result["actions"]["LINK"]:
                parse_result = parse_explicit_url_conda(
                    "%s#%s" % (lnk["url"], lnk["md5"])
                )
                packages.append(
                    CondaPackageSpecification(
                        filename=parse_result.filename,
                        url=parse_result.url,
                        url_format=parse_result.url_format,
                        hashes={parse_result.url_format: cast(str, parse_result.hash)},
                    )
                )
        else:

            def _pkg_key(
                name: str, platform: str, build_string: str, build_number: str
            ) -> str:
                return "%s_%s_%s_%s" % (name, platform, build_string, build_number)

            fetched_packages = {}  # type: Dict[str, Tuple[str, str]]
            for pkg in conda_result["actions"]["FETCH"]:
                fetched_packages[
                    _pkg_key(
                        pkg["name"], pkg["subdir"], pkg["build"], pkg["build_number"]
                    )
                ] = (pkg["url"], pkg["md5"])
            for lnk in conda_result["actions"]["LINK"]:
                k = _pkg_key(
                    lnk["name"],
                    lnk["platform"],
                    lnk["build_string"],
                    lnk["build_number"],
                )
                url, md5_hash = fetched_packages[k]
                if not url.startswith(lnk["base_url"]):
                    raise CondaException(
                        "Unexpected record for %s: %s" % (k, str(conda_result))
                    )
                parse_result = parse_explicit_url_conda("%s#%s" % (url, md5_hash))
                packages.append(
                    CondaPackageSpecification(
                        filename=parse_result.filename,
                        url=parse_result.url,
                        url_format=parse_result.url_format,
                        hashes={parse_result.url_format: cast(str, parse_result.hash)},
                    )
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
