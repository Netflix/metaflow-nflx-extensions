import json
import os
import re
import subprocess
from typing import Dict, List, Optional

from metaflow import Step
from metaflow_extensions.netflix_ext.plugins.conda.conda import Conda
from metaflow_extensions.netflix_ext.plugins.conda.env_descr import (
    EnvID,
    ResolvedEnvironment,
    TStr,
)
from metaflow_extensions.netflix_ext.plugins.conda.utils import arch_id

_deps_parse = re.compile(r"([^<>=!~]+)(.*)")
_ext_parse = re.compile(r"([-_\w]+)\(([^)]+)\)")

name_to_pkg = {"netflix-ext": "metaflow-netflixext"}

# NOT used for now -- used in the commented out list command from environment_cmd.py
# May require cleanup
# def parse_deps(deps: str) -> List[TStr]:
#     deps = deps.split(";")
#     result = []
#     for dep in deps:
#         match = _deps_parse.match(dep)
#         if match is None:
#             raise ValueError("Dependency %s is not a valid constraint")
#         pkg, version = match.groups()
#         pkg_with_category = pkg.split("::")
#         if len(pkg_with_category) > 1:
#             category = pkg_with_category[0]
#             pkg = pkg_with_category[1]
#         else:
#             category = "conda"
#         version = version.lstrip("=")
#         result.append(TStr(category, "%s==%s" % (pkg, version)))
#     return result


# def parse_channels(channels: Optional[str]) -> List[TStr]:
#     if not channels:
#         return []

#     result = []
#     channels = channels.split(",")
#     for c in channels:
#         channel_with_category = c.split("::")
#         if len(channel_with_category) > 1:
#             category = channel_with_category[0]
#             c = channel_with_category[1]
#         else:
#             category = "conda"
#         result.append(TStr(category, c))
#     return result

# def req_id_from_spec(python: str, deps: str, channels: str) -> str:
#    deps = parse_deps(deps)
#    channels = parse_channels(channels)
#    deps.append(TStr("conda", "python==%s" % python))
#    return ResolvedEnvironment.get_req_id(deps, channels)


# def local_instances_for_req_id(conda: Conda, req_id: str):
#     created_envs = conda.created_environments()  # This returns all MF environments
#     local_instances = {}  # type: Dict[str, List[str]]
#     for env_id, present_list in created_envs.items():
#         if env_id.req_id != req_id:
#             continue
#         local_instances[env_id.full_id] = present_list
#     return local_instances


def download_mf_version(executable: str, version_str: str):
    def _install_pkg(pkg: str, ver: str):
        try:
            subprocess.check_call(
                [
                    executable,
                    "-m",
                    "pip",
                    "install",
                    "-t",
                    ".",
                    "--no-deps",
                    "%s==%s" % (pkg, ver),
                ]
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                "Could not install version '%s' of '%s': %s" % (ver, pkg, e.stderr)
            )

    s = version_str.split("+", 1)
    _install_pkg("metaflow", s[0])
    if len(s) == 1:
        return
    # We now install the other packages, they are in the format name(ver);name(ver)...
    s = s[1].split(";")
    for pkg_desc in s:
        m = _ext_parse.match(pkg_desc)
        if not m:
            raise ValueError("Metaflow extension '%s' is not a valid format" % pkg_desc)
        pkg_name, pkg_version = m.groups()
        pkg = name_to_pkg.get(pkg_name)
        if pkg is None:
            raise ValueError("Metaflow extension '%s' is not known" % pkg_name)
        _install_pkg(pkg, pkg_version)
