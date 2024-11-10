import json
import os
import re
import shutil
import subprocess
import sys
from typing import Any, Dict, Optional

from metaflow.extension_support import (
    dump_module_info,
    get_extensions_in_dir,
    update_package_info,
)
from metaflow.info_file import INFO_FILE

# _deps_parse = re.compile(r"([^<>=!~]+)(.*)")
_ext_parse = re.compile(r"([-_\w]+)\(([^)]+)\)")
_git_version = re.compile(r"-git([0-9a-f]+)(-dirty)?$")

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


def _merge_directories(src_dir, dest_dir):
    # Due to a bug in PIP, we can't use --target to install namespace packages
    # so we hack around it by merging directories manually.

    for root, dirs, files in os.walk(src_dir):
        # Determine the path of the current directory relative to src_dir
        relative_path = os.path.relpath(root, src_dir)
        # Determine the corresponding path in the destination directory
        dest_path = os.path.join(dest_dir, relative_path)

        # Create directories in the destination directory
        for dir_name in dirs:
            dest_dir_path = os.path.join(dest_path, dir_name)
            if not os.path.exists(dest_dir_path):
                os.makedirs(dest_dir_path)

        # Copy files to the destination directory
        for file_name in files:
            src_file_path = os.path.join(root, file_name)
            dest_file_path = os.path.join(dest_path, file_name)
            shutil.copy2(src_file_path, dest_file_path)


def download_mf_version(
    executable: str, version_str: str, extension_info: Dict[str, Any], echo, fail_hard
):
    def echo_or_fail(msg):
        if fail_hard:
            raise RuntimeError(
                msg
                + ". Use --no-strict to install latest versions if possible instead."
            )
        echo("WARNING: " + msg + " -- installing latest version if possible.")

    def _install_pkg(pkg: str, ver: Optional[str]):
        try:
            subprocess.check_call(
                [
                    executable,
                    "-m",
                    "pip",
                    "install",
                    "--quiet",
                    "-t",
                    ".",
                    "--no-deps",
                    "%s==%s" % (pkg, ver) if ver else pkg,
                ]
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                "Could not install version '%s' of '%s': %s" % (ver, pkg, e.stderr)
            ) from e

    if not version_str:
        raise ValueError("Unknown version of Metaflow")

    s = version_str.split("+", 1)
    if _git_version.search(s[0]):
        # This is not a "public" release so we install the latest
        echo_or_fail("Metaflow's version is non public (%s)" % s[0])
        _install_pkg("metaflow", None)
    else:
        _install_pkg("metaflow", s[0])
    if len(s) == 1:
        return
    # We now install the other packages (extensions).
    # If we have extension_info, we can get that information from there.
    # That is ideal if we have that. If not, we do our best from the version string
    # where packages are in the form name(vers);name(vers) but name is not necessarily
    # the package name.
    if extension_info:
        wrong_version_info = set()
        first_round = True
        for pkg_name, pkg_info in extension_info["installed"].items():
            if pkg_name.startswith("_pythonpath"):
                # Local package with *zero* information
                echo_or_fail("Unknown extension present at runtime")
                continue
            if pkg_info["package_version"] == "<unk>" or _git_version.search(
                pkg_info["package_version"]
            ):
                echo_or_fail(
                    "Extension '%s' has a non-public version (%s)"
                    % (pkg_info["extension_name"], pkg_info["package_version"])
                )
                pkg_version = pkg_info["dist_version"]
                wrong_version_info.add(pkg_name)
            _install_pkg(pkg_name, pkg_version)
            if first_round:
                shutil.move("metaflow_extensions", "metaflow_extensions_tmp")
                first_round = False
            else:
                _merge_directories("metaflow_extensions", "metaflow_extensions_tmp")
                shutil.rmtree("metaflow_extensions")
    else:
        s = s[1].split(";")
        first_round = True
        for pkg_desc in s:
            m = _ext_parse.match(pkg_desc)
            if not m:
                # In some cases (older Metaflow), the version is not recorded so
                # we just install the latest
                echo_or_fail("Extension '%s' does not have a version" % pkg_desc)
                pkg_name, pkg_version = pkg_desc, None
            else:
                pkg_name, pkg_version = m.groups()
            pkg = name_to_pkg.get(pkg_name)
            if pkg is None:
                raise ValueError("Metaflow extension '%s' is not known" % pkg_name)
            _install_pkg(pkg, pkg_version)
            if first_round:
                shutil.move("metaflow_extensions", "metaflow_extensions_tmp")
                first_round = False
            else:
                _merge_directories("metaflow_extensions", "metaflow_extensions_tmp")
                shutil.rmtree("metaflow_extensions")
    # We now do a few things to make sure the Metaflow environment is recreated
    # as closely as possible:
    #  - add a __init__.py file to the metaflow_extensions directory to prevent
    #    other extensions from being loaded
    #  - create a INFO file with the extension information. This will allow for the
    #    __init__.py file (since otherwise it is an error) and will also remove
    #    conflicts when trying to load the extensions.
    #  - we clean up all the dist-info directories that were created as part of the
    #    pip install. This is not strictly necessary but it is cleaner.
    shutil.move("metaflow_extensions_tmp", "metaflow_extensions")
    sys.path.insert(0, ".")
    installed_packages, pkgs_per_extension_point = get_extensions_in_dir(os.getcwd())
    # Update the information with the reported version and name from extension_info
    for pkg_name, pkg_info in extension_info["installed"].items():
        if pkg_name in installed_packages:
            update_package_info(
                pkg_to_update=installed_packages[pkg_name],
                dist_version=pkg_info["dist_version"],
                extension_name=pkg_info["extension_name"],
            )
            if pkg_name not in wrong_version_info:
                update_package_info(
                    pkg_to_update=installed_packages[pkg_name],
                    package_version=pkg_info["package_version"],
                )

    key, val = dump_module_info(installed_packages, pkgs_per_extension_point)
    sys.path.pop(0)

    with open("metaflow_extensions/__init__.py", "w+", encoding="utf-8") as f:
        f.write("# This file is automatically generated by Metaflow\n")

    with open(os.path.basename(INFO_FILE), "w+", encoding="utf-8") as f:
        json.dump({key: val}, f)

    # Clean up the dist-info directories
    for root, dirs, _ in os.walk(".", topdown=False):
        for d in dirs:
            if d.endswith(".dist-info"):
                shutil.rmtree(os.path.join(root, d))
