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


def parse_deps(deps: str) -> List[TStr]:
    deps = deps.split(";")
    result = []
    for dep in deps:
        match = _deps_parse.match(dep)
        if match is None:
            raise ValueError("Dependency %s is not a valid constraint")
        pkg, version = match.groups()
        pkg_with_category = pkg.split("::")
        if len(pkg_with_category) > 1:
            category = pkg_with_category[0]
            pkg = pkg_with_category[1]
        else:
            category = "conda"
        version = version.lstrip("=")
        result.append(TStr(category, "%s==%s" % (pkg, version)))
    return result


def parse_channels(channels: Optional[str]) -> List[TStr]:
    if not channels:
        return []

    result = []
    channels = channels.split(",")
    for c in channels:
        channel_with_category = c.split("::")
        if len(channel_with_category) > 1:
            category = channel_with_category[0]
            c = channel_with_category[1]
        else:
            category = "conda"
        result.append(TStr(category, c))
    return result


def req_id_from_spec(python: str, deps: str, channels: str) -> str:
    deps = parse_deps(deps)
    channels = parse_channels(channels)
    deps.append(TStr("conda", "python==%s" % python))
    return ResolvedEnvironment.get_req_id(deps, channels)


def env_id_from_step(step: Step) -> EnvID:
    conda_env_id = step.task.metadata_dict.get("conda_env_id")
    if conda_env_id:
        conda_env_id = json.loads(conda_env_id)
        if isinstance(conda_env_id, type([])) and len(conda_env_id) == 3:
            return EnvID(
                req_id=conda_env_id[0], full_id=conda_env_id[1], arch=arch_id()
            )
        else:
            raise ValueError(
                "%s ran with a version of Metaflow that is too old for this functionality"
                % str(step)
            )
    else:
        raise ValueError("%s did not run with a Conda environment" % str(step))


def local_instances_for_req_id(conda: Conda, req_id: str):
    created_envs = conda.created_environments()  # This returns all MF environments
    local_instances = {}  # type: Dict[str, List[str]]
    for env_id, present_list in created_envs.items():
        if env_id.req_id != req_id:
            continue
        local_instances[env_id.full_id] = present_list
    return local_instances


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


def quiet_print_env(
    obj, env: ResolvedEnvironment, local_instances: Optional[List[str]]
):
    env_id = env.env_id
    obj.echo_always(
        "%s %s %s %s %s %s %s %s"
        % (
            env_id.req_id,
            env_id.full_id,
            env_id.arch,
            env.resolved_on.isoformat(),
            env.resolved_by,
            ",".join(env.co_resolved_archs),
            ",".join([p.filename for p in env.packages]),
            ",".join(local_instances) if local_instances else "NONE",
        )
    )


def pretty_print_env(
    obj,
    env: ResolvedEnvironment,
    is_default: bool,
    local_instances: Optional[List[str]],
):
    obj.echo(
        "\n\n*%sEnvironment full hash* %s\n"
        % ("DEFAULT " if is_default else "", env.env_id.full_id)
    )
    obj.echo("*Arch* %s" % env.env_id.arch)
    obj.echo("*Available on* %s\n" % ", ".join(env.co_resolved_archs))
    obj.echo("*Resolved on* %s" % env.resolved_on)
    obj.echo("*Resolved by* %s\n" % env.resolved_by)
    if local_instances:
        obj.echo(
            "*Locally present as* %s\n"
            % ", ".join(map(lambda x: os.path.basename(x), local_instances))
        )

    obj.echo("*Packages installed* %s" % ", ".join([p.filename for p in env.packages]))
