# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
import os

from functools import partial
from itertools import chain
from typing import Any, Dict, Iterable, Tuple, List, Union, Optional, cast

from metaflow._vendor import click

from metaflow.debug import debug
from metaflow.exception import CommandException
from metaflow.flowspec import FlowSpec

from .conda.conda import Conda
from .conda.conda_step_decorator import get_conda_decorator
from .conda.env_descr import EnvID, PackageSpecification, ResolvedEnvironment
from .conda.terminal_menu import TerminalMenu

# Very simple functions to create some highlights without importing other packages
_BOLD = 1
_ITALIC = 3
_UNDERLINE = 4
_RED = 31
_GREEN = 32
_YELLOW = 33
_BLUE = 34


def _color_str(
    s: str,
    style: Optional[Union[int, List[int]]] = None,
    fg: Optional[int] = None,
    bg: Optional[int] = None,
) -> str:
    if style is None:
        style = []
    elif isinstance(style, int):
        style = [style]
    if fg is not None:
        style.append(fg)
    if bg is not None:
        style.append(bg + 10)
    return "\033[%sm%s\033[0m" % (";".join((str(x) for x in style)), s)


def _get_envs(
    conda: Conda, flow: FlowSpec, skip_known: bool, local_only: bool
) -> List[Tuple[str, Dict[str, ResolvedEnvironment], str, Tuple[str]]]:
    reqs_to_steps = {}  # type: Dict[Tuple[str, Tuple[str]], List[str]]
    for step in flow:
        step_conda_dec = get_conda_decorator(flow, step.__name__)
        # We want to gather steps by the type of requirements they have. This is
        # basically their req ID as well as the requested architectures.
        env_id = step_conda_dec.env_id
        archs = tuple(sorted(step_conda_dec.requested_architectures))
        req_id = env_id.req_id
        reqs_to_steps.setdefault((req_id, archs), []).append(step.__name__)
    # We now look for environments that have the proper req_id and a superset of the
    # architectures asked for
    if debug.conda:
        debug.conda_exec("Requirements to resolve are:")
        for (req_id, archs), steps in reqs_to_steps.items():
            debug.conda_exec(
                "%s (archs: %s) for %s" % (req_id, ", ".join(archs), ", ".join(steps))
            )

    steps_to_potentials = (
        []
    )  # type: List[Tuple[str, Dict[str, ResolvedEnvironment], str, Tuple[str]]]
    for (req_id, archs), steps in reqs_to_steps.items():
        # We can pick just a single arch to look for potentials; we know we need
        # at least that one
        arch = archs[0]
        need_archs = set(archs)
        possible_envs = conda.environments(req_id, arch, local_only=local_only)
        possible_envs = [
            r for r in possible_envs if need_archs.issubset(r[1].co_resolved_archs)
        ]
        if skip_known:
            default_env_id = conda.get_default_environment(req_id, arch)
            if default_env_id and default_env_id in [env[0] for env in possible_envs]:
                debug.conda_exec(
                    "Have known local default for %s -- skipping" % ", ".join(steps)
                )
                continue
        steps_to_potentials.append(
            (
                ", ".join(steps),
                {env_id.full_id: env for env_id, env in possible_envs},
                req_id,
                archs,
            )
        )
    return steps_to_potentials


@click.group()
def cli():
    pass


@cli.group(help="Commands related to environment manipulation.")
@click.pass_obj
def environment(obj):
    if obj.environment.TYPE != "conda":
        raise CommandException("'environment' requires a Conda environment")
    pass


@environment.command(help="Show available environments for steps in a flow")
@click.option(
    "--local-only/--no-local-only",
    show_default=True,
    default=False,
    help="Search only locally cached environments",
)
@click.option(
    "--menu/--no-menu",
    show_default=True,
    default=True,
    help="Use a menu to display the environment or print",
)
@click.pass_obj
def list(obj: Any, local_only: bool = True, menu: bool = False):
    conda = Conda(obj.echo, obj.flow_datastore.TYPE)
    steps_to_potentials = _get_envs(
        conda, obj.flow, skip_known=False, local_only=local_only
    )
    for steps_query, potentials, req_id, _ in steps_to_potentials:
        local_instances = _local_instances_for_req_id(conda, req_id)
        plural = "s" if len(steps_query.split(",", 1)) > 1 else ""
        if len(potentials) == 0:
            if not obj.is_quiet:
                obj.echo(
                    "No %sknown environments for step%s %s"
                    % (("locally " if local_only else ""), plural, steps_query)
                )
            continue
        if menu:
            m = TerminalMenu(
                menu_entries=(
                    _format_resolved_env_for_menu(conda, env)
                    for env in potentials.values()
                ),
                title="Available environments for step%s %s (hash: %s)"
                % (plural, steps_query, req_id),
                #% _color_str(steps_query, style=_BOLD, fg=_RED),
                preview_title="Packages and local instances",
                column_headers=_columns_for_menu(),
                preview_cache_result=True,
                preview_border=True,
                preview_command=partial(_info_for_env, potentials, local_instances),
                preview_size=0.5,
            )
            m.show()
        elif obj.is_quiet:
            print(steps_query)
            for env in potentials.values():
                _quiet_print_env(obj, env, local_instances.get(env.env_id.full_id))
        else:
            obj.echo(
                "### Environments for step%s *%s* (hash: %s) ###"
                % (plural, steps_query, req_id)
            )
            first_env = True
            for env in potentials.values():
                if not first_env:
                    obj.echo("\n*~~~*\n")
                first_env = False
                _pretty_print_env(
                    obj,
                    env,
                    env.env_id
                    == conda.get_default_environment(
                        env.env_id.req_id, env.env_id.arch
                    ),
                    local_instances.get(env.env_id.full_id),
                )
            obj.echo("\n\n")


@environment.command(help="Select resolved environments for steps in a flow")
@click.option(
    "--skip-known/--no-skip-known",
    show_default=True,
    default=True,
    help="Skip steps with a locally known resolved environment",
)
@click.option(
    "--local-only/--no-local-only",
    show_default=True,
    default=False,
    help="Search only locally cached environments",
)
@click.option(
    "--dry-run/--no-dry-run",
    show_default=True,
    default=False,
    help="Dry-run -- do not update cached environments",
)
@click.pass_obj
def select_resolved(
    obj: Any, skip_known: bool = True, local_only: bool = False, dry_run: bool = False
):

    conda = Conda(obj.echo, obj.flow_datastore.TYPE)
    steps_to_potentials = _get_envs(
        conda, obj.flow, skip_known=skip_known, local_only=local_only
    )
    for steps_query, potentials, req_id, archs in steps_to_potentials:
        local_instances = _local_instances_for_req_id(conda, req_id)
        plural = "s" if len(steps_query.split(",", 1)) > 1 else ""
        menu = TerminalMenu(
            menu_entries=chain(
                [_unset_env_for_menu()],
                (
                    _format_resolved_env_for_menu(conda, env)
                    for env in potentials.values()
                ),
            ),
            title="Select environment for step%s %s" % (plural, steps_query),
            #% _color_str(steps_query, style=_BOLD, fg=_RED),
            preview_title="Packages",
            column_headers=_columns_for_menu(),
            preview_cache_result=True,
            preview_border=True,
            preview_command=partial(_info_for_env, potentials, local_instances),
            preview_size=0.5,
        )
        idx = None
        while idx is None:
            idx = menu.show()
        idx = cast(int, idx)
        if idx == 0:
            # This is the unset option, so we clear the default
            for arch in archs:
                conda.clear_default_environment(req_id, arch)
            if dry_run and not obj.is_quiet:
                obj.echo(
                    "Clearing default environment for step%s %s" % (plural, steps_query)
                )
        else:
            # This is a set option so we set the default to that value for all
            # requested archs
            full_id = tuple(potentials.keys())[idx - 1]

            for arch in archs:
                conda.set_default_environment(
                    EnvID(req_id=req_id, full_id=full_id, arch=arch)
                )
            if dry_run and not obj.is_quiet:
                obj.echo(
                    "Setting default environment to %s for step%s %s"
                    % (full_id, plural, steps_query)
                )

    if not dry_run:
        conda.write_out_environments()


def _info_for_env(
    env_dict: Dict[str, ResolvedEnvironment],
    local_instances: Dict[str, List[str]],
    full_id: str,
) -> List[str]:
    resolved_env = env_dict.get(full_id)
    if resolved_env is None:
        return [""]
    to_return = []  # type: List[str]
    pip_packages = []  # type: List[PackageSpecification]
    conda_packages = []  # type: List[PackageSpecification]
    for p in resolved_env.packages:
        if p.TYPE == "pip":
            pip_packages.append(p)
        else:
            conda_packages.append(p)

    pip_packages.sort(key=lambda x: x.package_name)
    conda_packages.sort(key=lambda x: x.package_name)
    local_dirs = local_instances.get(full_id)
    if local_dirs:
        to_return.append(
            _color_str("Locally Present", style=[_UNDERLINE, _BOLD], bg=_YELLOW)
        )
        to_return.extend(
            (
                "- %s"
                % d.replace(resolved_env.env_id.req_id, "<req-id>").replace(
                    full_id, "<id>"
                )
                for d in local_dirs
            )
        )
    if conda_packages:
        to_return.append(
            _color_str("Conda Packages", style=[_UNDERLINE, _BOLD], bg=_YELLOW)
        )
        to_return.extend(
            (
                _color_str("- %s==%s" % (p.package_name, p.package_version), fg=_GREEN)
                for p in conda_packages
            )
        )
    if pip_packages:
        to_return.append(
            _color_str("Pip Packages", style=[_UNDERLINE, _BOLD], bg=_YELLOW)
        )
        to_return.extend(
            (
                _color_str("- %s==%s" % (p.package_name, p.package_version), fg=_BLUE)
                for p in pip_packages
            )
        )
    return to_return


def _columns_for_menu() -> List[str]:
    return ["Env ID", "Archs", "Creator", "Resolved On", "Tags"]


def _unset_env_for_menu() -> List[str]:
    return ["re-resolve", "", "", "", ""]


def _format_resolved_env_for_menu(conda: Conda, env: ResolvedEnvironment) -> List[str]:
    # Returns full_id, resolved_archs, resolved_by, resolved_on, tags
    is_default = env.env_id == conda.get_default_environment(
        env.env_id.req_id, env.env_id.arch
    )
    return [
        "%s|%s" % (env.env_id.full_id, env.env_id.full_id),
        ", ".join(env.co_resolved_archs),
        env.resolved_by,
        env.resolved_on.isoformat(),
        ("[default]" if is_default else ""),
    ]


def _local_instances_for_req_id(conda: Conda, req_id: str) -> Dict[str, List[str]]:
    created_envs = conda.created_environments()  # This returns all MF environments
    local_instances = {}  # type: Dict[str, List[str]]
    for env_id, present_list in created_envs.items():
        if env_id.req_id != req_id:
            continue
        local_instances[env_id.full_id] = [os.path.basename(p) for p in present_list]
    return local_instances


def _quiet_print_env(
    obj, env: ResolvedEnvironment, local_instances: Optional[List[str]]
):
    env_id = env.env_id

    pip_packages = []  # type: List[PackageSpecification]
    conda_packages = []  # type: List[PackageSpecification]
    for p in env.packages:
        if p.TYPE == "pip":
            pip_packages.append(p)
        else:
            conda_packages.append(p)

    pip_packages.sort(key=lambda x: x.package_name)
    conda_packages.sort(key=lambda x: x.package_name)
    obj.echo_always(
        "%s %s %s %s %s %s %s %s conda:%s pip:%s %s"
        % (
            env_id.req_id,
            env_id.full_id,
            env_id.arch,
            env.resolved_on.isoformat(),
            env.resolved_by,
            ",".join(env.co_resolved_archs),
            ",".join([str(d) for d in env.deps]),
            ",".join([str(s) for s in env.sources]) if env.sources else "NONE",
            ",".join(
                ["%s==%s" % (p.package_name, p.package_version) for p in conda_packages]
            ),
            ",".join(
                ["%s==%s" % (p.package_name, p.package_version) for p in pip_packages]
            ),
            ",".join(local_instances) if local_instances else "NONE",
        )
    )


def _pretty_print_env(
    obj,
    env: ResolvedEnvironment,
    is_default: bool,
    local_instances: Optional[List[str]],
):

    pip_packages = []  # type: List[PackageSpecification]
    conda_packages = []  # type: List[PackageSpecification]
    for p in env.packages:
        if p.TYPE == "pip":
            pip_packages.append(p)
        else:
            conda_packages.append(p)

    pip_packages.sort(key=lambda x: x.package_name)
    conda_packages.sort(key=lambda x: x.package_name)
    obj.echo(
        "*%sEnvironment full hash* %s\n"
        % ("DEFAULT " if is_default else "", env.env_id.full_id)
    )
    obj.echo("*Arch* %s" % env.env_id.arch)
    obj.echo("*Available on* %s\n" % ", ".join(env.co_resolved_archs))
    obj.echo("*Resolved on* %s" % env.resolved_on)
    obj.echo("*Resolved by* %s\n" % env.resolved_by)
    if local_instances:
        obj.echo("*Locally present as* %s\n" % ", ".join(local_instances))

    obj.echo("*User-requested packages* %s" % ", ".join([str(d) for d in env.deps]))
    obj.echo("*User sources* %s" % ", ".join([str(s) for s in env.sources]))
    obj.echo(
        "*Conda Packages installed* %s"
        % ", ".join(
            ["%s==%s" % (p.package_name, p.package_version) for p in conda_packages]
        )
    )
    obj.echo(
        "*Pip Packages installed* %s"
        % ", ".join(
            ["%s==%s" % (p.package_name, p.package_version) for p in pip_packages]
        )
    )
