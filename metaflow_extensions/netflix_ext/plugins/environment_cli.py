# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
import os

from functools import partial
from itertools import chain
from typing import Any, Dict, List, Set, Tuple, Union, Optional, cast

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
    help="Use a menu to display the environment or print -- menu is incompatible with quiet",
)
@click.argument(
    "steps-to-list",
    required=False,
    nargs=-1,
    help="Steps to list -- if absent environments for all steps are listed",
)
@click.pass_obj
def list(
    obj: Any,
    local_only: bool = True,
    menu: bool = False,
    steps_to_list: Optional[Tuple[str]] = None,
):
    conda = Conda(obj.echo, obj.flow_datastore.TYPE)
    steps_to_potentials = _get_envs(
        conda, obj.flow, skip_known=False, local_only=local_only, steps=steps_to_list
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
            if obj.is_quiet:
                raise click.UsageError(
                    message="Menu flag is incompatible with quiet flag", ctx=obj
                )
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
    "--only-unknown/--no-only-unknown",
    show_default=True,
    default=True,
    help="If True, skip steps for which an environment is already resolved/known",
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
@click.argument(
    "steps-to-select",
    required=False,
    nargs=-1,
    help="Steps to re-resolve -- if absent, all steps will be presented",
)
@click.pass_obj
def select_resolved(
    obj: Any,
    only_unknown: bool = True,
    local_only: bool = False,
    dry_run: bool = False,
    steps_to_select: Optional[Tuple[str]] = None,
):

    if obj.is_quiet:
        raise click.UsageError(
            message="select-resolved not compatible with quiet flag", ctx=obj
        )
    conda = Conda(obj.echo, obj.flow_datastore.TYPE)
    steps_to_potentials = _get_envs(
        conda,
        obj.flow,
        skip_known=only_unknown,
        local_only=local_only,
        steps=steps_to_select,
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


@environment.command(help="Resolve environments for steps in a flow")
@click.option(
    "--tag",
    default=None,
    multiple=True,
    help="Tag the resolved environment; can be name or name:tag",
)
@click.option(
    "--force/--no-force",
    show_default=True,
    default=False,
    help="Force re-resolution of already resolved environments",
)
@click.option(
    "--dry-run/--no-dry-run",
    show_default=True,
    default=False,
    help="Dry-run -- only resolve, do not download, cache, persist or tag anything",
)
@click.argument(
    "steps-to-resolve",
    required=False,
    nargs=-1,
    help="Steps to resolve -- if absent all steps are resolved",
)
@click.pass_obj
def resolve(
    obj: Any,
    tag: Optional[str] = None,
    force: bool = False,
    dry_run: bool = False,
    steps_to_resolve: Optional[Tuple[str]] = None,
):
    conda = Conda(obj.echo, obj.flow_datastore.TYPE)
    resolver = EnvsResolver(conda)
    for step in obj.flow:
        if steps_to_resolve is None or step.name in steps_to_resolve:
            step_conda_dec = get_conda_decorator(obj.flow, step.name)
            resolver.add_environment_for_step(step.name, step_conda_dec, force=force)
    per_req_id = {}  # type: Dict[str, Set[str]]
    for env_id, steps in resolver.non_resolved_environments():
        per_req_id.setdefault(env_id.req_id, set()).update(steps)
    if len(per_req_id) == 0:
        # Nothing to do
        obj.echo("No environments to resolve, use --force to force re-resolution")
        return
    if tag and len(per_req_id) > 1:
        raise CommandException(
            "Cannot specify a tag if more than one environment to resolve "
            "-- found %d environments: one for each of steps %s"
            % (
                len(per_req_id),
                ", and ".join(", ".join(s) for s in per_req_id.values()),
            )
        )

    resolver.resolve_environments(obj.echo)
    # We group by req_id which will effectively be by set of steps. We only
    # print newly resolved environments
    resolved_per_req_id = (
        {}
    )  # type: Dict[str, Tuple[List[ResolvedEnvironment], Set[str]]]
    for env_id, env, steps in resolver.new_environments():
        v = resolved_per_req_id.setdefault(env_id.req_id, ([], set()))
        v[0].append(env)
        v[1].update(steps)

    if obj.is_quiet:
        for req_id, (envs, steps) in resolved_per_req_id.items():
            obj.echo_always(",".join(steps))
            local_instances = _local_instances_for_req_id(conda, req_id)
            for env in envs:
                _quiet_print_env(obj, env, local_instances.get(env.env_id.full_id))
    else:
        for req_id, (envs, steps) in resolved_per_req_id.items():
            local_instances = _local_instances_for_req_id(conda, req_id)
            obj.echo(
                "### Environment%s for step%s *%s* (hash: %s) ###"
                % (
                    plural_marker(len(envs)),
                    plural_marker(len(steps)),
                    ", ".join(steps),
                    req_id,
                )
            )
            first_env = True
            for env in envs:
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
    if dry_run:
        obj.echo("Dry-run -- not caching or tagging")
        return

    # If this is not a dry-run, we cache the environments and write out the resolved
    # information
    update_envs = []  # type: List[ResolvedEnvironment]
    if obj.flow_datastore.TYPE != "local":
        # We may need to update caches
        # Note that it is possible that something we needed to resolve, we don't need
        # to cache (if we resolved to something already cached).
        formats = set()  # type: Set[str]
        for _, resolved_env, f, _ in resolver.need_caching_environments():
            update_envs.append(resolved_env)
            formats.update(f)

        conda.cache_environments(update_envs, {"conda": list(formats)})
    else:
        update_envs = [
            resolved_env for _, resolved_env, _ in resolver.new_environments()
        ]

    conda.add_environments(update_envs)

    # Update the default environment
    for env_id, resolved_env, _ in resolver.resolved_environments():
        if env_id.full_id == "_default":
            conda.set_default_environment(resolved_env.env_id)
        if tag:
            resolved_env.env_alias = tag

    # We are done -- write back out the environments
    conda.write_out_environments()


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
    conda: Conda,
    flow: FlowSpec,
    skip_known: bool,
    local_only: bool,
    steps: Optional[Tuple[str]] = None,
) -> List[Tuple[str, Dict[str, ResolvedEnvironment], str, Tuple[str]]]:
    reqs_to_steps = {}  # type: Dict[Tuple[str, Tuple[str]], List[str]]
    for step in flow:
        if steps and step.name not in steps:
            continue
        step_conda_dec = get_conda_decorator(flow, step.__name__)
        # We want to gather steps by the type of requirements they have. This is
        # basically their req ID as well as the requested architectures.
        env_ids = step_conda_dec.env_ids
        archs = tuple(sorted(env.arch for env in env_ids))
        req_id = env_ids[0].req_id
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
    tags = env.env_alias if env.env_alias else ""
    if is_default:
        if tags:
            tags += ", [default]"
        tags = "[default]"

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
        "%s %s %s %s %s %s %s %s %s conda:%s pip:%s %s"
        % (
            env_id.req_id,
            env_id.full_id,
            env.env_alias if env.env_alias else "",
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
    if env.env_alias:
        obj.echo("*Tag* %s" % env.env_alias)
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
