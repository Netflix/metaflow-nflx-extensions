# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
import json
import os
import sys

from typing import Any, Dict, List, Set, TYPE_CHECKING, Tuple, Union, Optional, cast

from metaflow._vendor import click

from metaflow.exception import CommandException
from metaflow.metaflow_config import CONDA_PREFERRED_FORMAT, CONDA_TEST

from .conda.conda import Conda
from .conda.conda_common_decorator import StepRequirement
from .conda.conda_environment import CondaEnvironment
from .conda.envsresolver import EnvsResolver
from .conda.env_descr import (
    AliasType,
    EnvID,
    ResolvedEnvironment,
    MetaflowResolvedEnvironment,
)
from .conda.utils import dict_to_tstr, plural_marker, resolve_env_alias

if TYPE_CHECKING:
    import metaflow

# Very simple functions to create some highlights without importing other packages
_BOLD = 1
_ITALIC = 3
_UNDERLINE = 4
_RED = 31
_GREEN = 32
_YELLOW = 33
_BLUE = 34

# reqid->fullid->[paths]
_all_local_instances = None  # type: Optional[Dict[str, Dict[str, List[str]]]]


# This is a dataclass -- can move to that when we only support 3.7+
class StepEnvInfo:
    """
    Information about the environment needed for a step.

    Attributes
    ----------
    is_disabled : bool
        If True, environment management is disabled for the step

    error : Optional[str]
        If the specification of the environment is invalid, this will contain the
        reason for it. Note that the absence of an error does not guarantee that the
        environment will be fully resolved correctly (this is un-knowable in all cases).

    requested_hash : Optional[str]
        Hash of the requested environment. This is used to uniquely identify the requirements
        set by the user. Note that for a given requested hash, multiple environments may
        be created. A unique environment is determined by both its requested hash as well
        as its full hash. The full hash is only available for resolved environments.
        Will be none if is_disabled or error is set. If the environment is resolved then
        env.env_id.req_id will be the same as requested_hash.

    requested_packages : Dict[str, Dict[str, str]]
        Packages requested for the step. The first key is the type of package (pypi,
        conda or sys), the second key is the package name and the value is the version
        constraint.

    requested_sources : Dict[str, List[str]]
        Sources requested for the step. The first key is the type of source (conda or pypi)
        and the value is a list of the sources. The order of the list is preserved
        based on what the user indicated.

    from_env_name : Optional[str]
        If non-None, the environment is derived from another environment (either a named
        environment or a step's environment).

    is_fetch_at_exec : bool
        If True, the environment does not need to be known when deploying the flow
        but only at runtime.

    env : Optional[MetaflowResolvedEnvironment]
        If the environment is known an resolved, it will be returned here. Otherwise,
        this value will be None. This means that the environment will need to be
        resolved prior to deploying unless is_fetch_at_exec is True.
    """

    def __init__(
        self,
        is_disabled: bool,
        error: Optional[str] = None,
        requested_hash: Optional[str] = None,
        requested_packages: Optional[Dict[str, Dict[str, str]]] = None,
        requested_sources: Optional[Dict[str, List[str]]] = None,
        from_env_name: Optional[str] = None,
        is_fetch_at_exec: bool = False,
        env: Optional[MetaflowResolvedEnvironment] = None,
    ):
        self.is_disabled = is_disabled
        self.error = error
        self.requested_hash = requested_hash
        self.requested_packages = requested_packages or {}
        self.requested_sources = requested_sources or {}
        self.from_env_name = from_env_name
        self.is_fetch_at_exec = is_fetch_at_exec
        self.env = env

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_disabled": self.is_disabled,
            "error": self.error,
            "requested_hash": self.requested_hash,
            "requested_packages": self.requested_packages,
            "requested_sources": self.requested_sources,
            "from_env_name": self.from_env_name,
            "is_fetch_at_exec": self.is_fetch_at_exec,
            "env_id": self.env.env_id if self.env else None,
            "env": self.env.to_dict() if self.env else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StepEnvInfo":
        return cls(
            data["is_disabled"],
            data["error"],
            data["requested_hash"],
            data["requested_packages"],
            data["requested_sources"],
            data["from_env_name"],
            data["is_fetch_at_exec"],
            (
                MetaflowResolvedEnvironment(
                    ResolvedEnvironment.from_dict(EnvID(*data["env_id"]), data["env"])
                )
                if data["env"]
                else None
            ),
        )


class RunnerCLI(object):
    name = "environment"

    def __init__(self, runner: "metaflow.Runner"):
        """
        Environment command for the flow. Requires the conda environment.
        """
        self.runner = runner

    def resolve(
        self,
        *args: str,
        arch: Optional[Union[str, List[str]]] = None,
        alias: Optional[List[str]] = None,
        force: bool = False,
        dry_run: bool = False,
        timeout: int = 1200,
    ) -> Dict[str, Dict[str, Tuple[MetaflowResolvedEnvironment, bool]]]:
        """
        Resolve the environments needed for this flow. Returns a dictionary associating
        the name of the step with the resolved environment.

        Note that steps that have conda disabled will not be present in the returned
        dictionary.

        Parameters
        ----------
        *args : str
            Name of the steps to resolve. If not specified, all steps are resolved
        arch : Union[str, List[str]], optional, default None
            Architecture(s) to resolve the environments for. If None, the default
            architecture is used (dependent on where the step should run)
        alias : List[str], optional, default None
            If specified, aliases to alias the resolved environment by. Only valid if
            only one environment is being resolved (ie: only one step)
        force : bool, default False
            Force the resolution of all environments even if they are already resolved.
        dry_run : bool, default False
            Perform the resolution but do not cache or write out the resolved environments.
        timeout : int, default 1200
            Timeout in seconds -- note that this effectively caps the total amount of time
            available for environment resolution.

        Returns
        -------
        Dict[str, Dict[str, Tuple[MetaflowResolvedEnvironment, bool]]]
            Resolved environments for all steps requested.
            The outer key is the name of the step
            The inner key is the architecture
            The element is a tuple consisting of the resolved environment and a boolean
            indicating if the environment was newly resolved or not (True if just
            resolved or False if previously resolved)
        """
        from metaflow.runner.utils import (
            handle_timeout,
            make_process_error_message,
            temporary_fifo,
        )

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            command = (
                self.runner.api(**self.runner.top_level_kwargs)
                .environment()
                .resolve(
                    arch=arch,
                    alias=alias,
                    force=force,
                    dry_run=dry_run,
                    output=attribute_file_path,
                    steps_to_resolve=args if args else None,
                )
            )
            pid = self.runner.spm.run_command(
                [sys.executable, *command],
                env=self.runner.env_vars,
                cwd=self.runner.cwd,
                show_output=False,
            )
            command_obj = self.runner.spm.get(pid)
            if not command_obj:
                raise RuntimeError("Failed to get command object")

            content = handle_timeout(attribute_file_fd, command_obj, timeout)
            command_obj.sync_wait()

            if command_obj.process.returncode == 0:
                # We parse content and return the resolved environments
                content = json.loads(content)
                return {
                    k: {
                        kk: (
                            MetaflowResolvedEnvironment(
                                ResolvedEnvironment.from_dict(
                                    EnvID(vv["req_id"], vv["full_id"], kk),
                                    vv["env"],
                                )
                            ),
                            vv["newly_resolved"],
                        )
                        for kk, vv in v.items()
                    }
                    for k, v in content.items()
                }
            raise RuntimeError(make_process_error_message(command_obj))

    def show(
        self, *args: str, local_only: bool = False, timeout: int = 60
    ) -> Dict[str, StepEnvInfo]:
        """
        Returns information about the environments needed for the steps in the flow.

        For each requested step, information about the packages and sources requested
        as well as an optional resolved environment is returned.

        Parameters
        ----------
        args : str
            Steps to show information for. If none are specified, information for all
            steps will be returned.
        local_only : bool, default False
            If True, will only list environments as known if they are locally known. This
            primarily affects named environments or if you have
            METAFLOW_CONDA_USE_REMOTE_LATEST set to something different than ":none:".
        timeout : int, default 60
            Timeout in seconds for this call.

        Returns
        -------
        Dict[str, StepEnvInfo]
            For each step requested, returns information about the environment needed for
            that step.
        """
        from metaflow.runner.utils import (
            handle_timeout,
            make_process_error_message,
            temporary_fifo,
        )

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            command = (
                self.runner.api(**self.runner.top_level_kwargs)
                .environment()
                .show(
                    local_only=local_only,
                    output=attribute_file_path,
                    steps_to_show=args if args else None,
                )
            )
            pid = self.runner.spm.run_command(
                [sys.executable, *command],
                env=self.runner.env_vars,
                cwd=self.runner.cwd,
                show_output=False,
            )
            command_obj = self.runner.spm.get(pid)
            assert command_obj
            content = handle_timeout(attribute_file_fd, command_obj, timeout)
            command_obj.sync_wait()

            if command_obj.process.returncode == 0:
                # We parse content and return the resolved environments
                content = json.loads(content)
                return {k: StepEnvInfo.from_dict(v) for k, v in content.items()}

            raise RuntimeError(make_process_error_message(command_obj))


@click.group()
def cli():
    pass


@cli.group(help="Commands related to environment manipulation.")
@click.pass_obj
def environment(obj):
    if obj.environment.TYPE != "conda":
        raise CommandException("'environment' requires a Conda environment")


@environment.command(help="Resolve environments for steps in a flow")
@click.option(
    "--alias",
    default=None,
    multiple=True,
    help="Alias the resolved environment; can be name or name:tag",
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
    help="Dry-run -- only resolve, do not download, cache, persist or alias anything",
)
@click.option(
    "--arch",
    default=None,
    multiple=True,
    help="Architecture to resolve for. Can be specified multiple times. "
    "If not specified, defaults to the architecture for the step",
    required=False,
)
@click.option(
    "--output",
    show_default=True,
    default=None,
    type=click.Path(file_okay=True, writable=True, resolve_path=True),
    help="If specified, the environments for the requested steps are written out "
    "to this file. Note that this includes only newly resolved environments.",
)
@click.argument(
    "steps-to-resolve",
    required=False,
    nargs=-1,
)
@click.pass_obj
def resolve(
    obj: Any,
    steps_to_resolve: Tuple[str],
    arch: Optional[Tuple[str]] = None,
    alias: Optional[Tuple[str]] = None,
    force: bool = False,
    dry_run: bool = False,
    output: Optional[str] = None,
):

    conda = Conda(obj.echo, obj.flow_datastore.TYPE)
    resolver = EnvsResolver(conda)

    def _write_out_environments():
        # If we have an output file, write out to it
        if output:
            resolved_per_step = {}  # type: Dict[str, Dict[str, ResolvedEnvironment]]
            new_env_ids = set(env.env_id for _, env, _ in resolver.new_environments())
            for env_id, env, steps in resolver.resolved_environments():
                for step in steps:
                    resolved_per_step.setdefault(step, {})[env_id.arch] = env
            out_json = {}
            with open(output, "w", encoding="utf-8") as f:
                for step, envs in resolved_per_step.items():
                    for env_arch, env in envs.items():
                        out_json.setdefault(step, {})[env_arch] = {
                            "req_id": env.env_id.req_id,
                            "full_id": env.env_id.full_id,
                            "newly_resolved": env.env_id in new_env_ids,
                            "env": env.to_dict(),
                        }
                json.dump(out_json, f, indent=2)

    for step in obj.flow:
        if not steps_to_resolve or step.name in steps_to_resolve:
            for resolve_arch in arch or [None]:
                (
                    _,
                    step_arch,
                    req,
                    base_env,
                ) = CondaEnvironment.extract_merged_reqs_for_step(
                    conda,
                    obj.flow,
                    obj.flow_datastore.TYPE,
                    step,
                    override_arch=resolve_arch,
                )
                if req.is_disabled or req.is_fetch_at_exec:
                    break  # No point resolving anything else for this step
                base_from_full_id = False
                if req.from_env_name:
                    base_from_full_id = (
                        resolve_env_alias(req.from_env_name)[0] == AliasType.FULL_ID
                    )
                resolver.add_environment(
                    step_arch,
                    req.packages_as_str,
                    req.sources,
                    {},
                    step.name,
                    base_env,
                    base_from_full_id=base_from_full_id,
                    force=force,
                )

    per_req_id = {}  # type: Dict[str, Set[str]]

    if alias:
        aliases = list(alias)
    else:
        aliases = None
    for env_id, steps in resolver.non_resolved_environments():
        per_req_id.setdefault(env_id.req_id, set()).update(steps)
    if len(per_req_id) == 0:
        # Nothing to do but we may still need to alias
        if aliases is not None and not dry_run:
            all_resolved_envs = list(resolver.resolved_environments())
            if len(all_resolved_envs) == 0:
                obj.echo("No environments to resolve")
            elif len(all_resolved_envs) == 1:
                obj.echo("Environment already resolved -- aliasing only")
                conda.alias_environment(all_resolved_envs[0][0], aliases)
                conda.write_out_environments()
            else:
                raise CommandException(
                    "Cannot specify aliases if more than one environments to alias "
                    "-- found %d environments: one for each of steps %s"
                    % (
                        len(all_resolved_envs),
                        ", and ".join(", ".join(v[2]) for v in all_resolved_envs),
                    )
                )
        else:
            obj.echo("No environments to resolve, use --force to force re-resolution")
        _write_out_environments()
        return
    if aliases is not None and len(per_req_id) > 1:
        raise CommandException(
            "Cannot specify aliases if more than one environment to resolve "
            "-- found %d environments: one for each of steps %s"
            % (
                len(per_req_id),
                ", and ".join(", ".join(s) for s in per_req_id.values()),
            )
        )

    resolver.resolve_environments(obj.echo)
    # We group by req_id which will effectively be by set of steps. We only
    # print newly resolved environments
    resolved_per_req_id = {}  # type: Dict[str, Tuple[ResolvedEnvironment, Set[str]]]
    for env_id, env, steps in resolver.resolved_environments():
        v = resolved_per_req_id.setdefault(env_id.req_id, (env, set()))
        v[1].update(steps)
        if aliases and not dry_run:
            # We know this executes only once since we already checked that if tag
            # is set, there is only one environment being resolved
            conda.alias_environment(env.env_id, aliases)

    _compute_local_instances(conda)
    assert _all_local_instances is not None

    _write_out_environments()

    if obj.is_quiet:
        for req_id, (env, steps) in resolved_per_req_id.items():
            obj.echo_always(",".join(steps))
            obj.echo_always(
                env.quiet_print(
                    _all_local_instances.get(req_id, {}).get(env.env_id.full_id)
                )
            )
    else:
        for req_id, (env, steps) in resolved_per_req_id.items():
            obj.echo(
                "### Environment for step%s *%s* ###"
                % (plural_marker(len(steps)), ", ".join(steps))
            )
            obj.echo(
                env.pretty_print(
                    _all_local_instances.get(req_id, {}).get(env.env_id.full_id)
                )
            )
            obj.echo("\n\n")

    if dry_run:
        obj.echo("Dry-run -- not caching or aliasing")
        return

    # If this is not a dry-run, we cache the environments and write out the resolved
    # information
    update_envs = []  # type: List[ResolvedEnvironment]
    if obj.flow_datastore.TYPE != "local" or CONDA_TEST:
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

    # We are done -- write back out the environments
    conda.write_out_environments()


@environment.command(help="Show status of environment resolution for a flow")
@click.option(
    "--local-only/--no-local-only",
    show_default=True,
    default=False,
    help="Search only locally cached environments",
)
@click.option(
    "--output",
    show_default=True,
    default=None,
    type=click.Path(file_okay=True, writable=True, resolve_path=True),
    help="If specified, the environments for the requested steps are written out "
    "to this file.",
)
@click.argument(
    "steps-to-show",
    required=False,
    nargs=-1,
)
@click.pass_obj
def show(obj: Any, local_only: bool, output: Optional[str], steps_to_show: Tuple[str]):
    # Goes from step_name to information about the step
    result = {}  # type: Dict[str, Any]
    conda = Conda(obj.echo, obj.flow_datastore.TYPE)
    for step in obj.flow:
        if not steps_to_show or step.name in steps_to_show:
            (
                _,
                step_arch,
                req,
                base_env,
            ) = CondaEnvironment.extract_merged_reqs_for_step(
                conda, obj.flow, obj.flow_datastore.TYPE, step, local_only=local_only
            )
            from_name = req.from_env_name
            result[step.name] = {
                "req": req,
                "req_hash": ResolvedEnvironment.get_req_id(
                    req.packages_as_str, req.sources
                ),
            }
            if from_name:
                result[step.name]["from_env_name"] = from_name
                if base_env is None:
                    result[step.name]["error"] = (
                        "Base environment '%s' was not found for architecture '%s'"
                        % (
                            from_name,
                            step_arch,
                        )
                    )
                    continue
                result[step.name]["base_env"] = base_env
                result[step.name]["state"] = ["derived from %s" % from_name]
            else:
                result[step.name]["state"] = []

            step_env_id = CondaEnvironment.get_env_id(conda, step.name)
            resolved_env = None
            if req.is_disabled:
                result[step.name]["state"].append("disabled")
            elif isinstance(step_env_id, EnvID):
                resolved_env = conda.environment(step_env_id, local_only)
                if resolved_env:
                    result[step.name]["env"] = resolved_env
                    result[step.name]["state"].append("resolved")

                    if (
                        obj.flow_datastore.TYPE != "local" or CONDA_TEST
                    ) and resolved_env.is_cached(
                        {
                            "conda": (
                                [CONDA_PREFERRED_FORMAT]
                                if CONDA_PREFERRED_FORMAT
                                and CONDA_PREFERRED_FORMAT != "none"
                                else ["_any"]
                            )
                        }
                    ):
                        result[step.name]["state"].append("cached")
                    if conda.created_environment(resolved_env.env_id):
                        result[step.name]["state"].append("locally present")
                else:
                    result[step.name]["state"].append("unresolved")
            elif isinstance(step_env_id, str):
                result[step.name]["state"].append("fetch-at-exec of %s" % step_env_id)
            else:
                result[step.name]["state"].append("not resolved")

    _compute_local_instances(conda)

    assert _all_local_instances is not None
    if output:
        out_json = {}
        with open(output, "w", encoding="utf-8") as f:
            for step_name, step_info in result.items():
                if "disabled" in step_info["state"]:
                    out_json[step_name] = StepEnvInfo(is_disabled=True).to_dict()
                elif "error" in step_info:
                    out_json[step_name] = StepEnvInfo(
                        is_disabled=False, error=step_info["error"]
                    ).to_dict()
                else:
                    is_fetch_at_exec = False
                    req_hash = step_info["req_hash"]
                    if any(s.startswith("fetch-at-exec") for s in step_info["state"]):
                        is_fetch_at_exec = True
                    out_json[step_name] = StepEnvInfo(
                        is_disabled=False,
                        requested_hash=req_hash,
                        requested_packages=cast(
                            StepRequirement, step_info["req"]
                        ).packages,
                        requested_sources=cast(
                            StepRequirement, step_info["req"]
                        ).sources,
                        from_env_name=step_info.get("from_env_name"),
                        is_fetch_at_exec=is_fetch_at_exec,
                        env=(
                            MetaflowResolvedEnvironment(step_info.get("env"))
                            if step_info.get("env")
                            else None
                        ),
                    ).to_dict()

            json.dump(out_json, f, indent=2)
    if not obj.is_quiet:
        for name, info in result.items():
            obj.echo("\nStep *%s*" % name)
            if "error" in info:
                obj.echo("\t%s" % info["error"], err=True)
            else:
                obj.echo("\tEnvironment is %s" % ", ".join(info["state"]))
            if "env" in info:
                env_id = cast(ResolvedEnvironment, info["env"]).env_id
                obj.echo(
                    cast(ResolvedEnvironment, info["env"]).pretty_print(
                        _all_local_instances.get(env_id.req_id, {}).get(env_id.full_id)
                    )
                )
            else:
                obj.echo(
                    "*User-requested packages* %s"
                    % ", ".join(
                        [
                            str(d).replace("*", "x")
                            for d in dict_to_tstr(
                                cast(StepRequirement, info["req"]).packages_as_str
                            )
                        ]
                    )
                )
                obj.echo(
                    "*User sources* %s"
                    % ", ".join(
                        [
                            str(s)
                            for s in dict_to_tstr(
                                cast(StepRequirement, info["req"]).sources
                            )
                        ]
                    )
                )


def _compute_local_instances(conda: Conda):
    global _all_local_instances
    if _all_local_instances is None:
        _all_local_instances = {}
        created_envs = conda.created_environments()  # This returns all MF environments
        for env_id, present_list in created_envs.items():
            _all_local_instances.setdefault(env_id.req_id, {})[env_id.full_id] = [
                os.path.basename(p) for p in present_list
            ]


# @environment.command(help="Show available environments for steps in a flow")
# @click.option(
#     "--local-only/--no-local-only",
#     show_default=True,
#     default=False,
#     help="Search only locally cached environments",
# )
# @click.option(
#     "--menu/--no-menu",
#     show_default=True,
#     default=True,
#     help="Use a menu to display the environment or print -- menu is incompatible with quiet",
# )
# @click.argument(
#     "steps-to-list",
#     required=False,
#     nargs=-1,
# )
# @click.pass_obj
# def list(
#     obj: Any,
#     local_only: bool = True,
#     menu: bool = False,
#     steps_to_list: Optional[Tuple[str]] = None,
# ):
#     conda = Conda(obj.echo, obj.flow_datastore.TYPE)
#     steps_to_potentials = _get_envs(
#         conda, obj.flow, skip_known=False, local_only=local_only, steps=steps_to_list
#     )
#     for steps_query, potentials, req_id, _ in steps_to_potentials:
#         local_instances = _local_instances_for_req_id(conda, req_id)
#         plural = "s" if len(steps_query.split(",", 1)) > 1 else ""
#         if len(potentials) == 0:
#             if not obj.is_quiet:
#                 obj.echo(
#                     "No %sknown environments for step%s %s"
#                     % (("locally " if local_only else ""), plural, steps_query)
#                 )
#             continue
#         if menu:
#             if obj.is_quiet:
#                 raise click.UsageError(
#                     message="Menu flag is incompatible with quiet flag", ctx=obj
#                 )
#             m = TerminalMenu(
#                 menu_entries=(
#                     _format_resolved_env_for_menu(conda, env)
#                     for env in potentials.values()
#                 ),
#                 title="Available environments for step%s %s (hash: %s)"
#                 % (plural, steps_query, req_id),
#                 #% _color_str(steps_query, style=_BOLD, fg=_RED),
#                 preview_title="Packages and local instances",
#                 column_headers=_columns_for_menu(),
#                 preview_cache_result=True,
#                 preview_border=True,
#                 preview_command=partial(_info_for_env, potentials, local_instances),
#                 preview_size=0.5,
#             )
#             m.show()
#         elif obj.is_quiet:
#             print(steps_query)
#             for env in potentials.values():
#                 _quiet_print_env(obj, env, local_instances.get(env.env_id.full_id))
#         else:
#             obj.echo(
#                 "### Environments for step%s *%s* (hash: %s) ###"
#                 % (plural, steps_query, req_id)
#             )
#             first_env = True
#             for env in potentials.values():
#                 if not first_env:
#                     obj.echo("\n*~~~*\n")
#                 first_env = False
#                 _pretty_print_env(
#                     obj,
#                     env,
#                     env.env_id
#                     == conda.get_default_environment(
#                         env.env_id.req_id, env.env_id.arch
#                     ),
#                     local_instances.get(env.env_id.full_id),
#                 )
#             obj.echo("\n\n")

# def _color_str(
#     s: str,
#     style: Optional[Union[int, List[int]]] = None,
#     fg: Optional[int] = None,
#     bg: Optional[int] = None,
# ) -> str:
#     if style is None:
#         style = []
#     elif isinstance(style, int):
#         style = [style]
#     if fg is not None:
#         style.append(fg)
#     if bg is not None:
#         style.append(bg + 10)
#     return "\033[%sm%s\033[0m" % (";".join((str(x) for x in style)), s)


# def _get_envs(
#     conda: Conda,
#     flow: FlowSpec,
#     skip_known: bool,
#     local_only: bool,
#     steps: Optional[Tuple[str]] = None,
# ) -> List[Tuple[str, Dict[str, ResolvedEnvironment], str, Tuple[str]]]:
#     reqs_to_steps = {}  # type: Dict[Tuple[str, Tuple[str]], List[str]]
#     for step in flow:
#         if steps and step.name not in steps:
#             continue
#         step_conda_dec = get_conda_decorator(flow, step.__name__)
#         # We want to gather steps by the type of requirements they have. This is
#         # basically their req ID as well as the requested architectures.
#         env_ids = step_conda_dec.env_ids
#         archs = tuple(sorted(env.arch for env in env_ids))
#         req_id = env_ids[0].req_id
#         reqs_to_steps.setdefault((req_id, archs), []).append(step.__name__)
#     # We now look for environments that have the proper req_id and a superset of the
#     # architectures asked for
#     if debug.conda:
#         debug.conda_exec("Requirements to resolve are:")
#         for (req_id, archs), steps in reqs_to_steps.items():
#             debug.conda_exec(
#                 "%s (archs: %s) for %s" % (req_id, ", ".join(archs), ", ".join(steps))
#             )

#     steps_to_potentials = (
#         []
#     )  # type: List[Tuple[str, Dict[str, ResolvedEnvironment], str, Tuple[str]]]
#     for (req_id, archs), steps in reqs_to_steps.items():
#         # We can pick just a single arch to look for potentials; we know we need
#         # at least that one
#         arch = archs[0]
#         need_archs = set(archs)
#         possible_envs = conda.environments(req_id, arch, local_only=local_only)
#         possible_envs = [
#             r for r in possible_envs if need_archs.issubset(r[1].co_resolved_archs)
#         ]
#         if skip_known:
#             default_env_id = conda.get_default_environment(req_id, arch)
#             if default_env_id and default_env_id in [env[0] for env in possible_envs]:
#                 debug.conda_exec(
#                     "Have known local default for %s -- skipping" % ", ".join(steps)
#                 )
#                 continue
#         steps_to_potentials.append(
#             (
#                 ", ".join(steps),
#                 {env_id.full_id: env for env_id, env in possible_envs},
#                 req_id,
#                 archs,
#             )
#         )
#     return steps_to_potentials


# def _info_for_env(
#     env_dict: Dict[str, ResolvedEnvironment],
#     local_instances: Dict[str, List[str]],
#     full_id: str,
# ) -> List[str]:
#     resolved_env = env_dict.get(full_id)
#     if resolved_env is None:
#         return [""]
#     to_return = []  # type: List[str]
#     pip_packages = []  # type: List[PackageSpecification]
#     conda_packages = []  # type: List[PackageSpecification]
#     for p in resolved_env.packages:
#         if p.TYPE == "pip":
#             pip_packages.append(p)
#         else:
#             conda_packages.append(p)

#     pip_packages.sort(key=lambda x: x.package_name)
#     conda_packages.sort(key=lambda x: x.package_name)
#     local_dirs = local_instances.get(full_id)
#     if local_dirs:
#         to_return.append(
#             _color_str("Locally Present", style=[_UNDERLINE, _BOLD], bg=_YELLOW)
#         )
#         to_return.extend(
#             (
#                 "- %s"
#                 % d.replace(resolved_env.env_id.req_id, "<req-id>").replace(
#                     full_id, "<id>"
#                 )
#                 for d in local_dirs
#             )
#         )
#     if conda_packages:
#         to_return.append(
#             _color_str("Conda Packages", style=[_UNDERLINE, _BOLD], bg=_YELLOW)
#         )
#         to_return.extend(
#             (
#                 _color_str("- %s==%s" % (p.package_name, p.package_version), fg=_GREEN)
#                 for p in conda_packages
#             )
#         )
#     if pip_packages:
#         to_return.append(
#             _color_str("Pip Packages", style=[_UNDERLINE, _BOLD], bg=_YELLOW)
#         )
#         to_return.extend(
#             (
#                 _color_str("- %s==%s" % (p.package_name, p.package_version), fg=_BLUE)
#                 for p in pip_packages
#             )
#         )
#     return to_return


# def _columns_for_menu() -> List[str]:
#     return ["Env ID", "Archs", "Creator", "Resolved On", "Tags"]


# def _unset_env_for_menu() -> List[str]:
#     return ["re-resolve", "", "", "", ""]


# def _format_resolved_env_for_menu(conda: Conda, env: ResolvedEnvironment) -> List[str]:
#     # Returns full_id, resolved_archs, resolved_by, resolved_on, tags
#     is_default = env.env_id == conda.get_default_environment(
#         env.env_id.req_id, env.env_id.arch
#     )
#     tags = env.env_alias if env.env_alias else ""
#     if is_default:
#         if tags:
#             tags += ", [default]"
#         tags = "[default]"

#     return [
#         "%s|%s" % (env.env_id.full_id, env.env_id.full_id),
#         ", ".join(env.co_resolved_archs),
#         env.resolved_by,
#         env.resolved_on.isoformat(),
#         ("[default]" if is_default else ""),
#     ]
