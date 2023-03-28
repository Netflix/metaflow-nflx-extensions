import os
import platform
import shutil

from functools import wraps
from typing import Any, Optional, cast
from metaflow._vendor import click

from metaflow.cli import echo_dev_null, echo_always


from metaflow import Step, metaflow_config
from metaflow.plugins import DATASTORES
from metaflow.metaflow_config import DEFAULT_DATASTORE, DEFAULT_METADATA
from metaflow.plugins import METADATA_PROVIDERS
from metaflow_extensions.netflix_ext.plugins.conda.conda import Conda
from metaflow_extensions.netflix_ext.plugins.conda.env_descr import (
    AliasType,
    arch_id,
    resolve_env_alias,
)

from .utils import (
    download_mf_version,
)


class CommandObj:
    def __init__(self):
        pass


def env_spec_options(func):
    @click.option(
        "--python",
        default=platform.python_version(),
        show_default=True,
        help="Python version for the environment",
    )
    @click.option(
        "--deps",
        required=True,
        help="Semi-colon separated list of dependencies. Specify version constraints per "
        "dependency using a comma separated list of constraints",
    )
    @click.option(
        "--channels",
        default="",
        show_default=True,
        help="Comma separated list of channels to search in addition to the default ones",
    )
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.group(help="Execution environment related commands.")
@click.option(
    "--quiet/--no-quiet",
    show_default=True,
    default=False,
    help="Suppress unnecessary messages",
)
@click.option(
    "--metadata",
    default=DEFAULT_METADATA,
    show_default=True,
    type=click.Choice([m.TYPE for m in METADATA_PROVIDERS]),
    help="Metadata service type",
)
@click.option(
    "--datastore",
    default=DEFAULT_DATASTORE,
    show_default=True,
    type=click.Choice([d.TYPE for d in DATASTORES]),
    help="Datastore type",
)
# For later support of different environment types.
@click.option(
    "--environment",
    default="conda",
    show_default=True,
    type=click.Choice(["conda"]),
    help="Type of environment to manage",
)
@click.option(
    "--conda-root",
    default=None,
    help="Root path for Conda cached information -- if None, uses METAFLOW_CONDA_<DS>ROOT",
)
@click.pass_context
def environment(
    ctx: Any,
    quiet: bool,
    metadata: str,
    datastore: str,
    environment: str,
    conda_root: Optional[str],
):
    if quiet:
        echo = echo_dev_null
    else:
        echo = echo_always

    obj = CommandObj()
    obj.quiet = quiet
    obj.echo = echo
    obj.echo_always = echo_always
    obj.datastore_type = datastore

    if conda_root:
        if obj.datastore_type == "s3":
            metaflow_config.CONDA_S3ROOT = conda_root
        elif obj.datastore_type == "azure":
            metaflow_config.CONDA_AZUREROOT = conda_root
        elif obj.datastore_type == "gs":
            metaflow_config.CONDA_GSROOT = conda_root
        else:
            # Should never happen due to click validation
            raise RuntimeError("Unknown datastore type: %s" % obj.datastore_type)
    obj.conda_root = getattr(
        metaflow_config, "CONDA_%sROOT" % obj.datastore_type.upper()
    )

    obj.metadata_type = metadata

    obj.conda = Conda(obj.echo, obj.datastore_type)
    ctx.obj = obj


@environment.command()
@click.option("--name", default=None, help="Name of the environment to create")
@click.option(
    "--local-only/--no-local-only",
    show_default=True,
    default=False,
    help="Only create if environment is known locally",
)
@click.option(
    "--force/--no-force",
    default=False,
    show_default=True,
    help="Recreate the environment if it already exists and remove the `into` directory "
    "if it exists",
)
@click.option(
    "--into",
    default=None,
    show_default=False,
    type=click.Path(file_okay=False, writable=True, readable=True, resolve_path=True),
    help="If the `envspec` refers to a Metaflow executed Step, downloads the step's "
    "code package into this directory",
)
@click.argument("env-name")
@click.pass_obj
def create(
    obj,
    name: Optional[str] = None,
    local_only: bool = False,
    force: bool = False,
    into: Optional[str] = None,
    env_name: str = "",
):
    """
    Create a local environment based on env-name.

    env-name can either be:
      - a pathspec in the form <flowname>/<runid>/<stepname>
      - a partial hash (in the form of <req id>) -- this assumes the default environment
        for that hash.
      - a full hash (in the form of <req id>:<full id> or <full-id>
      - a name for the environment (as added using `--alias` when resolving the environment
        or using `metaflow environment alias`)
    """
    if into:
        if os.path.exists(into):
            if force:
                shutil.rmtree(into)
            elif not os.path.isdir(into) or len(os.listdir(into)) != 0:
                raise ValueError(
                    "'%s' is not a directory or not empty -- use --force to force"
                    % into
                )
        else:
            os.makedirs(into)
    code_pkg = None
    mf_version = None

    alias_type, resolved_alias = resolve_env_alias(env_name)
    if alias_type == AliasType.PATHSPEC:
        task = Step(resolved_alias).task
        code_pkg = task.code
        mf_version = task.metadata_dict["metaflow_version"]

    env_id_for_alias = cast(Conda, obj.conda).env_id_from_alias(env_name, local_only)
    if env_id_for_alias is None:
        raise ValueError(
            "Environment '%s' does not refer to a known environment" % env_name
        )
    env = cast(Conda, obj.conda).environment(env_id_for_alias, local_only)
    if env is None:
        raise ValueError(
            "Environment '%s' does not exist for this architecture" % (env_name)
        )

    name = name or "metaflowtmp_%s_%s" % (env.env_id.req_id, env.env_id.full_id)

    existing_env = obj.conda.created_environment(name)
    if existing_env:
        if not force:
            raise ValueError(
                "Environment '%s' already exists; use --force to force recreating"
                % name
            )
        obj.conda.remove_for_name(name)

    if into:
        os.chdir(into)
        obj.conda.create_for_name(name, env, do_symlink=True)
        if code_pkg:
            code_pkg.tarball.extractall(path=".")
        elif alias_type == AliasType.PATHSPEC:
            # We get metaflow version and recreate it in the directory
            obj.echo(
                "Step '%s' does not have a code package -- "
                "downloading active Metaflow version only" % env_name
            )
            download_mf_version("./__conda_python", mf_version)
        obj.echo(
            "Code package for %s downloaded into '%s' -- `__conda_python` is "
            "the executable to use" % (env_name, into)
        )
    else:
        obj.conda.create_for_name(name, env)

    obj.echo(
        "Created environment '%s' locally, activate with `%s activate %s`"
        % (name, obj.conda.binary("conda"), name)
    )
    cast(Conda, obj.conda).write_out_environments()


@environment.command()
@click.option(
    "--local-only/--no-local-only",
    show_default=True,
    default=False,
    help="Only resolve source env using local information",
)
@click.argument("source_env")
@click.argument("alias")
@click.pass_obj
def alias(obj, local_only: bool, source_env: str, alias: str):

    env_id_for_alias = cast(Conda, obj.conda).env_id_from_alias(source_env, local_only)
    if env_id_for_alias is None:
        raise ValueError(
            "Environment '%s' does not refer to a known environment" % source_env
        )
    cast(Conda, obj.conda).alias_environment(env_id_for_alias, ["env:%s" % alias])
    cast(Conda, obj.conda).write_out_environments()


@environment.command()
@click.option(
    "--default/--no-default",
    default=True,
    show_default=True,
    help="Set the downloaded environment as default for its requirement ID",
)
@click.option(
    "--arch",
    default=None,
    show_default=True,
    help="Request this architecture -- defaults to current architecture if not specified",
)
@click.argument("source_env")
@click.pass_obj
def get(obj, default: bool, arch: Optional[str], source_env: str):
    env = cast(Conda, obj.conda).environment_from_alias(source_env, arch)
    if env is None:
        raise ValueError(
            "Environment '%s' does not refer to a known environment for %s"
            % (source_env, arch or arch_id())
        )
    if default:
        cast(Conda, obj.conda).set_default_environment(env.env_id)
    cast(Conda, obj.conda).write_out_environments()


# @environment.command(help="List resolved environments for a set of dependencies")
# @click.option(
#     "--local-only/--no-local-only",
#     default=False,
#     show_default=True,
#     help="Only list environments locally known",
# )
# @click.option(
#     "--archs",
#     default=arch_id(),
#     show_default=True,
#     help="Comma separated list of architectures to list environments for",
# )
# @click.pass_obj
# @env_spec_options
# def list(obj, local_only, archs, python, deps, channels):
#     req_id = req_id_from_spec(python, deps, channels)
#     my_arch = arch_id()

#     obj.echo(
#         "Listing environments for python: %s, dependencies: %s and channels: %s "
#         "(requirement hash: %s)"
#         % (python, str(parse_deps(deps)), str(parse_channels(channels)), req_id)
#     )

#     # Get all the environments that we know about
#     envs = []
#     for arch in archs.split(","):
#         envs.extend(obj.conda.environments(req_id, arch, local_only))

#     # Get the local environments so we can say if an environment is present
#     local_instances = local_instances_for_req_id(obj.conda, req_id)

#     if obj.quiet:
#         obj.echo_always(
#             "# req_id full_id arch resolved_on resolved_by resolved_on packages local_instances"
#         )
#         for env_id, env in envs:
#             quiet_print_env(
#                 obj, env, local_instances=local_instances.get(env_id.full_id)
#             )
#     else:
#         # Extract the mapping between default and regular env
#         default_env_id = {}
#         for env_id, env in envs:
#             if env_id.full_id == "_default":
#                 default_env_id[env_id.arch] = env.env_id

#         # Print out the environments
#         for env_id, env in envs:
#             if env_id.full_id == "_default":
#                 # We will print it out as a regular environment
#                 continue
#             pretty_print_env(
#                 obj,
#                 env,
#                 env.env_id == default_env_id.get(env_id.arch),
#                 local_instances=local_instances.get(env_id.full_id),
#             )
#         if len(envs) == 0:
#             if local_only:
#                 obj.echo(
#                     "*No environments exist locally, try without "
#                     "--local-only to search remotely*"
#                 )
#             else:
#                 obj.echo("*No environments exist*")
#         if default_env_id is None:
#             obj.echo("\n\nNo default environment defined -- will resolve when needed")


# @environment.command(help="Resolve an environment")
# @click.option(
#     "--archs",
#     default=arch_id(),
#     show_default=True,
#     help="Co-resolve for all the architectures listed (comma-separated list)",
# )
# @click.option(
#     "--force/--no-force",
#     default=False,
#     show_default=True,
#     help="Force re-resolution even if already resolved",
# )
# @click.option(
#     "--cache/--no-cache",
#     default=True,
#     show_default=True,
#     help="Cache resolved environment and packages if possible",
# )
# @click.pass_obj
# @env_spec_options
# def resolve(obj, archs, force, cache, python, deps, channels):
#     def _inner_resolve(
#         deps: List[str], channels: List[str], arch: str
#     ) -> ResolvedEnvironment:
#         resolved_env = obj.conda.resolve(["<local>"], deps, channels, arch)
#         return resolved_env

#     req_id = req_id_from_spec(python, deps, channels)
#     deps = parse_deps(deps) + ["python==%s" % python]
#     channels = parse_channels(channels)
#     archs = archs.split(",")
#     my_arch = arch_id()
#     if obj.conda.environment(EnvID(req_id=req_id, full_id="_default", arch=my_arch)):
#         if force:
#             obj.echo("Re-resolving environment")
#         else:
#             raise ValueError(
#                 "Environment already exists -- use --force to force resolution"
#             )
#     envs = []  # type: List[ResolvedEnvironment]
#     obj.echo(
#         "Resolving for dependencies: %s and channels: %s on %s (requirement hash: %s)..."
#         % (str(deps), str(channels), str(archs), req_id)
#     )
#     with ThreadPoolExecutor() as executor:
#         resolution_result = [
#             executor.submit(_inner_resolve, deps, channels, arch) for arch in archs
#         ]
#         for f in as_completed(resolution_result):
#             envs.append(f.result())
#     ResolvedEnvironment.set_coresolved_full_id(envs)

#     if cache:
#         obj.echo("Caching environments...")
#         obj.conda.cache_environments(envs)

#     obj.conda.add_environments(envs)
#     # We always resolve for the "_default" environment so update that
#     for env in envs:
#         obj.conda.set_default_environment(env.env_id)
#     obj.conda.write_out_environments()

#     # Get the local environments so we can say if an environment is present
#     local_instances = local_instances_for_req_id(obj.conda, req_id)

#     if obj.quiet:
#         for env in envs:
#             quiet_print_env(
#                 obj, env, local_instances=local_instances.get(env.env_id.full_id)
#             )
#     else:
#         for env in envs:
#             pretty_print_env(
#                 obj,
#                 env,
#                 is_default=True,
#                 local_instances=local_instances.get(env.env_id.full_id),
#             )
