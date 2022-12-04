from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import platform

from functools import wraps
from threading import local
from typing import Any, Dict, List, Optional
from metaflow._vendor import click

from metaflow.cli import echo_dev_null, echo_always


from metaflow import Step, metaflow_config, namespace
from metaflow.plugins import DATASTORES
from metaflow.metaflow_config import DEFAULT_DATASTORE, DEFAULT_METADATA
from metaflow.plugins import METADATA_PROVIDERS
from metaflow_extensions.netflix_ext.plugins.conda.conda import Conda
from metaflow_extensions.netflix_ext.plugins.conda.env_descr import (
    EnvID,
    ResolvedEnvironment,
)
from metaflow_extensions.netflix_ext.plugins.conda.utils import arch_id

from .utils import (
    env_id_from_step,
    download_mf_version,
    local_instances_for_req_id,
    parse_channels,
    parse_deps,
    pretty_print_env,
    quiet_print_env,
    req_id_from_spec,
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
    "--quiet/--not-quiet",
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
    help="Data backend type",
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
    "--conda-root", default=None, help="Root path for Conda cached information"
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
    global echo
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
        metaflow_config.CONDA_S3ROOT = conda_root
    obj.conda_root = metaflow_config.CONDA_S3ROOT

    obj.metadata_type = metadata

    obj.conda = Conda(obj.echo, obj.datastore_type)
    ctx.obj = obj


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


@environment.command()
@click.argument(
    "envspec",
    nargs=-1,
)
@click.option("--name", default=None, help="Name of the environment to create")
@click.option(
    "--recreate/--no-recreate",
    default=False,
    show_default=True,
    help="Recreate the environment if it already exists",
)
@click.option(
    "--into",
    default=None,
    show_default=False,
    type=click.Path(file_okay=False, writable=True, readable=True, resolve_path=True),
    help="If specified and for pathspec ENVSPEC, downloads the code to this directory",
)
@click.pass_obj
def create_local(obj, name, recreate, into, envspec):
    """
    Create a local environment based on ENVSPEC.

    ENVSPEC can either be:
      - a pathspec in the form <flowname>/<runid>/<stepname>
      - two values: the requirement hash and the full hash of the environment.

    The latter two values are returned for example when using `list` for example
    """
    if into:
        if os.path.exists(into):
            if len(os.listdir(into)) != 0:
                raise ValueError("Directory '%s' is not empty" % into)
        else:
            os.makedirs(into)
    code_pkg = None
    if len(envspec) == 1:
        # This should be a pathspec
        if len(envspec[0].split("/")) != 3:
            raise ValueError(
                "Pathspec should be in the form <flowname>/<runid>/<stepname>"
            )
        namespace(None)
        step = Step(
            envspec[0]
        )  # Will raise MetaflowNotFound as expected if it doesn't exist
        env_id = env_id_from_step(step)
        code_pkg = step.task.code
        mf_version = step.task.metadata_dict["metaflow_version"]
    elif len(envspec) == 2:
        # This should be req_id and full_id
        req_id, full_id = envspec
        env_id = EnvID(req_id=req_id, full_id=full_id, arch=arch_id())
        if into:
            obj.echo("Ignoring --into option since a pathspec was not given")
            into = None
    else:
        raise ValueError("envspec not in the expected format")

    env = obj.conda.environment(env_id, local_only=True)
    if env is None:
        env = obj.conda.environment(env_id)
        # We are going to cache locally as well
        if env:
            obj.conda.write_out_environments()
    if env is None:
        raise ValueError(
            "Environment for requirement hash %s (full hash %s) does not exist for "
            "this architecture" % (env_id.req_id, env_id.full_id)
        )
    name = name or "metaflowtmp_%s_%s" % (env_id.req_id, env_id.full_id)

    existing_env = obj.conda.created_environment(name)
    if existing_env:
        if not recreate:
            raise ValueError(
                "Environment '%s' already exists; use --recreate to force recreating"
                % name
            )
        obj.conda.remove_for_name(name)

    if into:
        os.chdir(into)
        obj.conda.create_for_name(name, env, do_symlink=True)
        if code_pkg:
            code_pkg.tarball.extractall(path=".")
        else:
            # We get metaflow version and recreate it in the directory
            obj.echo(
                "Step '%s' does not have a code package -- "
                "downloading active Metaflow version only" % envspec[0]
            )
            download_mf_version("./__conda_python", mf_version)
        obj.echo(
            "Code package for %s downloaded into '%s' -- `__conda_python` is "
            "the executable to use" % (envspec[0], into)
        )
    else:
        obj.conda.create_for_name(name, env)

    obj.echo(
        "Created environment '%s' locally, activate with `%s activate %s`"
        % (name, obj.conda.binary("conda"), name)
    )
