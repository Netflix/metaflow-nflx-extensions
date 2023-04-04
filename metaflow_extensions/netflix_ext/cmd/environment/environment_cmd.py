import json
import os
import platform
import shutil
import subprocess
import tempfile
import time

from functools import wraps
from itertools import chain
from shutil import copy
from typing import Any, Dict, List, Optional, Set, Tuple, cast
from metaflow._vendor import click

from metaflow.cli import echo_dev_null, echo_always


from metaflow import Step, metaflow_config
from metaflow.exception import CommandException
from metaflow.plugins import DATASTORES
from metaflow.metaflow_config import (
    CONDA_PREFERRED_FORMAT,
    DEFAULT_DATASTORE,
    DEFAULT_METADATA,
    get_pinned_conda_libs,
)
from metaflow.plugins import METADATA_PROVIDERS
from metaflow_extensions.netflix_ext.plugins.conda.conda import (
    Conda,
    InvalidEnvironmentException,
)
from metaflow_extensions.netflix_ext.plugins.conda.env_descr import (
    EnvID,
    EnvType,
    ResolvedEnvironment,
    TStr,
)
from metaflow_extensions.netflix_ext.plugins.conda.envsresolver import EnvsResolver
from metaflow_extensions.netflix_ext.plugins.conda.utils import (
    AliasType,
    arch_id,
    resolve_env_alias,
    plural_marker,
)

from .utils import (
    download_mf_version,
)


class CommandObj:
    def __init__(self):
        pass


def env_spec_options(func):
    @click.option(
        "--arch",
        default=None,
        multiple=True,
        help="Architecture to resolve for. Can be specified multiple times. "
        "If not specified, defaults to the architecture for the step",
        required=False,
    )
    @click.option(
        "--python",
        default=None,
        show_default=True,
        help="Python version for the environment -- defaults to current version",
    )
    @click.option(
        "-r",
        "--requirement",
        "req_file",
        default=None,
        type=click.Path(exists=True, readable=True, dir_okay=False, resolve_path=True),
        help="Get the environment definition from a requirements.txt file (pip format).",
    )
    @click.option(
        "-f",
        "--file",
        "yml_file",
        default=None,
        type=click.Path(exists=True, readable=True, dir_okay=False, resolve_path=True),
        help="Get the environment defintion from a environment.yml file (conda-lock format).",
    )
    @click.option(
        "--using-pathspec",
        default=None,
        help="Environment created starting with the environment for the Pathspec specified",
    )
    @click.option(
        "--using",
        default=None,
        help="Environment created starting with the environment referenced here",
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
    help="Root path for Conda cached information. If not set, "
    "looks for METAFLOW_CONDA_S3ROOT (for S3)",
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
    "--local-only/--non-local",
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
    "--into-dir",
    default=None,
    show_default=False,
    type=click.Path(file_okay=False, writable=True, readable=True, resolve_path=True),
    help="If the `envspec` refers to a Metaflow executed Step, downloads the step's "
    "code package into this directory",
)
@click.option(
    "--install-notebook/--no-install-notebook",
    default=False,
    show_default=True,
    help="Install the created environment as a Jupyter kernel -- requires --name.",
)
@click.option(
    "--pathspec",
    default=False,
    is_flag=True,
    show_default=True,
    help="The environment name given is a pathspec",
)
@click.argument("env-name")
@click.pass_obj
def create(
    obj,
    name: Optional[str],
    local_only: bool,
    force: bool,
    into_dir: Optional[str],
    install_notebook: bool,
    pathspec: bool,
    env_name: str,
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
    if into_dir:
        into_dir = os.path.abspath(into_dir)
        if os.path.exists(into_dir):
            if force:
                shutil.rmtree(into_dir)
                os.makedirs(into_dir)
            elif not os.path.isdir(into_dir) or len(os.listdir(into_dir)) != 0:
                raise CommandException(
                    "'%s' is not a directory or not empty -- use --force to force"
                    % into_dir
                )
        else:
            os.makedirs(into_dir)
    if install_notebook and name is None:
        raise click.BadOptionUsage("--install-notebook requires --name")

    code_pkg = None
    mf_version = None

    if pathspec:
        env_name = "step:%s" % env_name

    alias_type, resolved_alias = resolve_env_alias(env_name)
    if alias_type == AliasType.PATHSPEC:
        if not pathspec:
            raise click.BadOptionUsage(
                "--pathspec used but environment name is not a pathspec"
            )
        task = Step(resolved_alias).task
        code_pkg = task.code
        mf_version = task.metadata_dict["metaflow_version"]
    else:
        if pathspec:
            raise click.BadOptionUsage(
                "--pathspec not used but environment name is a pathspec"
            )

    env_id_for_alias = cast(Conda, obj.conda).env_id_from_alias(env_name, local_only)
    if env_id_for_alias is None:
        raise CommandException(
            "Environment '%s' does not refer to a known environment" % env_name
        )
    env = cast(Conda, obj.conda).environment(env_id_for_alias, local_only)
    if env is None:
        raise CommandException(
            "Environment '%s' does not exist for this architecture" % (env_name)
        )

    if install_notebook:
        start = time.time()
        # We need to install ipykernel into the resolved environment
        obj.echo("    Resolving an environment compatible with Jupyter ...", nl=False)
        # We don't pin to avoid issues with python versions -- it will take the
        # best one it can.
        env = cast(Conda, obj.conda).add_to_resolved_env(
            env,
            using_steps=["ipykernel"],
            deps=[TStr(category="conda", value="ipykernel")],
            sources=[],
            architecture=arch_id(),
        )
        # Cache this information for the next time around
        cast(Conda, obj.conda).cache_environments([env])
        cast(Conda, obj.conda).add_environments([env])
        cast(Conda, obj.conda).set_default_environment(env.env_id)
        delta_time = int(time.time() - start)
        obj.echo(" done in %d second%s." % (delta_time, plural_marker(delta_time)))

    name = name or "metaflowtmp_%s_%s" % (env.env_id.req_id, env.env_id.full_id)

    existing_env = obj.conda.created_environment(name)
    if existing_env:
        if not force:
            raise CommandException(
                "Environment '%s' already exists; use --force to force recreating"
                % name
            )
        obj.conda.remove_for_name(name)

    if into_dir:
        os.chdir(into_dir)
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
            "the executable to use" % (env_name, into_dir)
        )
    else:
        obj.conda.create_for_name(name, env)

    if install_notebook:
        start = time.time()
        obj.echo("    Installing Jupyter kernel ...", nl=False)
        try:
            with tempfile.TemporaryDirectory() as d:
                kernel_info = {
                    "argv": [
                        obj.conda.python(name),
                        "-m",
                        "ipykernel_launcher",
                        "-f",
                        "{connection_file}",
                    ],
                    "display_name": "Metaflow %s" % resolved_alias,
                    "language": "python",
                    "metadata": {"debugger": True},
                }
                if into_dir:
                    kernel_info["env"] = {"PYTHONPATH": into_dir}
                images_dir = os.path.abspath(
                    os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        "..",
                        "..",
                        "plugins",
                        "conda",
                        "resources",
                    )
                )
                for image in ("logo-32x32.png", "logo-64x64.png", "logo-svg.svg"):
                    copy(os.path.join(images_dir, image), os.path.join(d, image))
                with open(
                    os.path.join(d, "kernel.json"), mode="w", encoding="utf-8"
                ) as f:
                    json.dump(kernel_info, f)

                # Kernel name
                translation_table = str.maketrans(":/.-", "____")
                kernel_name = "mf_%s" % resolved_alias.translate(translation_table)
                try:
                    # Clean up in case it already exists -- ignore failures if it
                    # doesn't
                    subprocess.check_call(
                        ["jupyter", "kernelspec", "uninstall", "-y", kernel_name],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                except subprocess.CalledProcessError:
                    pass
                subprocess.check_call(
                    ["jupyter", "kernelspec", "install", "--name", kernel_name, d],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
        except Exception as e:
            obj.echo("Error installing into Jupyter: %s" % e)
        else:
            delta_time = int(time.time() - start)
            obj.echo(" done in %d second%s." % (delta_time, plural_marker(delta_time)))

    obj.echo(
        "Created environment '%s' locally, activate with `%s activate %s`"
        % (name, obj.conda.binary("conda"), name)
    )
    cast(Conda, obj.conda).write_out_environments()


@environment.command(help="Resolve an environment")
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
    help="Force resolution of already resolved environments",
)
@click.option(
    "--local-only/--non-local",
    show_default=True,
    default=False,
    help="Only look locally for using environments",
)
@click.option(
    "--dry-run/--no-dry-run",
    show_default=True,
    default=False,
    help="Dry-run -- only resolve, do not download, cache, persist or alias anything",
)
@click.option(
    "--set-default/--no-set-default",
    show_default=True,
    default=True,
    help="Set the resolved environment as the default environment for this set of requirements",
)
@env_spec_options
@click.pass_obj
def resolve(
    obj: Any,
    alias: Optional[Tuple[str]],
    force: bool,
    local_only: bool,
    dry_run: bool,
    set_default: bool,
    arch: Optional[Tuple[str]],
    python: str,
    req_file: Optional[str],
    yml_file: Optional[str],
    using_pathspec: Optional[str],
    using: Optional[str],
):
    # Check combinations -- TODO: we could leverage a click add-on to do this
    if req_file is not None and yml_file is not None:
        raise click.BadOptionUsage("-r/-f", "Can only specify one of -r or -f")
    if (using_pathspec is not None or using is not None) and python is not None:
        raise click.BadOptionUsage(
            "--python",
            "Cannot specify a Python version if using --using-pathspec or --using",
        )
    resolver = EnvsResolver(obj.conda)

    new_conda_deps = {}  # type: Dict[str, str]
    new_pip_deps = {}  # type: Dict[str, str]
    new_sources = []  # type: List[TStr]

    using_str = None  # type: Optional[str]
    if using:
        using_str = using
    if using_pathspec:
        using_str = "step:%s" % using_pathspec

    archs = list(arch) if arch else [arch_id()]
    base_env_id = None
    base_env_conda_deps = {}  # type: Dict[str, str]
    base_env_pip_deps = {}  # type: Dict[str, str]
    base_env_conda_channels = []  # type: List[str]
    base_env_pip_sources = []  # type: List[str]
    base_env_python = python
    base_env = None  # type: Optional[ResolvedEnvironment]
    if using_str:
        base_env_id = cast(Conda, obj.conda).env_id_from_alias(using_str, local_only)
        if base_env_id is None:
            raise CommandException("No known environment for '%s'" % using_str)

        # We can pick any of the architecture to get the info about the base environment
        base_env = cast(Conda, obj.conda).environment(
            EnvID(
                req_id=base_env_id.req_id,
                full_id=base_env_id.full_id,
                arch=archs[0],
            ),
            local_only,
        )
        if base_env is None:
            raise CommandException(
                "Environment for '%s' is not available on architecture '%s'"
                % (using_str, archs[0])
            )
        # TODO: This code is duplicated in the conda_step_decorator.py
        # Take care of dependencies first
        all_deps = base_env.deps
        for d in all_deps:
            vals = d.value.split("==")
            if len(vals) == 1:
                vals.append("")
            if d.category == "pip":
                base_env_pip_deps[vals[0]] = vals[1]
            elif d.category == "conda":
                if vals[0] == "python":
                    base_env_python = vals[1]
                else:
                    # We will re-add python later
                    base_env_conda_deps[vals[0]] = vals[1]

        # Now of channels/sources
        all_sources = base_env.sources
        base_env_conda_channels.extend(
            [s.value for s in all_sources if s.category == "conda"]
        )
        base_env_pip_sources.extend(
            [s.value for s in all_sources if s.category == "pip"]
        )

    # Parse yaml first to put conda sources first to be consistent with step decorator
    if yml_file:
        _parse_yml_file(yml_file, new_sources, new_conda_deps, new_pip_deps)
    if req_file:
        _parse_req_file(req_file, new_sources, new_pip_deps)

    if base_env_python is None:
        base_env_python = platform.python_version()

    # Compute the deps
    if len(new_conda_deps) == 0 and (
        not base_env or base_env.env_type == EnvType.PIP_ONLY
    ):
        # Assume a pip environment for base deps
        pip_deps = get_pinned_conda_libs(base_env_python, obj.datastore_type)
        conda_deps = {}
    else:
        conda_deps = get_pinned_conda_libs(base_env_python, obj.datastore_type)
        pip_deps = {}

    pip_deps.update(base_env_pip_deps)
    pip_deps.update(new_pip_deps)

    conda_deps.update(base_env_conda_deps)
    conda_deps.update(new_conda_deps)

    # Compute the sources
    seen = set()
    sources = []  # type: List[str]
    for c in chain(base_env_conda_channels, base_env_pip_sources, new_sources):
        if c in seen:
            continue
        seen.add(c)
        sources.append(c)

    deps = list(
        chain(
            [TStr("conda", "python==%s" % base_env_python)],
            (
                TStr("conda", "%s==%s" % (name, ver) if ver else name)
                for name, ver in conda_deps.items()
            ),
            (
                TStr("pip", "%s==%s" % (name, ver) if ver else name)
                for name, ver in pip_deps.items()
            ),
        )
    )

    requested_req_id = ResolvedEnvironment.get_req_id(deps, sources)

    for cur_arch in archs:
        if base_env_id:
            base_env = cast(Conda, obj.conda).environment(
                EnvID(
                    req_id=base_env_id.req_id,
                    full_id=base_env_id.full_id,
                    arch=cur_arch,
                ),
                local_only,
            )
            if base_env is None:
                raise CommandException(
                    "Environment for '%s' is not available on architecture '%s'"
                    % (using_str, cur_arch)
                )
        resolver.add_environment(
            EnvID(requested_req_id, "_default", cur_arch),
            deps,
            sources,
            base_env,
            local_only=local_only,
            force=force,
            force_co_resolve=len(archs) > 1,
        )

    has_something = False
    for _ in resolver.non_resolved_environments():
        has_something = True
        break
    if not has_something:
        # Nothing to do
        if alias and not dry_run:
            # We don't care about arch for aliasing so pick one
            obj.echo(
                "Not environments to resolve, aliasing only. Use --force to force "
                "re-resolution"
            )
            obj.conda.alias_environment(
                EnvID(
                    requested_req_id,
                    next(resolver.all_environments())[1].env_id.full_id,
                    arch=arch_id(),
                ),
                list(alias),
            )
            cast(Conda, obj.conda).write_out_environments()
        else:
            obj.echo("No environments to resolve, use --force to force re-resolution")
        return

    resolver.resolve_environments(obj.echo)
    existing_envs = cast(Conda, obj.conda).created_environments(requested_req_id)

    # Arch doesn't matter for aliasing
    if not dry_run and alias:
        obj.conda.alias_environment(
            EnvID(
                requested_req_id,
                next(resolver.resolved_environments())[1].env_id.full_id,
                arch=arch_id(),
            ),
            list(alias),
        )
    for env_id, env, _ in resolver.resolved_environments():
        if obj.quiet:
            obj.echo_always(env_id.arch)
            obj.echo_always(env.quiet_print(existing_envs.get(env.env_id)))
        else:
            obj.echo("### Environment for architecture %s" % env_id.arch)
            obj.echo(env.pretty_print(existing_envs.get(env.env_id)))

    if dry_run:
        obj.echo("Dry-run -- not caching or aliasing")
        return

    # If this is not a dry-run, we cache the environments and write out the resolved
    # information
    update_envs = []  # type: List[ResolvedEnvironment]
    if obj.datastore_type != "local":
        # We may need to update caches
        # Note that it is possible that something we needed to resolve, we don't need
        # to cache (if we resolved to something already cached).
        formats = set()  # type: Set[str]
        for _, resolved_env, f, _ in resolver.need_caching_environments():
            update_envs.append(resolved_env)
            formats.update(f)

        cast(Conda, obj.conda).cache_environments(update_envs, {"conda": list(formats)})
    else:
        update_envs = [
            resolved_env for _, resolved_env, _ in resolver.new_environments()
        ]

    cast(Conda, obj.conda).add_environments(update_envs)

    # Update the default environment
    if set_default:
        for env_id, resolved_env, _ in resolver.all_environments():
            obj.conda.set_default_environment(resolved_env.env_id)

    # We are done -- write back out the environments
    cast(Conda, obj.conda).write_out_environments()


@environment.command(help="Show information about an environment")
@click.option(
    "--local-only/--non-local",
    show_default=True,
    default=False,
    help="Only resolve source env using local information",
)
@click.option(
    "--arch",
    show_default=True,
    default=arch_id(),
    help="Show environment for this architecture",
)
@click.argument("envs", required=True, nargs=-1)
@click.pass_obj
def show(obj, local_only: bool, arch: str, envs: Tuple[str]):
    # req-id -> full-id -> List of paths
    all_envs = {}  # Dict[str, Dict[str, List[str]]]
    created_envs = cast(Conda, obj.conda).created_environments()
    for env_id, present_list in created_envs.items():
        all_envs.setdefault(env_id.req_id, {})[env_id.full_id] = [
            os.path.basename(p) for p in present_list
        ]

    for env_name in envs:
        resolved_env = cast(Conda, obj.conda).environment_from_alias(
            env_name, arch, local_only
        )
        if resolved_env is None:
            if obj.quiet:
                obj.echo_always("%s@%s" % (env_name, arch))
                obj.echo_always("NOT_FOUND")
            else:
                obj.echo(
                    "### Environment for '%s' (arch '%s') was not found"
                    % (env_name, arch)
                )
                obj.echo("")
        else:
            if obj.quiet:
                obj.echo_always("%s@%s" % (env_name, arch))
                obj.echo_always(
                    resolved_env.quiet_print(
                        all_envs.get(resolved_env.env_id.req_id, {}).get(
                            resolved_env.env_id.full_id
                        )
                    )
                )
            else:
                obj.echo("### Environment for '%s' (arch '%s'):" % (env_name, arch))
                obj.echo(
                    resolved_env.pretty_print(
                        all_envs.get(resolved_env.env_id.req_id, {}).get(
                            resolved_env.env_id.full_id
                        )
                    )
                )


@environment.command(help="Alias an existing environment")
@click.option(
    "--local-only/--non-local",
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
        raise CommandException(
            "Environment '%s' does not refer to a known environment" % source_env
        )
    cast(Conda, obj.conda).alias_environment(env_id_for_alias, [alias])
    cast(Conda, obj.conda).write_out_environments()


@environment.command(
    help="Locally fetch an environment already resolved and present remotely"
)
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
        raise CommandException(
            "Environment '%s' does not refer to a known environment for %s"
            % (source_env, arch or arch_id())
        )
    if default:
        cast(Conda, obj.conda).set_default_environment(env.env_id)
    existing_envs_dict = cast(Conda, obj.conda).created_environments(env.env_id.req_id)
    if existing_envs_dict:
        existing_envs = existing_envs_dict.get(env.env_id, [])
    else:
        existing_envs = []
    if obj.quiet:
        obj.echo_always(env.quiet_print(existing_envs))
    else:
        obj.echo(env.pretty_print(existing_envs))
    cast(Conda, obj.conda).write_out_environments()


def _parse_req_file(file_name: str, sources: List[TStr], deps: Dict[str, str]):
    with open(file_name, mode="r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            elif line.startswith("-i") or line.startswith("--index-url"):
                sources.append(
                    TStr(category="pip", value=line.split(" ", 1)[1].strip())
                )
            elif line.startswith("#"):
                continue
            elif line.startswith("-"):
                raise InvalidEnvironmentException(
                    "'%s' is not a supported line in a requirements.txt" % line
                )
            else:
                splits = line.split("=", 1)
                if len(splits) == 1:
                    deps[line.strip()] = ""
                else:
                    deps[splits[0].strip()] = splits[1].lstrip(" =").rstrip()


def _parse_yml_file(
    file_name: str,
    sources: List[TStr],
    conda_deps: Dict[str, str],
    pip_deps: Dict[str, str],
):
    with open(file_name, mode="r", encoding="utf-8") as f:
        # Very poor man's yaml parsing
        mode = None
        for line in f:
            if not line:
                continue
            elif line[0] not in (" ", "-"):
                line = line.strip()
                if line == "channels:":
                    mode = "sources"
                elif line == "dependencies:":
                    mode = "deps"
                else:
                    mode = "ignore"
            elif mode == "sources":
                line = line.lstrip(" -").rstrip()
                sources.append(TStr(category="conda", value=line))
            elif mode == "deps" or mode == "pip_deps":
                line = line.lstrip(" -").rstrip()
                if line == "pip:":
                    mode = "pip_deps"
                else:
                    to_update = conda_deps if mode == "deps" else pip_deps
                    splits = line.split("=", 1)
                    if len(splits) == 1:
                        to_update[line] = ""
                    else:
                        to_update[splits[0].strip()] = splits[1].lstrip(" =")


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
