import json
import os
import platform
import re
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
    CONDA_SYS_DEPENDENCIES,
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
    env_type_for_deps,
)
from metaflow_extensions.netflix_ext.plugins.conda.envsresolver import EnvsResolver
from metaflow_extensions.netflix_ext.plugins.conda.utils import (
    AliasType,
    arch_id,
    channel_or_url,
    conda_deps_to_pypi_deps,
    get_sys_packages,
    resolve_env_alias,
    plural_marker,
    merge_dep_dicts,
)

from metaflow._vendor.packaging.requirements import InvalidRequirement, Requirement
from metaflow._vendor.packaging.specifiers import SpecifierSet
from metaflow._vendor.packaging.utils import canonicalize_version

from .utils import download_mf_version


REQ_SPLIT_LINE = re.compile(r"([^~<=>]*)([~<=>]+.*)?")

# Allows things like:
# pkg = <= version
# pkg <= version
# pkg = version
# pkg = ==version or pkg = =version
# In other words, the = is optional but possible
YML_SPLIT_LINE = re.compile(r"(?:=\s)?(<=|>=|~=|==|<|>|=)")


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
@click.option(
    "--quiet-file-output",
    default=None,
    hidden=True,
    type=str,
    help="Output the quiet output to this file; used for scripting",
)
@click.pass_context
def environment(
    ctx: Any,
    quiet: bool,
    metadata: str,
    datastore: str,
    environment: str,
    conda_root: Optional[str],
    quiet_file_output: Optional[str],
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
    obj.quiet_file_output = quiet_file_output

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
        task = Step(resolved_alias, _namespace_check=False).task
        code_pkg = task.code
        mf_version = task.metadata_dict["metaflow_version"]
    else:
        if pathspec:
            raise click.BadOptionUsage(
                "--pathspec not used but environment name is a pathspec"
            )

    env_id_for_alias = cast(Conda, obj.conda).env_id_from_alias(
        env_name, local_only=local_only
    )
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

        # We use envsresolver to properly deal with builder environments and what not
        resolver = EnvsResolver(obj.conda)
        # We force the env_type to be the same as the base env since we don't modify that
        # by adding these deps.
        resolver.add_environment(
            arch_id(),
            user_deps={
                "pypi" if env.env_type == EnvType.PYPI_ONLY else "conda": ["ipykernel"]
            },
            user_sources={},
            extras={},
            base_env=env,
        )
        resolver.resolve_environments(obj.echo)
        update_envs = []  # type: List[ResolvedEnvironment]
        if obj.datastore_type != "local":
            # We may need to update caches
            # Note that it is possible that something we needed to resolve, we don't need
            # to cache (if we resolved to something already cached).
            formats = set()  # type: Set[str]
            for _, resolved_env, f, _ in resolver.need_caching_environments(
                include_builder_envs=True
            ):
                update_envs.append(resolved_env)
                formats.update(f)

            cast(Conda, obj.conda).cache_environments(
                update_envs, {"conda": list(formats)}
            )
        else:
            update_envs = [
                resolved_env
                for _, resolved_env, _ in resolver.new_environments(
                    include_builder_envs=True
                )
            ]
        cast(Conda, obj.conda).add_environments(update_envs)

        # Update the default environment
        for _, resolved_env, _ in resolver.resolved_environments(
            include_builder_envs=True
        ):
            cast(Conda, obj.conda).set_default_environment(resolved_env.env_id)

        cast(Conda, obj.conda).write_out_environments()

        # We are going to be creating this new environment going forward (not the
        # initial env we got)
        _, env, _ = next(resolver.resolved_environments())

        delta_time = int(time.time() - start)
        obj.echo(" done in %d second%s." % (delta_time, plural_marker(delta_time)))

    name = name or "metaflowtmp_%s_%s" % (env.env_id.req_id, env.env_id.full_id)

    if obj.conda.created_environment(name):
        if not force:
            raise CommandException(
                "Environment '%s' already exists; use --force to force recreating"
                % name
            )
        obj.conda.remove_for_name(name)

    python_bin = None  # type: Optional[str]
    if into_dir:
        os.chdir(into_dir)
        python_bin = os.path.join(
            obj.conda.create_for_name(name, env, do_symlink=True), "bin", "python"
        )
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
        python_bin = os.path.join(obj.conda.create_for_name(name, env), "bin", "python")

    if install_notebook:
        start = time.time()
        obj.echo("    Installing Jupyter kernel ...", nl=False)
        try:
            with tempfile.TemporaryDirectory() as d:
                kernel_info = {
                    "argv": [
                        python_bin,
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

    if obj.quiet:
        obj.echo_always(python_bin)
        if obj.quiet_file_output:
            with open(obj.quiet_file_output, "w") as f:
                f.write(python_bin)
                f.write("\n")
    else:
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
    python: Optional[str],
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
    new_pypi_deps = {}  # type: Dict[str, str]
    new_sys_deps = {}  # type: Dict[str, str]
    new_np_conda_deps = {}  # type: Dict[str, str]
    new_sources = {}  # type: Dict[str, List[str]]
    new_extras = {}  # type: Dict[str, List[str]]

    using_str = None  # type: Optional[str]
    if using:
        using_str = using
    if using_pathspec:
        using_str = "step:%s" % using_pathspec

    archs = list(arch) if arch else [arch_id()]
    base_env_id = None
    base_env = None  # type: Optional[ResolvedEnvironment]
    base_env_python = python  # type: Optional[str]
    if using_str:
        # We can pick any of the architecture to get the info about the base environment
        base_env_id = cast(Conda, obj.conda).env_id_from_alias(
            using_str, archs[0], local_only
        )
        if base_env_id is None:
            raise CommandException("No known environment for '%s'" % using_str)

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

        for p in base_env.packages:
            if p.package_name == "python":
                base_env_python = p.package_version
                break
        if base_env_python is None:
            raise InvalidEnvironmentException(
                "Cannot determine python version of base environment"
            )

    # Parse yaml first to put conda sources first to be consistent with step decorator
    parsed_python_version = None
    if yml_file:
        parsed_python_version = _parse_yml_file(
            yml_file,
            new_extras,
            new_sources,
            new_conda_deps,
            new_pypi_deps,
            new_sys_deps,
        )
    if req_file:
        parsed_python_version = _parse_req_file(
            req_file,
            new_extras,
            new_sources,
            new_pypi_deps,
            new_np_conda_deps,
            new_sys_deps,
        )

    if base_env_python:
        if parsed_python_version:
            if using_pathspec is not None or using is not None:
                # Check if the python version matches properly
                if not SpecifierSet(parsed_python_version).contains(base_env_python):
                    raise InvalidEnvironmentException(
                        "The base environment's Python version (%s) does not match the "
                        "one specified in the requirements file (%s)"
                        % (base_env_python, parsed_python_version)
                    )
            else:
                raise InvalidEnvironmentException(
                    "Cannot specify --python if the python dependency is already set "
                    "in the requirements file"
                )
    else:
        base_env_python = parsed_python_version
    if base_env_python is None:
        base_env_python = platform.python_version()

    # Compute the deps
    if len(new_conda_deps) == 0 and (
        not base_env or base_env.env_type == EnvType.PYPI_ONLY
    ):
        # Assume a pypi environment for base deps
        pypi_deps = conda_deps_to_pypi_deps(
            get_pinned_conda_libs(base_env_python, obj.datastore_type)
        )

        conda_deps = {}
    else:
        conda_deps = dict(get_pinned_conda_libs(base_env_python, obj.datastore_type))
        pypi_deps = {}

    pypi_deps = merge_dep_dicts(pypi_deps, new_pypi_deps)
    conda_deps = merge_dep_dicts(conda_deps, new_conda_deps)

    deps = {
        "conda": [
            "%s==%s" % (name, ver) if ver else name for name, ver in conda_deps.items()
        ],
        "pypi": [
            "%s==%s" % (name, canonicalize_version(ver)) if ver else name
            for name, ver in pypi_deps.items()
        ],
        "npconda": [
            "%s==%s" % (name, ver) if ver else name
            for name, ver in new_np_conda_deps.items()
        ],
    }

    env_type = env_type_for_deps(deps)
    if not base_env:
        deps["conda"].append("python==%s" % base_env_python)
    else:
        # We try to see if we can keep the env_type as close as possible to base_env
        if base_env.env_type == EnvType.PYPI_ONLY and len(new_conda_deps) == 0:
            env_type = EnvType.PYPI_ONLY

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

        sys_pkgs = get_sys_packages(
            cast(Conda, obj.conda).virtual_packages, cur_arch, False
        )

        deps["sys"] = [
            "%s==%s" % (name, ver) if ver else name
            for name, ver in new_sys_deps.items()
        ]
        for p, v in sys_pkgs.items():
            if p not in new_sys_deps:
                deps["sys"].append("%s==%s" % (p, v) if p else p)

        # We add the default sources as well -- those sources go last and we convert
        # to simple channels if we can
        # Conda sources are always there since we get the base environment
        sources = {
            "conda": list(
                dict.fromkeys(
                    map(
                        channel_or_url,
                        chain(
                            new_sources.get("conda", []),
                            obj.conda.default_conda_channels,
                        ),
                    )
                )
            )
        }
        if env_type != EnvType.CONDA_ONLY:
            sources["pypi"] = list(
                dict.fromkeys(
                    chain(obj.conda.default_pypi_sources, new_sources.get("pypi", []))
                )
            )

        resolver.add_environment(
            cur_arch,
            deps,
            sources,
            new_extras,
            base_env=base_env,
            base_from_full_id=base_env.is_info_accurate if base_env else False,
            local_only=local_only,
            force=force,
            force_co_resolve=len(archs) > 1,
        )

    has_something = False
    for _ in resolver.non_resolved_environments():
        has_something = True
        break
    if not has_something:
        resolved_env_id = next(resolver.all_environments())[1].env_id
        # Nothing to do
        if alias and not dry_run:
            # We don't care about arch for aliasing so pick one
            obj.echo(
                "No environments to resolve, aliasing only. Use --force to force "
                "re-resolution"
            )
            obj.conda.alias_environment(resolved_env_id, list(alias))
            cast(Conda, obj.conda).write_out_environments()
        else:
            raise CommandException(
                "No environments to resolve, use --force to force re-resolution"
            )
        return

    resolver.resolve_environments(obj.echo)

    resolved_env_id = next(resolver.all_environments())[1].env_id
    existing_envs = cast(Conda, obj.conda).created_environments(resolved_env_id.req_id)

    # Arch doesn't matter for aliasing
    if not dry_run and alias:
        obj.conda.alias_environment(resolved_env_id, list(alias))
    for env_id, env, _ in resolver.resolved_environments():
        if obj.quiet:
            obj.echo_always(env_id.arch)
            obj.echo_always(env.quiet_print(existing_envs.get(env.env_id)))
            if obj.quiet_file_output:
                with open(obj.quiet_file_output, "w") as f:
                    f.write(env_id.arch)
                    f.write("\n")
                    f.write(env.quiet_print(existing_envs.get(env.env_id)))
                    f.write("\n")
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
        for _, resolved_env, f, _ in resolver.need_caching_environments(
            include_builder_envs=True
        ):
            update_envs.append(resolved_env)
            formats.update(f)

        cast(Conda, obj.conda).cache_environments(update_envs, {"conda": list(formats)})
    else:
        update_envs = [
            resolved_env
            for _, resolved_env, _ in resolver.new_environments(
                include_builder_envs=True
            )
        ]

    cast(Conda, obj.conda).add_environments(update_envs)

    # Update the default environment
    if set_default:
        for env_id, resolved_env, _ in resolver.all_environments(
            include_builder_envs=True
        ):
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
@click.option(
    "--pathspec",
    default=False,
    is_flag=True,
    show_default=True,
    help="The environments given are pathspecs",
)
@click.argument("envs", required=True, nargs=-1)
@click.pass_obj
def show(obj, local_only: bool, arch: str, pathspec: bool, envs: Tuple[str]):
    # req-id -> full-id -> List of paths
    all_envs = {}  # Dict[str, Dict[str, List[str]]]
    created_envs = cast(Conda, obj.conda).created_environments()
    for env_id, present_list in created_envs.items():
        all_envs.setdefault(env_id.req_id, {})[env_id.full_id] = [
            os.path.basename(p) for p in present_list
        ]

    for env_name in envs:
        if pathspec:
            env_name = "step:%s" % env_name
        resolved_env = cast(Conda, obj.conda).environment_from_alias(
            env_name, arch, local_only
        )
        if resolved_env is None:
            if obj.quiet:
                obj.echo_always("%s@%s" % (env_name, arch))
                obj.echo_always("NOT_FOUND")
                if obj.quiet_file_output:
                    with open(obj.quiet_file_output, "w") as f:
                        f.write("%s@%s\n" % (env_name, arch))
                        f.write("NOT_FOUND\n")
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
                if obj.quiet_file_output:
                    with open(obj.quiet_file_output, "w") as f:
                        f.write("%s@%s\n" % (env_name, arch))
                        f.write(
                            resolved_env.quiet_print(
                                all_envs.get(resolved_env.env_id.req_id, {}).get(
                                    resolved_env.env_id.full_id
                                )
                            )
                        )
                        f.write("\n")
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
@click.option(
    "--pathspec",
    default=False,
    is_flag=True,
    show_default=True,
    help="The source environment given is a pathspec",
)
@click.argument("source_env")
@click.argument("alias")
@click.pass_obj
def alias(obj, local_only: bool, pathspec: bool, source_env: str, alias: str):
    if pathspec:
        source_env = "step:%s" % source_env
    env_id_for_alias = cast(Conda, obj.conda).env_id_from_alias(
        source_env, local_only=local_only
    )
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
@click.option(
    "--pathspec",
    default=False,
    is_flag=True,
    show_default=True,
    help="The environment name given is a pathspec",
)
@click.argument("source_env")
@click.pass_obj
def get(obj, default: bool, arch: Optional[str], pathspec: bool, source_env: str):
    if pathspec:
        source_env = "step:%s" % source_env
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
        if obj.quiet_file_output:
            with open(obj.quiet_file_output, "w") as f:
                f.write(env.quiet_print(existing_envs))
                f.write("\n")
    else:
        obj.echo(env.pretty_print(existing_envs))
    cast(Conda, obj.conda).write_out_environments()


def _parse_req_file(
    file_name: str,
    extra_args: Dict[str, List[str]],
    sources: Dict[str, List[str]],
    deps: Dict[str, str],
    np_deps: Dict[str, str],
    sys_deps: Dict[str, str],
) -> Optional[str]:
    python_version = None
    with open(file_name, mode="r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            splits = line.split(maxsplit=1)
            first_word = splits[0]
            if len(splits) > 1:
                rem = splits[1]
            else:
                rem = None
            if first_word in ("-i", "--index-url"):
                raise InvalidEnvironmentException(
                    "To specify a base PYPI index, set `METAFLOW_CONDA_DEFAULT_PYPI_SOURCE; "
                    "you can specify additional indices using --extra-index-url"
                )
            elif first_word == "--extra-index-url" and rem:
                sources.setdefault("pypi", []).append(rem)
            elif first_word in ("-f", "--find-links", "--trusted-host") and rem:
                extra_args.setdefault("pypi", []).append(" ".join([first_word, rem]))
            elif first_word in ("--pre", "--no-index"):
                extra_args.setdefault("pypi", []).append(first_word)
            elif first_word == "--conda-pkg":
                # Special extension to allow non-python conda package specification
                split_res = REQ_SPLIT_LINE.match(splits[1])
                if split_res is None:
                    raise InvalidEnvironmentException(
                        "Could not parse conda package '%s'" % splits[1]
                    )
                s = split_res.groups()
                if s[1] is None:
                    np_deps[s[0].replace(" ", "")] = ""
                else:
                    np_deps[s[0].replace(" ", "")] = s[1].replace(" ", "").lstrip("=")
            elif first_word == "--sys-pkg":
                # Special extension to allow the specification of system dependencies
                # (currently __cuda and __glibc)
                split_res = REQ_SPLIT_LINE.match(splits[1])
                if split_res is None:
                    raise InvalidEnvironmentException(
                        "Could not parse system package '%s'" % splits[1]
                    )
                s = split_res.groups()
                pkg_name = s[0].replace(" ", "")
                if pkg_name not in CONDA_SYS_DEPENDENCIES:
                    raise InvalidEnvironmentException(
                        "System package '%s' not allowed. Values allowed are: %s"
                        % (pkg_name, str(CONDA_SYS_DEPENDENCIES))
                    )
                if s[1] is None:
                    raise InvalidEnvironmentException(
                        "System package '%s' requires a version" % pkg_name
                    )
                sys_deps[pkg_name] = s[1].replace(" ", "").lstrip("=")
            elif first_word.startswith("#"):
                continue
            elif first_word.startswith("-"):
                raise InvalidEnvironmentException(
                    "'%s' is not a supported line in a requirements.txt" % line
                )
            else:
                try:
                    parsed_req = Requirement(line)
                except InvalidRequirement as ex:
                    raise InvalidEnvironmentException(
                        "Could not parse '%s'" % line
                    ) from ex
                if parsed_req.marker is not None:
                    raise InvalidEnvironmentException(
                        "Environment markers are not supported for '%s'" % line
                    )
                dep_name = parsed_req.name
                if parsed_req.extras:
                    dep_name += "[%s]" % ",".join(parsed_req.extras)
                if parsed_req.url:
                    dep_name += "@%s" % parsed_req.url
                specifier = str(parsed_req.specifier).lstrip(" =")
                if dep_name == "python":
                    if specifier:
                        python_version = specifier
                else:
                    deps[dep_name] = specifier
    return python_version


def _parse_yml_file(
    file_name: str,
    _: Dict[str, List[str]],
    sources: Dict[str, List[str]],
    conda_deps: Dict[str, str],
    pypi_deps: Dict[str, str],
    sys_deps: Dict[str, str],
) -> Optional[str]:
    python_version = None  # type: Optional[str]
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
                elif line == "pypi-indices:":
                    mode = "pypi_sources"
                else:
                    mode = "ignore"
            elif mode and mode.endswith("sources"):
                line = line.lstrip(" -").rstrip()
                sources.setdefault("conda" if mode == "sources" else "pypi", []).append(
                    line
                )
            elif mode and mode.endswith("deps"):
                line = line.lstrip(" -").rstrip()
                if line == "pip:":
                    mode = "pypi_deps"
                elif line == "sys:":
                    mode = "sys_deps"
                else:
                    to_update = (
                        conda_deps
                        if mode == "deps"
                        else pypi_deps
                        if mode == "pypi_deps"
                        else sys_deps
                    )
                    splits = YML_SPLIT_LINE.split(line.replace(" ", ""), maxsplit=1)
                    if len(splits) == 1:
                        if splits[0] != "python":
                            if mode == "sys_deps":
                                raise InvalidEnvironmentException(
                                    "System package '%s' requires a version" % splits[0]
                                )
                            to_update[splits[0]] = ""
                    else:
                        dep_name, dep_operator, dep_version = splits
                        if dep_operator not in ("=", "=="):
                            if mode == "sys_deps":
                                raise InvalidEnvironmentException(
                                    "System package '%s' requires a specific version not '%s'"
                                    % (splits[0], dep_operator + dep_version)
                                )
                            dep_version = dep_operator + dep_version
                        if dep_name == "python":
                            if dep_version:
                                if python_version:
                                    raise InvalidEnvironmentException(
                                        "Python versions specified multiple times in "
                                        "the YAML file."
                                    )
                                python_version = dep_version
                        else:
                            if (
                                dep_name.startswith("/")
                                or dep_name.startswith("git+")
                                or dep_name.startswith("https://")
                                or dep_name.startswith("ssh://")
                            ):
                                # Handle the case where only the URL is specified
                                # without a package name
                                depname_and_maybe_tag = dep_name.split("/")[-1]
                                depname = depname_and_maybe_tag.split("@")[0]
                                if depname.endswith(".git"):
                                    depname = depname[:-4]
                                dep_name = "%s@%s" % (depname, dep_name)

                            if (
                                mode == "sys_deps"
                                and dep_name not in CONDA_SYS_DEPENDENCIES
                            ):
                                raise InvalidEnvironmentException(
                                    "System package '%s' not allowed. Values allowed are: %s"
                                    % (dep_name, str(CONDA_SYS_DEPENDENCIES))
                                )
                            to_update[dep_name] = dep_version

    return python_version


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
#
#     obj.echo(
#         "Listing environments for python: %s, dependencies: %s and channels: %s "
#         "(requirement hash: %s)"
#         % (python, str(parse_deps(deps)), str(parse_channels(channels)), req_id)
#     )
#
#     # Get all the environments that we know about
#     envs = []
#     for arch in archs.split(","):
#         envs.extend(obj.conda.environments(req_id, arch, local_only))
#
#     # Get the local environments so we can say if an environment is present
#     local_instances = local_instances_for_req_id(obj.conda, req_id)
#
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
#
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
