"""Requirements that apply only when a native PyPI package is selected.

Discovery and enforcement live above the individual solvers so mixed, pip,
and locked environments follow the same policy. Unrelated environments keep
their original requests and cache identities.
"""

from typing import Dict, List, Sequence

from metaflow._vendor.packaging.markers import default_environment
from metaflow._vendor.packaging.requirements import InvalidRequirement, Requirement
from metaflow._vendor.packaging.specifiers import SpecifierSet
from metaflow._vendor.packaging.utils import canonicalize_name

from .env_descr import ResolvedEnvironment
from .utils import CondaException, clean_up_double_equal, compute_target_envs


def pypi_requirement(value: str) -> Requirement:
    try:
        return Requirement(value)
    except InvalidRequirement:
        return Requirement(clean_up_double_equal([value])[0])


def conditional_requirements(
    environment: ResolvedEnvironment, datastore_type: str
) -> List[Requirement]:
    """Collect bundles whose trigger is in the selected native PyPI graph.

    Config hooks return {trigger_name: {required_name: version_specifier}}.
    Names are plain distribution names, and values are PEP 440 specifiers.
    The trigger may constrain itself and require companion packages.
    """
    from metaflow.extension_support import get_modules

    packages = list(environment.packages)
    native_names = {
        canonicalize_name(p.package_name) for p in packages if p.TYPE == "pypi"
    }
    if not native_names:
        return []
    python_version = next(
        (
            p.package_version
            for p in packages
            if p.TYPE == "conda" and p.package_name == "python"
        ),
        None,
    )
    requirements = {}
    for extension in get_modules("config"):
        hook = getattr(extension.module, "get_conditional_pypi_libs", None)
        if hook is None:
            continue
        for trigger, bundle in hook(python_version, datastore_type).items():
            trigger_req = Requirement(trigger)
            if str(trigger_req) != trigger_req.name:
                raise CondaException(
                    "Conditional PyPI triggers must be plain package names"
                )
            if canonicalize_name(trigger_req.name) not in native_names:
                continue
            for name, specifier in bundle.items():
                req = Requirement(name)
                if str(req) != req.name:
                    raise CondaException(
                        "Conditional PyPI requirements must use plain package names"
                    )
                req.name = canonicalize_name(req.name)
                req.specifier = SpecifierSet(specifier)
                requirements[str(req)] = req
    return list(requirements.values())


def unsatisfied_requirements(
    environment: ResolvedEnvironment, requirements: Sequence[Requirement]
) -> List[Requirement]:
    versions = {
        canonicalize_name(p.package_name): p.package_version
        for p in environment.packages
        if p.TYPE == "pypi"
    }
    return [
        req
        for req in requirements
        if canonicalize_name(req.name) not in versions
        or not req.specifier.contains(
            versions[canonicalize_name(req.name)], prereleases=True
        )
    ]


def constrained_dependencies(
    deps: Dict[str, List[str]],
    requirements: Sequence[Requirement],
    environment: ResolvedEnvironment,
) -> Dict[str, List[str]]:
    """Intersect active requirements with user pins, never overwrite them."""
    packages = list(environment.packages)
    python_version = next(
        p.package_version
        for p in packages
        if p.TYPE == "conda" and p.package_name == "python"
    )
    target = default_environment()
    target_systems = {
        "linux-64": ("linux", "Linux", "x86_64"),
        "linux-aarch64": ("linux", "Linux", "aarch64"),
        "osx-64": ("darwin", "Darwin", "x86_64"),
        "osx-arm64": ("darwin", "Darwin", "arm64"),
    }
    if environment.env_id.arch in target_systems:
        platform, system, machine = target_systems[environment.env_id.arch]
        target.update(
            sys_platform=platform,
            platform_system=system,
            platform_machine=machine,
            os_name="posix",
        )
    target = compute_target_envs(target, [python_version], environment.env_id.arch)[0]
    names = {canonicalize_name(req.name) for req in requirements}
    grouped = {name: [] for name in names}
    untouched = []
    for value in deps.get("pypi", []):
        req = pypi_requirement(value)
        name = canonicalize_name(req.name)
        if name not in names:
            untouched.append(value)
        elif req.marker is None or req.marker.evaluate(target):
            grouped[name].append(req)
    for req in requirements:
        grouped[canonicalize_name(req.name)].append(req)
    for name, reqs in sorted(grouped.items()):
        extras = sorted({extra for req in reqs for extra in req.extras})
        package = name + ("[%s]" % ",".join(extras) if extras else "")
        urls = {req.url for req in reqs if req.url}
        if len(urls) > 1:
            raise CondaException(
                "Conflicting URLs for conditional PyPI package %s" % name
            )
        if urls:
            # A direct URL cannot be intersected with a version in pip's input.
            # Keep the URL; the selected version is validated after resolution.
            untouched.append(package + " @ " + urls.pop())
        else:
            specifiers = str(
                SpecifierSet(
                    ",".join(str(req.specifier) for req in reqs if req.specifier)
                )
            )
            # Existing solvers consume the internal name==<specifier> format.
            version = (
                specifiers
                if not specifiers or specifiers.startswith("==")
                else "==" + specifiers
            )
            untouched.append(package + version)
    return dict(deps, pypi=untouched)


def validate_conditional_requirements(
    environment: ResolvedEnvironment, datastore_type: str
) -> None:
    missing = unsatisfied_requirements(
        environment, conditional_requirements(environment, datastore_type)
    )
    if missing:
        raise CondaException(
            "Native PyPI environment does not satisfy conditional requirements: %s. "
            "Update the environment inputs and regenerate its UV/pylock lockfile "
            "if it uses one." % ", ".join(map(str, missing))
        )
