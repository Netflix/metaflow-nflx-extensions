import re
import warnings
from typing import Any, Dict, List, NamedTuple, Optional

from metaflow.metaflow_config import CONDA_SYS_DEPENDENCIES
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow._vendor.packaging.requirements import InvalidRequirement, Requirement


REQ_SPLIT_LINE = re.compile(r"([^~<=>]*)([~<=>]+.*)?")

# Allows things like:
# pkg = <= version
# pkg <= version
# pkg = version
# pkg = ==version or pkg = =version
# In other words, the = is optional but possible
YML_SPLIT_LINE = re.compile(r"(?:=\s)?(<=|>=|~=|==|<|>|=)")


def req_parser(config_value: str) -> Dict[str, Any]:
    extra_args: Dict[str, Any] = {}
    sources: Dict[str, List[str]] = {}
    deps: Dict[str, str] = {}
    np_deps: Dict[str, str] = {}
    sys_deps: Dict[str, str] = {}
    python_version = parse_req_value(
        config_value, extra_args, sources, deps, np_deps, sys_deps
    )
    result: Dict[str, Any] = {}
    if python_version:
        result["python"] = python_version

    if extra_args:
        result["extras"] = extra_args
    if np_deps:
        result["conda_only"] = np_deps
    if sys_deps:
        raise InvalidEnvironmentException(
            "System dependencies are not supported when parsing requirements.txt for "
            "the pypi decorator -- use a named environment instead"
        )

    if "pypi" in sources:
        result["extra_indices"] = sources["pypi"]  # type: ignore[assignment]
        del sources["pypi"]
    if len(sources):
        raise InvalidEnvironmentException(
            "Only PYPI sources are allowed in requirements.txt"
        )

    result["packages"] = deps  # type: ignore[assignment]

    return result


def yml_parser(config_value: str) -> Dict[str, Any]:
    sources: Dict[str, List[str]] = {}
    conda_deps: Dict[str, str] = {}
    pypi_deps: Dict[str, str] = {}
    sys_deps: Dict[str, str] = {}
    python_version = parse_yml_value(
        config_value, {}, sources, conda_deps, pypi_deps, sys_deps
    )
    result = {}
    if sys_deps:
        raise InvalidEnvironmentException(
            "System dependencies are not supported when parsing environment.yml for "
            "the conda decorator -- use a named environment instead"
        )

    if python_version:
        result["python"] = python_version

    if "conda" in sources:
        result["channels"] = sources["conda"]  # type: ignore[assignment]
    if "pypi" in sources:
        result["pip_sources"] = sources["pypi"]  # type: ignore[assignment]

    if len(conda_deps):
        result["libraries"] = conda_deps  # type: ignore[assignment]
    if len(pypi_deps):
        result["pip_packages"] = pypi_deps  # type: ignore[assignment]

    return result


def toml_parser(config_value: str) -> Dict[str, Any]:
    extra_args: Dict[str, List[str]] = {}
    sources: Dict[str, List[str]] = {}
    deps: Dict[str, str] = {}
    np_deps: Dict[str, str] = {}
    sys_deps: Dict[str, str] = {}
    python_version = parse_toml_value(
        config_value, extra_args, sources, deps, np_deps, sys_deps
    )
    result = {}
    if python_version:
        result["python"] = python_version

    if "pypi" in sources:
        result["extra_indices"] = sources["pypi"]  # type: ignore[assignment]
        del sources["pypi"]
    if len(sources):
        raise InvalidEnvironmentException(
            "Only PYPI sources are allowed in requirements.txt"
        )

    result["packages"] = deps  # type: ignore[assignment]

    return result


def parse_req_value(
    file_content: str,
    extra_args: Dict[str, List[str]],
    sources: Dict[str, List[str]],
    deps: Dict[str, str],
    np_deps: Dict[str, str],
    sys_deps: Dict[str, str],
) -> Optional[str]:
    python_version = None
    for line in file_content.splitlines():
        # Strip inline comments (but not inside URL fragments) BEFORE the
        # empty-line check, so that whole-line comments (e.g. "# GIT repo")
        # collapse to an empty string and are skipped rather than crashing
        # the splits[0] access below.
        comment_idx = line.find("#")
        if comment_idx >= 0:
            # Only strip if the '#' is preceded by whitespace or is at the start
            # (avoids stripping '#egg=...' or URL fragments)
            before = line[:comment_idx]
            if not before or before[-1] in (" ", "\t"):
                line = before
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
            warnings.warn(
                "Ignoring '%s' in requirements.txt; the system-configured index "
                "or METAFLOW_CONDA_DEFAULT_PYPI_SOURCE will be used instead. "
                "You can specify additional indices using --extra-index-url." % line
            )
            continue
        elif first_word == "--extra-index-url" and rem:
            sources.setdefault("pypi", []).append(rem)
        elif first_word in ("-f", "--find-links", "--trusted-host") and rem:
            extra_args.setdefault("pypi", []).append(" ".join([first_word, rem]))
        elif first_word in ("--pre", "--no-index"):
            extra_args.setdefault("pypi", []).append(first_word)
        elif first_word == "--conda-channel" and rem:
            sources.setdefault("conda", []).append(rem)
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
                raise InvalidEnvironmentException("Could not parse '%s'" % line) from ex
            dep_name = parsed_req.name
            if parsed_req.extras:
                dep_name += "[%s]" % ",".join(parsed_req.extras)
            if parsed_req.url:
                dep_name += "@%s" % parsed_req.url
            specifier = str(parsed_req.specifier).lstrip(" =")
            if parsed_req.marker:
                specifier += ";" + str(parsed_req.marker)
            if dep_name == "python":
                if specifier:
                    python_version = specifier
            else:
                deps[dep_name] = specifier
    return python_version


def parse_yml_value(
    file_content: str,
    _: Dict[str, List[str]],
    sources: Dict[str, List[str]],
    conda_deps: Dict[str, str],
    pypi_deps: Dict[str, str],
    sys_deps: Dict[str, str],
) -> Optional[str]:
    python_version = None  # type: Optional[str]
    mode = None
    for line in file_content.splitlines():
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
            if not line:
                continue
            if line == "pip:":
                mode = "pypi_deps"
            elif line == "sys:":
                mode = "sys_deps"
            else:
                to_update = (
                    conda_deps
                    if mode == "deps"
                    else pypi_deps if mode == "pypi_deps" else sys_deps
                )
                splits = YML_SPLIT_LINE.split(line.replace(" ", ""), maxsplit=1)
                if len(splits) == 1:
                    if splits[0] != "python":
                        if mode == "sys_deps":
                            raise InvalidEnvironmentException(
                                "System package '%s' requires a version" % splits[0]
                            )

                        dep_name = splits[0]
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
                        to_update[dep_name] = ""
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
                            mode == "sys_deps"
                            and dep_name not in CONDA_SYS_DEPENDENCIES
                        ):
                            raise InvalidEnvironmentException(
                                "System package '%s' not allowed. Values allowed are: %s"
                                % (dep_name, str(CONDA_SYS_DEPENDENCIES))
                            )
                        to_update[dep_name] = dep_version

    return python_version


def parse_toml_value(
    file_content: str,
    _: Dict[str, List[str]],
    sources: Dict[str, List[str]],
    deps: Dict[str, str],
    __: Dict[str, str],
    ___: Dict[str, str],
) -> Optional[str]:
    try:
        import tomllib as toml  # Python 3.11+
    except ImportError:
        try:
            import tomli as toml  # Python < 3.11 (requires "tomli" package)
        except ImportError as e:
            raise InvalidEnvironmentException(
                "Could not import a TOML library. For Python <3.11, please install 'tomli'."
            ) from e

    data = toml.loads(file_content)

    project = data.get("project", {})
    requirements = project.get("dependencies", [])
    python_version = project.get("requires-python")
    for dep_line in requirements:
        try:
            parsed_req = Requirement(dep_line)
        except InvalidRequirement as ex:
            raise InvalidEnvironmentException("Could not parse '%s'" % dep_line) from ex

        dep_name = parsed_req.name
        if parsed_req.extras:
            dep_name += "[%s]" % ",".join(parsed_req.extras)
        if parsed_req.url:
            dep_name += "@%s" % parsed_req.url
        specifier = str(parsed_req.specifier).lstrip(" =")
        if parsed_req.marker:
            specifier += ";" + str(parsed_req.marker)

        if dep_name == "python":
            raise InvalidEnvironmentException(
                "Python specification should be specified as 'requires-python'"
            )
        deps[dep_name] = specifier

    # Also parse poetry sources as extra indices
    poetry_sources = data.get("tool", {}).get("poetry", {}).get("source", [])
    for s in poetry_sources:
        if "url" in s:
            sources.setdefault("pypi", []).append(s["url"])

    # Also parse uv index as extra indices
    uv_sources = data.get("tool", {}).get("uv", {}).get("index", [])
    for s in uv_sources:
        if "url" in s:
            sources.setdefault("pypi", []).append(s["url"])

    return python_version


# conda-lock writes a `version` key at the top of the lockfile. Only v1 has been
# released so far; refuse anything else rather than silently mis-reading a future
# schema.
CONDA_LOCK_SUPPORTED_VERSIONS = (1,)

# conda-lock records `manager: conda` or `manager: pip` per package. Map those onto the
# categories the rest of the codebase uses ("pypi" rather than "pip").
CONDA_LOCK_MANAGERS = {"conda": "conda", "pip": "pypi"}


class CondaLockPackage(NamedTuple):
    """One entry of the `package:` list of a conda-lock lockfile."""

    name: str
    version: str
    manager: str  # "conda" or "pypi" (normalized from conda-lock's "pip")
    platform: str
    url: str
    hashes: Dict[str, str]  # e.g. {"md5": ..., "sha256": ...}
    category: str  # "main", "dev", ...
    optional: bool
    dependencies: Dict[str, str]


class CondaLockFile(NamedTuple):
    """A parsed conda-lock lockfile (the output of `conda-lock lock`)."""

    version: int
    channels: List[str]
    platforms: List[str]
    sources: List[str]
    content_hash: Dict[str, str]
    packages: List[CondaLockPackage]

    def packages_for(
        self, platform: str, include_dev: bool = False
    ) -> List[CondaLockPackage]:
        """
        Packages of a single platform, skipping non-main categories by default.

        conda-lock marks every package outside the `main` category both with its
        category and with `optional: true`, so the two have to be tested together:
        testing `optional` on its own would make `include_dev` a no-op. The default
        matches `conda-lock install`, which installs the main category only.
        """
        return [
            pkg
            for pkg in self.packages
            if pkg.platform == platform
            and (include_dev or (pkg.category == "main" and not pkg.optional))
        ]


def parse_conda_lock_yml(file_content: str) -> CondaLockFile:
    """
    Parse a conda-lock lockfile (`conda-lock.yml`, the output of `conda-lock lock`).

    This is *not* the same format as an `environment.yml`: a lockfile is already
    resolved and carries a `metadata` header plus a flat `package:` list with one entry
    per (package, platform) holding an exact URL and hash. Use `yml_parser` for an
    `environment.yml`.

    Parameters
    ----------
    file_content : str
        Content of the lockfile.

    Returns
    -------
    CondaLockFile
        The parsed lockfile.
    """
    try:
        import yaml
    except ImportError as e:
        raise InvalidEnvironmentException(
            "Parsing a conda-lock lockfile requires PyYAML. Please install 'pyyaml'."
        ) from e

    try:
        data = yaml.safe_load(file_content)
    except yaml.YAMLError as e:
        raise InvalidEnvironmentException(
            "Could not parse the conda-lock lockfile: %s" % str(e)
        ) from e

    if not isinstance(data, dict):
        raise InvalidEnvironmentException(
            "A conda-lock lockfile must be a YAML mapping; got %s" % type(data).__name__
        )

    version = data.get("version")
    if version not in CONDA_LOCK_SUPPORTED_VERSIONS:
        raise InvalidEnvironmentException(
            "Unsupported conda-lock lockfile version %s; supported versions are: %s. "
            "This does not look like the output of 'conda-lock lock' -- note that an "
            "environment.yml is passed with -f/--file, not --lockfile."
            % (version, ", ".join(str(v) for v in CONDA_LOCK_SUPPORTED_VERSIONS))
        )

    metadata = data.get("metadata") or {}
    if not isinstance(metadata, dict):
        raise InvalidEnvironmentException(
            "The 'metadata' section of the conda-lock lockfile must be a mapping"
        )

    platforms = metadata.get("platforms") or []
    if not platforms:
        raise InvalidEnvironmentException(
            "The conda-lock lockfile does not list any platform in "
            "'metadata.platforms'"
        )

    # Channels are recorded as a list of mappings ({url: ..., used_env_vars: [...]}),
    # but tolerate plain strings in case a lockfile was written by hand.
    channels = []  # type: List[str]
    for channel in metadata.get("channels") or []:
        if isinstance(channel, dict):
            url = channel.get("url")
            if url:
                channels.append(url)
        elif isinstance(channel, str):
            channels.append(channel)

    packages = [
        _parse_conda_lock_package(raw, idx)
        for idx, raw in enumerate(data.get("package") or [])
    ]

    known_platforms = set(platforms)
    unknown = sorted({p.platform for p in packages} - known_platforms)
    if unknown:
        raise InvalidEnvironmentException(
            "The conda-lock lockfile has packages for platform(s) %s which are not "
            "listed in 'metadata.platforms' (%s)"
            % (", ".join(unknown), ", ".join(platforms))
        )

    return CondaLockFile(
        version=version,
        channels=channels,
        platforms=list(platforms),
        sources=list(metadata.get("sources") or []),
        content_hash=dict(metadata.get("content_hash") or {}),
        packages=packages,
    )


def _parse_conda_lock_package(raw: Any, idx: int) -> CondaLockPackage:
    if not isinstance(raw, dict):
        raise InvalidEnvironmentException(
            "Entry %d of the 'package' list of the conda-lock lockfile is not a "
            "mapping" % idx
        )

    def _required(key: str) -> str:
        value = raw.get(key)
        if not value or not isinstance(value, str):
            raise InvalidEnvironmentException(
                "Package '%s' (entry %d) of the conda-lock lockfile is missing a "
                "'%s'" % (raw.get("name", "<unnamed>"), idx, key)
            )
        return value

    name = _required("name")
    manager = _required("manager")
    if manager not in CONDA_LOCK_MANAGERS:
        raise InvalidEnvironmentException(
            "Package '%s' of the conda-lock lockfile has an unsupported manager "
            "'%s'; supported managers are: %s"
            % (name, manager, ", ".join(sorted(CONDA_LOCK_MANAGERS)))
        )

    hashes = raw.get("hash") or {}
    if not isinstance(hashes, dict):
        raise InvalidEnvironmentException(
            "Package '%s' of the conda-lock lockfile has a malformed 'hash' section"
            % name
        )

    dependencies = raw.get("dependencies") or {}
    if not isinstance(dependencies, dict):
        raise InvalidEnvironmentException(
            "Package '%s' of the conda-lock lockfile has a malformed 'dependencies' "
            "section" % name
        )

    return CondaLockPackage(
        name=name,
        version=_required("version"),
        manager=CONDA_LOCK_MANAGERS[manager],
        platform=_required("platform"),
        url=_required("url"),
        hashes={k: str(v) for k, v in hashes.items()},
        category=raw.get("category") or "main",
        optional=bool(raw.get("optional", False)),
        dependencies={str(k): str(v) for k, v in dependencies.items()},
    )
