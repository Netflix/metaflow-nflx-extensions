import re
from typing import Any, Dict, List, Optional

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
    extra_args = {}
    sources = {}
    deps = {}
    np_deps = {}
    sys_deps = {}
    python_version = parse_req_value(
        config_value, extra_args, sources, deps, np_deps, sys_deps
    )
    result = {}
    if python_version:
        result["python"] = python_version

    if extra_args:
        raise InvalidEnvironmentException(
            "Additional arguments are not supported when parsing requirements.txt for "
            "the pypi decorator -- use a named environment instead"
        )
    if np_deps:
        raise InvalidEnvironmentException(
            "Non-python dependencies are not supported when parsing requirements.txt for "
            "the pypi decorator -- use a named environment instead"
        )
    if sys_deps:
        raise InvalidEnvironmentException(
            "System dependencies are not supported when parsing requirements.txt for "
            "the pypi decorator -- use a named environment instead"
        )

    if "pypi" in sources:
        result["extra_indices"] = sources["pypi"]
        del sources["pypi"]
    if len(sources):
        raise InvalidEnvironmentException(
            "Only PYPI sources are allowed in requirements.txt"
        )

    result["packages"] = deps

    return result


def yml_parser(config_value: str) -> Dict[str, Any]:
    sources = {}
    conda_deps = {}
    pypi_deps = {}
    sys_deps = {}
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
        result["channels"] = sources["conda"]
    if "pypi" in sources:
        result["pip_sources"] = sources["pypi"]

    if len(conda_deps):
        result["libraries"] = conda_deps
    if len(pypi_deps):
        result["pip_packages"] = pypi_deps

    return result


def toml_parser(config_value: str) -> Dict[str, Any]:
    extra_args = {}
    sources = {}
    deps = {}
    np_deps = {}
    sys_deps = {}
    python_version = parse_toml_value(
        config_value, extra_args, sources, deps, np_deps, sys_deps
    )
    result = {}
    if python_version:
        result["python"] = python_version

    if "pypi" in sources:
        result["extra_indices"] = sources["pypi"]
        del sources["pypi"]
    if len(sources):
        raise InvalidEnvironmentException(
            "Only PYPI sources are allowed in requirements.txt"
        )

    result["packages"] = deps

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
        if parsed_req.marker is not None:
            raise InvalidEnvironmentException(
                "Environment markers are not supported for '%s'" % dep_line
            )
        dep_name = parsed_req.name
        if parsed_req.extras:
            dep_name += "[%s]" % ",".join(parsed_req.extras)
        if parsed_req.url:
            dep_name += "@%s" % parsed_req.url
        specifier = str(parsed_req.specifier).lstrip(" =")
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

    return python_version
