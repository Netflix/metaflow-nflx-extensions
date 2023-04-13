# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

import os
import platform
import re

from enum import Enum
from itertools import chain, product
from typing import List, NamedTuple, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse, unquote

from metaflow_extensions.netflix_ext.vendor.packaging.tags import (
    compatible_tags,
    _cpython_abis,
    cpython_tags,
    Tag,
)

from metaflow.debug import debug
from metaflow.exception import MetaflowException
import metaflow.metaflow_config as mf_config
from metaflow.metaflow_config import (
    CONDA_MAGIC_FILE_V2,  # type: ignore
    CONDA_PREFERRED_FORMAT,  # type: ignore
)

from metaflow.metaflow_environment import InvalidEnvironmentException

# NOTA: Most of the code does not assume that there are only two formats BUT the
# transmute code does (since you can only specify the infile -- the outformat and file
# are inferred)
_ALL_CONDA_FORMATS = (".tar.bz2", ".conda")
_VALID_IMAGE_NAME = "[^a-z0-9_/]"
_VALID_TAG_NAME = "[^a-z0-9_]"


class AliasType(Enum):
    PATHSPEC = "pathspec"
    FULL_ID = "full-id"
    REQ_FULL_ID = "both-id"
    GENERIC = "generic"


# List of formats that guarantees the preferred format is first. This is important as
# functions that rely on selecting the "preferred" source of a package rely on the
# preferred format being first.
if CONDA_PREFERRED_FORMAT:
    CONDA_FORMATS = (
        CONDA_PREFERRED_FORMAT,
        *[x for x in _ALL_CONDA_FORMATS if x != CONDA_PREFERRED_FORMAT],
    )  # type: Tuple[str, ...]
else:
    CONDA_FORMATS = _ALL_CONDA_FORMATS  # type: Tuple[str, ...]
TRANSMUT_PATHCOMPONENT = "_transmut"


class CondaException(MetaflowException):
    headline = "Conda ran into an error while setting up environment."

    def __init__(self, error: Union[Sequence[Exception], str]):
        if isinstance(error, list):
            error = "\n".join([str(x) for x in error])
        super(CondaException, self).__init__(error)  # type: ignore


class CondaStepException(CondaException):
    def __init__(self, exception: CondaException, steps: Sequence[str]):
        msg = "Step(s): {steps}, Error: {error}".format(
            steps=steps, error=exception.message
        )
        super(CondaStepException, self).__init__(msg)


def convert_filepath(path: str, file_format: Optional[str] = None) -> Tuple[str, str]:
    if file_format and not path.endswith(file_format):
        for f in CONDA_FORMATS:
            if path.endswith(f):
                path = path[: -len(f)] + file_format
                break
        else:
            raise ValueError(
                "URL '%s' does not end with a supported file format %s"
                % (path, str(CONDA_FORMATS))
            )
    return os.path.split(path)


def get_conda_manifest_path(ds_root: str) -> str:
    return os.path.join(ds_root, CONDA_MAGIC_FILE_V2)  # type: ignore


def get_conda_root(datastore_type: str) -> str:
    conda_root = getattr(mf_config, "CONDA_%sROOT" % datastore_type.upper())
    if conda_root is None:
        # We error on METAFLOW_DATASTORE_SYSROOT_<ds> because that is the default used
        raise MetaflowException(
            msg="METAFLOW_DATASTORE_SYSROOT_%s must be set!" % datastore_type.upper()
        )
    debug.conda_exec("Conda root is at %s" % conda_root)
    return conda_root  # type: ignore


def arch_id() -> str:
    bit = "32"
    if platform.machine().endswith("64"):
        bit = "64"
    if platform.system() == "Linux":
        return "linux-%s" % bit
    elif platform.system() == "Darwin":
        # Support M1 Mac
        if platform.machine() == "arm64":
            return "osx-arm64"
        else:
            return "osx-%s" % bit
    else:
        raise InvalidEnvironmentException(
            "The *@conda* decorator is not supported "
            "outside of Linux and Darwin platforms"
        )


def pip_tags_from_arch(python_version: str, arch: str) -> List[Tag]:
    # Converts a Conda architecture to a tuple containing (implementation, platforms, abis)
    # This function will assume a CPython implementation

    # This is inspired by what pip does:
    # https://github.com/pypa/pip/blob/0442875a68f19b0118b0b88c747bdaf6b24853ba/src/pip/_internal/utils/compatibility_tags.py
    py_version = tuple(map(int, python_version.split(".")[:2]))
    if arch.startswith("linux-"):
        detail = arch.split("-")[-1]
        if detail == "64":
            detail = "x86_64"
        platforms = [
            "manylinux%s_%s" % (tag, arch) for tag in ["_2_17", "2014", "2010", "1"]
        ]
        platforms.append("linux_%s" % detail)
    elif arch == "osx-64":
        platforms = [
            "macosx_10_9_x86_64",
            *("macosx_10_%s_universal2" % v for v in range(16, 3, -1)),
            *("macosx_10_%s_universal" % v for v in range(16, 3, -1)),
        ]
    elif arch == "osx-arm64":
        platforms = [
            "macosx_11_0_arm64",
            *("macosx_10_%s_universal2" % v for v in range(16, 3, -1)),
        ]
    else:
        raise InvalidEnvironmentException("Unsupported platform: %s" % arch)

    interpreter = "cp%s" % ("".join(map(str, py_version)))

    abis = _cpython_abis(py_version)

    supported = []  # type: List[Tag]
    supported.extend(cpython_tags(py_version, abis, platforms))
    supported.extend(compatible_tags(py_version, interpreter, platforms))
    return supported


ParseExplicitResult = NamedTuple(
    "ParseExplicitResult",
    [("filename", str), ("url", str), ("url_format", str), ("hash", Optional[str])],
)


def parse_explicit_url_conda(url: str) -> ParseExplicitResult:
    # Takes a URL in the form url#hash and returns:
    #  - the filename
    #  - the URL (without the hash)
    #  - the format for the URL
    #  - the hash
    filename = None
    url_format = None

    url_clean, url_hash = url.rsplit("#", 1)

    filename = os.path.split(urlparse(url_clean).path)[1]
    for f in _ALL_CONDA_FORMATS:
        if filename.endswith(f):
            url_format = f
            filename = unquote(filename[: -len(f)])
            break
    else:
        raise CondaException(
            "URL '%s' is not a supported format (%s)" % (url, CONDA_FORMATS)
        )
    return ParseExplicitResult(
        filename=filename, url=url_clean, url_format=url_format, hash=url_hash
    )


def parse_explicit_path_pip(path: str) -> ParseExplicitResult:
    # Takes a filename in the form file://<path> and returns:
    #  - the filename
    #  - the URL (always file://local/<filename> so caching works across systems)
    #  - the format of the URL
    #  - the hash will be set to None
    if not path.startswith("file://"):
        raise CondaException("Local path '%s' does not start with file://" % path)
    path = path[7:]
    orig_filename = os.path.basename(path)
    for f in [".whl", ".tar.gz"]:
        if orig_filename.endswith(f):
            url_format = f
            filename = unquote(orig_filename[: -len(f)])
            break
    else:
        raise CondaException(
            "Path '%s' is not a supported format (%s)"
            % (path, str([".whl", ".tar.gz"]))
        )
    return ParseExplicitResult(
        filename=filename,
        url="file://local-file/%s" % orig_filename,
        url_format=url_format,
        hash=None,
    )


def parse_explicit_url_pip(url: str) -> ParseExplicitResult:
    # Takes a URL in the form url#hash and returns:
    #  - the filename
    #  - the URL (without the hash)
    #  - the format for the URL
    #  - the hash
    filename = None
    url_format = None

    url_clean, url_hash = url.rsplit("#", 1)
    if not url_hash.startswith("sha256="):
        raise CondaException("URL '%s' has a SHA type which is not supported" % url)
    url_hash = url_hash[7:]

    filename = os.path.split(urlparse(url_clean).path)[1]
    for f in [".whl", ".tar.gz"]:
        if filename.endswith(f):
            url_format = f
            filename = unquote(filename[: -len(f)])
            break
    else:
        raise CondaException(
            "URL '%s' is not a supported format (%s)" % (url, str([".whl", ".tar.gz"]))
        )
    return ParseExplicitResult(
        filename=filename, url=url_clean, url_format=url_format, hash=url_hash
    )


def plural_marker(count: int) -> str:
    return "s" if count != 1 else ""


def is_hexadecimal(s: str) -> bool:
    return not re.search("[^0-9a-f]", s)


def resolve_env_alias(env_alias: str) -> Tuple[AliasType, str]:
    if env_alias.startswith("step:"):
        env_alias = env_alias[5:]
        if len(env_alias.split("/")) == 3:
            return AliasType.PATHSPEC, env_alias
    elif len(env_alias) == 81 and env_alias[40] == ":":
        # req-id:full-id possibly
        req_id, full_id = env_alias.split(":", 1)
        if is_hexadecimal(req_id) and is_hexadecimal(full_id):
            return AliasType.REQ_FULL_ID, env_alias
    elif len(env_alias) == 40 and is_hexadecimal(env_alias):
        # For now we do not support this -- remove if you want to support
        # The issue with supporting this is that a full-id can refer to multiple req-id
        # so it is impossible to unambiguously identify the source environment (ie:
        # the user requested dependencies, etc)
        raise MetaflowException(
            "Invalid format for environment alias: '%s'" % env_alias
        )
        # return AliasType.FULL_ID, env_alias
    else:
        splits = env_alias.rsplit(":", 1)
        if len(splits) == 2:
            image_name = splits[0]
            image_tag = splits[1]
        else:
            image_name = env_alias
            image_tag = "latest"
        if re.search(_VALID_IMAGE_NAME, image_name):
            raise MetaflowException(
                "An environment name must contain only "
                "lowercase alphanumeric characters, underscores and forward slashes."
            )
        if image_name[0] == "/" or image_name[-1] == "/":
            raise MetaflowException(
                "An environment name must not start or end with '/'"
            )
        if re.search(_VALID_TAG_NAME, image_tag):
            raise MetaflowException(
                "An environment tag name must contain only "
                "lowercase alphanumeric characters and underscores."
            )
        return AliasType.GENERIC, "/".join([image_name, image_tag])
    raise MetaflowException("Invalid format for environment alias: '%s'" % env_alias)


def is_alias_mutable(alias_type: AliasType, resolved_alias: str) -> bool:
    if alias_type != AliasType.GENERIC:
        return False
    splits = resolved_alias.rsplit("/", 1)
    return len(splits) == 2 and splits[1] in ("latest", "candidate", "stable")
