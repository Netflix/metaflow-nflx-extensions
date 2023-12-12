# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
from __future__ import annotations

import email.parser
import email.policy
import os
import platform
import re
import shutil
import subprocess
import tempfile
import uuid

from enum import Enum
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Dict,
    FrozenSet,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)
from urllib.parse import urlparse, unquote

from requests import PreparedRequest
from requests.auth import AuthBase, HTTPBasicAuth

from metaflow._vendor.packaging.tags import (
    compatible_tags,
    _cpython_abis,
    cpython_tags,
    mac_platforms,
    Tag,
)

from metaflow._vendor.packaging.utils import BuildTag, parse_wheel_filename

from metaflow._vendor.packaging.version import Version

from metaflow.debug import debug
from metaflow.exception import MetaflowException
import metaflow.metaflow_config as mf_config
from metaflow.metaflow_config import (
    CONDA_MAGIC_FILE_V2,  # type: ignore
    CONDA_PREFERRED_FORMAT,  # type: ignore
    CONDA_SRCS_AUTH_INFO,  # type: ignore
    CONDA_SYS_DEPENDENCIES,  # type: ignore
    CONDA_SYS_DEFAULT_PACKAGES,  # type: ignore
    CONDA_SYS_DEFAULT_GPU_PACKAGES,  # type: ignore
)

from metaflow.metaflow_environment import InvalidEnvironmentException

if TYPE_CHECKING:
    from .env_descr import TStr

# NOTA: Most of the code does not assume that there are only two formats BUT the
# transmute code does (since you can only specify the infile -- the outformat and file
# are inferred)
_ALL_CONDA_FORMATS = (".tar.bz2", ".conda")
# NOTE: Order is important as it is a preference order
_ALL_PYPI_FORMATS = (".whl", ".tar.gz", ".zip")
_VALID_IMAGE_NAME = "[^-a-z0-9_/]"
_VALID_TAG_NAME = "[^-a-z0-9_]"

# Things that need to be put in any of the builder environments
_BUILDER_ENVS_PACKAGES = ("pip", "wheel", "tomli", "setuptools")


class AliasType(Enum):
    PATHSPEC = "pathspec"
    FULL_ID = "full-id"
    REQ_FULL_ID = "both-id"
    GENERIC = "generic"


# List of formats that guarantees the preferred format is first. This is important as
# functions that rely on selecting the "preferred" source of a package rely on the
# preferred format being first.
if CONDA_PREFERRED_FORMAT and CONDA_PREFERRED_FORMAT != "none":
    CONDA_FORMATS = (
        CONDA_PREFERRED_FORMAT,
        *[x for x in _ALL_CONDA_FORMATS if x != CONDA_PREFERRED_FORMAT],
    )  # type: Tuple[str, ...]
else:
    CONDA_FORMATS = _ALL_CONDA_FORMATS  # type: Tuple[str, ...]
FAKEURL_PATHCOMPONENT = "_fake"


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


def get_builder_envs_dep(orig_deps: List[str]) -> List[str]:
    builder_deps = []  # type: List[str]
    for pkg in _BUILDER_ENVS_PACKAGES:
        dep = [d for d in orig_deps if d.startswith("%s==" % pkg)]
        builder_deps.extend(dep or [pkg])
    return builder_deps


def convert_filepath(path: str, file_format: Optional[str] = None) -> Tuple[str, str]:
    if file_format and not path.endswith(file_format):
        path, cur_ext = correct_splitext(path)
        if cur_ext not in _ALL_CONDA_FORMATS:
            raise ValueError(
                "URL '%s' does not end with a supported file format %s"
                % (path, str(_ALL_CONDA_FORMATS))
            )
        path = "%s%s" % (path, file_format)
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


def get_sys_packages(
    virtual_packages: Dict[str, str],
    arch_requested: str,
    gpu_requested: bool,
) -> Dict[str, str]:
    # For now we handle __cuda and __glibc.

    # The goal of this function is to take the list of virtual_packages and to return
    # the sys packages we need to add to the user dependencies (so packages that start with
    # sys::). The reason for having them as part of user packages is that we can then
    # use them in computing the user request hash (since these will impact the packages
    # that are installed). The sys:: packages will then be properly added when resolving
    # the environment as virtual packages (either through CONDA_OVERRIDE_XXX or directly
    # as part of a virtual_packages.yml for conda-lock)
    if virtual_packages:
        result = dict(virtual_packages)
    else:
        result = dict(CONDA_SYS_DEFAULT_PACKAGES.get(arch_requested, {}))

    if gpu_requested:
        result.update(CONDA_SYS_DEFAULT_GPU_PACKAGES)

    # Clean up
    result = {
        k: v for k, v in result.items() if k in CONDA_SYS_DEPENDENCIES and v is not None
    }
    return result


def get_glibc_version() -> Optional[str]:
    # Uses ldd which prints the glibc version on the system and has the version
    # as the last element in the first line.
    try:
        output = subprocess.check_output("ldd --version", shell=True)
        vers_line = output.splitlines()[0]
        version = vers_line.split(b" ")[-1]
        return version.decode("utf-8")
    except Exception:
        return None


def pypi_tags_from_arch(
    python_version: str, arch: str, glibc_version: str
) -> List[Tag]:
    # Converts a Conda architecture to a tuple containing (implementation, platforms, abis)
    # This function will assume a CPython implementation

    # This is inspired by what pip does:
    # https://github.com/pypa/pip/blob/0442875a68f19b0118b0b88c747bdaf6b24853ba/src/pip/_internal/utils/compatibility_tags.py
    py_version = tuple(map(int, python_version.split(".")[:2]))
    if arch == "linux-64":
        max_glibc = "_%s" % glibc_version
        platforms = []  # type: List[str]
        for s in (
            "1",
            "2010",
            "2014",
            "_2_17",
            "_2_18",
            "_2_19",
            "_2_20",
            "_2_21",
            "_2_23",
            "_2_24",
            "_2_25",
            "_2_26",
            "_2_27",
            "_2_28",
            "_2_29",
            "_2_30",
            "_2_31",
            "_2_32",
            "_2_33",
            "_2_34",
            "_2_35",
            "_2_36",
            "_2_37",
            "_2_38",
        ):
            platforms.append("manylinux%s_x86_64" % s)
            if s == max_glibc:
                break

        platforms.append("linux_x86_64")
    elif arch == "osx-64":
        platforms = mac_platforms((11, 0), "x86_64")
    elif arch == "osx-arm64":
        platforms = mac_platforms((11, 0), "arm64")
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
    filename, url_format = correct_splitext(os.path.split(urlparse(url_clean).path)[1])
    if url_format not in _ALL_CONDA_FORMATS:
        raise CondaException(
            "URL '%s' is not a supported format (%s)" % (url, CONDA_FORMATS)
        )
    filename = unquote(filename)
    return ParseExplicitResult(
        filename=filename, url=url_clean, url_format=url_format, hash=url_hash
    )


def parse_explicit_path_pypi(path: str) -> ParseExplicitResult:
    # Takes a filename in the form file://<path> and returns:
    #  - the filename
    #  - the URL (always file://local-<uuid>/<filename> so there is no way another
    #    build/user conflicts. We consider them to be all distinct.
    #  - the format of the URL
    #  - the hash will be set to None
    if not path.startswith("file://"):
        raise CondaException("Local path '%s' does not start with file://" % path)
    path = path[7:]
    orig_filename, url_format = correct_splitext(os.path.basename(path))
    if url_format not in _ALL_PYPI_FORMATS:
        raise CondaException(
            "Path '%s' is not a supported format (%s)" % (path, str(_ALL_PYPI_FORMATS))
        )
    return ParseExplicitResult(
        filename=unquote(orig_filename),
        url="file://local-%s/%s" % (str(uuid.uuid4()), orig_filename),
        url_format=url_format,
        hash=None,
    )


def parse_explicit_url_pypi(url: str) -> ParseExplicitResult:
    # Takes a URL in the form url#hash and returns:
    #  - the filename
    #  - the URL (without the hash)
    #  - the format for the URL
    #  - the hash
    filename = None
    url_format = None

    url_clean, url_hash = url.rsplit("#", 1)
    if url_hash:
        if not url_hash.startswith("sha256="):
            raise CondaException("URL '%s' has a SHA type which is not supported" % url)
        url_hash = url_hash[7:]
    else:
        url_hash = None

    filename, url_format = correct_splitext(os.path.split(urlparse(url_clean).path)[1])
    if url_format not in _ALL_PYPI_FORMATS:
        raise CondaException(
            "URL '%s' is not a supported format (%s)" % (url, str(_ALL_PYPI_FORMATS))
        )
    filename = unquote(filename)
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
                "lowercase alphanumeric characters, dashes, underscores and forward slashes."
            )
        if image_name[0] == "/" or image_name[-1] == "/":
            raise MetaflowException(
                "An environment name must not start or end with '/'"
            )
        if re.search(_VALID_TAG_NAME, image_tag):
            raise MetaflowException(
                "An environment tag name must contain only "
                "lowercase alphanumeric characters, dashes and underscores."
            )
        return AliasType.GENERIC, "/".join([image_name, image_tag])
    raise MetaflowException("Invalid format for environment alias: '%s'" % env_alias)


def is_alias_mutable(alias_type: AliasType, resolved_alias: str) -> bool:
    if alias_type != AliasType.GENERIC:
        return False
    splits = resolved_alias.rsplit("/", 1)
    return len(splits) == 2 and splits[1] in ("latest", "candidate", "stable")


def dict_to_tstr(deps: Dict[str, List[str]]) -> List[TStr]:
    from .env_descr import TStr  # Avoid circular import

    result = []  # type: List[TStr]
    for category, values in deps.items():
        result.extend([TStr(category, v) for v in values])
    return result


def tstr_to_dict(deps: List[TStr]) -> Dict[str, List[str]]:
    result = {}  # type: Dict[str, List[str]]
    for dep in deps:
        result.setdefault(dep.category, []).append(dep.value)
    return result


def split_into_dict(deps: List[str]) -> Dict[str, str]:
    result = {}  # type: Dict[str, str]
    for dep in deps:
        s = dep.split("==", 1)
        if len(s) == 1:
            result[s[0]] = ""
        else:
            result[s[0]] = s[1]
    return result


def merge_dep_dicts(
    d1: Dict[str, str], d2: Dict[str, str], only_last_deps: bool = False
) -> Dict[str, str]:
    # Merge dictionaries of version constraints taking *all* constraints
    # into account instead of replacing them.
    # Order will be preserved based on the order of the first dict
    # only_last_deps will only return things that are already in d2. This is to merge with
    # anything in d1 but only return things that are in d2. This is used to compute
    # the new set of dependencies introduced by the user.
    result = {}  # type: Dict[str, str]
    for key in set(d2.keys() if only_last_deps else chain(d1.keys(), d2.keys())):
        v1 = [v for v in d1.get(key, "").split(",") if v]
        v2 = [v for v in d2.get(key, "").split(",") if v]
        sv1 = set([v.lstrip("=") for v in v1])
        # We dedup so that if we have the exact same constraints, we don't change
        # things. We try to keep the order of the first dict
        v2 = [v for v in v2 if v.lstrip("=") not in sv1]

        # We need to make sure that things that don't start with ~, <, >, ! or = get
        # == if they are not first.
        resulting_versions = list(chain(v1, v2))
        if resulting_versions:
            resulting_versions = [resulting_versions[0]] + [
                v if v[0] in ("~", "<", ">", "!", "=") else "==%s" % v
                for v in resulting_versions[1:]
            ]
        result[key] = ",".join(resulting_versions)
    return result


def reform_pypi_filename(
    name: str, version: Version, build: BuildTag, tags: FrozenSet[Tag]
) -> str:
    if build:
        build_str = "%d%s" % (build[0], build[1])
    else:
        build_str = None
    # We need to iterate over all the tags to form the string
    # interp1.interp2-abi1.abi2-platform1.platform2
    interpreters, abis, platforms = zip(
        *map(lambda x: (x.interpreter, x.abi, x.platform), tags)
    )
    tag_str = "-".join(
        [
            ".".join(sorted(interpreters)),
            ".".join(sorted(abis)),
            ".".join(sorted(platforms)),
        ]
    )

    if build_str:
        return "-".join([str(name), str(version), build_str, tag_str])
    return "-".join([str(name), str(version), tag_str])


def correct_splitext(filename_with_ext: str) -> Tuple[str, str]:
    # This handles .tar.gz and .tar.bz2 which is what we care about.
    # Tried with pathlib too but that has other issues since . is a valid character
    # in conda packages
    all_splits = filename_with_ext.rsplit(".", 2)
    if len(all_splits) > 2:
        if ".".join(all_splits[1:]) in ("tar.gz", "tar.bz2"):
            return all_splits[0], ".".join([""] + all_splits[1:])
        return ".".join(all_splits[:-1]), "." + all_splits[2]
    if len(all_splits) == 2:
        return all_splits[0], "." + all_splits[1]
    if len(all_splits) == 1:
        return filename_with_ext, ""
    # Should never happen
    return "", ""


def conda_deps_to_pypi_deps(d: Dict[str, str]) -> Dict[str, str]:
    # This is a hack for now -- real fix would be to specify get_pinned_pypi_libs
    # but at least for now this allows channels to be specified in the conda_libs
    # but still work with pip
    r = {}  # type: Dict[str, str]
    for k, v in d.items():
        s = k.split("::")
        if len(s) > 1:
            r[s[1]] = v
        else:
            r[k] = v
    return r


_UNDERSCORE_REGEX = re.compile(r"[-_.]+")


def normalize_to_underscore(name: str) -> str:
    return _UNDERSCORE_REGEX.sub("_", name).lower()


def channel_from_url(url: str) -> Optional[str]:
    up = urlparse(url)
    if up.hostname == "conda.anaconda.org":
        return up.path.split("/", 2)[1]
    return None


def channel_or_url(url: str) -> str:
    up = urlparse(url)
    if up.hostname == "conda.anaconda.org":
        return up.path.split("/", 2)[1]
    return url


def auth_from_urls(urls: List[str]) -> Optional[AuthBase]:
    auths_per_hostname = {
        cast(str, h): HTTPBasicAuth(username, pwd)
        for h, (username, pwd) in CONDA_SRCS_AUTH_INFO.items()
    }

    for url in urls:
        up = urlparse(url.strip("'\" \t\n"))
        if up.hostname and up.username:
            auths_per_hostname[up.hostname] = HTTPBasicAuth(
                up.username, up.password or ""
            )
    if auths_per_hostname:
        return PerHostAuth(auths_per_hostname)
    return None


class PerHostAuth(AuthBase):
    def __init__(self, auths_per_hostname: Mapping[str, AuthBase]):
        self._my_auths = auths_per_hostname

    def __call__(self, r: PreparedRequest):
        up = urlparse(r.url)
        if up.hostname:
            h = (
                up.hostname
                if isinstance(up.hostname, str)
                else up.hostname.decode(encoding="utf-8")
            )
            auth_provider = self._my_auths.get(h or "<none>")
            if auth_provider:
                return auth_provider(r)
        return r


# Function heavily inspired from https://github.com/hauntsaninja/change_wheel_version
# MIT license of that source file:
# MIT License

# Copyright (c) 2023 hauntsaninja

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.


# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
def change_pypi_package_version(
    python_path: str, wheel_path: str, new_version_str: str
) -> str:
    base_dir, wheel_name = os.path.split(wheel_path)
    with tempfile.TemporaryDirectory() as build_dir:
        old_parts = parse_wheel_filename(wheel_name)

        distribution = wheel_name.split("-", 1)[0]
        old_version = old_parts[1]
        new_version = Version(new_version_str)
        new_version_str = str(new_version)

        old_slug = "%s-%s" % (distribution, old_version)
        new_slug = "%s-%s" % (distribution, new_version)

        subprocess.check_output(
            [python_path, "-m", "wheel", "unpack", "-d", build_dir, wheel_path]
        )
        if not os.path.isdir(os.path.join(build_dir, old_slug)) or not os.path.isdir(
            os.path.join(build_dir, old_slug, "%s.dist-info" % old_slug)
        ):
            raise ValueError("Invalid wheel expansion for '%s'" % wheel_path)

        # Copy everything
        shutil.move(
            os.path.join(build_dir, old_slug), os.path.join(build_dir, new_slug)
        )

        # Rename dist-info directory
        shutil.move(
            os.path.join(build_dir, new_slug, "%s.dist-info" % old_slug),
            os.path.join(build_dir, new_slug, "%s.dist-info" % new_slug),
        )

        # Rename data if it exists
        if os.path.exists(os.path.join(build_dir, new_slug, "%s.data" % old_slug)):
            shutil.move(
                os.path.join(build_dir, new_slug, "%s.data" % old_slug),
                os.path.join(build_dir, new_slug, "%s.data" % new_slug),
            )

        # Rewrite METADATA file
        metadata = os.path.join(
            build_dir, new_slug, "%s.dist-info" % new_slug, "METADATA"
        )

        # This is actually a non-conformant email policy as per
        # https://packaging.python.org/en/latest/specifications/core-metadata/
        # However, it works around this bug in setuptools in cases where the version is really long
        # https://github.com/pypa/setuptools/issues/3808
        max_line_length = 200
        policy = email.policy.compat32.clone(max_line_length=200)
        if len(new_version_str) >= max_line_length:
            raise ValueError(f"Version '%s' is too long" % new_version_str)

        with open(metadata, "rb") as f:
            parser = email.parser.BytesParser(policy=policy)
            msg = parser.parse(f)

        msg.replace_header("Version", new_version_str)
        with open(metadata, "wb") as f:
            f.write(msg.as_bytes())

        # Rewrites the RECORD file and generate the file.
        subprocess.check_output(
            [
                python_path,
                "-m",
                "wheel",
                "pack",
                "-d",
                build_dir,
                os.path.join(build_dir, new_slug),
            ]
        )

        # We check if we get the expected name
        expected_name = (
            reform_pypi_filename(distribution, new_version, old_parts[2], old_parts[3])
            + ".whl"
        )
        if os.path.isfile(os.path.join(build_dir, expected_name)):
            shutil.move(
                os.path.join(build_dir, expected_name),
                os.path.join(base_dir, expected_name),
            )
            os.unlink(wheel_path)
            return os.path.join(base_dir, expected_name)
        raise RuntimeError(
            "Could not rename wheel '%s'; expected '%s' got: %s"
            % (wheel_path, expected_name, ", ".join(os.listdir(build_dir)))
        )


class WithDir:
    # WARNING: os.chdir is not compatible with thread processing so do not use in
    # a context where multiple threads can exist.
    def __init__(self, new_dir: str):
        self._current_dir = os.getcwd()
        self._new_dir = new_dir

    def __enter__(self):
        os.chdir(self._new_dir)
        return self._new_dir

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.chdir(self._current_dir)
