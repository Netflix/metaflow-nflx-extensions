# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
from datetime import datetime
import errno
import fcntl
import json
import os
from enum import Enum
from hashlib import md5, sha1, sha256
from itertools import chain

from metaflow._vendor.packaging.utils import (
    parse_sdist_filename,
    parse_wheel_filename,
)
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
)
from urllib.parse import urlparse, urlunparse

from metaflow.metaflow_config import CONDA_PACKAGES_DIRNAME
from metaflow.util import get_username

from .utils import (
    _ALL_PYPI_FORMATS,
    FAKEURL_PATHCOMPONENT,
    AliasType,
    arch_id,
    channel_from_url,
    correct_splitext,
    dict_to_tstr,
    get_conda_manifest_path,
    is_alias_mutable,
    tstr_to_dict,
)

# Order should be maintained
EnvID = NamedTuple("EnvID", [("req_id", str), ("full_id", str), ("arch", str)])

VALID_IMAGE_NAME_RE = "[^a-z0-9_]"


class EnvType(Enum):
    CONDA_ONLY = "conda-only"
    PYPI_ONLY = "pypi-only"
    PIP_ONLY = "pip-only"  # Here for legacy reasons -- we now use pypi-only
    MIXED = "mixed"


class TStr:
    def __init__(self, category: str, value: str):
        if category == "pip":
            # Legacy support for "pip" packages (now called pypi)
            category = "pypi"
        self._category = category
        self._value = value

    @property
    def category(self):
        return self._category

    @property
    def value(self):
        return self._value

    @value.setter
    def value_setter(self, value: str):
        self._value = value

    def __str__(self):
        return "%s::%s" % (self._category, self._value)

    def __repr__(self):
        return str(self)

    @staticmethod
    def from_str(value: str):
        splits = value.split("::", 1)
        if len(splits) != 2:
            raise ValueError("Cannot parse a TStr from %s" % value)
        c = splits[0]
        if c == "pip":
            c = "pypi"
        return TStr(c, splits[1])


def env_type_for_deps(deps: Dict[str, List[str]]) -> EnvType:
    """
    Returns the environment type based on a set of dependencies

    Parameters
    ----------
    deps : List[TStr]
        User-requested dependencies for this environment

    Returns
    -------
    EnvType
        The environment type, either CONDA_ONLY, PYPI_ONLY or MIXED
    """
    env_type = EnvType.CONDA_ONLY
    if len(deps.get("conda", [])) <= 1:
        # This is a pypi only mode
        env_type = EnvType.PYPI_ONLY
    elif deps.get("pypi", []):
        env_type = EnvType.MIXED
    return env_type


class CachePackage:
    # Cache URL explanation:
    #  - we form the cache_url based on the source URL so that we can easily check
    #    if the file is in cache.
    #  - in some cases, we want to store multiple formats for the same package (for
    #    example .conda and .tar.bz2 for conda packages or a source .tar.gz and a .whl
    #    for pypi packages)
    #  - some URLs are "fake" URLs that don't actually correspond to something we can
    #    actually download or use like local paths for locally built PYPI packages or
    #    pointers to GIT repositories (ie: there is no one file pointed to by that URL).
    #    We still use the URL as a unique identifier but mark it as fake
    #  - we therefore form the cache url using the following components ("/" separated):
    #    - pkg_type: so pypi or conda (pip as legacy)
    #    - a special marker for fake URLs (if needed)
    #    - the netloc of the base source URL
    #    - the path in the base source URL
    #    - the filename
    #    - the hash of that file
    #    - the filename
    #
    # Concretely, say our source url is https://foo/bar/baz.conda and we want to store
    # a .tar.bz2 version of the file and the .conda version of the file, in cache we
    # would have:
    #  - conda/foo/bar/baz.conda/baz.conda/<hash>/baz.conda
    #  - conda/foo/bar/baz.conda/baz.tar.bz2/<hash>/baz.tar.bz2
    #
    # If we have a GIT repository like git+https://github.com/foo/myrepo/@123#subdirectory=bar,
    # we would have:
    #  - pypi/github.com/foo/myrepo/123/bar/mypackage.whl/<hash>/mypackage.whl
    TYPE = "invalid"

    _class_per_type = None  # type: Optional[Dict[str, Type[CachePackage]]]

    @staticmethod
    def _ensure_class_per_type():
        if CachePackage._class_per_type is None:
            CachePackage._class_per_type = {
                c.TYPE: c for c in CachePackage.__subclasses__()
            }

    @classmethod
    def make_partial_cache_url(cls, base_url: str, is_real_url: bool = True):
        # This method returns the base cache URL to use (so does not include the filename
        # onwards)
        cls._ensure_class_per_type()
        url = urlparse(base_url)

        if is_real_url or url.netloc.split("/")[0] == FAKEURL_PATHCOMPONENT:
            return os.path.join(
                cast(str, CONDA_PACKAGES_DIRNAME),
                cls.TYPE,
                url.netloc,
                url.path.lstrip("/"),
            )
        else:
            return os.path.join(
                cast(str, CONDA_PACKAGES_DIRNAME),
                cls.TYPE,
                FAKEURL_PATHCOMPONENT,
                url.netloc,
                url.path.lstrip("/"),
            )

    @classmethod
    def make_cache_url(
        cls,
        base_url: str,
        filename: str,
        file_format: str,
        file_hash: str,
        is_real_url: bool = True,
    ) -> str:
        cls._ensure_class_per_type()

        filename_with_ext = "%s%s" % (filename, file_format)
        if not file_format or file_format not in cls.allowed_formats():
            raise ValueError(
                "File format '%s' for make_cache_url should be a supported file format %s"
                % (file_format, str(cls.allowed_formats()))
            )
        base_cache_url = cls.make_partial_cache_url(base_url, is_real_url)
        return os.path.join(
            base_cache_url, filename_with_ext, file_hash, filename_with_ext
        )

    def __init__(self, url: str):
        self._url = url
        basename, filename = os.path.split(url)
        _, self._pkg_fmt = correct_splitext(filename)
        if self._pkg_fmt not in self.allowed_formats():
            raise ValueError(
                "URL '%s' does not end with a supported file format %s"
                % (url, str(self.allowed_formats()))
            )
        basename, self._hash = os.path.split(basename)

    @classmethod
    def allowed_formats(cls) -> Sequence[str]:
        raise NotImplementedError()

    @property
    def url(self) -> str:
        return self._url

    @property
    def hash(self) -> str:
        return self._hash

    @property
    def format(self) -> str:
        return self._pkg_fmt  # type: ignore

    def to_dict(self) -> Dict[str, Any]:
        return {"_type": self.TYPE, "url": self._url}

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        cls._ensure_class_per_type()
        assert cls._class_per_type
        t = d["_type"]
        if t == "pip":
            # Support legacy files that mention "pip" packages (now called "pypi")
            t = "pypi"
        return cls._class_per_type[t](url=d["url"])

    def __str__(self):
        return "%s#%s" % (self.url, self.hash)


class CondaCachePackage(CachePackage):
    TYPE = "conda"

    @classmethod
    def allowed_formats(cls) -> Sequence[str]:
        from .utils import CONDA_FORMATS

        return CONDA_FORMATS


class PypiCachePackage(CachePackage):
    TYPE = "pypi"

    @classmethod
    def allowed_formats(cls) -> Sequence[str]:
        return _ALL_PYPI_FORMATS


class PackageSpecification:
    TYPE = "invalid"

    _class_per_type = None  # type: Optional[Dict[str, Type[PackageSpecification]]]

    def __init__(
        self,
        filename: str,
        url: str,
        is_real_url: bool = True,
        url_format: Optional[str] = None,
        hashes: Optional[Dict[str, str]] = None,
        cache_info: Optional[Dict[str, CachePackage]] = None,
    ):
        # Some URLs are fake when the package is built on the fly. We use the URL
        # as a unique identifier for the package so we still have a URL but it is not
        # downloadable.
        # if "/" in filename and not filename.startswith(self.TYPE):
        #     raise ValueError(
        #         "Attempting to create a package of type %s with filename %s"
        #         % (self.TYPE, filename)
        #     )
        # if self.TYPE != "conda" and not filename.startswith("%s/" % self.TYPE):  # type: ignore
        #     filename = "/".join([self.TYPE, filename])
        self._filename = filename

        self._url = url
        if url_format is None:
            url_format = correct_splitext(self._url)[1]
            if url_format not in self.allowed_formats():
                raise ValueError(
                    "URL '%s' does not end in a known ending (%s)"
                    % (self._url, str(self.allowed_formats()))
                )
        self._url_format = url_format
        self._hashes = hashes or {}
        self._cache_info = cache_info or {}

        if not is_real_url:
            # If it is not a real URL, add the FAKEURL_PATHCOMPONENT but only if not
            # already there.
            url_parse_result = urlparse(self._url)
            if not url_parse_result.netloc.startswith(FAKEURL_PATHCOMPONENT):
                self._url = urlunparse(
                    (
                        url_parse_result.scheme,
                        os.path.join(FAKEURL_PATHCOMPONENT, url_parse_result.netloc),
                        url_parse_result.path,
                        url_parse_result.params,
                        url_parse_result.query,
                        url_parse_result.fragment,
                    )
                )

        (
            self._package_name,
            self._package_version,
            self._package_detailed_version,
        ) = self._split_filename()

        # Additional information used for local book-keeping as we are updating
        # the package
        self._local_dir = None  # type: Optional[str]
        self._local_path = {}  # type: Dict[str, str]
        self._is_fetched = []  # type: List[str]
        self._dirty = False

    def clone_with_filename(self, new_filename: str) -> "PackageSpecification":
        r = self.__class__(
            new_filename,
            self._url,
            self.is_downloadable_url(),
            url_format=self._url_format,
            hashes=self._hashes,
            cache_info=self._cache_info,
        )
        r._local_dir = self._local_dir
        r._local_path = self._local_path
        r._is_fetched = self._is_fetched
        return r

    @property
    def filename(self) -> str:
        return self._filename

    @property
    def package_name(self) -> str:
        return self._package_name

    def package_name_with_channel(
        self, ignore_channels: Optional[List[str]] = None
    ) -> str:
        if ignore_channels is None:
            ignore_channels = []
        if self.TYPE != "conda":
            return self.package_name
        if self.is_downloadable_url():
            # Extract the channel information from the URL. We only extract implicit
            # channels (ie: things like comet_ml) that can be embedded in the name
            # (for example: comet_ml::comet_ml)
            channel = channel_from_url(self.url)
            if channel and channel not in ignore_channels:
                return "%s::%s" % (channel, self.package_name)
        return self.package_name

    @property
    def package_version(self) -> str:
        return self._package_version

    @property
    def package_detailed_version(self) -> str:
        return self._package_detailed_version

    @property
    def url(self) -> str:
        return self._url

    @property
    def url_format(self) -> str:
        return self._url_format

    @property
    def dirty(self) -> bool:
        return self._dirty

    @classmethod
    def cache_pkg_type(cls) -> Type[CachePackage]:
        raise NotImplementedError

    def pkg_hash(self, pkg_format: str) -> Optional[str]:
        return self._hashes.get(pkg_format)

    @property
    def pkg_hashes(self) -> Iterable[Tuple[str, str]]:
        for pkg_fmt, pkg_hash in self._hashes.items():
            yield (pkg_fmt, pkg_hash)

    def add_pkg_hash(self, pkg_format: str, pkg_hash: str):
        old_hash = self.pkg_hash(pkg_format)
        if old_hash:
            if old_hash != pkg_hash:
                raise ValueError(
                    "Attempting to add an inconsistent hash for package %s; "
                    "adding %s when already have %s"
                    % (self.filename, pkg_hash, old_hash)
                )
            return
        self._dirty = True
        self._hashes[pkg_format] = pkg_hash

    @property
    def local_dir(self) -> Optional[str]:
        # Returns the local directory found for this package
        return self._local_dir

    def local_file(self, pkg_format: str) -> Optional[str]:
        # Return the local tar-ball for this package (depending on the format)
        return self._local_path.get(pkg_format)

    @property
    def local_files(self) -> Iterable[Tuple[str, str]]:
        for pkg_fmt in self.allowed_formats():
            local_path = self._local_path.get(pkg_fmt)
            if local_path:
                yield (pkg_fmt, local_path)

    def is_fetched(self, pkg_format: str) -> bool:
        # Return whether the local tar-ball for this package had to be fetched from
        # either cache or web
        return pkg_format in self._is_fetched

    def is_downloadable_url(self, pkg_format: Optional[str] = None) -> bool:
        if not pkg_format:
            pkg_format = self._url_format
        return pkg_format == self._url_format and not urlparse(
            self._url
        ).netloc.startswith(FAKEURL_PATHCOMPONENT)

    def is_derived(self) -> bool:
        # If the filename component of the URL does not match the filename of this package,
        # this means we derived the package from the URL in a non obvious manner
        # (transmutations don't count here -- this would return false because both
        # formats are equivalent -- this is not necessarily the case from a source
        # tar ball and a built wheel)
        url_filename_with_ext = os.path.split(urlparse(self._url).path)[1]
        url_filename = correct_splitext(url_filename_with_ext)[0]
        return url_filename != self._filename

    def can_add_filename(self, filename_with_ext: str) -> bool:
        # Tests if a filename is a compatible filename for this package. This is used
        # when there are multiple possibilities with PYPI packages for example
        raise NotImplementedError

    def add_local_dir(self, local_path: str):
        # Add a local directory that is present for this package
        local_dir = self.local_dir
        if local_dir:
            if local_dir != local_path:
                raise ValueError(
                    "Attempting to add an inconsistent local directory for package %s; "
                    "adding %s when already have %s"
                    % (self.filename, local_path, local_dir)
                )
            return
        self._dirty = True
        self._local_dir = local_path

    def add_local_file(
        self,
        pkg_format: str,
        local_path: str,
        pkg_hash: Optional[str] = None,
        downloaded: bool = False,
        replace: bool = False,
    ):
        # Add a local file for this package indicating whether it was downloaded
        existing_path = self.local_file(pkg_format)
        if not replace and existing_path and local_path != existing_path:
            raise ValueError(
                "Attempting to add inconsistent local files of format %s for a package %s; "
                "adding %s when already have %s"
                % (pkg_format, self.filename, local_path, existing_path)
            )

        known_hash = self._hashes.get(pkg_format)
        added_hash = pkg_hash or self.hash_pkg(local_path)
        if not replace and known_hash and known_hash != added_hash:
            raise ValueError(
                "Attempting to add inconsistent local files of format %s for package %s; "
                "got a hash of %s but expected %s"
                % (pkg_format, self.filename, added_hash, known_hash)
            )
        self._dirty = replace or existing_path is None or known_hash is None
        self._local_path[pkg_format] = local_path
        self._hashes[pkg_format] = added_hash

        if downloaded and pkg_format not in self._is_fetched:
            self._dirty = True
            self._is_fetched.append(pkg_format)

    def cached_version(self, pkg_format: str) -> Optional[CachePackage]:
        return self._cache_info.get(
            self._url_format if pkg_format == "_any" else pkg_format
        )

    @property
    def cached_versions(self) -> Iterable[Tuple[str, CachePackage]]:
        for pkg_fmt, cached in self._cache_info.items():
            yield (pkg_fmt, cached)

    def add_cached_version(self, pkg_format: str, cache_info: CachePackage) -> None:
        if cache_info.TYPE != self.TYPE:
            raise ValueError(
                "Attempting to add a cache package of type %s for a package of type %s"
                % (cache_info.TYPE, self.TYPE)
            )

        old_cache_info = self.cached_version(pkg_format)
        if old_cache_info:
            if (
                old_cache_info.url != cache_info.url
                or old_cache_info.hash != cache_info.hash
            ):
                raise ValueError(
                    "Attempting to add inconsistent cache information for format %s of package %s; "
                    "adding %s when already have %s"
                    % (pkg_format, self.filename, cache_info, old_cache_info)
                )
        else:
            self._dirty = True
            self._cache_info[pkg_format] = cache_info

        old_pkg_hash = self.pkg_hash(pkg_format)
        if old_pkg_hash:
            if old_pkg_hash != cache_info.hash:
                raise ValueError(
                    "Attempting to add inconsistent cache information for format %s of package %s; "
                    "adding a package with hash %s when expected %s"
                    % (pkg_format, self.filename, cache_info.hash, old_pkg_hash)
                )
        else:
            self._dirty = True
            self._hashes[pkg_format] = cache_info.hash

    def is_cached(self, formats: List[str]) -> bool:
        if formats:
            return all(
                [
                    self._url_format if f == "_any" else f in self._cache_info
                    for f in formats
                ]
            )
        return len(self._cache_info) > 0

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "_type": self.TYPE,
            "filename": self.filename,
            "url": self.url,
            "url_format": self.url_format,
            "hashes": self._hashes,
        }  # type: Dict[str, Any]
        if self._cache_info:
            cache_d = {
                pkg_format: info.to_dict()
                for pkg_format, info in self._cache_info.items()
            }
            d["cache_info"] = cache_d
        return d

    def _split_filename(self) -> Tuple[str, str, str]:
        raise NotImplementedError

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        if cls._class_per_type is None:
            cls._class_per_type = {c.TYPE: c for c in cls.__subclasses__()}

        cache_info = d.get("cache_info", {})  # type: Dict[str, Any]
        url_format = d["url_format"]
        t = d["_type"]
        if t == "pip":
            # Legacy support for "pip" packages, now called "pypi"
            t = "pypi"
        return cls._class_per_type[t](
            filename=d["filename"],
            url=d["url"],
            url_format=url_format,
            hashes=d["hashes"],
            cache_info={
                pkg_fmt: CachePackage.from_dict(info)
                for pkg_fmt, info in cache_info.items()
            },
        )

    @classmethod
    def allowed_formats(cls) -> Sequence[str]:
        raise NotImplementedError()

    @classmethod
    def base_hash(cls):
        raise NotImplementedError()

    @classmethod
    def base_hash_name(cls) -> str:
        raise NotImplementedError()

    @classmethod
    def hash_pkg(cls, path: str) -> str:
        base_hash = cls.base_hash()
        with open(path, "rb") as f:
            for byte_block in iter(lambda: f.read(8192), b""):
                base_hash.update(byte_block)
        return base_hash.hexdigest()

    def __str__(self):
        return "<%s package: %s>" % (self.TYPE, self.filename)


class CondaPackageSpecification(PackageSpecification):
    TYPE = "conda"

    @classmethod
    def cache_pkg_type(cls):
        return CondaCachePackage

    @classmethod
    def allowed_formats(cls) -> Sequence[str]:
        from .utils import CONDA_FORMATS

        return CONDA_FORMATS

    @classmethod
    def base_hash(cls):
        return md5()

    @classmethod
    def base_hash_name(cls) -> str:
        return "md5"

    def can_add_filename(self, filename_with_ext: str) -> bool:
        # Tests if a filename is a compatible filename for this package. This is used
        # when there are multiple possibilities with PYPI packages for example
        ext = correct_splitext(filename_with_ext)[1]
        return ext in self.allowed_formats()

    def _split_filename(self) -> Tuple[str, str, str]:
        pkg, v, addl = self._filename.rsplit("-", 2)
        return pkg, v, "-".join([v, addl])


class PypiPackageSpecification(PackageSpecification):
    TYPE = "pypi"

    @classmethod
    def cache_pkg_type(cls):
        return PypiCachePackage

    @classmethod
    def allowed_formats(cls) -> Sequence[str]:
        return _ALL_PYPI_FORMATS

    @classmethod
    def base_hash(cls):
        return sha256()

    @classmethod
    def base_hash_name(cls) -> str:
        return "sha256"

    def can_add_filename(self, filename_with_ext: str) -> bool:
        # Tests if a filename is a compatible filename for this package. This is used
        # when there are multiple possibilities with PYPI packages for example
        base_filename, ext = correct_splitext(filename_with_ext)

        if ext == ".whl":
            # This will make sure the wheel matches the tags and what not
            return base_filename == self.filename
        else:
            # Source packages are always ok to add. It may have a different name
            return ext in _ALL_PYPI_FORMATS

    def _split_filename(self) -> Tuple[str, str, str]:
        try:
            # Try a source distribution first. We don't know which to try because the
            # url format may now repersent what is actually in this package (the URL
            # may be to the source but this represents a wheel).
            name, version = parse_sdist_filename("".join([self._filename, ".tar.gz"]))
            return (str(name), str(version), "")
        except ValueError:
            name, version, buildtag, _ = parse_wheel_filename(
                "".join([self._filename, ".whl"])
            )
            return (
                str(name),
                str(version),
                "" if len(buildtag) == 0 else "%s-%s" % (buildtag[0], buildtag[1]),
            )


class ResolvedEnvironment:
    def __init__(
        self,
        user_dependencies: Dict[str, List[str]],
        user_sources: Optional[Dict[str, List[str]]],
        user_extra_args: Optional[Dict[str, List[str]]],
        arch: Optional[str] = None,
        env_id: Optional[EnvID] = None,
        all_packages: Optional[Sequence[PackageSpecification]] = None,
        resolved_on: Optional[datetime] = None,
        resolved_by: Optional[str] = None,
        co_resolved: Optional[List[str]] = None,
        env_type: EnvType = EnvType.MIXED,
        accurate_source: bool = True,
    ):
        self._env_type = env_type
        self._user_dependencies = dict_to_tstr(user_dependencies)
        # We sort the user dependencies as the order does not matter and it makes
        # it easier for users to find their dependencies
        self._user_dependencies.sort(key=lambda k: k.value)
        self._user_sources = dict_to_tstr(user_sources) if user_sources else []
        self._user_extra_args = dict_to_tstr(user_extra_args) if user_extra_args else []

        self._accurate_source = accurate_source

        if not env_id:
            env_req_id = ResolvedEnvironment.get_req_id(
                user_dependencies, user_sources or {}, user_extra_args or {}
            )

            env_full_id = "_unresolved"
            if all_packages is not None:
                env_full_id = self._compute_hash(
                    [
                        "%s#%s" % (p.filename, p.pkg_hash(p.url_format))
                        for p in all_packages
                    ]
                    + [arch or arch_id()]
                )
            self._env_id = EnvID(
                req_id=env_req_id, full_id=env_full_id, arch=arch or arch_id()
            )
        else:
            self._env_id = env_id
        self._all_packages = list(all_packages) if all_packages else []
        self._resolved_on = resolved_on or datetime.now()
        self._resolved_by = (
            resolved_by or cast(Optional[str], get_username()) or "unknown"
        )
        self._co_resolved = co_resolved or [arch or arch_id()]
        self._parent = None  # type: Optional["CachedEnvironmentInfo"]
        self._dirty = False

    @staticmethod
    def get_req_id(
        deps: Dict[str, List[str]],
        sources: Optional[Dict[str, List[str]]] = None,
        extra_args: Optional[Dict[str, List[str]]] = None,
    ) -> str:
        if sources is None:
            sources = {}
        if extra_args is None:
            extra_args = {}
        return ResolvedEnvironment._compute_hash(
            chain(
                *(
                    map(lambda x, c=c: str(TStr(c, x)), sorted(deps[c]))
                    for c in sorted(deps)
                ),
                *(
                    map(lambda x, c=c: str(TStr(c, x)), sorted(sources[c]))
                    for c in sorted(sources)
                ),
                *(
                    map(lambda x, c=c: str(TStr(c, x)), sorted(extra_args[c]))
                    for c in sorted(extra_args)
                )
            )
        )

    @staticmethod
    def set_coresolved_full_id(envs: Sequence["ResolvedEnvironment"]) -> None:
        envs = sorted(envs, key=lambda x: x.env_id.arch)
        to_hash = []  # type: List[str]
        archs = []  # type: List[str]
        for env in envs:
            archs.append(env.env_id.arch)
            to_hash.append(env.env_id.arch)
            to_hash.extend(
                [
                    "%s#%s" % (p.filename, p.pkg_hash(p.url_format))
                    for p in sorted(env.packages, key=lambda p: p.filename)
                ]
            )
        new_full_id = ResolvedEnvironment._compute_hash(to_hash)
        for env in envs:
            env.set_coresolved(archs, new_full_id)

    @property
    def is_info_accurate(self) -> bool:
        # Returns True if the requirements for this environment are accurate. This is
        # currently always the case. This code was initially added in case we could
        # get an environment from a FULL_ID which doesn't guarantee uniqueness of the
        # requirements part (different requirements can resolve to the same environment)
        return self._accurate_source

    @property
    def deps(self) -> List[TStr]:
        return self._user_dependencies

    @property
    def sources(self) -> List[TStr]:
        return self._user_sources

    @property
    def extras(self) -> List[TStr]:
        return self._user_extra_args

    @property
    def env_id(self) -> EnvID:
        if self._env_id.full_id in ("_default", "_unresolved") and self._all_packages:
            all_packages = sorted(self._all_packages, key=lambda p: p.filename)
            env_full_id = self._compute_hash(
                [p.filename for p in all_packages] + [self._env_id.arch or arch_id()]
            )
            self._env_id = self._env_id._replace(full_id=env_full_id)
        return self._env_id

    @property
    def packages(self) -> Iterable[PackageSpecification]:
        # This returns the packages in the order in which they were added which
        # corresponds to the installation order. In some cases, the installation order
        # matters so we make sure to preserve it.
        for p in self._all_packages:
            yield p

    @property
    def resolved_on(self) -> datetime:
        return self._resolved_on

    @property
    def resolved_by(self) -> str:
        return self._resolved_by

    @property
    def co_resolved_archs(self) -> List[str]:
        return self._co_resolved

    @property
    def env_type(self) -> EnvType:
        return self._env_type

    @property
    def dirty(self) -> bool:
        if self._dirty:
            return True
        return any((p.dirty for p in self._all_packages))

    def set_parent(self, parent: "CachedEnvironmentInfo"):
        self._parent = parent

    def add_package(self, pkg: PackageSpecification):
        self._dirty = True
        self._all_packages.append(pkg)
        if self._env_id.full_id not in ("_default", "_unresolved"):
            self._env_id._replace(full_id="_unresolved")

    def set_coresolved(self, archs: List[str], full_id: str) -> None:
        self._dirty = True
        self._env_id = EnvID(
            req_id=self._env_id.req_id, full_id=full_id, arch=self._env_id.arch
        )
        self._co_resolved = archs

    def pretty_print(self, local_instances: Optional[List[str]]) -> str:
        lines = []  # type: List[str]
        pypi_packages = []  # type: List[PackageSpecification]
        conda_packages = []  # type: List[PackageSpecification]
        for p in self.packages:
            if p.TYPE == "pypi":
                pypi_packages.append(p)
            else:
                conda_packages.append(p)

        pypi_packages.sort(key=lambda x: x.package_name)
        conda_packages.sort(key=lambda x: x.package_name)

        if self._parent:
            immutable_aliases, mutable_aliases = self._parent.aliases_for_env(
                self.env_id
            )
            immutable_aliases = [
                a for a in immutable_aliases if a != self.env_id.full_id
            ]
            is_default = (
                self._parent.get_default(self.env_id.req_id, self.env_id.arch)
                == self.env_id
            )
        else:
            immutable_aliases = []
            mutable_aliases = []
            is_default = False

        lines.append(
            "*%sEnvironment of type %s full hash* %s:%s"
            % (
                "DEFAULT " if is_default else "",
                self.env_type.value,
                self.env_id.req_id,
                self.env_id.full_id,
            )
        )
        if immutable_aliases or mutable_aliases:
            lines.append(
                "*Aliases* %s"
                % ", ".join(
                    chain(
                        map(lambda x: ":".join(x.rsplit("/", 1)), immutable_aliases),
                        (
                            "%s (mutable)" % ":".join(a.rsplit("/", 1))
                            for a in mutable_aliases
                        ),
                    )
                )
            )
        lines.extend(
            [
                "*Arch* %s" % self.env_id.arch,
                "*Available on* %s" % ", ".join(self.co_resolved_archs),
                "",
                "*Resolved on* %s" % self.resolved_on,
                "*Resolved by* %s" % self.resolved_by,
                "",
            ]
        )
        if local_instances:
            lines.extend(["*Locally present as* %s" % ", ".join(local_instances), ""])

        # The `replace` is because the echo function uses "*" to split in bold/not bold
        # It is quite likely to have a package spec like "3.8.*" so we replace it with
        # the similar looking "3.8.x"
        lines.append(
            "*User-requested packages* %s"
            % ", ".join([str(d).replace("*", "x") for d in self.deps])
        )

        if self.sources:
            lines.append(
                "*User sources* %s" % ", ".join([str(s) for s in self.sources])
            )

        if self.extras:
            lines.append(
                "*Extra resolution flags* %s" % ", ".join([str(s) for s in self.extras])
            )
        lines.append("")

        if conda_packages:
            lines.append(
                "*Conda Packages installed* %s"
                % ", ".join(
                    [
                        "%s==%s"
                        % (
                            p.package_name,
                            p.package_detailed_version,
                        )
                        for p in conda_packages
                    ]
                )
            )

        if pypi_packages:
            lines.append(
                "*Pypi Packages installed* %s"
                % ", ".join(
                    [
                        "%s==%s" % (p.package_name, p.package_version)
                        for p in pypi_packages
                    ]
                )
            )
        lines.append("")
        return "\n".join(lines)

    def quiet_print(self, local_instances: Optional[List[str]] = None) -> str:
        pypi_packages = []  # type: List[PackageSpecification]
        conda_packages = []  # type: List[PackageSpecification]
        for p in self.packages:
            if p.TYPE == "pypi":
                pypi_packages.append(p)
            else:
                conda_packages.append(p)

        pypi_packages.sort(key=lambda x: x.package_name)
        conda_packages.sort(key=lambda x: x.package_name)

        if self._parent:
            immutable_aliases, mutable_aliases = self._parent.aliases_for_env(
                self.env_id
            )
            immutable_aliases = [
                a for a in immutable_aliases if a != self.env_id.full_id
            ]
        else:
            immutable_aliases = []
            mutable_aliases = []

        aliases = ",".join(
            chain(
                map(lambda x: ":".join(x.rsplit("/", 1)), immutable_aliases),
                ("%s(m)" % ":".join(a.rsplit("/", 1)) for a in mutable_aliases),
            )
        )
        return "%s %s %s %s %s %s %s %s %s conda:%s pypi:%s %s" % (
            self.env_id.req_id,
            self.env_id.full_id,
            aliases,
            self.env_id.arch,
            self.resolved_on.isoformat(),
            self.resolved_by,
            ",".join(self.co_resolved_archs),
            ",".join([str(d) for d in self.deps]),
            ",".join([str(s) for s in self.sources]) if self.sources else "NONE",
            ",".join(
                [
                    "%s==%s" % (p.package_name, p.package_detailed_version)
                    for p in conda_packages
                ]
            ),
            ",".join(
                ["%s==%s" % (p.package_name, p.package_version) for p in pypi_packages]
            ),
            ",".join(local_instances) if local_instances else "NONE",
        )

    def is_cached(self, formats: Dict[str, List[str]]) -> bool:
        return all([pkg.is_cached(formats.get(pkg.TYPE, [])) for pkg in self.packages])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "deps": [str(x) for x in self._user_dependencies],
            "sources": [str(x) for x in self._user_sources],
            "extras": [str(x) for x in self._user_extra_args],
            "packages": [p.to_dict() for p in self.packages],
            "resolved_on": self._resolved_on.isoformat(),
            "resolved_by": self._resolved_by,
            "resolved_archs": self._co_resolved,
            "env_type": self._env_type.value,
            "accurate_source": self._accurate_source,
        }

    @classmethod
    def from_dict(
        cls,
        env_id: EnvID,
        d: Mapping[str, Any],
    ):
        all_packages = [PackageSpecification.from_dict(pd) for pd in d["packages"]]
        env_type = EnvType(d.get("env_type", EnvType.MIXED.value))
        # Backward compatible aliasing
        if env_type == EnvType.PIP_ONLY:
            env_type = EnvType.PYPI_ONLY
        return cls(
            user_dependencies=tstr_to_dict([TStr.from_str(x) for x in d["deps"]]),
            user_sources=tstr_to_dict([TStr.from_str(x) for x in d["sources"]]),
            user_extra_args=tstr_to_dict(
                [TStr.from_str(x) for x in d.get("extras", [])]
            ),
            env_id=env_id,
            all_packages=all_packages,
            resolved_on=datetime.fromisoformat(d["resolved_on"]),
            resolved_by=d["resolved_by"],
            co_resolved=d["resolved_archs"],
            env_type=env_type,
            accurate_source=d.get("accurate_source", True),
        )

    @staticmethod
    def _compute_hash(inputs: Iterable[str]):
        return sha1(b" ".join([s.encode("ascii") for s in inputs])).hexdigest()


class CachedEnvironmentInfo:
    def __init__(
        self,
        version: int = 1,
        step_mappings: Optional[Dict[str, Tuple[str, str]]] = None,
        env_mutable_aliases: Optional[Dict[str, Tuple[str, str]]] = None,
        env_aliases: Optional[Dict[str, Tuple[str, str]]] = None,
        resolved_environments: Optional[
            Dict[str, Dict[str, Dict[str, Union[ResolvedEnvironment, str]]]]
        ] = None,
    ):
        self._version = version
        self._step_mappings = step_mappings if step_mappings else {}
        self._env_mutable_aliases = env_mutable_aliases if env_mutable_aliases else {}
        self._env_aliases = env_aliases if env_aliases else {}
        self._resolved_environments = (
            resolved_environments if resolved_environments else {}
        )

    def set_default(self, env_id: EnvID):
        per_arch_envs = self._resolved_environments.get(env_id.arch)
        if per_arch_envs is None:
            raise ValueError("No cached environments for %s" % env_id.arch)
        per_req_id_envs = per_arch_envs.get(env_id.req_id)
        if per_req_id_envs is None:
            raise ValueError(
                "No cached environments for requirement ID: %s" % env_id.req_id
            )
        per_req_id_envs["_default"] = env_id.full_id

    def get_default(self, req_id: str, arch: Optional[str]) -> Optional[EnvID]:
        arch = arch or arch_id()
        per_arch_envs = self._resolved_environments.get(arch)
        if per_arch_envs is None:
            return None
        per_req_id_envs = per_arch_envs.get(req_id)
        if per_req_id_envs is None:
            return None
        full_id = per_req_id_envs.get("_default")
        if full_id is None:
            return None
        elif isinstance(full_id, str):
            return EnvID(req_id=req_id, full_id=full_id, arch=arch)
        return full_id.env_id

    def clear_default(self, req_id: str, arch: Optional[str]) -> None:
        arch = arch or arch_id()
        per_arch_envs = self._resolved_environments.get(arch)
        if per_arch_envs is None:
            return
        per_req_id_envs = per_arch_envs.get(req_id)
        if per_req_id_envs is None:
            return
        if "_default" in per_req_id_envs:
            del per_req_id_envs["_default"]

    def add_resolved_env(self, env: ResolvedEnvironment):
        env_id = env.env_id
        per_arch_envs = self._resolved_environments.setdefault(env_id.arch, {})
        per_req_id_envs = per_arch_envs.setdefault(env_id.req_id, {})
        per_req_id_envs[env_id.full_id] = env

        env.set_parent(self)
        # Update _env_aliases adding the full_id so we can look up an environment
        # directly by the full_id
        v = self._env_aliases.setdefault(
            env_id.full_id, (env_id.req_id, env_id.full_id)
        )
        if v != (env_id.req_id, env_id.full_id):
            # This can happen if two req_ids resolve to the same full_id
            # For now we ignore and we will just use the first resolved environment
            # for that full-id
            pass

    def add_alias(
        self, alias_type: AliasType, resolved_alias: str, req_id: str, full_id: str
    ):
        if alias_type == AliasType.PATHSPEC:
            v = self._step_mappings.setdefault(resolved_alias, (req_id, full_id))
            if v != (req_id, full_id):
                raise ValueError(
                    "Assigning (%s, %s) to step '%s' but already have (%s, %s)"
                    % (req_id, full_id, resolved_alias, v[0], v[1])
                )
        elif alias_type == AliasType.FULL_ID or alias_type == AliasType.GENERIC:
            if is_alias_mutable(alias_type, resolved_alias):
                self._env_mutable_aliases[resolved_alias] = (req_id, full_id)
            else:
                # At this point, this is not a mutable alias
                v = self._env_aliases.setdefault(resolved_alias, (req_id, full_id))
                if v != (req_id, full_id):
                    raise ValueError("Alias '%s' is not mutable" % resolved_alias)
        # Missing AliasType.REQ_FULL_ID but we don't record aliases for that.

    def env_for(
        self, req_id: str, full_id: str = "_default", arch: Optional[str] = None
    ) -> Optional[ResolvedEnvironment]:
        arch = arch or arch_id()
        per_arch_envs = self._resolved_environments.get(arch)
        if per_arch_envs:
            per_req_id_envs = per_arch_envs.get(req_id)
            if per_req_id_envs:
                if full_id == "_default":
                    full_id = per_req_id_envs.get("_default", "_invalid")  # type: ignore
                return per_req_id_envs.get(full_id)  # type: ignore
        return None

    def envs_for(
        self, req_id: str, arch: Optional[str] = None
    ) -> Iterator[Tuple[EnvID, ResolvedEnvironment]]:
        arch = arch or arch_id()
        per_arch_envs = self._resolved_environments.get(arch)
        if per_arch_envs:
            per_req_id_envs = per_arch_envs.get(req_id)
            if per_req_id_envs:
                for env in per_req_id_envs.values():
                    if isinstance(env, ResolvedEnvironment):
                        yield env.env_id, env
                    # The other case is that env is a string (when the key in
                    # per_req_id_envs is "_default" but we will list that
                    # when we get to the actual environment for it so we don't output
                    # anything.

    def env_id_for_alias(
        self, alias_type: AliasType, resolved_alias: str, arch: Optional[str] = None
    ) -> Optional[EnvID]:
        if alias_type == AliasType.REQ_FULL_ID:
            req_id, full_id = resolved_alias.rsplit(":", 1)
        elif alias_type == AliasType.PATHSPEC:
            req_id, full_id = self._step_mappings.get(resolved_alias, (None, None))
        elif alias_type == AliasType.GENERIC or alias_type == AliasType.FULL_ID:
            req_id, full_id = self._env_aliases.get(
                resolved_alias,
                self._env_mutable_aliases.get(resolved_alias, (None, None)),
            )
        else:
            raise ValueError("Alias type not supported")
        if req_id and full_id:
            return EnvID(req_id=req_id, full_id=full_id, arch=arch or arch_id())
        return None

    def env_for_alias(
        self,
        alias_type: AliasType,
        resolved_alias: str,
        arch: Optional[str] = None,
    ) -> Optional[ResolvedEnvironment]:
        env_id = self.env_id_for_alias(alias_type, resolved_alias, arch)
        if env_id:
            return self.env_for(resolved_alias[0], resolved_alias[1], arch)

    def aliases_for_env(self, env_id: EnvID) -> Tuple[List[str], List[str]]:
        # Returns immutable and mutable aliases
        # We don't cache this as this is fairly infrequent use (just CLI)
        immutable_aliases = [
            k
            for k, v in self._env_aliases.items()
            if v == (env_id.req_id, env_id.full_id)
        ]
        mutable_aliases = [
            k
            for k, v in self._env_mutable_aliases.items()
            if v == (env_id.req_id, env_id.full_id)
        ]
        return (immutable_aliases, mutable_aliases)

    @property
    def envs(self) -> Iterator[Tuple[EnvID, ResolvedEnvironment]]:
        for arch, per_arch_envs in self._resolved_environments.items():
            for req_id, per_req_id_envs in per_arch_envs.items():
                for full_id, env in per_req_id_envs.items():
                    if isinstance(env, ResolvedEnvironment):
                        yield (
                            EnvID(req_id=req_id, full_id=full_id, arch=arch),
                            env,
                        )

    def update(self, cached_info: "CachedEnvironmentInfo"):
        if self._version != cached_info._version:
            raise ValueError(
                "Cannot update an environment of version %d with one of version %d"
                % (self._version, cached_info._version)
            )
        self._step_mappings.update(cached_info._step_mappings)
        self._env_mutable_aliases.update(cached_info._env_mutable_aliases)
        self._env_aliases.update(cached_info._env_aliases)

        for arch, per_arch_envs in cached_info._resolved_environments.items():
            per_arch_resolved_env = self._resolved_environments.setdefault(arch, {})
            for req_id, per_req_id_envs in per_arch_envs.items():
                per_req_id_resolved_env = per_arch_resolved_env.setdefault(req_id, {})
                per_req_id_resolved_env.update(per_req_id_envs)

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "version": 1,
            "mappings": self._step_mappings,
            "mutable_aliases": self._env_mutable_aliases,
            "aliases": self._env_aliases,
        }  # type: Dict[str, Any]
        resolved_envs = {}
        for arch, per_arch_envs in self._resolved_environments.items():
            per_arch_resolved_env = {}
            for req_id, per_req_id_envs in per_arch_envs.items():
                per_req_id_resolved_env = {}
                for full_id, env in per_req_id_envs.items():
                    if isinstance(env, ResolvedEnvironment):
                        per_req_id_resolved_env[full_id] = env.to_dict()
                    elif full_id == "_unresolved":
                        # This is a non resolved environment -- this should not happen
                        continue
                    else:
                        # This is for the special "_default" key
                        per_req_id_resolved_env[full_id] = env
                per_arch_resolved_env[req_id] = per_req_id_resolved_env
            resolved_envs[arch] = per_arch_resolved_env
        result["environments"] = resolved_envs
        return result

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        version = int(d.get("version", -1))
        if version != 1:
            raise ValueError("Wrong version information for CachedInformationInfo")

        step_mappings = d.get("mappings", {})  # type: Dict[str, Tuple[str, str]]
        aliases = d.get("aliases", {})  # type: Dict[str, Tuple[str, str]]
        mutable_aliases = d.get(
            "mutable_aliases", {}
        )  # type: Dict[str, Tuple[str, str]]

        for k, v in step_mappings.items():
            step_mappings[k] = tuple(v)
        for k, v in aliases.items():
            aliases[k] = tuple(v)
        for k, v in mutable_aliases.items():
            mutable_aliases[k] = tuple(v)

        resolved_environments_dict = d.get("environments", {})
        resolved_environments = (
            {}
        )  # type: Dict[str, Dict[str, Dict[str, Union[ResolvedEnvironment, str]]]]
        for arch, per_arch_envs in resolved_environments_dict.items():
            # Parse the aliases first to make sure we have all of them
            resolved_per_req_id = {}
            for req_id, per_req_id_envs in per_arch_envs.items():
                resolved_per_full_id = (
                    {}
                )  # type: Dict[str, Union[ResolvedEnvironment, str]]
                for full_id, env in per_req_id_envs.items():
                    if full_id == "_default":
                        # Special key meaning to use this fully resolved environment
                        # for anything that matches env_id but does not have a specific
                        # mapping
                        resolved_per_full_id[full_id] = env
                    else:
                        resolved_env = ResolvedEnvironment.from_dict(
                            EnvID(req_id=req_id, full_id=full_id, arch=arch), env
                        )
                        resolved_per_full_id[full_id] = resolved_env
                resolved_per_req_id[req_id] = resolved_per_full_id
            resolved_environments[arch] = resolved_per_req_id
        me = cls(
            version,
            step_mappings=step_mappings,
            env_mutable_aliases=mutable_aliases,
            env_aliases=aliases,
            resolved_environments=resolved_environments,
        )
        for per_arch in resolved_environments.values():
            for per_req in per_arch.values():
                for env in per_req.values():
                    if isinstance(env, ResolvedEnvironment):
                        env.set_parent(me)

        return me


def read_conda_manifest(ds_root: str) -> CachedEnvironmentInfo:
    path = get_conda_manifest_path(ds_root)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with os.fdopen(os.open(path, os.O_RDONLY), "r", encoding="utf-8") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_SH)
                return CachedEnvironmentInfo.from_dict(json.load(f))
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
    else:
        return CachedEnvironmentInfo()


def write_to_conda_manifest(ds_root: str, info: CachedEnvironmentInfo):
    path = get_conda_manifest_path(ds_root)
    try:
        os.makedirs(os.path.dirname(path))
    except OSError as x:
        if x.errno != errno.EEXIST:
            raise
    with os.fdopen(os.open(path, os.O_RDWR | os.O_CREAT), "r+", encoding="utf-8") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX)
            # We first read the file and then update it with the info we have
            # This makes sure that if the file was updated by another process between
            # the time we read it and now, we have a sum of all information. This can
            # happen if multiple runs are running on the same machine from the same
            # directory.
            current_content = CachedEnvironmentInfo()
            if os.path.getsize(path) > 0:
                # Not a new file
                f.seek(0)
                current_content = CachedEnvironmentInfo.from_dict(json.load(f))
            f.seek(0)
            f.truncate(0)
            current_content.update(info)
            json.dump(current_content.to_dict(), f)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
