# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
from datetime import datetime
import errno
import fcntl
import json
import os
from enum import Enum
from hashlib import md5, sha1, sha256
from itertools import chain

from metaflow_extensions.netflix_ext.vendor.packaging.utils import (
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
from urllib.parse import urlparse

from metaflow.metaflow_config import CONDA_PACKAGES_DIRNAME
from metaflow.util import get_username

from .utils import (
    TRANSMUT_PATHCOMPONENT,
    AliasType,
    arch_id,
    convert_filepath,
    get_conda_manifest_path,
    is_alias_mutable,
)

# Order should be maintained
EnvID = NamedTuple("EnvID", [("req_id", str), ("full_id", str), ("arch", str)])

VALID_IMAGE_NAME_RE = "[^a-z0-9_]"


class EnvType(Enum):
    CONDA_ONLY = "conda-only"
    PIP_ONLY = "pip-only"
    MIXED = "mixed"


class TStr:
    def __init__(self, category: str, value: str):
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
        return TStr(splits[0], splits[1])


class CachePackage:

    TYPE = "invalid"

    _class_per_type = None  # type: Optional[Dict[str, Type[CachePackage]]]

    @staticmethod
    def _ensure_class_per_type():
        if CachePackage._class_per_type is None:
            CachePackage._class_per_type = {
                c.TYPE: c for c in CachePackage.__subclasses__()
            }

    @classmethod
    def make_partial_cache_url(cls, base_url: str):
        if cls.TYPE != "pip":
            raise ValueError("make_partial_cache_url only for pip packages")
        cls._ensure_class_per_type()
        url = urlparse(base_url)
        return os.path.join(
            cast(str, CONDA_PACKAGES_DIRNAME),
            cls.TYPE,
            url.netloc,
            url.path.lstrip("/"),
        )

    @classmethod
    def make_cache_url(
        cls,
        base_url: str,
        file_hash: str,
        file_format: Optional[str] = None,
        is_transmuted: bool = False,
    ) -> str:
        cls._ensure_class_per_type()
        url = urlparse(base_url)
        file_path, filename = convert_filepath(url.path, file_format)

        if is_transmuted:
            return os.path.join(
                cast(str, CONDA_PACKAGES_DIRNAME),
                cls.TYPE,
                TRANSMUT_PATHCOMPONENT,
                url.netloc,
                file_path.lstrip("/"),
                filename,
                file_hash,
                filename,
            )
        else:
            return os.path.join(
                cast(str, CONDA_PACKAGES_DIRNAME),
                cls.TYPE,
                url.netloc,
                file_path.lstrip("/"),
                filename,
                file_hash,
                filename,
            )

    def __init__(self, url: str):

        self._url = url
        basename, filename = os.path.split(url)

        self._pkg_fmt = None
        for f in self.allowed_formats():
            if filename.endswith(f):
                self._pkg_fmt = f
                break
        else:
            raise ValueError(
                "URL '%s' does not end with a supported file format %s"
                % (url, str(self.allowed_formats()))
            )
        basename, self._hash = os.path.split(basename)
        self._is_transmuted = TRANSMUT_PATHCOMPONENT in basename

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

    @property
    def is_transmuted(self) -> bool:
        return self._is_transmuted

    def to_dict(self) -> Dict[str, Any]:
        return {"_type": self.TYPE, "url": self._url}

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        cls._ensure_class_per_type()
        assert cls._class_per_type
        return cls._class_per_type[d["_type"]](url=d["url"])

    def __str__(self):
        return "%s#%s" % (self.url, self.hash)


class CondaCachePackage(CachePackage):
    TYPE = "conda"

    @classmethod
    def allowed_formats(cls) -> Sequence[str]:
        from .utils import CONDA_FORMATS

        return CONDA_FORMATS


class PipCachePackage(CachePackage):
    TYPE = "pip"

    @classmethod
    def allowed_formats(cls) -> Sequence[str]:
        return [".whl", ".tar.gz"]


class PackageSpecification:
    TYPE = "invalid"

    _class_per_type = None  # type: Optional[Dict[str, Type[PackageSpecification]]]

    def __init__(
        self,
        filename: str,
        url: str,
        url_format: Optional[str] = None,
        hashes: Optional[Dict[str, str]] = None,
        cache_info: Optional[Dict[str, CachePackage]] = None,
    ):
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
            for ending in self.allowed_formats():
                if self._url.endswith(ending):
                    url_format = ending
                    break
            else:
                raise ValueError(
                    "URL '%s' does not end in a known ending (%s)"
                    % (self._url, str(self.allowed_formats()))
                )
        self._url_format = url_format
        self._hashes = hashes or {}
        self._cache_info = cache_info or {}

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
        self._is_transmuted = []  # type: List[str]
        self._dirty = False

    @property
    def filename(self) -> str:
        return self._filename

    @property
    def package_name(self) -> str:
        return self._package_name

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
        for pkg_fmt, local_path in self._local_path.items():
            yield (pkg_fmt, local_path)

    def is_fetched(self, pkg_format: str) -> bool:
        # Return whether the local tar-ball for this package had to be fetched from
        # either cache or web
        return pkg_format in self._is_fetched

    def is_transmuted(self, pkg_format: str) -> bool:
        return pkg_format in self._is_transmuted

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
        transmuted: bool = False,
    ):
        # Add a local file for this package indicating whether it was downloaded or
        # transmuted
        existing_path = self.local_file(pkg_format)
        if existing_path:
            if local_path != existing_path:
                raise ValueError(
                    "Attempting to add inconsistent local files of format %s for a package %s; "
                    "adding %s when already have %s"
                    % (pkg_format, self.filename, local_path, existing_path)
                )
        else:
            self._dirty = True
            self._local_path[pkg_format] = local_path
        known_hash = self._hashes.get(pkg_format)
        added_hash = pkg_hash or self._hash_pkg(local_path)
        if known_hash:
            if known_hash != added_hash:
                raise ValueError(
                    "Attempting to add inconsistent local files of format %s for package %s; "
                    "got a hash of %s but expected %s"
                    % (pkg_format, self.filename, added_hash, known_hash)
                )
        else:
            self._dirty = True
            self._hashes[pkg_format] = added_hash
        if downloaded and pkg_format not in self._is_fetched:
            self._dirty = True
            self._is_fetched.append(pkg_format)
        if transmuted and pkg_format not in self._is_transmuted:
            self._dirty = True
            self._is_transmuted.append(pkg_format)

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
        return cls._class_per_type[d["_type"]](
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
    def _hash_pkg(cls, path: str) -> str:
        base_hash = cls.base_hash()
        with open(path, "rb") as f:
            for byte_block in iter(lambda: f.read(8192), b""):
                base_hash.update(byte_block)
        return base_hash.hexdigest()


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

    def _split_filename(self) -> Tuple[str, str, str]:
        pkg, v, addl = self._filename.rsplit("-", 2)
        return pkg, v, "-".join([v, addl])


class PipPackageSpecification(PackageSpecification):
    TYPE = "pip"

    @classmethod
    def cache_pkg_type(cls):
        return PipCachePackage

    @classmethod
    def allowed_formats(cls) -> Sequence[str]:
        return [".whl", ".tar.gz"]

    @classmethod
    def base_hash(cls):
        return sha256()

    @classmethod
    def base_hash_name(cls) -> str:
        return "sha256"

    def _split_filename(self) -> Tuple[str, str, str]:
        if self._url_format == ".whl":
            name, version, buildtag, _ = parse_wheel_filename(
                ".".join([self._filename, "whl"])
            )
            return (
                str(name),
                str(version),
                "" if len(buildtag) == 0 else "%s-%s" % (buildtag[0], buildtag[1]),
            )
        # In the case of a source distribution
        name, version = parse_sdist_filename(".".join([self._filename, ".tar.gz"]))
        return (str(name), str(version), "")


class ResolvedEnvironment:
    def __init__(
        self,
        user_dependencies: Sequence[TStr],
        user_sources: Optional[Sequence[TStr]],
        user_extra_args: Optional[Sequence[TStr]],
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
        self._user_dependencies = list(user_dependencies)
        self._user_sources = list(user_sources) if user_sources else []
        self._user_extra_args = list(user_extra_args) if user_extra_args else []
        if all_packages is not None:
            # It should already be sorted but being very safe
            all_packages = sorted(all_packages, key=lambda p: p.filename)

        self._accurate_source = accurate_source

        if not env_id:
            env_req_id = ResolvedEnvironment.get_req_id(
                self._user_dependencies, self._user_sources, self._user_extra_args
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
        self._resolved_by = resolved_by or get_username() or "unknown"
        self._co_resolved = co_resolved or [arch or arch_id()]
        self._parent = None  # type: Optional["CachedEnvironmentInfo"]
        self._dirty = False

    @staticmethod
    def get_req_id(
        deps: Sequence[TStr],
        sources: Optional[Sequence[TStr]] = None,
        extra_args: Optional[Sequence[TStr]] = None,
    ) -> str:
        # Extract per category so we can sort independently for each category
        deps_by_category = {}  # type: Dict[str, List[str]]
        sources_by_category = {}  # type: Dict[str, List[str]]
        extras_by_category = {}  # type: Dict[str, List[str]]
        for d in deps:
            deps_by_category.setdefault(d.category, []).append(d.value)
        if sources:
            for s in sources:
                sources_by_category.setdefault(s.category, []).append(s.value)
        if extra_args:
            for e in extra_args:
                extras_by_category.setdefault(e.category, []).append(e.value)
        return ResolvedEnvironment._compute_hash(
            chain(
                *(sorted(deps_by_category[c]) for c in sorted(deps_by_category)),
                *(sorted(sources_by_category[c]) for c in sorted(sources_by_category)),
                *(sorted(extras_by_category[c]) for c in sorted(extras_by_category))
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
                ["%s#%s" % (p.filename, p.pkg_hash(p.url_format)) for p in env.packages]
            )
        new_full_id = ResolvedEnvironment._compute_hash(to_hash)
        for env in envs:
            env.set_coresolved(archs, new_full_id)

    @property
    def is_info_accurate(self) -> bool:
        return not self._accurate_source

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
            self._all_packages.sort(key=lambda p: p.filename)
            env_full_id = self._compute_hash(
                [p.filename for p in self._all_packages]
                + [self._env_id.arch or arch_id()]
            )
            self._env_id = self._env_id._replace(full_id=env_full_id)
        return self._env_id

    @property
    def packages(self) -> Iterable[PackageSpecification]:
        # We always make sure it is sorted and we can do this by checking the env_id
        # which will sort _all_packages if not sorted
        _ = self.env_id
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
        pip_packages = []  # type: List[PackageSpecification]
        conda_packages = []  # type: List[PackageSpecification]
        for p in self.packages:
            if p.TYPE == "pip":
                pip_packages.append(p)
            else:
                conda_packages.append(p)

        pip_packages.sort(key=lambda x: x.package_name)
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
                        "%s==%s" % (p.package_name, p.package_version)
                        for p in conda_packages
                    ]
                )
            )

        if pip_packages:
            lines.append(
                "*Pip Packages installed* %s"
                % ", ".join(
                    [
                        "%s==%s" % (p.package_name, p.package_version)
                        for p in pip_packages
                    ]
                )
            )
        lines.append("")
        return "\n".join(lines)

    def quiet_print(self, local_instances: Optional[List[str]] = None) -> str:
        pip_packages = []  # type: List[PackageSpecification]
        conda_packages = []  # type: List[PackageSpecification]
        for p in self.packages:
            if p.TYPE == "pip":
                pip_packages.append(p)
            else:
                conda_packages.append(p)

        pip_packages.sort(key=lambda x: x.package_name)
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
        return "%s %s %s %s %s %s %s %s %s conda:%s pip:%s %s" % (
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
                ["%s==%s" % (p.package_name, p.package_version) for p in conda_packages]
            ),
            ",".join(
                ["%s==%s" % (p.package_name, p.package_version) for p in pip_packages]
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
        return cls(
            user_dependencies=[TStr.from_str(x) for x in d["deps"]],
            user_sources=[TStr.from_str(x) for x in d["sources"]],
            user_extra_args=[TStr.from_str(x) for x in d.get("extras", [])],
            env_id=env_id,
            all_packages=all_packages,
            resolved_on=datetime.fromisoformat(d["resolved_on"]),
            resolved_by=d["resolved_by"],
            co_resolved=d["resolved_archs"],
            env_type=EnvType(d.get("env_type", EnvType.MIXED.value)),
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
        with open(path, mode="r", encoding="utf-8") as f:
            return CachedEnvironmentInfo.from_dict(json.load(f))
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
            current_content.update(info)
            json.dump(current_content.to_dict(), f)
        except IOError as e:
            if e.errno != errno.EAGAIN:
                raise
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
