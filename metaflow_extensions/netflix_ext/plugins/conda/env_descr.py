# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
from datetime import datetime
import errno
import fcntl
import json
import os
from hashlib import md5, sha1, sha256
from itertools import chain
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
    TypeVar,
    Union,
    cast,
)
from urllib.parse import urlparse

from metaflow.metaflow_config import ENV_PACKAGES_DIRNAME
from metaflow.util import get_username

from .utils import (
    CONDA_FORMATS,
    TRANSMUT_PATHCOMPONENT,
    CondaException,
    arch_id,
    convert_filepath,
    get_conda_manifest_path,
)

# Order should be maintained
EnvID = NamedTuple("EnvID", [("req_id", str), ("full_id", str), ("arch", str)])


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
                cast(str, ENV_PACKAGES_DIRNAME),
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
                cast(str, ENV_PACKAGES_DIRNAME),
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
        (
            self._package_name,
            self._package_version,
            self._package_detailed_version,
        ) = self._split_filename()

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

        # Additional information used for local book-keeping as we are updating
        # the package
        self._local_dir = None  # type: Optional[str]
        self._local_path = {}  # type: Dict[str, str]
        self._is_fetched = []  # type: List[str]
        self._is_transmuted = []  # type: List[str]

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
        if old_hash and old_hash != pkg_hash:
            raise ValueError(
                "Attempting to add an inconsistent hash for package %s; "
                "adding %s when already have %s" % (self.filename, pkg_hash, old_hash)
            )
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
        if local_dir and local_dir != local_path:
            raise ValueError(
                "Attempting to add an inconsistent local directory for package %s; "
                "adding %s when already have %s"
                % (self.filename, local_path, local_dir)
            )
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
        if existing_path and local_path != existing_path:
            raise ValueError(
                "Attempting to add inconsistent local files of format %s for a package %s; "
                "adding %s when already have %s"
                % (pkg_format, self.filename, local_path, existing_path)
            )
        self._local_path[pkg_format] = local_path
        known_hash = self._hashes.get(pkg_format)
        added_hash = pkg_hash or self._hash_pkg(local_path)
        if known_hash and known_hash != added_hash:
            raise ValueError(
                "Attempting to add inconsistent local files of format %s for package %s; "
                "got a hash of %s but expected %s"
                % (pkg_format, self.filename, added_hash, known_hash)
            )
        self._hashes[pkg_format] = added_hash
        if downloaded:
            self._is_fetched.append(pkg_format)
        if transmuted:
            self._is_transmuted.append(pkg_format)

    def cached_version(self, pkg_format: str) -> Optional[CachePackage]:
        return self._cache_info.get(pkg_format)

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
        if old_cache_info and (
            old_cache_info.url != cache_info.url
            or old_cache_info.hash != cache_info.hash
        ):
            raise ValueError(
                "Attempting to add inconsistent cache information for format %s of package %s; "
                "adding %s when already have %s"
                % (pkg_format, self.filename, cache_info, old_cache_info)
            )

        old_pkg_hash = self.pkg_hash(pkg_format)
        if old_pkg_hash and old_pkg_hash != cache_info.hash:
            raise ValueError(
                "Attempting to add inconsistent cache information for format %s of package %s; "
                "adding a package with hash %s when expected %s"
                % (pkg_format, self.filename, cache_info.hash, old_pkg_hash)
            )

        self._cache_info[pkg_format] = cache_info
        self._hashes[pkg_format] = cache_info.hash

    def is_cached(self, formats: List[str]) -> bool:
        if formats:
            return all([f in self._cache_info for f in formats])
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
        url_format = d.get("url_format")
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

    def _split_filename(self) -> Tuple[str, str, str]:
        name, version, _, _, _ = self._filename.rsplit("-", 4)
        if "." in name:
            # This means the version is actually a build identifier and version
            # is in name
            name, version = name.rsplit("-", 1)
        return (name, version, version)


TResolvedEnvironment = TypeVar("TResolvedEnvironment", bound="ResolvedEnvironment")


class ResolvedEnvironment:
    def __init__(
        self,
        user_dependencies: Sequence[TStr],
        user_sources: Optional[Sequence[TStr]],
        arch: Optional[str] = None,
        env_id: Optional[EnvID] = None,
        user_alias: Optional[str] = None,
        all_packages: Optional[Sequence[PackageSpecification]] = None,
        resolved_on: Optional[datetime] = None,
        resolved_by: Optional[str] = None,
        co_resolved: Optional[List[str]] = None,
    ):
        self._user_dependencies = list(user_dependencies)
        self._user_sources = list(user_sources) if user_sources else []
        if not env_id:
            env_req_id = ResolvedEnvironment.get_req_id(
                self._user_dependencies, self._user_sources
            )
            env_full_id = "_unresolved"
            if all_packages is not None:
                env_full_id = self._compute_hash([p.filename for p in all_packages])
            self._env_id = EnvID(
                req_id=env_req_id, full_id=env_full_id, arch=arch or arch_id()
            )
        else:
            self._env_id = env_id
        self._alias = user_alias
        self._all_packages = list(all_packages) if all_packages else []
        self._resolved_on = resolved_on or datetime.now()
        self._resolved_by = resolved_by or get_username() or "unknown"
        self._co_resolved = co_resolved or [arch or arch_id()]

    @staticmethod
    def get_req_id(
        deps: Sequence[TStr], sources: Optional[Sequence[TStr]] = None
    ) -> str:
        # Extract per category so we can sort independently for each category
        deps_by_category = {}  # type: Dict[str, List[str]]
        sources_by_category = {}  # type: Dict[str, List[str]]
        for d in deps:
            deps_by_category.setdefault(d.category, []).append(d.value)
        if sources:
            for s in sources:
                sources_by_category.setdefault(s.category, []).append(s.value)
        return ResolvedEnvironment._compute_hash(
            chain(
                *(sorted(deps_by_category[c]) for c in sorted(deps_by_category)),
                *(sorted(sources_by_category[c]) for c in sorted(sources_by_category))
            )
        )

    @staticmethod
    def set_coresolved_full_id(envs: Sequence[TResolvedEnvironment]) -> None:
        envs = sorted(envs, key=lambda x: x.env_id.arch)
        to_hash = []  # type: List[str]
        archs = []  # type: List[str]
        for env in envs:
            archs.append(env.env_id.arch)
            to_hash.append(env.env_id.arch)
            to_hash.extend([p.filename for p in env.packages])
        new_full_id = ResolvedEnvironment._compute_hash(to_hash)
        for env in envs:
            env.set_coresolved(archs, new_full_id)

    @property
    def deps(self) -> List[TStr]:
        return self._user_dependencies

    @property
    def sources(self) -> List[TStr]:
        return self._user_sources

    @property
    def env_id(self) -> EnvID:
        if self._env_id.full_id in ("_default", "_unresolved") and self._all_packages:
            env_full_id = self._compute_hash([p.filename for p in self._all_packages])
            self._env_id = self._env_id._replace(full_id=env_full_id)
        return self._env_id

    @property
    def env_alias(self) -> Optional[str]:
        return self._alias

    @property
    def packages(self) -> Iterable[PackageSpecification]:
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

    def add_package(self, pkg: PackageSpecification):
        self._all_packages.append(pkg)
        if self._env_id.full_id not in ("_default", "_unresolved"):
            self._env_id._replace(full_id="_unresolved")

    def set_coresolved(self, archs: List[str], full_id: str) -> None:
        self._env_id = EnvID(
            req_id=self._env_id.req_id, full_id=full_id, arch=self._env_id.arch
        )
        self._co_resolved = archs

    def is_cached(self, formats: Dict[str, List[str]]) -> bool:
        return all([pkg.is_cached(formats.get(pkg.TYPE, [])) for pkg in self.packages])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "deps": [str(x) for x in self._user_dependencies],
            "sources": [str(x) for x in self._user_sources],
            "packages": [p.to_dict() for p in self._all_packages],
            "alias": self._alias,
            "resolved_on": self._resolved_on.isoformat(),
            "resolved_by": self._resolved_by,
            "resolved_archs": self._co_resolved,
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
            env_id=env_id,
            user_alias=d["alias"],
            all_packages=all_packages,
            resolved_on=datetime.fromisoformat(d["resolved_on"]),
            resolved_by=d["resolved_by"],
            co_resolved=d["resolved_archs"],
        )

    @staticmethod
    def _compute_hash(inputs: Iterable[str]):
        return sha1(b" ".join([s.encode("ascii") for s in inputs])).hexdigest()


class CachedEnvironmentInfo:
    def __init__(
        self,
        step_mappings: Optional[Dict[str, Tuple[str, str]]],
        env_aliases: Optional[Dict[str, Dict[str, Tuple[str, str]]]],
        resolved_environments: Optional[
            Dict[str, Dict[str, Dict[str, Union[ResolvedEnvironment, str]]]]
        ],
    ):

        self._step_mappings = step_mappings if step_mappings else {}
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
        if env.env_alias:
            alias_info = self._env_aliases.setdefault(env.env_alias, {})
            alias_info[env_id.arch] = (env_id.req_id, env_id.full_id)

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

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "version": 1,
            "mappings": self._step_mappings,
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
        if int(d.get("version", -1)) != 1:
            raise ValueError("Wrong version information for CachedInformationInfo")

        aliases = d.get("aliases", {})  # type: Dict[str, Dict[str, Tuple[str, str]]]
        resolved_environments_dict = d.get("environments", {})
        resolved_environments = (
            {}
        )  # type: Dict[str, Dict[str, Dict[str, Union[ResolvedEnvironment, str]]]]
        for arch, per_arch_envs in resolved_environments_dict.items():
            # Parse the aliases first to make sure we have all of them
            aliases_per_arch = aliases.get(arch, {})
            reverse_map = {v: k for k, v in aliases_per_arch.items()}
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
                        alias = reverse_map.get((req_id, full_id), None)
                        resolved_env = ResolvedEnvironment.from_dict(
                            EnvID(req_id=req_id, full_id=full_id, arch=arch), env
                        )
                        if alias:
                            if resolved_env.env_alias != alias:
                                raise ValueError(
                                    "Alias '%s' does not match the one set in the environment '%s'"
                                    % (alias, resolved_env.env_alias)
                                )
                            del reverse_map[(req_id, full_id)]
                        resolved_per_full_id[full_id] = resolved_env
                resolved_per_req_id[req_id] = resolved_per_full_id
            resolved_environments[arch] = resolved_per_req_id
            if reverse_map:
                # If there is something left, it means we have aliases that are not resolved
                raise CondaException(
                    "Aliases %s do not map to known environments"
                    % ", ".join(d["aliases"].keys())
                )
        return cls(
            step_mappings=d["mappings"],
            env_aliases=d["aliases"],
            resolved_environments=resolved_environments,
        )


def read_conda_manifest(ds_root: str) -> CachedEnvironmentInfo:
    path = get_conda_manifest_path(ds_root)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, mode="r", encoding="utf-8") as f:
            return CachedEnvironmentInfo.from_dict(json.load(f))
    else:
        return CachedEnvironmentInfo(
            step_mappings=None, env_aliases=None, resolved_environments=None
        )


def write_to_conda_manifest(ds_root: str, info: CachedEnvironmentInfo):
    path = get_conda_manifest_path(ds_root)
    try:
        os.makedirs(os.path.dirname(path))
    except OSError as x:
        if x.errno != errno.EEXIST:
            raise
    with os.fdopen(
        os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC), "w", encoding="utf-8"
    ) as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(info.to_dict(), f)
        except IOError as e:
            if e.errno != errno.EAGAIN:
                raise
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
