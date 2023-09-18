# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
from __future__ import annotations

import os
import shutil

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Set, Tuple, cast

if TYPE_CHECKING:
    from metaflow.datastore.datastore_storage import DataStoreStorage
    from .conda import Conda

from metaflow.debug import debug

from metaflow_extensions.netflix_ext.vendor.packaging.tags import Tag
from metaflow_extensions.netflix_ext.vendor.packaging.utils import parse_wheel_filename

from .env_descr import (
    EnvType,
    PackageSpecification,
    PypiCachePackage,
    PypiPackageSpecification,
    ResolvedEnvironment,
)

from .resolvers.builder_envs_resolver import BuilderEnvsResolver

from .utils import (
    CondaException,
    arch_id,
    auth_from_urls,
    change_pypi_package_version,
    correct_splitext,
    parse_explicit_path_pypi,
)

_DEV_TRANS = str.maketrans("abcdef", "123456")


# This is a dataclass -- can move to that when we only support 3.7+
class PackageToBuild:
    def __init__(
        self,
        url: str,
        spec: Optional[PackageSpecification] = None,
        have_formats: Optional[List[str]] = None,
    ):
        self.url = url
        self.spec = spec
        self.have_formats = have_formats or []


def build_pypi_packages(
    conda: Conda,
    storage: DataStoreStorage,
    python_version: str,
    to_build_pkg_info: Dict[str, PackageToBuild],
    builder_envs: Optional[List[ResolvedEnvironment]],
    build_dir: str,
    architecture: str,
    supported_tags: List[Tag],
    pypi_sources: List[str],
) -> Tuple[List[PackageSpecification], Optional[List[ResolvedEnvironment]]]:
    # We check in the cache -- we don't actually have the filename or
    # hash so we check things starting with the partial URL.
    # The URL in cache will be:
    #  - <base url>/<filename>/<hash>/<filename>

    # REC: I extend here by overriding this method with my own
    return RuntimeError("Not supported in OSS")
