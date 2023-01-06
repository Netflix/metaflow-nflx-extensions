import os
from metaflow.metaflow_config import DATASTORE_SYSROOT_S3
from metaflow.metaflow_config_funcs import from_conf, get_validate_choice_fn


CONDA_S3ROOT = from_conf(
    "CONDA_S3ROOT",
    os.path.join(DATASTORE_SYSROOT_S3, "conda_env") if DATASTORE_SYSROOT_S3 else None,
)

CONDA_MAGIC_FILE_V2 = "conda_v2.cnd"

# Use an alternate dependency resolver for conda packages instead of conda
# Mamba promises faster package dependency resolution times, which
# should result in an appreciable speedup in flow environment initialization.
CONDA_DEPENDENCY_RESOLVER = from_conf(
    "CONDA_DEPENDENCY_RESOLVER", "mamba", get_validate_choice_fn(["mamba", "conda"])
)

# Timeout trying to acquire the lock to create environments
CONDA_LOCK_TIMEOUT = from_conf("CONDA_LOCK_TIMEOUT", 3600)

ENV_PACKAGES_DIRNAME = from_conf("ENV_PACKAGES_DIRNAME", "packages")

# CONDA_REMOTE_INSTALLER_DIRNAME = from_conf(
#     "CONDA_REMOTE_INSTALLER_DIRNAME", "conda-remote"
# )
CONDA_REMOTE_INSTALLER_DIRNAME = from_conf("CONDA_REMOTE_INSTALLER_DIRNAME")

# Binary within CONDA_REMOTE_INSTALLER_DIRNAME to use as the binary on remote instances.
# Use {arch} to specify the architecture. This should be fully functional binary
# CONDA_REMOTE_INSTALLER = from_conf("CONDA_REMOTE_INSTALLER", "conda-{arch}")
CONDA_REMOTE_INSTALLER = from_conf("CONDA_REMOTE_INSTALLER")


# Directory in CONDA_*ROOT that contains the local distribution tarballs. If not specified,
# a conda executable needs to be installed and will be used (if locatable in PATH)
# CONDA_LOCAL_DIST_DIRNAME = from_conf("CONDA_LOCAL_DIST_DIRNAME", "conda-local")
CONDA_LOCAL_DIST_DIRNAME = from_conf("CONDA_LOCAL_DIST_DIRNAME")

# Tar-ball containing the local distribution of conda to use.
# CONDA_LOCAL_DIST = from_conf("CONDA_LOCAL_DIST", "conda-{arch}.tgz")
CONDA_LOCAL_DIST = from_conf("CONDA_LOCAL_DIST")

# Path to the local conda distribution. If this is specified and a conda distribution
# does not exist at this path, Metaflow will attempt to install it using
# CONDA_LOCAL_DIST_DIRNAME and CONDA_LOCAL_DIST
# CONDA_LOCAL_PATH = from_conf("CONDA_LOCAL_PATH", "/usr/local/libexec/metaflow-conda")
CONDA_LOCAL_PATH = from_conf("CONDA_LOCAL_PATH")

# Preferred Format for Conda packages
CONDA_PREFERRED_FORMAT = from_conf(
    "CONDA_PREFERRED_FORMAT",
    None,
    get_validate_choice_fn([".tar.bz2", ".conda"]),
)

# Conda or conda-lock to resolve env
CONDA_PREFERRED_RESOLVER = from_conf(
    "CONDA_PREFERRED_RESOLVER",
    "conda-lock",
    get_validate_choice_fn(["conda", "conda-lock"]),
)

CONDA_DEFAULT_PIP_SOURCES = from_conf("CONDA_DEFAULT_PIP_SOURCES", [])

CONDA_REMOTE_COMMANDS = ("batch", "kubernetes")

DEBUG_OPTIONS = ["conda"]
