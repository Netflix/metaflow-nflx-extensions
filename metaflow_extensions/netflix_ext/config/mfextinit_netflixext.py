import os
from metaflow.metaflow_config import (
    DATASTORE_SYSROOT_S3,
    DATASTORE_SYSROOT_AZURE,
    DATASTORE_SYSROOT_GS,
    DATASTORE_SYSROOT_LOCAL,
)
from metaflow.metaflow_config_funcs import from_conf, get_validate_choice_fn


CONDA_S3ROOT = from_conf(
    "CONDA_S3ROOT",
    os.path.join(DATASTORE_SYSROOT_S3, "conda_env") if DATASTORE_SYSROOT_S3 else None,
)

CONDA_AZUREROOT = from_conf(
    "CONDA_AZUREROOT",
    os.path.join(DATASTORE_SYSROOT_AZURE, "conda_env")
    if DATASTORE_SYSROOT_AZURE
    else None,
)

CONDA_GSROOT = from_conf(
    "CONDA_GSROOT",
    os.path.join(DATASTORE_SYSROOT_GS, "conda_env") if DATASTORE_SYSROOT_GS else None,
)

CONDA_LOCALROOT = from_conf(
    "CONDA_LOCALROOT",
    os.path.join(DATASTORE_SYSROOT_LOCAL, "conda_env")
    if DATASTORE_SYSROOT_LOCAL
    else None,
)

CONDA_MAGIC_FILE_V2 = "conda_v2.cnd"

# Use an alternate dependency resolver for conda packages instead of conda
# Mamba promises faster package dependency resolution times, which
# should result in an appreciable speedup in flow environment initialization.
CONDA_DEPENDENCY_RESOLVER = from_conf(
    "CONDA_DEPENDENCY_RESOLVER",
    "mamba",
    get_validate_choice_fn(["mamba", "conda", "micromamba"]),
)

# For pure PYPI environments, if you want to support those, set to the pypi resolver.
# Set to "none" if you do not want to support this functionality.
CONDA_PYPI_DEPENDENCY_RESOLVER = from_conf(
    "CONDA_PYPI_DEPENDENCY_RESOLVER", "pip", get_validate_choice_fn(["pip", "none"])
)

# For mixed conda/pypi environments, if you want to support those, set this to 'conda-lock'
# Set to "none" if you want to disable this functionality.
CONDA_MIXED_DEPENDENCY_RESOLVER = from_conf(
    "CONDA_MIXED_DEPENDENCY_RESOLVER",
    "conda-lock",
    get_validate_choice_fn(["conda-lock", "none"]),
)

# Timeout trying to acquire the lock to create environments
CONDA_LOCK_TIMEOUT = from_conf("CONDA_LOCK_TIMEOUT", 3600)

# Location within CONDA_<DS>ROOT of the packages directory
CONDA_PACKAGES_DIRNAME = from_conf("ENV_PACKAGES_DIRNAME", "packages")
# Ditto for the envs directory
CONDA_ENVS_DIRNAME = from_conf("CONDA_ENVS_DIRNAME", "envs")


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
    "none",
    get_validate_choice_fn([".tar.bz2", ".conda", "none"]),
)

CONDA_DEFAULT_PYPI_SOURCE = from_conf("CONDA_DEFAULT_PYPI_SOURCE", None)

# Allows you to specify URLs that need to be authenticated (basically Conda or PYPI
# package sources). Metaflow will glean all the information it can from traditional
# configuration sources (like pip.conf) but this allows you to specify additional URLs
# for which authentication is required. A dictionary should be passed in (or a JSON
# version of it in string form).
# key: hostname that needs authentication
# value: a tuple/list of username and password
CONDA_SRCS_AUTH_INFO = from_conf("CONDA_SRCS_AUTH_INFO", {})

CONDA_REMOTE_COMMANDS = ("batch", "kubernetes")

# List of system dependencies that are allowed to indicate the system to build on
CONDA_SYS_DEPENDENCIES = ("__cuda", "__glibc")

# Default system dependencies when not specified. Note that the `linux-64` defaults are
# used as default when building on the remote platform.
# As an example, you can set it to:
# CONDA_SYS_DEFAULT_PACKAGES = {
#     "linux-64": {"__glibc": os.environ.get("CONDA_OVERRIDE_GLIBC", "2.27")},
# }
CONDA_SYS_DEFAULT_PACKAGES = {}

# Packages to add when building for GPU machines (ie: if there is a GPU resource
# requirement). As an example you can set this to:
# CONDA_SYS_DEFAULT_GPU_PACKAGES = {
#     "__cuda": os.environ.get("CONDA_OVERRIDE_CUDA", "11.8")
# }
CONDA_SYS_DEFAULT_GPU_PACKAGES = {}


def _validate_remote_latest(name, value):
    if not value:
        raise MetaflowException("%s must not be empty." % name)
    if value[0] == ":" and value not in (":none:", ":username:", ":any:"):
        raise MetaflowException(
            "%s can only have special values ':none:', ':username:' or ':any:'" % name
        )


# Set to:
#  - :none: if default environments should never be looked up remotely
#  - :username: if default environments should be fetched remotely picking the latest
#    environment by the user
#  - :any: if default environments should be fetched remotely picking the latest
#    environment
#  - comma separated list of names: same as :username: but with a group of users.
CONDA_USE_REMOTE_LATEST = from_conf(
    "CONDA_USE_REMOTE_LATEST", ":none:", validate_fn=_validate_remote_latest
)

DEBUG_OPTIONS = ["conda"]
