# Put Netflix specific configuration overrides here
import multiprocessing
import os
import sys
from typing import Optional

from metaflow.exception import MetaflowException
from metaflow.metaflow_config_funcs import from_conf, get_validate_choice_fn

USER = from_conf("USER", os.environ.get("BIGDATA_USER"))

###
# Default configuration
###
DEFAULT_DATASTORE = from_conf("DEFAULT_DATASTORE", "s3")
DEFAULT_ENVIRONMENT = from_conf("DEFAULT_ENVIRONMENT", "nflx")
DEFAULT_METADATA = from_conf("DEFAULT_METADATA", "mli")
DEFAULT_AWS_CLIENT_PROVIDER = from_conf("DEFAULT_AWS_CLIENT_PROVIDER", "bdp_boto")
DEFAULT_EVENT_LOGGER = from_conf("DEFAULT_EVENT_LOGGER", "keystoneLogger")
DEFAULT_MONITOR = from_conf("DEFAULT_MONITOR", "atlasMonitor")
DEFAULT_SECRETS_BACKEND_TYPE = from_conf(
    "DEFAULT_SECRETS_BACKEND_TYPE", "s3-secrets-provider"
)

### Spin Configuration
SPIN_PERSIST = from_conf("SPIN_PERSIST", True)

###
# Datastore configuration
###
# S3 bucket and prefix to store artifacts for 's3' datastore
# DATASTORE_SYSROOT_S3 =\
#     os.environ.get('METAFLOW_DATASTORE_SYSROOT_S3',
#                    's3://netflix-dataplatform-code/apps/metaflow/runs/convergence-test/')
# S3 datatools root location
# DATATOOLS_S3ROOT =\
#     os.environ.get('METAFLOW_DATATOOLS_S3ROOT',
#                    's3://netflix-dataplatform-code/apps/metaflow/userdata/convergence-test/')

###
# Conda configuration
###

# Set to True to be able to run local conda tests (without S3 etc).
# Used mostly in OSS
CONDA_TEST = from_conf("CONDA_TEST", False)
CONDA_IGNORE_CACHING_DATASTORES = from_conf(
    "CONDA_IGNORE_CACHING_DATASTORES", ["local", "spin"]
)

CONDA_S3ROOT = from_conf(
    "CONDA_S3ROOT", "s3://netflix-dataplatform-code/apps/metaflow/condav2/"
)

CONDA_MAGIC_FILE = "conda.cnd"
CONDA_MAGIC_FILE_V2 = from_conf("CONDA_MAGIC_FILE_V2", "condav2-1.cnd")

# Use an alternate dependency resolver for conda packages instead of conda
# Mamba promises faster package dependency resolution times, which
# should result in an appreciable speedup in flow environment initialization.
CONDA_DEPENDENCY_RESOLVER = from_conf(
    "CONDA_DEPENDENCY_RESOLVER",
    "mamba",
    get_validate_choice_fn(["mamba", "conda", "micromamba"]),
)

PYLOCK_TOML_DEPENDENCY_RESOLVER = from_conf(
    "PYLOCK_TOML_DEPENDENCY_RESOLVER",
    "pylock_toml",
    get_validate_choice_fn(["pylock_toml"]),
)

CONDA_PYPI_DEPENDENCY_RESOLVER = from_conf(
    "CONDA_PYPI_DEPENDENCY_RESOLVER", "uv", get_validate_choice_fn(["pip", "uv"])
)

CONDA_MIXED_DEPENDENCY_RESOLVER = from_conf(
    "CONDA_MIXED_DEPENDENCY_RESOLVER",
    "conda-lock",
    get_validate_choice_fn(["conda-lock"]),
)

# Timeout trying to acquire the lock to create environments
CONDA_LOCK_TIMEOUT = from_conf("CONDA_LOCK_TIMEOUT", 3600)

# Location within CONDA_<DS>ROOT of the packages directory
CONDA_PACKAGES_DIRNAME = from_conf("CONDA_PACKAGES_DIRNAME", "packages-1")
# Ditto for the envs directory
CONDA_ENVS_DIRNAME = from_conf("CONDA_ENVS_DIRNAME", "envs-1")
# Ditto for remote installers
CONDA_REMOTE_INSTALLER_DIRNAME = from_conf(
    "CONDA_REMOTE_INSTALLER_DIRNAME", "conda-remote"
)

# Override when using a consistent naming scheme for installed conda distribution.
# Note: set with METAFLOW__CONDA_VERSION_ID (double __)
# NOTE: For Macos, we have not been able to build a distribution yet so keeping the old
# one in place for now.
_CONDA_VERSION_ID = from_conf("_CONDA_VERSION_ID", "20251217")

# Ditto for local installation of conda. If not specified,
# a conda executable needs to be installed and will be used (if locatable in PATH)
CONDA_LOCAL_DIST_DIRNAME = from_conf("CONDA_LOCAL_DIST_DIRNAME", "conda-local")

# Binary within CONDA_REMOTE_INSTALLER_DIRNAME to use as the binary on remote instances.
# Use {arch} to specify the architecture. This should be fully functional binary
# To regenerate this binary, download micromamba for the proper platform and upload
# it here
CONDA_REMOTE_INSTALLER = from_conf(
    "CONDA_REMOTE_INSTALLER", f"conda-{{arch}}-{_CONDA_VERSION_ID}"
)

# Tar-ball containing the local distribution of conda to use.
# CHANGELOG:
#  - 20251217:
#    - build for linux-64 in /agent for jenkins
#  - 20251022:
#    - update all packages to the latest on this date
#    - conda-lock installed from git hash 41a6465ecf0c591eeb6e06d263e6bb84bf7dc55f and
#      requires a patch.
#      needed. NOTE: Mamba 2.3.2 is required (prior builds have a specific issue for
#      the way we use it)
#
#  - 20250203:
#    - MAMBA SEEMS TO STILL HAVE ISSUES AND 2.0.6 HAS BEEN MARKED AS BROKEN.
#    - update all packages to the latest on this date (but keep mamba and micromamba < 2)
#    - conda-lock installed from git hash fed224d505c352bec5fbdcefae0eb6622658d9c0
#    - add uv
#
#  - 20241207 (experimental):
#    - update all packages to the latest on this date (Mamba 2.0.4 in particular)
#    - conda-lock installed from git
#  - 20231206:
#    - update all packages to the latest on this date
#    - add patch for conda-lock to be able to deal with direct wheel URLs without hashes
#  - 20231024:
#    - update conda-lock to 2.4.0+ to get rid of pypi repository hack.
#    - all packages updated as well.
#  - 20230906:
#    - update all latest packages. Mamba no longer needs a patch
#  - 20230809:
#    - update mamba and micromamba to 1.4.9 -- this includes ZST support for Mamba in
#      particular. This broke some things so added a patch (should be fixed in next
#      mamba version)
#    - update conda-lock to 2.1.2 (no need for patch anymore)
#  - 20230530:
#    - use conda-lock 2.0.0 with patch (see below)
#  - 20230516:
#    - use conda-lock from Romain's branch (fixes pip confusion issue)
#    - update everything to latest (mamba to 1.4.2 for example)
#
# INSTRUCTIONS
# To generate the tar ball:
#   - run the script in scripts/create_conda_installer.sh passing it the name to assign
#     to the tar ball (so 20251022 for example) and the architecture to build for. You
#     should run the OSX architectures on a Mac machine and the linux one on a Linux
#     machine.
#
# What the script does (more or less):
#   - Download miniforge from https://github.com/conda-forge/miniforge
#   - install into CONDA_LOCAL_PATH (can change it to force re-downloading by user)
#   - update if needed (CONDA_LOCAL_PATH/bin/mamba -r CONDA_LOCAL_PATH update -n base mamba conda pip)
#   - install needed packages: (CONDA_LOCAL_PATH/bin/mamba -r CONDA_LOCAL_PATH install \
#       -n base conda-lock conda-package-handling micromamba boto3 uv)
#   - update .condarc (and copy to .mambarc) to contain:
#
# channels:
# - conda-forge
# pip_interop_enabled: true
# remote_max_retries: 10
# channel_priority: strict
# repodata_use_zst: true
#
#   - restrict search of env variables to just the one we install by modifying
#     conda/base/constants.py to have SEARCH_PATH = ('$CONDA_ROOT/.condarc', )
#   - clean the repodata (CONDA_LOCAL_PATH/mamba -r CONDA_LOCAL_PATH clean --all)
#   - From inside the directory, compress using tar -czvf ../CONDA_LOCAL_DIST .
#   - Note that you might have to use the --disable-copyfile flag in the previous command if you are generating the
#   distribution in a Mac machine. Thus, the command becomes: tar --disable-copyfile -czvf ../CONDA_LOCAL_DIST .
#   - Upload to S3 at the path specified by CONDA_LOCAL_DIST_DIRNAME
#   - if micromamba has been updated, you can also upload that to CONDA_REMOTE_INSTALLER_DIRNAME
#     as the proper file (for the right arch)
#
# OLD INSTRUCTIONS
#   - apply patch to mamba from https://github.com/mamba-org/mamba/3040/files
#     - NO LONGER NEEDED since 20250203 release
#   - patch conda-lock:
#     - NO LONGER NEEDED since 20250203 release
#     - go to lib/python3.10/site-packages
#     - patch -p1 < <repo>/nflx-metaflow/metaflow_extensions/nflx/plugins/conda/ops/conda-lock-2.patch
#   - patch mamba:
#     - NO LONGER NEEDED SINCE 20230906 release
#     - go to lib/python3.10/site-packages
#     - patch -p1 < <repo>/nflx-metaflow/metaflow_extensions/nflx/plugins/conda/ops/mamba.patch
#   - patch conda-lock:
#     - NO LONGER NEEDED SINCE 20230809 release
#     - go to lib/python3.10/site-packages
#     - patch -p1 < <repo>/nflx-metaflow/metaflow_extensions/nflx/plugins/conda/ops/conda-lock.patch


def _workbench_dir() -> Optional[str]:
    """
    Helper function to check if we're running in a Netflix workbench environment.

    Returns
    -------
        Optional[str]: Directory to use for Conda if running in a Netflix workbench
        environment, None otherwise
    """
    return "data" if os.environ.get("BD_ORIGIN_APP") is not None else None


def _workspace_dir() -> Optional[str]:
    """
    Helper function to check if we're running in a Netflix workspace environment.

    Returns
    -------
        Optional[str]: Directory to use for Conda if running in a Netflix workspace
        environment, None otherwise
    """
    return "mnt" if os.environ.get("WORKSPACE_NAME") is not None else None


def _darwin_dir() -> Optional[str]:
    """
    Helper function to check if we're running on a Mac machine.

    Returns
    -------
        Optional[str]: Directory to use for Conda if running on a Mac machine,
        None otherwise
    """
    return "tmp" if sys.platform == "darwin" else None


def _jenkins_dir() -> Optional[str]:
    """
    Helper function to check if we're running in Jenkins environment.

    Returns
    -------
        Optional[str]: Directory to use for Conda if running in Jenkins environment,
        None otherwise
    """
    # /agent in jenkins has more disk space
    # Slack thread: https://netflix.slack.com/archives/C0N913649/p1765898638364609
    return "agent" if os.environ.get("JENKINS_HOME") is not None else None


def _ppp_ec2_dir() -> Optional[str]:
    """
    Helper function to check if we're running in a Netflix PPP EC2 environment.

    Returns
    -------
        Optional[str]: Directory to use for Conda if running in a Netflix PPP EC2
        environment, None otherwise
    """
    return (
        "mnt"
        if os.environ.get("NETFLIX_APP") == "mlppythonmodelserving"
        and os.environ.get("EC2_INSTANCE_ID") is not None
        else None
    )


def _conda_dir_name() -> str:
    """
    Helper function to get the name of the directory to use for Conda.

    Returns
    -------
        str: Name of the directory to use for Conda, defaults to "tmp" if nothing specific
        is identified.
    """
    return (
        _workbench_dir()
        or _workspace_dir()
        or _darwin_dir()
        or _jenkins_dir()
        or _ppp_ec2_dir()
        or "tmp"
    )


DEFAULT_EC2_TMP_DIRNAME = from_conf("DEFAULT_EC2_TMP_DIRNAME", f"/{_conda_dir_name()}")

CONDA_LOCAL_DIST = from_conf(
    "CONDA_LOCAL_DIST", f"conda-{{arch}}-{_conda_dir_name()}-{_CONDA_VERSION_ID}.tgz"
)


# Preferred Format for Conda packages
CONDA_PREFERRED_FORMAT = from_conf(
    "CONDA_PREFERRED_FORMAT",
    ".conda",
    get_validate_choice_fn([".tar.bz2", ".conda"]),
)

CONDA_DEFAULT_PYPI_SOURCE = from_conf(
    "CONDA_DEFAULT_PYPI_SOURCE", "https://pypi.netflix.net/simple"
)

# Allows you to specify URLs that need to be authenticated (basically Conda or PYPI
# package sources). Metaflow will glean all the information it can from traditional
# configuration sources (like pip.conf) but this allows you to specify additional URLs
# for which authentication is required. A dictionary should be passed in (or a JSON
# version of it in string form).
# key: hostname that needs authentication
# value: a tuple/list of username and password
CONDA_SRCS_AUTH_INFO = from_conf("CONDA_SRCS_AUTH_INFO", {})

# Path to the local conda distribution. If this is specified and a conda distribution
# does not exist at this path, Metaflow will attempt to install it using
# CONDA_LOCAL_DIST_DIRNAME and CONDA_LOCAL_DIST
CONDA_LOCAL_PATH = from_conf(
    "CONDA_LOCAL_PATH",
    os.path.join("/", _conda_dir_name(), f"metaflow-condav2-{_CONDA_VERSION_ID}"),
)


CONDA_REMOTE_COMMANDS = ("titus",)

# List of handles system dependencies indicating what system we should build for
CONDA_SYS_DEPENDENCIES = ("__cuda", "__glibc")

# Default system dependencies if not specified per target platform. The "linux-64"
# is also used for "remote" execution default packages
CONDA_SYS_DEFAULT_PACKAGES = {
    "linux-64": {"__glibc": os.environ.get("CONDA_OVERRIDE_GLIBC", "2.35")}
}

# Markers to assume for different architectures. See
# https://packaging.pypa.io/en/stable/markers.html#packaging.markers.Environment for
# valid keys.
# By default:
#  - implementation_version is cleared
#  - python_version and python_full_version are cleared (and set based on environment
#    to resolve)
#  - others are taken from the default environment
# Any keys set here will override the above (except python_version and python_full_version)
CONDA_SYS_MARKERS = {
    "linux-64": {
        "os_name": "posix",
        "platform_machine": "x86_64",
        "platform_system": "Linux",
        "platform_release": "6.5.13netflix-00003-g301fd1c6f367",
        "sys_platform": "linux",
    }
}

# Default version of the CUDA dependency for known GPU machines
CONDA_SYS_DEFAULT_GPU_PACKAGES = {
    "__cuda": os.environ.get("CONDA_OVERRIDE_CUDA", "12.2=0")
}

# Packages that are needed when creating an intermediate conda environment to resolve
# a pypi environment. This can be used to add packages needed for authentication for
# example.
CONDA_BUILDER_ENV_PACKAGES = from_conf("CONDA_BUILDER_ENV_PACKAGES", [])

# All architectures used by Conda
CONDA_ALL_ARCHS = from_conf("CONDA_ALL_ARCHS", ["linux-64", "osx-64", "osx-arm64"])


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

# HACK -- work around an issue with micromamba where using a channel_alias
# causes the packages to be considered invalid (URL verification). This
# value should be set to the channel_alias prefix that will be replaced by
# conda.anaconda.org.
CONDA_HACK_CHANNEL_ALIAS = from_conf(
    "CONDA_HACK_CHANNEL_ALIAS",
    None,
)

###
# Other configuration (Netflix specific)
###

CONTACT_INFO = {
    "Read the documentation": "http://go/metaflow",
    "Slack us": "#ask-metaflow",
}

NETFLIX_ENV = os.environ.get("NETFLIX_ENVIRONMENT", "prod")
# MLI_API = 'http://dpa_training_service.cluster.us-east-1.%s.cloud.netflix.net:7001/api/v0' % NETFLIX_ENV
# MLI_API = "http://127.0.0.1:50001/api/v0"
# MLI_API = "http://dpa_training_service-staging.cluster.us-east-1.prod.cloud.netflix.net:7001/api/v0"
MLI_API = "https://mliservice.dyn%s.netflix.net:7002/api/v0" % NETFLIX_ENV

# S3 configuration.
if NETFLIX_ENV == "prod":
    DATASTORE_SYSROOT_S3 = from_conf(
        "DATASTORE_SYSROOT_S3",
        "s3://netflix-dataplatform-code/apps/metaflow/runs/v1/",
    )
    # S3 datatools root location
    DATATOOLS_S3ROOT = from_conf(
        "DATATOOLS_S3ROOT",
        "s3://netflix-dataplatform-code/apps/metaflow/userdata/v1/",
    )
else:
    DATASTORE_SYSROOT_S3 = from_conf(
        "DATASTORE_SYSROOT_S3",
        "s3://netflix-dataplatform-code/apps/metaflow/runs/test/v1/",
    )
    # S3 datatools root location
    DATATOOLS_S3ROOT = from_conf(
        "DATATOOLS_S3ROOT",
        "s3://netflix-dataplatform-code/apps/metaflow/userdata/test/v1/",
    )

# Set number of worker to 4 * CPU count.
# 1. If S3_WORKER_COUNT is already set, use that.
# 2. If TITUS_NUM_CPU is set it means we are running within a Titus container, we
#    can get the number of CPUs from that.
# 3. Otherwise, we try to get number of CPUs from the system.
# By default, we want number of workers to be 4 * CPU. However, since we may not get
# correct number of CPUs from the system in a multi-tenant environment, we cap the
# number of workers to 64.
S3_WORKER_COUNT = from_conf(
    "S3_WORKER_COUNT",
    min(int(os.environ.get("TITUS_NUM_CPU", multiprocessing.cpu_count())) * 4, 64),
)
CARD_S3ROOT = from_conf("CARD_S3ROOT", os.path.join(DATASTORE_SYSROOT_S3, "mf.cards"))

SKIP_CARD_DUALWRITE = from_conf("SKIP_CARD_DUALWRITE", True)

DATATOOLS_CLIENT_PARAMS = from_conf("DATATOOLS_CLIENT_PARAMS", {})
DATATOOLS_SESSION_VARS = from_conf("DATATOOLS_SESSION_VARS", {})

# CDE Data Gateway configuration
DGW_KV_HOST = "mli.dgwkv.vip.{REGION}.{ENV}.cloud.netflix.net:8980"
DGW_KV_APP = "dgwkv.mli"

# Process configurations when evaluating anything through the Runner/Deployer.
# This allows proper argument checking when configurations add more options to the CLI
CLICK_API_PROCESS_CONFIG = from_conf("CLICK_API_PROCESS_CONFIG", True)

# Upload code package even for local runs.
FEAT_ALWAYS_UPLOAD_CODE_PACKAGE = from_conf("FEAT_ALWAYS_UPLOAD_CODE_PACKAGE", True)

# The default images to use for Titus containers.
# These can be overridden by the @titus decorator or --with titus:image=...
TITUS_IMAGE = from_conf("TITUS_IMAGE", "drydock/big_data:stable")
TITUS_GPU_IMAGE = from_conf("TITUS_GPU_IMAGE", "drydock/big_data_gpu:stable")

###
# Metadata configuration
###
INCLUDE_FOREACH_STACK = from_conf("INCLUDE_FOREACH_STACK", True)
MAXIMUM_FOREACH_VALUE_CHARS = from_conf("MAXIMUM_FOREACH_VALUE_CHARS", 20)

# Metaflow UI
UI_URL = from_conf("UI_URL", "https://metaflowui.%s.netflix.net" % NETFLIX_ENV)

# Maestro
SETUP_GANDALF_POLICY = from_conf("SETUP_GANDALF_POLICY", True)
ENFORCE_GANDALF_POLICY = from_conf("ENFORCE_GANDALF_POLICY", True)

# Gandalf
GANDALF_PORTAL_API_URL = "https://portal.gandalf.netflix.net:7004/REST/v1"
GANDALF_PORTAL_IDGROUP_UI_URL = "https://portal.gandalf.netflix.net/id-group/detail"

# Coverage
COVERAGE_S3_PATH = from_conf("COVERAGE_S3_PATH", "")
COVERAGE_CONTEXT = from_conf("COVERAGE_CONTEXT", "")
COVERAGE_JENKINS_API_URL = "https://dse.builds.test.netflix.net:7004/job/metaflow-coverage-worker-jenkins/buildWithParameters"
COVERAGE_S3_PREFIX = "s3://netflix-dataoven-test-users/metaflow-test-coverage"

###
# Debug options
###
DEBUG_OPTIONS = ["hosting", "cache", "conda", "gandalf", "slack"]

ENABLE_DEFAULT_DECOSPECS = from_conf("ENABLE_DEFAULT_DECOSPECS", True)

# Add metrics card to every step with GPU by default.
TOGGLE_DECOSPECS = (
    ['card:type=metrics,options={"only_show_on_gpu_machine":true,"default":true}']
    if (ENABLE_DEFAULT_DECOSPECS)
    else []
)


### PPP/Dev Workspaces / EC2 Specific Configurations
if _workspace_dir() is not None or _ppp_ec2_dir() is not None:
    TEMPDIR = from_conf("TEMPDIR", DEFAULT_EC2_TMP_DIRNAME)
    ARTIFACT_LOCALROOT = from_conf("ARTIFACT_LOCALROOT", TEMPDIR)
    DATASTORE_SYSROOT_LOCAL = from_conf("DATASTORE_SYSROOT_LOCAL", TEMPDIR)
    FUNCTION_RUNTIME_PATH = from_conf(
        "FUNCTION_RUNTIME_PATH",
        os.path.join(TEMPDIR, "metaflow_functions"),
    )
    CLIENT_CACHE_PATH = from_conf(
        "CLIENT_CACHE_PATH", os.path.join(TEMPDIR, "metaflow_client")
    )
