# Metaflow Extensions from Netflix
This repository contains extensions for Metaflow that are in use at Netflix (or being tested
at Netflix) and that are more cutting edge than what is included in the OSS Metaflow package.

You can find support for this extension on the usual Metaflow [Slack](http://slack.outerbounds.co).

NOTE: of you are within Netflix and are looking for the Netflix version of Metaflow,
  this is *not* it (this only contains a part of the Netflix internal extensions).

Netflix released Metaflow as OSS in 2019. Since then, development of Metaflow internally
to Netflix has continued primarily around extensions to better support Netflix's
infrastructure and provide a more seamless integration with the compute and orchestration
platforms specific to Netflix. Netflix continues to collaboratively improve Metaflow's
OSS capabilities in collaboration with [OuterBounds](https://outerbounds.co) and,
as such, sometimes develops functionality that is not yet fully ready for inclusion in
the community supported Metaflow as interest in the functionality may not be clear or
there is not time in the community to properly integrate and fully test the functionality.

This repository will contain such functionality. While we do our best to ensure that
the functionality present works, it does not have the same levels of support and
backward compatibility guarantees that Metaflow does. Functionality present in
this package is likely to end up in the main Metaflow package with, potentially,
some modification (in which case it will be removed from this package) but that is
not a guarantee. If you find this functionality useful and would like to see it make
it to the main Metaflow package, let us know. Feedback is always welcome!

This extension is currently tested on python 3.7+.

If you have any question, feel free to open an issue here or contact us on the usual
Metaflow slack channels.

This extension currently contains:
- refactored and improved [Conda decorator](#conda-v2)

## Conda V2

*Version 1.0.0 is considered stable. Some UX changes have occurred compared to previous
versions. Please see the [docs](https://github.com/Netflix/metaflow-nflx-extensions/blob/main/docs/conda.md) for more information*

*Version 0.2.0 of this extension is not fully backward compatible with previous versions due to
where packages are cached. If you are using a previous version of the extension, it is recommended
that you change the `CONDA_MAGIC_FILE_V2`, `CONDA_PACKAGES_DIRNAME` and `CONDA_ENVS_DIRNAME` to
new values to be able to have both versions active at the same time.*

It is likely to evolve primarily in its implementation as we do further testing. Feedback
on what is working and what is not is most welcome.

### Main improvements over the standard Conda decorator in Metaflow
This decorator improves several aspects of the included Conda decorator:
- it allows you to mix and match Conda packages and Pypi packages.
- it supports a wider range of Pypi package sources (repositories, source tarballs, etc)
- it supports a command line tool allowing you to:
  - retrieve and re-hydrate any environment used by any previously executed step thereby enabling
    an easy way to inspect artifacts created in that environment
  - resolve environments using standard `requirements.txt` of `environment.yml` files
  - inspect packages present in any environment previously resolved
- it supports "named environments" which enables easy environment sharing and saving.
- it is generally more performant and efficient in terms of parallel resolution and
  downloading of packages
- it supports conda, mamba and micromamba

### Installation
To use, simply install this package alongside the `metaflow` package. This package
requires Metaflow v2.8.3 or later.

#### Configuration
You have several configuration options that can be set in
`metaflow_extensions/netflix_ext/config/mfextinit_netflixext.py`. Due to limitations in
the OSS implementation of decorators such as `batch` and `kubernetes`, prior to Metaflow v2.10,
you should set these values directly in the `mfextinit_netflixext.py` configuration file and
not in an external configuration or through environment variables. This limitation is
removed in Metaflow v2.10.

The useful configuration values are listed below:
- `CONDA_S3ROOT`/`CONDA_AZUREROOT`/`CONDA_GSROOT`: directory in S3/azure/gs containing
  all the cached packages and environments
  as well as eventual conda distributions to use. For safety, do not point this to the
  same prefix as for the current Conda implementation.
- `CONDA_DEPENDENCY_RESOLVER`: `mamba`, `conda` or `micromamba`; `mamba` is recommended as
  typically faster. `micromamba` is sometimes a bit more unstable but can be even faster
- `CONDA_PYPI_DEPENDENCY_RESOLVER`: `pip` or None; if None, you will not be able to resolve
  environments specifying only pypi dependencies.
- `CONDA_MIXED_DEPENDENCY_RESOLVER`:  `conda-lock` or `none`; if `none`, you will not be able
  to resolve environments specifying a mix of pypi and conda dependencies.
- `CONDA_REMOTE_INSTALLER_DIRNAME`: if set contains a prefix within
  `CONDA_S3ROOT`/`CONDA_AZUREROOT`/`CONDA_GSROOT`
  under which `micromamba` (or other similar executable) are cached. If not specified,
  `micromamba`'s latest version will be downloaded on remote environments when an
  environment needs to be re-hydrated.
- `CONDA_REMOTE_INSTALLER`: if set architecture specific installer in 
  `CONDA_REMOTE_INSTALLER_DIRNAME`.
- `CONDA_LOCAL_DIST_DIRNAME`: if set contains a prefix within
  `CONDA_S3ROOT`/`CONDA_AZUREROOT`/`CONDA_GSROOT` under
  which fully created conda environments for local execution are cached. If not set,
  the local machine's Conda installation is used.
- `CONDA_PACKAGES_DIRNAME`: directory within `CONDA_S3ROOT`/`CONDA_AZUREROOT`/`CONDA_GSROOT`
  under which cached packages are stored (defaults to `packages`)
- `CONDA_ENVS_DIRNAME`: same thing a `CONDA_PACKAGES_DIRNAME` but for environments
  (defaults to `envs`)
- `CONDA_LOCAL_DIST`: if set architecture specific tar ball in `CONDA_LOCAL_DIST_DIRNAME`.
- `CONDA_LOCAL_PATH`: if set, installs the tarball in `CONDA_LOCAL_DIST` in this path.
- `CONDA_PREFERRED_FORMAT`: `.tar.bz2` or `.conda` or `none` (default).
  Prefer `.conda` for speed gains; any
  package not available in the preferred format will be transmuted to it automatically.
  If left empty, whatever package is found will be used (ie: there is no preference)
- `CONDA_DEFAULT_PYPI_SOURCE`: mirror to use for PYPI.
- `CONDA_USE_REMOTE_LATEST`: by default, it is set to `:none:` which means that if a new
  environment is not locally known (for example first time resolving it on the machine), it
  will be re-resolved. You can also set it to `:username:`, `:any:` or a comma separated
  list of usernames to tell Metaflow to go check if there is a cached environment that matches
  the requested specification that has been resolved previously by either the current user,
  any user or the set of users.


##### Azure specific setup
For Azure, you need to do the following two steps once during setup:
- Manually create the blob container specified in `CONDA_AZUREROOT`
- Grant the `Storage Blob Data Contributor` role to the storage account to the service
  principal or user accounts that will be accessing as described
  [here](https://learn.microsoft.com/en-us/azure/storage/blobs/assign-azure-role-data-access?tabs=portal#assign-an-azure-role).

#### Conda environment requirements
Your local conda environment or the cached environment (in `CONDA_LOCAL_DIST_DIRNAME`)
needs to satisfy the following requirements:
- `conda`
- (optional but recommended) `mamba>=1.4.0`
- (strongly recommended) `micromamba>=1.4.0`

##### Pure pypi package support
If you want support for environments containing only pip packages, you will also need:
- `pip>=23.0`

##### Mixed (pypi + conda)  package support
If you want support for environments containing both pip and conda packages, you will also need:
- `conda-lock>=2.1.0`

##### Support for `.tar.bz2` and `.conda` packages
If you set `CONDA_PREFERRED_FORMAT` to either `.tar.bz2` or `.conda`, for some packages,
we will need to transmute them from one format to the other. For example if a package
is available for download as a `.tar.bz2` package but you request `.conda` packages,
the system will transmute (convert) the `.tar.bz2` package into one that ends in
`.conda`. To do so, you need to have one of the following package installed:
- `conda-package-handling>=1.9.0`
- `micromamba>=1.4.0` (not supported for cross-platform transmutation due to
   https://github.com/mamba-org/mamba/issues/2328 or if you are transmuting to
   .tar.bz2 files).


Also due to a bug in `conda` and the way we use it, if your resolved environment
contains `.conda` packages and you do not have `micromamba` installed, the
environment creation will fail.

### Known issues
This plugin relies on conda, mamba, and micromamba. These technologies are being
constantly improved and there are a few outstanding issues that we are aware of:
- if you have an environment with both `.conda` and `.tar.bz2` packages, conda/mamba
  will fail to create it because we use it in "offline" mode
  (see: https://github.com/conda/conda/issues/11775). The workaround is to have
  `micromamba` available which does not have this issue and which Metaflow will use
  if it is present
- Transmuting packages with `micromamba` is not supported for cross-platform
  transmutes due to https://github.com/mamba-org/mamba/issues/2328. It also does
  not work properly when transmuting from `.conda` packages to `.tar.bz2` packages.
  Install `conda-package-handling` as well to support this.

### Uninstallation
Uninstalling this package will revert the behavior of the conda decorator to the one
currently present in Metaflow. It is safe to switch back and forth and there should
be no conflict between both implementations provided they do not share the same
caching prefix in S3/azure/gs and that you do not use any of the new features.

### Usage
Your current code with `conda` decorators will continue working as is. However, at this
time, there is no method to "convert" previously resolved environment to this new
implementation so the first time you run Metaflow with this package, your previously
resolved environments will be ignored and re-resolved.

#### Environments that can be resolved
Environments listed below are examples that can be resolved using Metaflow. The environments
given here are either in the `requirements.txt` format or `environment.yml` format and can,
for example, be passed to `metaflow environment resolve` using the `-r` or `-f` option
respectively. They highlight some of the functionalities present. Note that the same
environments can also be specified directly using the `@conda` or `@pip` decorators.

##### Pure "pypi" environment with non-python Conda packages
```
--conda-pkg ffmpeg
ffmpeg-python
```
The `requirements.txt` file above will create an environment with the Pip package
`ffmpeg-python` as well as the `ffmpeg` Conda executable. This is useful to have
a pure pip environment (and therefore use the underlying `pip` ecosystem without
`conda-lock` but still have other non Python packages installed.

##### Pure "pypi" environment with non wheel files
```
--conda-pkg git-lfs
# Needs LFS to build
transnetv2 @ git+https://github.com/soCzech/TransNetV2.git#main
# GIT repo
clip @ git+https://github.com/openai/CLIP.git@d50d76daa670286dd6cacf3bcd80b5e4823fc8e1
# Source only distribution
outlier-detector==0.0.3
# Local package
foo @ file:///tmp/build_foo_pkg
```
The above `requirements.txt` shows that it is possible to specify repositories directly.
Note that this does not work cross platform. Behind the scenes, Metaflow will build wheel
packages and cache them.

##### Pypi + Conda packages
```
dependencies:
  - pandas = >=1.0.0
  - pip:
    - tensorflow = 2.7.4
    - apache-airflow[aiobotocore]
```
The above `environment.yml` shows that it is possible to mix and match pip and conda
packages. You can specify packages using "extras" but you cannot, in this form,
specify pip packages that come from git repositories or from your local file-system.
Pypi packages that are available as wheels or source tar balls are acceptable.

#### General environment restrictions
In general, the following restrictions are applicable:
  - you cannot specify packages that need to be built from a repository or a directory
    in mixed conda+pypi mode. This is a restriction of the underlying tool (conda-lock) and will not
    be fixed until supported by conda-lock.
  - you cannot specify editable packages. This restriction will not be lifted at this time.
  - you cannot specify packages that need to be built from a repository or a directory in
    pypi only mode across platforms (ie: resolving for `osx-arm64` from a `linux-64` machine). This
    restriction will not be removed as this would potentially require cross-platform build which
    can be tricky and error-prone.
  - in specifying packages, environment markers are not supported.

### Additional documentation
For additional documentation, please refer to the [documentation](https://github.com/Netflix/metaflow-nflx-extensions/blob/main/docs/conda.md)
which contains more detailed documentation.

### Technical details
This section dives a bit more in the technical aspects of this implementation.
#### General Concepts
##### Environments
An environment can either be un-resolved or resolved. An un-resolved environment is
simply defined by the set of high-level user-requirements that the environment must
satisfy. Typically, this is a list of Conda and/or Pypi packages and version constraints
on them. In our case, we also include the set of channels (Conda) or sources (Pip).
A resolved environment contains the concrete list of packages that are to be installed
to meet the aforementioned requirements. In a resolved environment, all packages are
pinned to a single unique version.

In Metaflow, two hashes identify environments and `EnvID` (from `env_descr.py`)
encapsulates these hashes:
- the set of user requirements are hashed to produce the first hash,
  the `req_id`. This hash encapsulates the packages and version constraints as well
  as the channels or sources. The packages are sorted to provide a stable hash for
  identical set of requirements.
- the full set of packages needed are hashed to produce the second hash, the `full_id`.

We also associate the architecture for which the environment was resolved to form the
complete `EnvID`.

Environments are named as `metaflow_<req_id>_<full_id>`. Note that environments that
are resolved versions of the same un-resolved environment therefore have the same
prefix.

##### Overview of the phases needed to execute a task in a Conda environment
This implementation of Conda clearly separates out the phases needed to execute a
Metaflow task in a Conda environment:
- resolving the environment: this is the step needed to go from an un-resolved
  environment to a fully resolved one. It does not require the downloading of packages
  (for the most part) nor the creation of an environment.
- caching the environment: this is an optional step which stores all the packages as
  well as the description of the environment in S3/azure/gs for later retrieval on environment
  creation. During this step, packages may be downloaded (from the web for example) but
  an environment is still not created.
- creating the environment: in this step, the exact set of packages needed are
  downloaded (if needed) and an environment is created from there. At this point, there
  is no resolution (we know the exact set of packages needed).

  ##### Code organization
  ###### Environment description
  `env_descr.py` contains a simple way to encode all the information needed for all the
  above steps, specifically it contains a set of `ResolvedEnvironment` which, in turn,
  contain the ID for the environment and information about each package. Each package,
  in turn, contains information about where it can be located on the web as well
  as caching information (where it is located in the cache). Each package can also
  support multiple formats (Conda uses either `.tar.bz2` or `.conda` -- note that this
  is meant to support *equivalent* formats and not `.whl` versus `.tar.gz` for Pypi
  packages for example).

  Very little effort is made to remove duplicate information (packages may for example
  be present in several resolved environments) as modularity is favored (ie: each
  `ResolvedEnvironment` is fully self contained).

  ###### Decorators
  The `conda_flow_decorator.py` and `conda_step_decorator.py` files simply contain
  trivial logic to convert the specification passed to those decorators (effectively
  information needed to construct the requirement ID of the environment) to something
  that is understandable by the rest of the system. In effect, they are mostly
  transformers that take user-information and convert it to the set of packages the
  user wants to have present in their environment.

  ###### Environment
  The `conda_environment.py` file contains methods to effectively:
  - resolve all un-resolved environments in a flow
  - bootstrap Conda environments (this is analogous to some functionality in
    `conda_step_decorator.py` that has to do with starting a task locally).

The actual work is all handled in the `conda.py` file which contains the crux of the
logic.

##### Detailed description of the phases
###### Resolving environments
All environments are resolved in parallel and independently. To do so, we either use
`conda-lock` or `mamba/conda` using the `--dry-run` option. The processing
for this takes place in `resolve_environment` in the `conda.py` file.

The input to this step is a set of user-level requirements and the output is a set
of `ResolvedEnvironment`. At this point, no package has been downloaded and the
`ResolvedEnvironment` is most likely missing any information about caching.

###### Caching environments
The `cache_environments` method in the `conda.py` file implements this.

There are several steps here. We perform these steps for all resolved environments
that need their cache information updated at once to be able to exploit the fact that
several environments may refer to the same package:
- first we check if we have the packages needed in cache. To do so, the path a package
  is uploaded to in cache is uniquely determined by its source URL.
- for all packages that are *not* present in the cache, we will "download" them. This
  is implemented in the `lazy_download_packages` method. We do this per architecture.
  The basic concept of this function is to locate the "nearest" source of the package.
  In order, we look for:
  - a locally present archive in some format
  - a cache present archive in some format
  - a web present archive in some format.
  We download the archive and transmute it if needed. The way we do downloads ensures
  that any downloaded package will be available if we need to create the environments
  locally. We take care of properly updating the list of URLs if needed (so Conda
  can reason about what is present in the directory).
- we then upload all packages to S3/azure/gs using our parallel uploader. Transmuted packages
  are also linked together so we can find them later.

The `ResolvedEnvironment`, now with updated cache information, is also cached to S3/azure/gs to
promote sharing.

###### Creating environments
This is the easiest step of all and simply consists of fetching all packages (again
using the `lazy_download_packages` method which will not download any package that
is already present) and then using `micromamba` (or `mamba/conda`) to simply install
all packages.

##### Detailed information about caching
There are two main things that are cached:
- environment themselves (so basically the `ResolvedEnvironment` in JSON format)
- the packages used in the environments.

There are also two levels of caching:
- locally:
  - Environment descriptions are stored in a special file called `conda_v2.cnd` which
    caches all environments already resolved. This allows us to reuse the same
    environment for similar user-level requirements (which is typically what the user
    wants).
  - Packages themselves may be cached in the `pkgs` directory of the Conda installation.
    They may be either fully expanded directories or archives.
- remotely:
  - Environment descriptiosn are also stored remotely and can be fetched to be added
    to the local `conda_v2.cnd` file.
  - Packages are stored as archived and may be downloaded in the `pkgs` directory. The
    implementation takes care of properly updating the `urls.txt` file to make it
    transparent to Conda (allowing it to operate in an "offline" mode effectively).

