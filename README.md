# Metaflow Experimental Extensions from Netflix
This repository contains *non-supported* extensions for Metaflow.
- If you are within Netflix and are looking for the Netflix version of Metaflow,
  this is *not* it.
- If you are looking for the community supported Metaflow package, this is also *not*
  it, please see [here](https://github.com/Netflix/metaflow) for that package.

Netflix released Metaflow as OSS in 2019. Since then, development of Metaflow internally
to Netflix has continued primarily around extensions to better support Netflix's
infrastructure and provide a more seamless integration with the compute and orchestration
platforms specific to Netflix. Netflix continues to collaboratively improve Metaflow's
OSS capabilities in collaboration with [OuterBounds](https://outerbounds.co) and,
as such, sometimes develops functionality that is not yet fully ready for inclusion in
the community supported Metaflow, either because it is not fully fleshed out or interest
in this functionality is not clear. Typically, functionality present here is either
deployed actively at Netflix or being tested for deployment at Netflix.

This repository will contain such functionality. While we do our best to ensure that
the functionality present works, it does not have the same levels of support and
backward compatibility guarantees that Metaflow does. Functionality present in
this package is likely to end up in the main Metaflow package with, potentially,
some modification (in which case it will be removed from this package) but that is
not a guarantee. If you find this functionality useful and would like to see it make
it to the main Metaflow package, let us know. Feedback is always welcome!

If you have any question, feel free to open an issue here or contact us on the usual
Metaflow slack channels.

This extension currently contains:
- refactored [Conda decorator](#conda-v2)

## Conda V2
This functionality is currently being actively tested within Netflix but has not yet
been deployed in production.

It is likely to evolve primarily in its implementation as we do further testing. Feedback
on what is working and what is not is most welcome.

### Improvements over the included Conda decorator
This decorator improves several aspects of the included Conda decorator:
- it has significant performance gains:
  - resolving environments in parallel
  - supporting `.conda` packages (instead of only the older `.tar.bz2` packages)
  - using `micromamba` for environment creation
- it allows the inclusion of `pypi` packages in the environment specification
- it is more efficient in its use of caching
- environment descriptions are also cached allowing anyone to reuse a previously
  resolved environment
- it provides more visibility into the environments created
- it allows you to recreate the environment used for a step locally to aid in
  accessing the artifacts produced and/or debug the execution of a step.

### Installation
To use, simply install this package alongside the `metaflow` package. This package
requires Metaflow v2.7.16 or later. This implementation currently only supports S3 for
environment caching. If you require additional datastores, please open an issue and we
can work together to add it (it is not very hard).

#### Configuration
You have several configuration options that can be set in
`metaflow_extensions/netflix_ext/config/mfextinit_netflixext.py`. Due to limitations in
the OSS implementation of decorators such as `batch` and `kubernetes`, you should
set these values directly in the configuration file and not in an external configuration
or through environment variables. The useful configuration values are listed below:
- `CONDA_S3ROOT`: directory in S3 containing all the cached packages and environments
  as well as eventual conda distributions to use. For safety, do not point this to the
  same prefix as for the current Conda implementation.
- `CONDA_DEPENDENCY_RESOLVER`: `mamba` or `conda`; `mamba` is recommended as
  typically faster.
- `CONDA_REMOTE_INSTALLER_DIRNAME`: if set contains a prefix within `CONDA_S3ROOT`
  under which `micromamba` (or other similar executable) are cached. If not specified,
  `micromamba`'s latest version will be downloaded on remote environments when an
  environment needs to be re-hydrated.
- `CONDA_REMOTE_INSTALLER`: if set architecture specific installer in 
  `CONDA_REMOTE_INSTALLER_DIRNAME`.
- `CONDA_LOCAL_DIST_DIRNAME`: if set contains a prefix within `CONDA_S3ROOT` under
  which fully created conda environments for local execution are cached. If not set,
  the local machine's Conda installation is used.
- `CONDA_LOCAL_DIST`: if set architecture specific tar ball in `CONDA_LOCAL_DIST_DIRNAME`.
- `CONDA_LOCAL_PATH`: if set, installs the tarball in `CONDA_LOCAL_DIST` in this path.
- `CONDA_PREFERRED_FORMAT`: `.tar.bz2` or `.conda`. Prefer `.conda` for speed gains; any
  package not available in the preferred format will be transmuted to it automatically.
  If left empty, whatever package is found will be used (ie: there is no preference)
- `CONDA_PREFERRED_RESOLVER`: `conda` or `conda-lock`; use `conda`/`mamba` or `conda-lock`
  to resolve environments. `conda-lock` is in more active development but allows for
  the inclusion of `pypi` dependencies.
- `CONDA_DEFAULT_PIP_SOURCES`: list of additional mirrors to search for packages. Useful
  if your company has an internal mirror.

#### Conda environment requirements
Your local conda environment or the cached environment (in `CONDA_LOCAL_DIST_DIRNAME`)
needs to satisfy the following requirements:
- `conda<22.11` (Conda 22.11+ has issues with `conda-lock`)
- `conda-lock>=1.3.0`
- `micromamba>=1.1.0`
- `conda-package-handling>=1.9.0`
- `lockfile`
- (optional but recommended) `mamba>=1.1.0`

In addition, and only if you want to support `pypi` packages, it is best to apply the
PR `https://github.com/conda-incubator/conda-lock/pull/290` to `conda-lock`. This is
the unfortunate result of a bug in how `conda-lock` handles packages that are both
present in the `conda` environment and `pypi` one.

Due to bugs in `conda` and the way we use it, if your resolved environment
contains `.conda` packages and you do not have `micromamba` installed, the
environment creation will fail.

#### Uninstallation
Uninstalling this package will revert the behavior of the conda decorator to the one
currently present in Metaflow. It is safe to switch back and forth and there should
be no conflict between both implementations provided they do not share the same
caching prefix in S3.

### Usage
Your current code with `conda` decorators will continue working as is. However, at this
time, there is no method to "convert" previously resolved environment to this new
implementation so the first time you run Metaflow with this package, your previously
resolved environments will be ignored and re-resolved.

#### Additional decorator options
The `conda` and `conda_base` decorators take the following additional options:
- `channels`: A list of additional Conda channels to search. This is useful if the
  channel is not on `anaconda.org` and cannot be referred to as using the `::` notation.
- `pip_packages`: A dictionary using the same format as the `libraries` option to
  specify packages present in `pypi`.
- `pip_sources`: A list of additional `pypi` repositories.
- `archs`: A list of strings indicating the architectures to resolve this environment
  for. By default, the environment is resolved for the current platform and `linux-64`
  if running on a remote environment.

#### Additional command-line tool
An additional `environment` command-line tool is available invoked as follows:
`python myflow.py --environment=conda environment --help`.
It provides the following two sub-commands:
- `list`: will list all available resolved environments for the steps in the flow.
  Environonments can be present locally or remotely cached.
- `select-resolved`: will list all available resolved environments for the steps in
  the flow and allow you to select which specific resolved environment should be used
  for each step. This allows you to re-resolve an environment for example (without
  changing the dependencies -- something not easily possible in the current
  implementation) as well as select a different environment. Note that once selected,
  the environment will apply to any step that has the same set of dependencies.

In both of these commands, by default a menu is used to display the information. You can
disable this using `--no-menu`. If using the menu, the list of environments can be
sorted (use the `~` key) or searched (use the `/` key). Normal up/down/page-up/page-down
navigation will also work (if you ever have that many environments).

Finally, the `metaflow` command is also augmented with an `environment` subcommand which
currently only has the `create-local` subcommand which allows you to specify the pathspec
to a step and it will recreate a local Conda environment duplicating the one present
for that step.

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
  well as the description of the environment in S3 for later retrieval on environment
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
- we then upload all packages to S3 using our parallel uploader. Transmuted packages
  are also linked together so we can find them later.

The `ResolvedEnvironment`, now with updated cache information, is also cached to S3 to
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

