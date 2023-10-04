# Managing External Dependencies

**Note that some options have changed since we first made this version of Conda available in Beta, all previous
options still work but some are deprecated and will be removed in a future release. Specifically, the arguments
`pip_packages`, `pip_sources`, `sources`, `name`, `pathspec` and `fetch_at_exec` have been moved around.**

**If you were using this in Beta, the first time you run this new version, some environments may be re-resolved
that may not have been previously (ie: it would be like starting from a brand new machine). All previous
environments will still function.**


## The problem

When you run Metaflow code locally, it behaves as any other Python code, so all libraries available to
your Python interpreter can be imported and used in steps.

A core benefit of Metaflow, however, is that the same code can be run in different environments without
modifications. Clearly this promise does not hold if a step code depends on locally installed libraries. Even in
the case of a local execution, *accessing* artifacts produced by your run may no longer be possible if your local
environment has changed or you no longer remember what environment you had. In other words, artifacts can, and
frequently do, depend on the environment in which they were created.

Reproducibility is a core value of Machine Learning Infrastructure. It is hard to collaborate on data science
projects effectively without being able to reproduce past results reliably. Metaflow tries to solve
several questions related to reproducible research, principle amongst them, dependency management:
how can you, the data scientist, specify libraries that your code needs so that the results are reproducible?

Note that reproducibility and dependency management are related but separate topics. We could solve either
one individually. A classic `os.system(‘pip install pandas’)` is an example of dependency management without
reproducibility (what if the version of pandas changes?). On the other hand, we could make code perfectly
reproducible by forbidding external libraries - reproducibility without dependency management.

Metaflow aims at solving both the questions at once: how can we handle dependencies so that the results are
reproducible? Specifically, it addresses the following three issues:
1. How to make external dependencies available locally during development?
1. How to execute code remotely on Batch or with Argo/Airflow/StepFunctions with external dependencies?
1. How to ensure that anyone can reproduce past results or access artifacts even months later?

The topic of providing isolated, encapsulated and re-hydratable execution environments is a surprisingly
complex one.

### Possible solutions and their downsides

Providing stable execution environments is not a new problem and there are several solutions out there you may
be familiar with:
- A common solution is Docker which allows you to create a fully reproducible and fixed environment. This is
the gold standard in terms of creating environments and has the lowest runtime latency as the
environment is already pre-built. Docker, however, can be heavy and unwieldy to use.
- Another common solution is to install dependencies on the fly at the start of the step using something like:
```
import subprocess
subprocess.check_call([sys.executable, "-m", "pip", "install", "my_package"])
import my_package
```

The downside of this approach is that it does not produce a reproductible environment (every time you resolve the
environment, you may get slightly different sets of packages). Note that even specifying pinned versions of
`my_package` is not enough as things that `my_package` depends on may not be pinned and so on. It is very
difficult to ensure that you will get the same environment everytime.
This approach is also slower (the environment is resolved, packages are downloaded and installed at runtime)
and runs the risk of overloading the servers hosting the packages (particularly in a very large
parallel execution).

Metaflow proposes a third solution which, while not being as runtime-efficient as Docker, is more robust than
installing packages by hand and the tooling is simple and well-integrated with the rest of Metaflow.

### A bit of terminology

There are several terms you are probably familiar that relate to the notion of dependencies and environments. In
this section, we clarify a few of them to ensure a good common understanding.

In Python, a **dependency** is another Python package you need to use in your code. You may, for example,
use functions from the `numpy` or `pandas` package and you would therefore depend on them. That package may, in
turn have other dependencies resulting in **transitive dependencies**. When specifying a dependency, you typically
need to specify some **constraints**, for example, you may want to say that you need `pandas` to be at least
version `1.5.0`. The process of determining the full set of packages needed (so the dependencies and all
transitive dependencies) is referred to as **resolving** and **locking**. The resolved packages are therefore
locked, or pinned, to specific versions to provide an immutable environment.

There are two major packaging and referencing technologies in use in Python: PyPi (`https://pypi.org`) and Conda
(`https://anaconda.org`). The packaging technologies they use (**wheel** for PyPi packages and **conda** for
Anaconda packages) are different but both provide overlapping sets of packages (as an example, `pandas` is
found [here](https://pypi.org/project/pandas/) in the PyPi universe and [here](https://anaconda.org/conda-forge/pandas)
in the Conda universe.

Different tools are also used in both universes: `pip` is the tool commonly used to resolve and install packages
from PyPi and [`conda`](https://docs.conda.io/projects/conda/en/latest/),
[`mamba`](https://mamba.readthedocs.io/en/latest/user_guide/mamba.html) or 
[`micromamba`](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html)
are all tools used to resolve and install packages from Anaconda.

A set of resolved packages can be used to create an **environment** which therefore provides the packages
requested as well as all their transient dependencies. Here again, there are two main technologies, **venv**
and **conda**. A venv environment typically allows you to only install PyPi packages and uses the system's
Python as well as system libraries. A conda environment is more hermetical and will allow you to install
a different version of Python, PyPi or conda packages as well as non Python libraries (for example
[ffmpeg](https://anaconda.org/conda-forge/ffmpeg)).

Metaflow's approach leverages the Conda universe, a language-agnostic open-source solution by the authors of
Numpy, to provide an easy way for you to:
- specify your dependencies
- resolve your environment
- re-hydrate your environment (create it on a remote node, on another local machine, in a notebook, etc.)
- share your environment between Flows or colleagues

## Is this approach for me?

If you need to depend on external libraries that are fairly large and/or have a large set of transitive
dependencies, the solution described here is the one you are looking for. If, however, you only need to depend
on a small set of Python files, you should probably just include those files with your code by placing them in
the same directory (or in sub-directories) as your flow file. When Metaflow packages your code for remote
execution, any `.py` file (or any other file ending in suffixes specified using `--package-suffixes`) in the
directory of your flow file or in any of the sub-directories will automatically get included and made available
when executing remotely. For example, if you place a `mymodule.py` file next to your flow file, you can then
do `import mymodule`.

## Configuration

There are many documented configuration options for this extension. The default value should
work in most cases but if you want to change them, you can set them in your usual metaflow configuration
file (all configuration options are given
[here](https://github.com/Netflix/metaflow-nflx-extensions/blob/main/metaflow_extensions/netflix_ext/config/mfextinit_netflixext.py)
(remember to prefix all configuration variable names with `METAFLOW_` when setting them in your profile).

This section lists some useful configuration options

### Resolvers
By default, this extension will not resolve mixed pypi and conda packages. To do so, you need to set
`METAFLOW_CONDA_MIXED_DEPENDENCY_RESOLVER` to `conda-lock`.

We also use `mamba` by default to resolve environments. You can change this to `micromamba` or
`conda` using `METAFLOW_CONDA_DEPENDENCY_RESOLVER`.

### Performance
You can get a significant performance gain if you use only `.conda` packages and not `.tar.bz2` packages
for Conda packages. To do so, set `METAFLOW_CONDA_PREFERRED_FORMAT` to `.conda`. Note that you need
either `micromamba` or `conda-package-handling` installed for this to work.

## Troubleshooting
If you run into issues, you can run with `METAFLOW_DEBUG_CONDA=1` to get a more detailed output of
what is happening. This output can help the Metaflow community figure out the problem.

## Environment management with Metaflow

As previously mentioned, there are a few distinct steps in environment management:
- specifying your dependencies
- resolving your dependencies to obtain a fixed list of all packages needed for your environment
- hydrating your environment so that it is usable

Metaflow has several tools for each of these stages.

### Specifying your dependencies

#### Using decorators
Metaflow has historically supported specifying your dependencies in code using the `@conda` decorator. This
approach is still supported today and expanded.


```py title="simplecondaflow.py"
from metaflow import FlowSpec, step, conda

class CondaTestFlow(FlowSpec):
    
    @conda(libraries={"pandas": "1.4.0"}, python=">=3.8,<3.9")
    @step
    def start(self):
        import pandas as pd
        assert pd.__version__ == "1.4.0"
        print("I am in start and Pandas version is %s" % pd.__version__)
        self.next(self.end)
        
    @conda(libraries={"pandas": "1.5.0"}, python=">=3.8,<3.9")
    @step
    def end(self):
        import pandas as pd
        assert pd.__version__ == "1.5.0"
        print("I am in end and Pandas version is %s" % pd.__version__)

if __name__ == "__main__":
    CondaTestFlow()
```

In the code above, both steps will have different versions of pandas -- start will have version 1.4.0 and end will
have version 1.5.0. You can verify this by executing the following -- note the required use of
`--environment=conda`.


```bash
python simplecondaflow.py --environment=conda run
```

<CodeOutputBlock lang="bash">

```
Workflow starting (run-id 142), see it in the UI at https://mfui.net/CondaTestFlow/142
     Using existing Conda environment 42a4ed94b63f12e1fe9dd29de21bf9ec6e271b1c (a3b104c4ce2215351a2b94076ef7827de3ad890a)
     [142/start/145426383 (pid 21786)] Task is starting.
     [142/start/145426383 (pid 21786)] I am in start and Pandas version is 1.4.0
     [142/start/145426383 (pid 21786)] Task finished successfully.
     Using existing Conda environment 3e07a415e7766b8ed359f00e5f48e35ec79ac056 (41a06733cd332951fa100475c3a05c6916272899)
     [142/end/145426398 (pid 21837)] Task is starting.
     [142/end/145426398 (pid 21837)] I am in end and Pandas version is 1.5.0
     [142/end/145426398 (pid 21837)] Task finished successfully.
     Done! See the run in the UI at https://mfui.net/CondaTestFlow/142
```

</CodeOutputBlock>

You can also specify something very similar using Pypi packages as follows.


```py title="simplecondaflow-pypi.py"
from metaflow import FlowSpec, step, pypi

class CondaTestFlowPypi(FlowSpec):
    
    @pypi(packages={"pandas": "1.4.0"}, python=">=3.8,<3.9")
    @step
    def start(self):
        import pandas as pd
        assert pd.__version__ == "1.4.0"
        print("I am in start and Pandas version is %s" % pd.__version__)
        self.next(self.end)
        
    @pypi(packages={"pandas": "1.5.0"}, python=">=3.8,<3.9")
    @step
    def end(self):
        import pandas as pd
        assert pd.__version__ == "1.5.0"
        print("I am in end and Pandas version is %s" % pd.__version__)

if __name__ == "__main__":
    CondaTestFlowPypi()
```


```bash
python simplecondaflow-pypi.py --environment=conda run
```

<CodeOutputBlock lang="bash">

```
Workflow starting (run-id 9), see it in the UI at https://mfui.net/CondaTestFlowPypi/9
     Creating Conda environment 7b6ff22b9c6c2af6acc2fa29f92d3aef60172cc6 (e6e7f85964171aafa334466817836d18914cc6f5)...
     [9/start/145426662 (pid 22320)] Task is starting.
     [9/start/145426662 (pid 22320)] I am in start and Pandas version is 1.4.0
     [9/start/145426662 (pid 22320)] Task finished successfully.
     Creating Conda environment fa4e3c89a7e41c881c22fe24f28732ac4a1e3b18 (65754ebb12457aff83eee1c9246095577fc934e6)...
     [9/end/145426756 (pid 22442)] Task is starting.
     [9/end/145426756 (pid 22442)] I am in end and Pandas version is 1.5.0
     [9/end/145426756 (pid 22442)] Task finished successfully.
     Done! See the run in the UI at https://mfui.net/CondaTestFlowPypi/9
```

</CodeOutputBlock>

Metaflow now supports six decorators:
- `@conda`
- `@conda_base`
- `@pypi`
- `@pypi_base`
- `@named_env`
- `@named_env_base`

The `*_base` decorators are flow level decorators and allow you to specify dependencies that apply by default
to all steps in your flow. You can override the information provided by `@conda_base` with a step specific
`@conda` (in other words, information provided in `@conda` overrides the one provided in `@conda_base`).

In the `@conda` and `@conda_base` decorators, you can provide the following:
- `libraries`: dictionary of Conda libraries to include in your environment. The key is the name of the
library and the value is the version constraint. Version constraints can either be simple pinned versions like
`1.2.3` or more complicated constraints like `>=4.0,<6.0` to indicate a version greater or equal to `4.0` but
less than `6.0`. You can also specify an empty version constraint to leave your dependency completely floating
(it will be pinned at resolution time). For Conda libraries only, you can also specify a package name as
`channel::package` to specify that a specific package should come from a given channel.
- `channels`: list of additional Conda channels to search for packages.
- `python`: string representing the Python version constraint. It can also be a simple version `3.7.12` or
a more complex constraint like `<3.8`
- `disabled`: boolean to disable conda for a particular step.

In the `@pypi` and `@pypi_base` decorators, you can provide the following:
- `python`: string representing the Python version constraint. It can also be a simple version `3.7.12` or
a more complex constraint like `<3.8`
- `packages`: dictionary like `libraries` for `@conda`
- `extra_indices`: list like `channels` for `@conda`
- `disabled`: boolean to disable the Conda environment for a particular step.

A given step can mix and match `@conda`, `@pypi` and `@named_env`. They are also all compatible with
`@conda_base`, `@pypi_base` and `@named_env_base`.

For a given step, the final requirements for that step will be computed as follows:
- the flow level requirements are merged. They have to be compatible with one another, for example, if you
specify:
```
@pypi_base(python="3.7")
@conda_base(python="3.8")
```
you will get an error. There are other restrictions described in this document particularly around the use of
[named environments](#named-environments).
- the step level requirement is similarly merged.
- the step level requirement *overrides* the calculated flow level requirement. In other words, if at the flow
level you specify `python=3.7` and at the step level you specify `python=3.8`, the step will run in an environment
with python 3.8.

Note that with these decorators, you can specify packages in one of three modes:
- pure Conda packages using the `@conda` and `@conda_base` decorators without specifying any pypi packages
- pure Pypi packages mode using the `@pypi` and `@pypi_base` decorators
- mixed mode using both `@conda`/`@conda_base` and `@pypi`/`@pypi_base`. This mode can provide a lot
of flexibility but it may be slower.

In terms of underlying tooling, the pure Pypi mode uses the `pip` tool to resolve dependencies while the mixed
mode uses `poetry`.

#### Using requirements.txt or environment.yml

Metaflow also allows you to resolve your environment using more traditional `requirements.txt` or
`environment.yml` files.

The `requirements.txt` file is great to resolve a pure Pypi environment and the `environment.yml` file can be used
for both pure Conda environments or mixed environments.

The syntax for `environment.yml` is given [here](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually). We only support the `channels` and
`dependencies` sections. You can specify packages in the `pip` section of the `dependencies` section to mix and
match Pypi and Conda packages

Two examples are given below to produce environments containing `numpy` version 1.21.5, one that will contain
only pure Pypi packages and the other containing Conda packages.


```txt title="req_numpy.txt"
numpy==1.21.5
```


```yml title="env_numpy.yml"
dependencies:
  - numpy=1.21.5
```

You can then resolve the environments using the `metaflow` command


```bash
metaflow environment resolve --python ">=3.8,<3.9" -r req_numpy.txt
```

<CodeOutputBlock lang="bash">

```
Metaflow (2.9.12+netflix-ext(1.0.0))
    
        Resolving 1 environment ... done in 26 seconds.
    ### Environment for architecture linux-64
    Environment of type pypi-only full hash 31389f7ee1e3378e4e5705388cbe109c551ac780:831fea8ae57a2ee59d93fca8e6643420c27f3cc6
    Arch linux-64
    Available on linux-64
    
    Resolved on 2023-09-06 18:11:25.714195
    Resolved by romain
    
    User-requested packages pypi::boto3==>=1.14.0, pypi::cffi==>=1.13.0,!=1.15.0, pypi::fastavro==>=1.6.0, pypi::numpy==1.21.5, pypi::pandas==>=0.24.0, pypi::pyarrow==>=0.17.1, conda::python==>=3.8,<3.9, pypi::requests==>=2.21.0
    User sources conda::conda-forge, pypi::https://pypi.org/simple
    
    Conda Packages installed _libgcc_mutex==0.1-conda_forge, _openmp_mutex==4.5-2_gnu, bzip2==1.0.8-h7f98852_4, ca-certificates==2023.7.22-hbcca054_0, ld_impl_linux-64==2.40-h41732ed_0, libffi==3.4.2-h7f98852_5, libgcc-ng==13.1.0-he5830b7_0, libgomp==13.1.0-he5830b7_0, libnsl==2.0.0-h7f98852_0, libsqlite==3.43.0-h2797004_0, libuuid==2.38.1-h0b41bf4_0, libzlib==1.2.13-hd590300_5, ncurses==6.4-hcb278e6_0, openssl==3.1.2-hd590300_0, pip==23.2.1-pyhd8ed1ab_0, python==3.8.17-he550d4f_0_cpython, readline==8.2-h8228510_1, setuptools==68.1.2-pyhd8ed1ab_0, tk==8.6.12-h27826a3_0, wheel==0.41.2-pyhd8ed1ab_0, xz==5.2.6-h166bdaf_0
    Pypi Packages installed boto3==1.28.41, botocore==1.31.41, certifi==2023.7.22, cffi==1.15.1, charset-normalizer==3.2.0, fastavro==1.8.2, idna==3.4, jmespath==1.0.1, numpy==1.21.5, pandas==2.0.3, pyarrow==13.0.0, pycparser==2.21, python-dateutil==2.8.2, pytz==2023.3.post1, requests==2.31.0, s3transfer==0.6.2, six==1.16.0, tzdata==2023.3, urllib3==1.26.16
    
        All packages already cached in s3.
        Caching 5 environments and aliases to s3 ... done in 0 seconds.
```

</CodeOutputBlock>


```bash
metaflow environment resolve --python ">=3.8,<3.9" -f env_numpy.yml
```

<CodeOutputBlock lang="bash">

```
Metaflow (2.9.12+netflix-ext(1.0.0))
    
        Resolving 1 environment ... done in 30 seconds.
    ### Environment for architecture linux-64
    Environment of type conda-only full hash d49465b2b45996e40aad1f3aaf00cba553a0f085:f671c941b27764ad6536f7fdadc6c57b18221c6e
    Arch linux-64
    Available on linux-64
    
    Resolved on 2023-09-06 07:16:01.794427
    Resolved by romain
    
    Locally present as /usr/local/libexec/metaflow-condav2-20230809/envs/metaflow_d49465b2b45996e40aad1f3aaf00cba553a0f085_f671c941b27764ad6536f7fdadc6c57b18221c6e
    
    User-requested packages conda::boto3==>=1.14.0, conda::cffi==>=1.13.0,!=1.15.0, conda::fastavro==>=1.6.0, conda::numpy==1.21.5, conda::pandas==>=0.24.0, conda::pyarrow==>=0.17.1, conda::python==>=3.8,<3.9, conda::requests==>=2.21.0
    User sources conda::conda-forge
    
    Conda Packages installed _libgcc_mutex==0.1-conda_forge, _openmp_mutex==4.5-2_gnu, arrow-cpp==11.0.0-ha770c72_13_cpu, aws-c-auth==0.6.26-h987a71b_2, aws-c-cal==0.5.21-h48707d8_2, aws-c-common==0.8.14-h0b41bf4_0, aws-c-compression==0.2.16-h03acc5a_5, aws-c-event-stream==0.2.20-h00877a2_4, aws-c-http==0.7.6-hf342b9f_0, aws-c-io==0.13.19-h5b20300_3, aws-c-mqtt==0.8.6-hc4349f7_12, aws-c-s3==0.2.7-h909e904_1, aws-c-sdkutils==0.1.9-h03acc5a_0, aws-checksums==0.1.14-h03acc5a_5, aws-crt-cpp==0.19.8-hf7fbfca_12, aws-sdk-cpp==1.10.57-h17c43bd_8, boto3==1.28.41-pyhd8ed1ab_0, botocore==1.31.41-pyhd8ed1ab_0, brotlipy==0.7.0-py38h0a891b7_1005, bzip2==1.0.8-h7f98852_4, c-ares==1.19.1-hd590300_0, ca-certificates==2023.7.22-hbcca054_0, certifi==2023.7.22-pyhd8ed1ab_0, cffi==1.15.1-py38h4a40e3a_3, charset-normalizer==3.2.0-pyhd8ed1ab_0, cryptography==41.0.3-py38hcdda232_0, fastavro==1.8.2-py38h01eb140_0, gflags==2.2.2-he1b5a44_1004, glog==0.6.0-h6f12383_0, idna==3.4-pyhd8ed1ab_0, jmespath==1.0.1-pyhd8ed1ab_0, keyutils==1.6.1-h166bdaf_0, krb5==1.21.2-h659d440_0, ld_impl_linux-64==2.40-h41732ed_0, libabseil==20230125.0-cxx17_hcb278e6_1, libarrow==11.0.0-h93537a5_13_cpu, libblas==3.9.0-18_linux64_openblas, libbrotlicommon==1.0.9-h166bdaf_9, libbrotlidec==1.0.9-h166bdaf_9, libbrotlienc==1.0.9-h166bdaf_9, libcblas==3.9.0-18_linux64_openblas, libcrc32c==1.1.2-h9c3ff4c_0, libcurl==8.2.1-hca28451_0, libedit==3.1.20191231-he28a2e2_2, libev==4.33-h516909a_1, libevent==2.1.12-hf998b51_1, libffi==3.4.2-h7f98852_5, libgcc-ng==13.1.0-he5830b7_0, libgfortran-ng==13.1.0-h69a702a_0, libgfortran5==13.1.0-h15d22d2_0, libgomp==13.1.0-he5830b7_0, libgoogle-cloud==2.8.0-h0bc5f78_1, libgrpc==1.52.1-hcf146ea_1, liblapack==3.9.0-18_linux64_openblas, libnghttp2==1.52.0-h61bc06f_0, libnsl==2.0.0-h7f98852_0, libnuma==2.0.16-h0b41bf4_1, libopenblas==0.3.24-pthreads_h413a1c8_0, libprotobuf==3.21.12-hfc55251_2, libsqlite==3.43.0-h2797004_0, libssh2==1.11.0-h0841786_0, libstdcxx-ng==13.1.0-hfd8a6a1_0, libthrift==0.18.1-h8fd135c_2, libutf8proc==2.8.0-h166bdaf_0, libuuid==2.38.1-h0b41bf4_0, libzlib==1.2.13-hd590300_5, lz4-c==1.9.4-hcb278e6_0, ncurses==6.4-hcb278e6_0, numpy==1.21.5-py38h1d589f8_1, openssl==3.1.2-hd590300_0, orc==1.8.3-h2f23424_1, pandas==2.0.0-py38hdc8b05c_0, parquet-cpp==1.5.1-2, pip==23.2.1-pyhd8ed1ab_0, pyarrow==11.0.0-py38hf05218d_13_cpu, pycparser==2.21-pyhd8ed1ab_0, pyopenssl==23.2.0-pyhd8ed1ab_1, pysocks==1.7.1-pyha2e5f31_6, python==3.8.17-he550d4f_0_cpython, python-dateutil==2.8.2-pyhd8ed1ab_0, python-tzdata==2023.3-pyhd8ed1ab_0, python_abi==3.8-3_cp38, pytz==2023.3.post1-pyhd8ed1ab_0, rdma-core==28.9-h59595ed_1, re2==2023.02.02-hcb278e6_0, readline==8.2-h8228510_1, requests==2.31.0-pyhd8ed1ab_0, s2n==1.3.41-h3358134_0, s3transfer==0.6.2-pyhd8ed1ab_0, setuptools==68.1.2-pyhd8ed1ab_0, six==1.16.0-pyh6c4a22f_0, snappy==1.1.10-h9fff704_0, tk==8.6.12-h27826a3_0, ucx==1.14.1-h64cca9d_3, urllib3==1.26.15-pyhd8ed1ab_0, wheel==0.41.2-pyhd8ed1ab_0, xz==5.2.6-h166bdaf_0, zlib==1.2.13-hd590300_5, zstd==1.5.5-hfc55251_0
    
        All packages already cached in s3.
        All environments already cached in s3.
```

</CodeOutputBlock>

Once these environments are resolved, you can use them in your flows, see [named environments](#named-environments)
for details on how.

##### Supported options in requirements.txt

Metaflow does not support all options in `requirements.txt`. It supports the following options (one per line):
- `--extra-index-url` to pass additional sources
- `--pre`, `--no-index`
- `-f`, `--find-links` and `--trusted-host` and their argument
- `--conda-pkg` to specify non Python conda packages. This is actually an extension and not supported in a
  normal `requirements.txt`. It allows you to use a Pypi only Python environment while still using Conda
  for non Python packages.

In addition to the options above, you can specify your requirements using the usual syntax for requirement
specification. This includes GIT repositories and local directories. Metaflow does not support constraints and
environment markers.

Note that GIT repositories, local directories and non wheel packages are not compatible with
cross-architecture resolution (ie: the target architecture for the step is different from the one you are
resolving on -- for example if you resolve on your Mac to run on Batch).
Metaflow will build the wheel on the fly when resolving the environment
and this is only possible if the same architecture is used.

#### Restrictions

When resolving environments, please keep in mind the following restrictions.

- For Pypi packages, if a package is not readily available as a wheel, you will not be able to use it
when resolving environments across architectures. For example, if you are resolving on your Mac laptop
to run a flow on Batch and you need a package that is not available as a wheel package, you will
get an error. This is typically the case with packages that are sources from GIT repositories or local directories
for example.
- In mixed mode (both Pypi and Conda packages), non wheel packages are not supported. This includes packages
from GIT repositories or local directories.

### Resolving your environment

Metaflow will resolve your environment in two cases:
- automatically prior to your flow executing or being deployed to Argo/Airflow/StepFunctions
- manually if you use the above `metaflow resolve` command or the flow level resolve command (we use `--force`
here to show how to re-resolve environments as they are already resolved from running the flow previously):


```bash
python simplecondaflow.py --environment=conda environment resolve --force
```

<CodeOutputBlock lang="bash">

```
Metaflow 2.9.12+netflix-ext(1.0.0) executing CondaTestFlow for user:romain
        Resolving 2 environments ... done in 27 seconds.
    ### Environment for step start ###
    DEFAULT Environment of type conda-only full hash 42a4ed94b63f12e1fe9dd29de21bf9ec6e271b1c:a3b104c4ce2215351a2b94076ef7827de3ad890a
    Arch linux-64
    Available on linux-64
    
    Resolved on 2023-09-06 07:11:05.629324
    Resolved by romain
    
    Locally present as metaflow_42a4ed94b63f12e1fe9dd29de21bf9ec6e271b1c_a3b104c4ce2215351a2b94076ef7827de3ad890a
    
    User-requested packages conda::boto3==>=1.14.0, conda::cffi==>=1.13.0,!=1.15.0, conda::fastavro==>=1.6.0, conda::pandas==1.4.0,>=0.24.0, conda::pyarrow==>=0.17.1, conda::python==>=3.8,<3.9, conda::requests==>=2.21.0
    User sources conda::conda-forge
    
    Conda Packages installed _libgcc_mutex==0.1-conda_forge, _openmp_mutex==4.5-2_gnu, aws-c-auth==0.7.3-he2921ad_3, aws-c-cal==0.6.2-hc309b26_0, aws-c-common==0.9.0-hd590300_0, aws-c-compression==0.2.17-h4d4d85c_2, aws-c-event-stream==0.3.2-h2e3709c_0, aws-c-http==0.7.12-hc865f51_1, aws-c-io==0.13.32-h019f825_2, aws-c-mqtt==0.9.5-h3a0376c_1, aws-c-s3==0.3.14-h1678ad6_3, aws-c-sdkutils==0.1.12-h4d4d85c_1, aws-checksums==0.1.17-h4d4d85c_1, aws-crt-cpp==0.23.0-h40cdbb9_5, aws-sdk-cpp==1.10.57-h6f6b8fa_21, boto3==1.28.41-pyhd8ed1ab_0, botocore==1.31.41-pyhd8ed1ab_0, brotlipy==0.7.0-py38h0a891b7_1005, bzip2==1.0.8-h7f98852_4, c-ares==1.19.1-hd590300_0, ca-certificates==2023.7.22-hbcca054_0, certifi==2023.7.22-pyhd8ed1ab_0, cffi==1.15.1-py38h4a40e3a_3, charset-normalizer==3.2.0-pyhd8ed1ab_0, cryptography==41.0.3-py38hcdda232_0, fastavro==1.8.2-py38h01eb140_0, gflags==2.2.2-he1b5a44_1004, glog==0.6.0-h6f12383_0, idna==3.4-pyhd8ed1ab_0, jmespath==1.0.1-pyhd8ed1ab_0, keyutils==1.6.1-h166bdaf_0, krb5==1.21.2-h659d440_0, ld_impl_linux-64==2.40-h41732ed_0, libabseil==20230125.3-cxx17_h59595ed_0, libarrow==13.0.0-hb6645e0_1_cpu, libblas==3.9.0-18_linux64_openblas, libbrotlicommon==1.1.0-hd590300_0, libbrotlidec==1.1.0-hd590300_0, libbrotlienc==1.1.0-hd590300_0, libcblas==3.9.0-18_linux64_openblas, libcrc32c==1.1.2-h9c3ff4c_0, libcurl==8.2.1-hca28451_0, libedit==3.1.20191231-he28a2e2_2, libev==4.33-h516909a_1, libevent==2.1.12-hf998b51_1, libffi==3.4.2-h7f98852_5, libgcc-ng==13.1.0-he5830b7_0, libgfortran-ng==13.1.0-h69a702a_0, libgfortran5==13.1.0-h15d22d2_0, libgomp==13.1.0-he5830b7_0, libgoogle-cloud==2.12.0-h840a212_1, libgrpc==1.56.2-h3905398_1, liblapack==3.9.0-18_linux64_openblas, libnghttp2==1.52.0-h61bc06f_0, libnsl==2.0.0-h7f98852_0, libnuma==2.0.16-h0b41bf4_1, libopenblas==0.3.24-pthreads_h413a1c8_0, libprotobuf==4.23.3-hd1fb520_1, libsqlite==3.43.0-h2797004_0, libssh2==1.11.0-h0841786_0, libstdcxx-ng==13.1.0-hfd8a6a1_0, libthrift==0.19.0-h8fd135c_0, libutf8proc==2.8.0-h166bdaf_0, libuuid==2.38.1-h0b41bf4_0, libzlib==1.2.13-hd590300_5, lz4-c==1.9.4-hcb278e6_0, ncurses==6.4-hcb278e6_0, numpy==1.24.4-py38h59b608b_0, openssl==3.1.2-hd590300_0, orc==1.9.0-h385abfd_1, pandas==1.4.0-py38h43a58ef_0, pip==23.2.1-pyhd8ed1ab_0, pyarrow==13.0.0-py38h96a5bb7_1_cpu, pycparser==2.21-pyhd8ed1ab_0, pyopenssl==23.2.0-pyhd8ed1ab_1, pysocks==1.7.1-pyha2e5f31_6, python==3.8.17-he550d4f_0_cpython, python-dateutil==2.8.2-pyhd8ed1ab_0, python_abi==3.8-3_cp38, pytz==2023.3.post1-pyhd8ed1ab_0, rdma-core==28.9-h59595ed_1, re2==2023.03.02-h8c504da_0, readline==8.2-h8228510_1, requests==2.31.0-pyhd8ed1ab_0, s2n==1.3.49-h06160fa_0, s3transfer==0.6.2-pyhd8ed1ab_0, setuptools==68.1.2-pyhd8ed1ab_0, six==1.16.0-pyh6c4a22f_0, snappy==1.1.10-h9fff704_0, tk==8.6.12-h27826a3_0, ucx==1.14.1-h64cca9d_3, urllib3==1.26.15-pyhd8ed1ab_0, wheel==0.41.2-pyhd8ed1ab_0, xz==5.2.6-h166bdaf_0, zstd==1.5.5-hfc55251_0
    
    
    
    
    ### Environment for step end ###
    DEFAULT Environment of type conda-only full hash 3e07a415e7766b8ed359f00e5f48e35ec79ac056:41a06733cd332951fa100475c3a05c6916272899
    Arch linux-64
    Available on linux-64
    
    Resolved on 2023-09-06 07:11:05.563465
    Resolved by romain
    
    Locally present as metaflow_3e07a415e7766b8ed359f00e5f48e35ec79ac056_41a06733cd332951fa100475c3a05c6916272899
    
    User-requested packages conda::boto3==>=1.14.0, conda::cffi==>=1.13.0,!=1.15.0, conda::fastavro==>=1.6.0, conda::pandas==1.5.0,>=0.24.0, conda::pyarrow==>=0.17.1, conda::python==>=3.8,<3.9, conda::requests==>=2.21.0
    User sources conda::conda-forge
    
    Conda Packages installed _libgcc_mutex==0.1-conda_forge, _openmp_mutex==4.5-2_gnu, aws-c-auth==0.7.3-he2921ad_3, aws-c-cal==0.6.2-hc309b26_0, aws-c-common==0.9.0-hd590300_0, aws-c-compression==0.2.17-h4d4d85c_2, aws-c-event-stream==0.3.2-h2e3709c_0, aws-c-http==0.7.12-hc865f51_1, aws-c-io==0.13.32-h019f825_2, aws-c-mqtt==0.9.5-h3a0376c_1, aws-c-s3==0.3.14-h1678ad6_3, aws-c-sdkutils==0.1.12-h4d4d85c_1, aws-checksums==0.1.17-h4d4d85c_1, aws-crt-cpp==0.23.0-h40cdbb9_5, aws-sdk-cpp==1.10.57-h6f6b8fa_21, boto3==1.28.41-pyhd8ed1ab_0, botocore==1.31.41-pyhd8ed1ab_0, brotlipy==0.7.0-py38h0a891b7_1005, bzip2==1.0.8-h7f98852_4, c-ares==1.19.1-hd590300_0, ca-certificates==2023.7.22-hbcca054_0, certifi==2023.7.22-pyhd8ed1ab_0, cffi==1.15.1-py38h4a40e3a_3, charset-normalizer==3.2.0-pyhd8ed1ab_0, cryptography==41.0.3-py38hcdda232_0, fastavro==1.8.2-py38h01eb140_0, gflags==2.2.2-he1b5a44_1004, glog==0.6.0-h6f12383_0, idna==3.4-pyhd8ed1ab_0, jmespath==1.0.1-pyhd8ed1ab_0, keyutils==1.6.1-h166bdaf_0, krb5==1.21.2-h659d440_0, ld_impl_linux-64==2.40-h41732ed_0, libabseil==20230125.3-cxx17_h59595ed_0, libarrow==13.0.0-hb6645e0_1_cpu, libblas==3.9.0-18_linux64_openblas, libbrotlicommon==1.1.0-hd590300_0, libbrotlidec==1.1.0-hd590300_0, libbrotlienc==1.1.0-hd590300_0, libcblas==3.9.0-18_linux64_openblas, libcrc32c==1.1.2-h9c3ff4c_0, libcurl==8.2.1-hca28451_0, libedit==3.1.20191231-he28a2e2_2, libev==4.33-h516909a_1, libevent==2.1.12-hf998b51_1, libffi==3.4.2-h7f98852_5, libgcc-ng==13.1.0-he5830b7_0, libgfortran-ng==13.1.0-h69a702a_0, libgfortran5==13.1.0-h15d22d2_0, libgomp==13.1.0-he5830b7_0, libgoogle-cloud==2.12.0-h840a212_1, libgrpc==1.56.2-h3905398_1, liblapack==3.9.0-18_linux64_openblas, libnghttp2==1.52.0-h61bc06f_0, libnsl==2.0.0-h7f98852_0, libnuma==2.0.16-h0b41bf4_1, libopenblas==0.3.24-pthreads_h413a1c8_0, libprotobuf==4.23.3-hd1fb520_1, libsqlite==3.43.0-h2797004_0, libssh2==1.11.0-h0841786_0, libstdcxx-ng==13.1.0-hfd8a6a1_0, libthrift==0.19.0-h8fd135c_0, libutf8proc==2.8.0-h166bdaf_0, libuuid==2.38.1-h0b41bf4_0, libzlib==1.2.13-hd590300_5, lz4-c==1.9.4-hcb278e6_0, ncurses==6.4-hcb278e6_0, numpy==1.24.4-py38h59b608b_0, openssl==3.1.2-hd590300_0, orc==1.9.0-h385abfd_1, pandas==1.5.0-py38h8f669ce_0, pip==23.2.1-pyhd8ed1ab_0, pyarrow==13.0.0-py38h96a5bb7_1_cpu, pycparser==2.21-pyhd8ed1ab_0, pyopenssl==23.2.0-pyhd8ed1ab_1, pysocks==1.7.1-pyha2e5f31_6, python==3.8.17-he550d4f_0_cpython, python-dateutil==2.8.2-pyhd8ed1ab_0, python_abi==3.8-3_cp38, pytz==2023.3.post1-pyhd8ed1ab_0, rdma-core==28.9-h59595ed_1, re2==2023.03.02-h8c504da_0, readline==8.2-h8228510_1, requests==2.31.0-pyhd8ed1ab_0, s2n==1.3.49-h06160fa_0, s3transfer==0.6.2-pyhd8ed1ab_0, setuptools==68.1.2-pyhd8ed1ab_0, six==1.16.0-pyh6c4a22f_0, snappy==1.1.10-h9fff704_0, tk==8.6.12-h27826a3_0, ucx==1.14.1-h64cca9d_3, urllib3==1.26.15-pyhd8ed1ab_0, wheel==0.41.2-pyhd8ed1ab_0, xz==5.2.6-h166bdaf_0, zstd==1.5.5-hfc55251_0
    
    
    
    
        All packages already cached in s3.
        All environments already cached in s3.
```

</CodeOutputBlock>

It is important to note that in both cases, the environment is resolved on your local machine (the one launching
the run or deploying to Argo/Airflow/StepFunctions) and never on a remote node.

When an environment is resolved, it is also cached to S3/GS/Azure for faster retrieval which may increase the initial
resolution process (but it will speed it up later).
We cache both the packages that form your environment as well as the definition of the environment itself
allowing you to share it (more in [named environments](#named-environments)).

It is important to note *when* your environments are resolved. By default Metaflow will only resolve environments
for which it doesn't locally know a resolution. Each environment is defined by two things:
- the set of requirements you, the user, set for it -- this is the requirement identifier
- the set of packages that compose it after resolution -- this is the full identifier

For a given requirement identifier, Metaflow will use a *default* environment which corresponds to the latest
locally resolved environment. In other words, if you execute a flow twice, the second time, the environment will
not be re-resolved but the previously resolved environment will be used. You can see this in the previous
output that shows that the environment is already resolved (it was resolved when we executed the flow).

You can, however, force Metaflow to re-resolve your environment by passing the `--force` flag to either
`metaflow resolve` or `python myflow.py --environment=conda environment resolve`.

[Named environments](#named-environments) are not resolved (as they are already resolved) but they are associated
with your step at the same time as an environment would be resolved (so just prior to execution or deployment).

Two advanced options described in [Using Remotely Resolved Environments](#using-remotely-resolved-environments)
and [Delaying Environment Fetching](#delaying-environment-fetching) allow you to modify the default behaviors
described above.

### Hydrating your environment

Now that your environment is resolved, you need to actually be able to use it. We refer to this as
"hydrating" your environment; it's the process of turning a list of packages to install into an actual
executable environment that you can use. Hydrating an environment always takes place on the target
machine. In other words, if you are executing `--with batch`, the environments will not be hydrated on
your local machine but on each individual Batch node. Similarly, when you deploy to Argo/Airflow/StepFunctions, the environments
will be hydrated on each node launched by Argo/Airflow/StepFunctions.
Metaflow will automatically hydrate your environment when executing your flow:
- if your step executes locally, a local Conda environment will be created and used to execute your step
- if your step executes remotely (on Batch or through Argo/Airflow/StepFunctions), Metaflow will download the
packages needed, create an environment on the remote node and use it to execute your step

You may, however, want to manually re-hydrate your environment:
- you can use a re-hydrated environment to inspect artifacts produced by your flow
- you can use a re-hydraded environment to debug errors you notice in your flow

When re-hydrating an environment, you also have the option of creating a Jupyter kernel on your
machine that you can then use in a notebook environment giving you an easy way to experiment
in an environment that is identical to the one your step will execute in (or executed in already).

The following command will create a new environment called `testenv` and will create a new Jupyter
kernel called `Metaflow CondaTestFlow/118/start`. It will also download the step's code in a directory `testdir`
and make its content available in the notebook (if you try this at home,
you can do `from mymodule import hello; hello()`).


```bash
metaflow environment create --name testenv --install-notebook --force --into-dir testdir --pathspec CondaTestFlow/118/start
```

<CodeOutputBlock lang="bash">

```
Metaflow (2.9.12+netflix-ext(1.0.0))
    
        Resolving an environment compatible with Jupyter ...    Resolving 1 environment ... done in 37 seconds.
        All packages already cached in s3.
        All environments already cached in s3.
     done in 40 seconds.
        Extracting and linking Conda environment ... done in 7 seconds.
    Code package for step:CondaTestFlow/118/start downloaded into '/root/testdir' -- `__conda_python` is the executable to use
        Installing Jupyter kernel ... done in 1 second.
    Created environment 'testenv' locally, activate with `/usr/local/libexec/metaflow-condav2-20230809/bin/mamba activate testenv`
```

</CodeOutputBlock>

You can of course naturally use this directly in the shell (without Jupyter notebooks):


```bash
cd testdir && /usr/local/libexec/metaflow-condav2-20230809/envs/testenv/bin/python -c "from mymodule import hello; hello(); import pandas as pd; print(pd.__version__)"
```

<CodeOutputBlock lang="bash">

```
Hello World!
    1.4.0
```

</CodeOutputBlock>

## Named environments

We have now seen how to specify dependencies, resolve and re-hydrade environments. Metaflow supports another tool
to help you make the most of environments: named environments through the `@named_env` decorator.

You can view named environments similarly to Docker tags: they are aliases for a fully resolved environment. As
previously mentioned, environments have a full identifier which encodes all the packages that are used
in that environment. This full identifier uniquely identifies an environment but is very unwieldy (it's a 40
character hexadecimal string). Metaflow therefore allows you to *alias* this identifier with one of your own
and to then refer to it in steps (for example).

### Format of aliases

Aliases are simple strings. To simplify the process of finding unique strings, however, we recommend using the
following naming convention:
- a "/" separated name
- an optional ":" followed by a tag (for example a version number)

As an example, the following is a good alias identifying an environment in the MLP team, for the Metaflow sub-team
and the "conda-example" project: `mlp/metaflow/conda_example/numpy_test_env:v1`.

If the optional ":" and tag are not specified, they are assumed to be ":latest".

Aliases are immutable (ie: once an alias is used for an environment, it cannot be used for another one) with
the following notable exception: aliases that have the tag "latest", "candidate" or "stable" **are** mutable which
allows you to support a pattern of "latest environment with this name".

### Creating named environments
A simple way to create a named environment is using the `resolve` command above with the `--alias` option:


```bash
metaflow environment resolve --python ">=3.8,<3.9" --alias mlp/metaflow/conda_example/numpy_test_env -f env_numpy.yml
```

<CodeOutputBlock lang="bash">

```
Metaflow (2.9.12+netflix-ext(1.0.0))
    
    No environments to resolve, aliasing only. Use --force to force re-resolution
```

</CodeOutputBlock>

In the command above, you can see that the environment is already resolved (we resolved it previously earlier)
so Metaflow will simply add the alias to the environment.

You can then refer to this environment in your step as shown below:


```py title="simplecondaflow2.py"
from metaflow import FlowSpec, conda, step, named_env

class CondaTestFlow(FlowSpec):
    
    @named_env(name="mlp/metaflow/conda_example/numpy_test_env")
    @step
    def start(self):
        import numpy as np
        print("I have numpy version %s" % np.__version__)
        assert np.__version__ == "1.21.5"
        self.next(self.end)
        
    @conda(libraries={"pandas": "1.5.0"}, python=">=3.8,<3.9")
    @step
    def end(self):
        import pandas as pd
        assert pd.__version__ == "1.5.0"
        print("I am in end and Pandas version is %s" % pd.__version__)

if __name__ == "__main__":
    CondaTestFlow()
```


```bash
python simplecondaflow2.py --environment=conda run
```

<CodeOutputBlock lang="bash">

```
Workflow starting (run-id 143), see it in the UI at https://mfui.net/CondaTestFlow/143
     Using existing Conda environment d49465b2b45996e40aad1f3aaf00cba553a0f085 (f671c941b27764ad6536f7fdadc6c57b18221c6e)
     [143/start/145427077 (pid 23192)] Task is starting.
     [143/start/145427077 (pid 23192)] I have numpy version 1.21.5
     [143/start/145427077 (pid 23192)] Task finished successfully.
     Using existing Conda environment 3e07a415e7766b8ed359f00e5f48e35ec79ac056 (41a06733cd332951fa100475c3a05c6916272899)
     [143/end/145427092 (pid 23242)] Task is starting.
     [143/end/145427092 (pid 23242)] I am in end and Pandas version is 1.5.0
     [143/end/145427092 (pid 23242)] Task finished successfully.
     Done! See the run in the UI at https://mfui.net/CondaTestFlow/143
```

</CodeOutputBlock>

### Pathspecs are aliases too

Similar to the `name` argument, you can also specify a `pathspec` argument to `@named_env`
to refer to the environment of
a previously executed step. A `pathspec` is in the form `FlowName/RunId/StepName`.

### Sharing named environments

Metaflow will automatically save all aliases and will resolve automatically if someone else uses the same
named environment in a flow through a decorator (as shown in `simplecondaflow2.py`). You can also explicitly
fetch a named environment using `metaflow environment get` as shown below:


```bash
metaflow environment get mlp/metaflow/conda_example/numpy_test_env
```

<CodeOutputBlock lang="bash">

```
Metaflow (2.9.12+netflix-ext(1.0.0))
    
    DEFAULT Environment of type conda-only full hash d49465b2b45996e40aad1f3aaf00cba553a0f085:f671c941b27764ad6536f7fdadc6c57b18221c6e
    Aliases mlp/metaflow/conda_example/numpy_test_env:latest (mutable)
    Arch linux-64
    Available on linux-64
    
    Resolved on 2023-09-06 07:16:01.794427
    Resolved by romain
    
    Locally present as /usr/local/libexec/metaflow-condav2-20230809/envs/metaflow_d49465b2b45996e40aad1f3aaf00cba553a0f085_f671c941b27764ad6536f7fdadc6c57b18221c6e
    
    User-requested packages conda::boto3==>=1.14.0, conda::cffi==>=1.13.0,!=1.15.0, conda::fastavro==>=1.6.0, conda::numpy==1.21.5, conda::pandas==>=0.24.0, conda::pyarrow==>=0.17.1, conda::python==>=3.8,<3.9, conda::requests==>=2.21.0
    User sources conda::conda-forge
    
    Conda Packages installed _libgcc_mutex==0.1-conda_forge, _openmp_mutex==4.5-2_gnu, arrow-cpp==11.0.0-ha770c72_13_cpu, aws-c-auth==0.6.26-h987a71b_2, aws-c-cal==0.5.21-h48707d8_2, aws-c-common==0.8.14-h0b41bf4_0, aws-c-compression==0.2.16-h03acc5a_5, aws-c-event-stream==0.2.20-h00877a2_4, aws-c-http==0.7.6-hf342b9f_0, aws-c-io==0.13.19-h5b20300_3, aws-c-mqtt==0.8.6-hc4349f7_12, aws-c-s3==0.2.7-h909e904_1, aws-c-sdkutils==0.1.9-h03acc5a_0, aws-checksums==0.1.14-h03acc5a_5, aws-crt-cpp==0.19.8-hf7fbfca_12, aws-sdk-cpp==1.10.57-h17c43bd_8, boto3==1.28.41-pyhd8ed1ab_0, botocore==1.31.41-pyhd8ed1ab_0, brotlipy==0.7.0-py38h0a891b7_1005, bzip2==1.0.8-h7f98852_4, c-ares==1.19.1-hd590300_0, ca-certificates==2023.7.22-hbcca054_0, certifi==2023.7.22-pyhd8ed1ab_0, cffi==1.15.1-py38h4a40e3a_3, charset-normalizer==3.2.0-pyhd8ed1ab_0, cryptography==41.0.3-py38hcdda232_0, fastavro==1.8.2-py38h01eb140_0, gflags==2.2.2-he1b5a44_1004, glog==0.6.0-h6f12383_0, idna==3.4-pyhd8ed1ab_0, jmespath==1.0.1-pyhd8ed1ab_0, keyutils==1.6.1-h166bdaf_0, krb5==1.21.2-h659d440_0, ld_impl_linux-64==2.40-h41732ed_0, libabseil==20230125.0-cxx17_hcb278e6_1, libarrow==11.0.0-h93537a5_13_cpu, libblas==3.9.0-18_linux64_openblas, libbrotlicommon==1.0.9-h166bdaf_9, libbrotlidec==1.0.9-h166bdaf_9, libbrotlienc==1.0.9-h166bdaf_9, libcblas==3.9.0-18_linux64_openblas, libcrc32c==1.1.2-h9c3ff4c_0, libcurl==8.2.1-hca28451_0, libedit==3.1.20191231-he28a2e2_2, libev==4.33-h516909a_1, libevent==2.1.12-hf998b51_1, libffi==3.4.2-h7f98852_5, libgcc-ng==13.1.0-he5830b7_0, libgfortran-ng==13.1.0-h69a702a_0, libgfortran5==13.1.0-h15d22d2_0, libgomp==13.1.0-he5830b7_0, libgoogle-cloud==2.8.0-h0bc5f78_1, libgrpc==1.52.1-hcf146ea_1, liblapack==3.9.0-18_linux64_openblas, libnghttp2==1.52.0-h61bc06f_0, libnsl==2.0.0-h7f98852_0, libnuma==2.0.16-h0b41bf4_1, libopenblas==0.3.24-pthreads_h413a1c8_0, libprotobuf==3.21.12-hfc55251_2, libsqlite==3.43.0-h2797004_0, libssh2==1.11.0-h0841786_0, libstdcxx-ng==13.1.0-hfd8a6a1_0, libthrift==0.18.1-h8fd135c_2, libutf8proc==2.8.0-h166bdaf_0, libuuid==2.38.1-h0b41bf4_0, libzlib==1.2.13-hd590300_5, lz4-c==1.9.4-hcb278e6_0, ncurses==6.4-hcb278e6_0, numpy==1.21.5-py38h1d589f8_1, openssl==3.1.2-hd590300_0, orc==1.8.3-h2f23424_1, pandas==2.0.0-py38hdc8b05c_0, parquet-cpp==1.5.1-2, pip==23.2.1-pyhd8ed1ab_0, pyarrow==11.0.0-py38hf05218d_13_cpu, pycparser==2.21-pyhd8ed1ab_0, pyopenssl==23.2.0-pyhd8ed1ab_1, pysocks==1.7.1-pyha2e5f31_6, python==3.8.17-he550d4f_0_cpython, python-dateutil==2.8.2-pyhd8ed1ab_0, python-tzdata==2023.3-pyhd8ed1ab_0, python_abi==3.8-3_cp38, pytz==2023.3.post1-pyhd8ed1ab_0, rdma-core==28.9-h59595ed_1, re2==2023.02.02-hcb278e6_0, readline==8.2-h8228510_1, requests==2.31.0-pyhd8ed1ab_0, s2n==1.3.41-h3358134_0, s3transfer==0.6.2-pyhd8ed1ab_0, setuptools==68.1.2-pyhd8ed1ab_0, six==1.16.0-pyh6c4a22f_0, snappy==1.1.10-h9fff704_0, tk==8.6.12-h27826a3_0, ucx==1.14.1-h64cca9d_3, urllib3==1.26.15-pyhd8ed1ab_0, wheel==0.41.2-pyhd8ed1ab_0, xz==5.2.6-h166bdaf_0, zlib==1.2.13-hd590300_5, zstd==1.5.5-hfc55251_0
```

</CodeOutputBlock>

## CLI

Metaflow includes a command line interface to interact with environments. This document already mentioned the
`resolve`, `create`, and `get` commands. There are a few more:


```bash
metaflow environment --help
```

<CodeOutputBlock lang="bash">

```
Metaflow (2.9.12+netflix-ext(1.0.0))
    
    Usage: metaflow environment [OPTIONS] COMMAND [ARGS]...
    
      Execution environment related commands.
    
    Options:
      --quiet / --no-quiet    Suppress unnecessary messages  [default: False]
      --metadata [local|service]  Metadata service type  [default: service]
      --datastore [s3|local]  Datastore type  [default: s3]
      --environment [conda]   Type of environment to manage  [default: conda]
      --conda-root TEXT       Root path for Conda cached information. If not set,
                              looks for METAFLOW_CONDA_S3ROOT (for S3)
    
      --help                  Show this message and exit.
    
    Commands:
      alias    Alias an existing environment
      create   Create a local environment based on env-name.
      get      Locally fetch an environment already resolved and present...
      resolve  Resolve an environment
      show     Show information about an environment
```

</CodeOutputBlock>

### The `show` command

The `show` command allows you to see the content of any environment for any step that ever ran using Conda (this
new iteration of Conda). You can pass it either an environment alias, a full environment ID (in the form
`requirement-id:full-id` or a pathspec. As an example:


```bash
metaflow environment show --pathspec CondaTestFlow/118/start
```

<CodeOutputBlock lang="bash">

```
Metaflow (2.9.12+netflix-ext(1.0.0))
    
    ### Environment for 'step:CondaTestFlow/118/start' (arch 'linux-64'):
    Environment of type conda-only full hash b7b01560a5e30245d7aa963834acc9c4fd199674:3ddd7bb8ff74ccb76f2f3405c6299068234a90aa
    Arch linux-64
    Available on linux-64
    
    Resolved on 2023-09-01 18:44:16.196203
    Resolved by romain
    
    Locally present as metaflow_b7b01560a5e30245d7aa963834acc9c4fd199674_3ddd7bb8ff74ccb76f2f3405c6299068234a90aa
    
    User-requested packages conda::boto3==>=1.14.0, conda::cffi==>=1.13.0,!=1.15.0, conda::pandas==1.4.0,>=0.24.0, conda::pyarrow==>=0.17.1, conda::python==>=3.8,<3.9, conda::requests==>=2.21.0
    User sources conda::conda-forge
    
    Conda Packages installed _libgcc_mutex==0.1-conda_forge, _openmp_mutex==4.5-2_gnu, aws-c-auth==0.7.3-he2921ad_3, aws-c-cal==0.6.2-hc309b26_0, aws-c-common==0.9.0-hd590300_0, aws-c-compression==0.2.17-h4d4d85c_2, aws-c-event-stream==0.3.2-h2e3709c_0, aws-c-http==0.7.12-hc865f51_1, aws-c-io==0.13.32-h019f825_2, aws-c-mqtt==0.9.5-h3a0376c_1, aws-c-s3==0.3.14-h1678ad6_3, aws-c-sdkutils==0.1.12-h4d4d85c_1, aws-checksums==0.1.17-h4d4d85c_1, aws-crt-cpp==0.23.0-h40cdbb9_5, aws-sdk-cpp==1.10.57-h6f6b8fa_21, boto3==1.28.39-pyhd8ed1ab_0, botocore==1.31.39-pyhd8ed1ab_0, brotlipy==0.7.0-py38h0a891b7_1005, bzip2==1.0.8-h7f98852_4, c-ares==1.19.1-hd590300_0, ca-certificates==2023.7.22-hbcca054_0, certifi==2023.7.22-pyhd8ed1ab_0, cffi==1.15.1-py38h4a40e3a_3, charset-normalizer==3.2.0-pyhd8ed1ab_0, cryptography==41.0.3-py38hcdda232_0, gflags==2.2.2-he1b5a44_1004, glog==0.6.0-h6f12383_0, idna==3.4-pyhd8ed1ab_0, jmespath==1.0.1-pyhd8ed1ab_0, keyutils==1.6.1-h166bdaf_0, krb5==1.21.2-h659d440_0, ld_impl_linux-64==2.40-h41732ed_0, libabseil==20230125.3-cxx17_h59595ed_0, libarrow==13.0.0-hb9dc469_0_cpu, libblas==3.9.0-17_linux64_openblas, libbrotlicommon==1.0.9-h166bdaf_9, libbrotlidec==1.0.9-h166bdaf_9, libbrotlienc==1.0.9-h166bdaf_9, libcblas==3.9.0-17_linux64_openblas, libcrc32c==1.1.2-h9c3ff4c_0, libcurl==8.2.1-hca28451_0, libedit==3.1.20191231-he28a2e2_2, libev==4.33-h516909a_1, libevent==2.1.12-hf998b51_1, libffi==3.4.2-h7f98852_5, libgcc-ng==13.1.0-he5830b7_0, libgfortran-ng==13.1.0-h69a702a_0, libgfortran5==13.1.0-h15d22d2_0, libgomp==13.1.0-he5830b7_0, libgoogle-cloud==2.12.0-h840a212_1, libgrpc==1.56.2-h3905398_1, liblapack==3.9.0-17_linux64_openblas, libnghttp2==1.52.0-h61bc06f_0, libnsl==2.0.0-h7f98852_0, libnuma==2.0.16-h0b41bf4_1, libopenblas==0.3.23-pthreads_h80387f5_0, libprotobuf==4.23.3-hd1fb520_1, libsqlite==3.43.0-h2797004_0, libssh2==1.11.0-h0841786_0, libstdcxx-ng==13.1.0-hfd8a6a1_0, libthrift==0.18.1-h8fd135c_2, libutf8proc==2.8.0-h166bdaf_0, libuuid==2.38.1-h0b41bf4_0, libzlib==1.2.13-hd590300_5, lz4-c==1.9.4-hcb278e6_0, ncurses==6.4-hcb278e6_0, numpy==1.24.4-py38h59b608b_0, openssl==3.1.2-hd590300_0, orc==1.9.0-h385abfd_1, pandas==1.4.0-py38h43a58ef_0, pip==23.2.1-pyhd8ed1ab_0, pyarrow==13.0.0-py38h96a5bb7_0_cpu, pycparser==2.21-pyhd8ed1ab_0, pyopenssl==23.2.0-pyhd8ed1ab_1, pysocks==1.7.1-pyha2e5f31_6, python==3.8.17-he550d4f_0_cpython, python-dateutil==2.8.2-pyhd8ed1ab_0, python_abi==3.8-3_cp38, pytz==2023.3-pyhd8ed1ab_0, rdma-core==28.9-h59595ed_1, re2==2023.03.02-h8c504da_0, readline==8.2-h8228510_1, requests==2.31.0-pyhd8ed1ab_0, s2n==1.3.49-h06160fa_0, s3transfer==0.6.2-pyhd8ed1ab_0, setuptools==68.1.2-pyhd8ed1ab_0, six==1.16.0-pyh6c4a22f_0, snappy==1.1.10-h9fff704_0, tk==8.6.12-h27826a3_0, ucx==1.14.1-h4a2ce2d_3, urllib3==1.26.15-pyhd8ed1ab_0, wheel==0.41.2-pyhd8ed1ab_0, xz==5.2.6-h166bdaf_0, zstd==1.5.5-hfc55251_0
```

</CodeOutputBlock>

## Tips and tricks

### Conda packages
You can find available Conda packages on `https://anaconda.org`. On that page, you can type the name of the
package you are looking for and select it from the resulting page to view the available versions.

The website makes it a bit tricky to do this but you can:
- select the package you are interested in
- select `Files`
- click on the `Version` drop-down

### What about storing and retrieving data artifacts between steps in my flow?
Metaflow relies on pickle for object serialization/deserialization. Make sure you have compatible versions
(ideally the same version) of the object module as a dependency in your steps which rely on interacting
with this object artifact.

### Interaction between `@conda_base` and `@conda`
Using the flow-level `@conda_base` decorator you can specify, for the flow, explicit library dependencies,
python version and also if you want to exclude all steps from executing within a conda environment.
Some examples:

```
from metaflow import FlowSpec, step, conda, conda_base

@conda_base(libraries={'numpy':'1.21.5'}, python='>3.8,<3.9')
class CondaTestFlow(FlowSpec):
    @step
    def a(self):
       ...

    @conda(libraries={'numpy':'1.21.6'})
    @step
    def b(self):
       ...

    @conda(disabled=True)
    @step
    def c(self):
       ...
```
In this example, step `a` executes in the environment `libraries={'numpy':'1.21.5'}, python='>3.8,<3.9'`,
step `b` executes in the environment `libraries={'numpy':'1.21.6'}, python='>3.8,<3.9`,
while step `c` executes outside the conda environment in the user environment.

## Advanced topics

### Extending named environments

Named environments can be *extended* by adding additional dependencies. This does not modify the
environment you are extending but instead creates a new environment. This allows you to take a
base environment and add additional packages. Note that all the locked dependencies of the original environment
are maintained so you cannot add an incompatible version of those packages. This functionality is
useful for adding other packages for example.

The code below shows how this is possible, a similar `--using-pathspec` also exists.


```yml title="env_extra.yml"
dependencies:
  - itsdangerous=2.1.2
```


```bash
metaflow environment resolve --using mlp/metaflow/conda_example/numpy_test_env --alias mlp/metaflow/conda_example/numpydanger_test_env -f env_extra.yml
```

<CodeOutputBlock lang="bash">

```
Metaflow (2.9.12+netflix-ext(1.0.0))
    
        Resolving 1 environment ... done in 91 seconds.
    ### Environment for architecture linux-64
    Environment of type conda-only full hash 91fd3cca65416fc4f2efe971520bbf1c4095d85c:f1b8d5c573f79074442c4d5d3d2da4e32497b41d
    Aliases mlp/metaflow/conda_example/numpydanger_test_env:latest (mutable)
    Arch linux-64
    Available on linux-64
    
    Resolved on 2023-09-06 07:19:50.413220
    Resolved by romain
    
    Locally present as /usr/local/libexec/metaflow-condav2-20230809/envs/metaflow_91fd3cca65416fc4f2efe971520bbf1c4095d85c_f1b8d5c573f79074442c4d5d3d2da4e32497b41d
    
    User-requested packages conda::boto3==>=1.14.0, conda::cffi==>=1.13.0,!=1.15.0, conda::fastavro==>=1.6.0, conda::itsdangerous==2.1.2, conda::numpy==1.21.5, conda::pandas==>=0.24.0, conda::pyarrow==>=0.17.1, conda::python==>=3.8,<3.9, conda::requests==>=2.21.0
    User sources conda::conda-forge
    
    Conda Packages installed _libgcc_mutex==0.1-conda_forge, _openmp_mutex==4.5-2_gnu, arrow-cpp==11.0.0-ha770c72_13_cpu, aws-c-auth==0.6.26-h987a71b_2, aws-c-cal==0.5.21-h48707d8_2, aws-c-common==0.8.14-h0b41bf4_0, aws-c-compression==0.2.16-h03acc5a_5, aws-c-event-stream==0.2.20-h00877a2_4, aws-c-http==0.7.6-hf342b9f_0, aws-c-io==0.13.19-h5b20300_3, aws-c-mqtt==0.8.6-hc4349f7_12, aws-c-s3==0.2.7-h909e904_1, aws-c-sdkutils==0.1.9-h03acc5a_0, aws-checksums==0.1.14-h03acc5a_5, aws-crt-cpp==0.19.8-hf7fbfca_12, aws-sdk-cpp==1.10.57-h17c43bd_8, boto3==1.28.41-pyhd8ed1ab_0, botocore==1.31.41-pyhd8ed1ab_0, brotlipy==0.7.0-py38h0a891b7_1005, bzip2==1.0.8-h7f98852_4, c-ares==1.19.1-hd590300_0, ca-certificates==2023.7.22-hbcca054_0, certifi==2023.7.22-pyhd8ed1ab_0, cffi==1.15.1-py38h4a40e3a_3, charset-normalizer==3.2.0-pyhd8ed1ab_0, cryptography==41.0.3-py38hcdda232_0, fastavro==1.8.2-py38h01eb140_0, gflags==2.2.2-he1b5a44_1004, glog==0.6.0-h6f12383_0, idna==3.4-pyhd8ed1ab_0, itsdangerous==2.1.2-pyhd8ed1ab_0, jmespath==1.0.1-pyhd8ed1ab_0, keyutils==1.6.1-h166bdaf_0, krb5==1.21.2-h659d440_0, ld_impl_linux-64==2.40-h41732ed_0, libabseil==20230125.0-cxx17_hcb278e6_1, libarrow==11.0.0-h93537a5_13_cpu, libblas==3.9.0-18_linux64_openblas, libbrotlicommon==1.0.9-h166bdaf_9, libbrotlidec==1.0.9-h166bdaf_9, libbrotlienc==1.0.9-h166bdaf_9, libcblas==3.9.0-18_linux64_openblas, libcrc32c==1.1.2-h9c3ff4c_0, libcurl==8.2.1-hca28451_0, libedit==3.1.20191231-he28a2e2_2, libev==4.33-h516909a_1, libevent==2.1.12-hf998b51_1, libffi==3.4.2-h7f98852_5, libgcc-ng==13.1.0-he5830b7_0, libgfortran-ng==13.1.0-h69a702a_0, libgfortran5==13.1.0-h15d22d2_0, libgomp==13.1.0-he5830b7_0, libgoogle-cloud==2.8.0-h0bc5f78_1, libgrpc==1.52.1-hcf146ea_1, liblapack==3.9.0-18_linux64_openblas, libnghttp2==1.52.0-h61bc06f_0, libnsl==2.0.0-h7f98852_0, libnuma==2.0.16-h0b41bf4_1, libopenblas==0.3.24-pthreads_h413a1c8_0, libprotobuf==3.21.12-hfc55251_2, libsqlite==3.43.0-h2797004_0, libssh2==1.11.0-h0841786_0, libstdcxx-ng==13.1.0-hfd8a6a1_0, libthrift==0.18.1-h8fd135c_2, libutf8proc==2.8.0-h166bdaf_0, libuuid==2.38.1-h0b41bf4_0, libzlib==1.2.13-hd590300_5, lz4-c==1.9.4-hcb278e6_0, ncurses==6.4-hcb278e6_0, numpy==1.21.5-py38h1d589f8_1, openssl==3.1.2-hd590300_0, orc==1.8.3-h2f23424_1, pandas==2.0.0-py38hdc8b05c_0, parquet-cpp==1.5.1-2, pip==23.2.1-pyhd8ed1ab_0, pyarrow==11.0.0-py38hf05218d_13_cpu, pycparser==2.21-pyhd8ed1ab_0, pyopenssl==23.2.0-pyhd8ed1ab_1, pysocks==1.7.1-pyha2e5f31_6, python==3.8.17-he550d4f_0_cpython, python-dateutil==2.8.2-pyhd8ed1ab_0, python-tzdata==2023.3-pyhd8ed1ab_0, python_abi==3.8-3_cp38, pytz==2023.3.post1-pyhd8ed1ab_0, rdma-core==28.9-h59595ed_1, re2==2023.02.02-hcb278e6_0, readline==8.2-h8228510_1, requests==2.31.0-pyhd8ed1ab_0, s2n==1.3.41-h3358134_0, s3transfer==0.6.2-pyhd8ed1ab_0, setuptools==68.1.2-pyhd8ed1ab_0, six==1.16.0-pyh6c4a22f_0, snappy==1.1.10-h9fff704_0, tk==8.6.12-h27826a3_0, ucx==1.14.1-h64cca9d_3, urllib3==1.26.15-pyhd8ed1ab_0, wheel==0.41.2-pyhd8ed1ab_0, xz==5.2.6-h166bdaf_0, zlib==1.2.13-hd590300_5, zstd==1.5.5-hfc55251_0
    
        All packages already cached in s3.
        All environments already cached in s3.
```

</CodeOutputBlock>


```py title="simplecondaflow3.py"
from metaflow import FlowSpec, conda, named_env, step

class CondaTestFlow(FlowSpec):
    
    @named_env(name="mlp/metaflow/conda_example/numpydanger_test_env")
    @step
    def start(self):
        import itsdangerous
        import numpy as np
        print("I have both numpy %s and itsdangerous %s" % (np.__version__, itsdangerous.__version__))
        assert np.__version__ == "1.21.5"
        assert itsdangerous.__version__ == "2.1.2"
        self.next(self.end)
        
    @conda(libraries={"pandas": "1.5.0"}, python=">=3.8,<3.9")
    @step
    def end(self):
        import pandas as pd
        assert pd.__version__ == "1.5.0"
        print("I am in end and Pandas version is %s" % pd.__version__)

if __name__ == "__main__":
    CondaTestFlow()
```


```bash
python simplecondaflow3.py --environment=conda run
```

<CodeOutputBlock lang="bash">

```
Workflow starting (run-id 144), see it in the UI at https://mfui.net/CondaTestFlow/144
     Using existing Conda environment 91fd3cca65416fc4f2efe971520bbf1c4095d85c (f1b8d5c573f79074442c4d5d3d2da4e32497b41d)
     [144/start/145427611 (pid 23565)] Task is starting.
     [144/start/145427611 (pid 23565)] I have both numpy 1.21.5 and itsdangerous 2.1.2
     [144/start/145427611 (pid 23565)] Task finished successfully.
     Using existing Conda environment 3e07a415e7766b8ed359f00e5f48e35ec79ac056 (41a06733cd332951fa100475c3a05c6916272899)
     [144/end/145427616 (pid 23619)] Task is starting.
     [144/end/145427616 (pid 23619)] I am in end and Pandas version is 1.5.0
     [144/end/145427616 (pid 23619)] Task finished successfully.
     Done! See the run in the UI at https://mfui.net/CondaTestFlow/144
```

</CodeOutputBlock>

You can accomplish the same result by using a combination of a named environment and a `@conda` or `@pypi`
decorator as shown below:


```py title="simplecondaflow4.py"
from metaflow import FlowSpec, step, conda, named_env

class CondaTestFlow(FlowSpec):
    
    @named_env(name="mlp/metaflow/conda_example/numpy_test_env")
    @conda(libraries={"itsdangerous": "2.1.2"})
    @step
    def start(self):
        import itsdangerous
        import numpy as np
        print("I have both numpy %s and itsdangerous %s" % (np.__version__, itsdangerous.__version__))
        assert np.__version__ == "1.21.5"
        assert itsdangerous.__version__ == "2.1.2"
        self.next(self.end)
        
    @conda(libraries={"pandas": "1.5.0"}, python=">=3.8,<3.9")
    @step
    def end(self):
        import pandas as pd
        assert pd.__version__ == "1.5.0"
        print("I am in end and Pandas version is %s" % pd.__version__)

if __name__ == "__main__":
    CondaTestFlow()
```


```bash
python simplecondaflow4.py --environment=conda run
```

<CodeOutputBlock lang="bash">

```
Workflow starting (run-id 145), see it in the UI at https://mfui.net/CondaTestFlow/145
     Using existing Conda environment 91fd3cca65416fc4f2efe971520bbf1c4095d85c (f1b8d5c573f79074442c4d5d3d2da4e32497b41d)
     [145/start/145427635 (pid 23733)] Task is starting.
     [145/start/145427635 (pid 23733)] I have both numpy 1.21.5 and itsdangerous 2.1.2
     [145/start/145427635 (pid 23733)] Task finished successfully.
     Using existing Conda environment 3e07a415e7766b8ed359f00e5f48e35ec79ac056 (41a06733cd332951fa100475c3a05c6916272899)
     [145/end/145427638 (pid 23783)] Task is starting.
     [145/end/145427638 (pid 23783)] I am in end and Pandas version is 1.5.0
     [145/end/145427638 (pid 23783)] Task finished successfully.
     Done! See the run in the UI at https://mfui.net/CondaTestFlow/145
```

</CodeOutputBlock>

### Using Remotely Resolved Environments

If you have never resolved an environment locally before, Metaflow will resolve it for you. You can, however,
also tell Metaflow to check if someone else has resolved the same environment before and use that directly
instead.

To do so, you can set the `METAFLOW_CONDA_USE_REMOTE_LATEST` environment variable, it takes one of the following
values:
- ":none:" (the default value): do not look for remote environments when resolving
- ":username:": look for the latest environment the current user resolved (maybe from another machine)
- ":any:" look for the latest environment resolved by any user
- comma separated list of names: look for the latest environment resolved by any of the users in
the list.

### Delaying Environment Fetching

By default, Metaflow associates an environment to a step when it resolves the environment. In the case of
[named environments](#named-environments), you can delay this association to *execution time*. In other words,
in the case of Argo/Airflow/StepFunctions for example, at deploy time, nothing would happen and at execution time, the environment
would be fetched. If you use the `latest` tag in your named environment, the latest environment *at the time of
execution* would be used. Note that this means that different runs may have different environments but this is
sometimes the desired behavior.

To specify this behavior, you add the `fetch_at_exec=True` option to `@named_env`. Note that for `fetch_at_exec`
environments, you cannot specify anything other than a named environment (no additional dependencies, etc) as
that would cause a re-resolution at execution time which we want to avoid.

#### Variable substitution

As an additional convenience, you can use simple substitution in the `name` or `pathspec` options to `@named_env`.
Specifically, you have the following values available:
- `METAFLOW_RUN_ID`: the run ID of the current run
- `METAFLOW_RUN_ID_BASE`: the same as `METAFLOW_RUN_ID`
- `METAFLOW_STEP_NAME`: the name of the current step
- `METAFLOW_TASK_ID`: the task ID of the current task
- `METAFLOW_INIT_X` where `X` is the name of any parameter passed to the flow

You can then specify something like the following:


```py title="buildinenv.py"
import subprocess
import sys
import tempfile

from metaflow import FlowSpec, Parameter, conda, current, named_env, step

class BuildInEnvExample(FlowSpec):
    my_var = Parameter(
        "my_var",
        default=123,
    )

    @conda(disabled=True)
    @step
    def start(self):
        full_run_id = base_run_id = current.run_id
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as req_file:
            req_file.write("itsdangerous==2.1.2")
            req_file.flush()
            subprocess.check_call(
                [
                    sys.executable,
                    "-m",
                    "metaflow.cmd.main_cli",
                    "environment",
                    "resolve",
                    "--alias",
                    "mlp/metaflow/test/build_in_step_example/id_%s_%s_%s"
                    "-r",
                    req_file.name,
                    "--python",
                    "3.8.*",
                ]
            )
        print(
            "Build environment and aliased using mlp/metaflow/test/build_in_step_example/id_%s_%s_%s"
        )
        self.next(self.end)

    @named_env(
        name="mlp/metaflow/test/build_in_step_example/id_@{METAFLOW_RUN_ID}_@{METAFLOW_RUN_ID_BASE}_@{METAFLOW_INIT_MY_VAR}",
        fetch_at_exec=True,
    )
    @step
    def end(self):
        import itsdangerous

        print("Imported itsdangerous and found version %s" % itsdangerous.__version__)
        self.found_version = itsdangerous.__version__


if __name__ == "__main__":
    BuildInEnvExample()
```


```bash
python buildinenv.py --environment=conda run
```

<CodeOutputBlock lang="bash">

```
Workflow starting (run-id 9), see it in the UI at https://mfui.net/BuildInEnvExample/9
     [9/start/145427656 (pid 23882)] Task is starting.
     [9/start/145427656 (pid 23882)] Metaflow (2.9.12+netflix-ext(1.0.0))
     [9/start/145427656 (pid 23882)] Resolving 1 environment ... done in 27 seconds.
     [9/start/145427656 (pid 23882)] ### Environment for architecture linux-64
     [9/start/145427656 (pid 23882)] Environment of type pypi-only full hash 362c72d3721f8705c1047a970f89b280af781a48:f2650491e1e71dd0754a5c7bde59578be7986978
     [9/start/145427656 (pid 23882)] Arch linux-64
     [9/start/145427656 (pid 23882)] Available on linux-64
     [9/start/145427656 (pid 23882)] 
     [9/start/145427656 (pid 23882)] Resolved on 780
     [9/start/145427656 (pid 23882)] Resolved by romain
     [9/start/145427656 (pid 23882)] 
     [9/start/145427656 (pid 23882)] User-requested packages pypi::boto3==>=1.14.0, pypi::cffi==>=1.13.0,!=1.15.0, pypi::fastavro==>=1.6.0, pypi::itsdangerous==2.1.2, pypi::pandas==>=0.24.0, pypi::pyarrow==>=0.17.1, conda::python==3.8.x, pypi::requests==>=2.21.0
     [9/start/145427656 (pid 23882)] User sources conda::conda-forge, pypi::https://pypi.org/simple
     [9/start/145427656 (pid 23882)] 
     [9/start/145427656 (pid 23882)] Conda Packages installed _libgcc_mutex==0.1-conda_forge, _openmp_mutex==4.5-2_gnu, bzip2==1.0.8-h7f98852_4, ca-certificates==2023.7.22-hbcca054_0, ld_impl_linux-64==2.40-h41732ed_0, libffi==3.4.2-h7f98852_5, libgcc-ng==13.1.0-he5830b7_0, libgomp==13.1.0-he5830b7_0, libnsl==2.0.0-h7f98852_0, libsqlite==3.43.0-h2797004_0, libuuid==2.38.1-h0b41bf4_0, libzlib==1.2.13-hd590300_5, ncurses==6.4-hcb278e6_0, openssl==3.1.2-hd590300_0, pip==23.2.1-pyhd8ed1ab_0, python==3.8.17-he550d4f_0_cpython, readline==8.2-h8228510_1, setuptools==68.1.2-pyhd8ed1ab_0, tk==8.6.12-h27826a3_0, wheel==0.41.2-pyhd8ed1ab_0, xz==5.2.6-h166bdaf_0
     [9/start/145427656 (pid 23882)] Pypi Packages installed boto3==1.28.41, botocore==1.31.41, certifi==2023.7.22, cffi==1.15.1, charset-normalizer==3.2.0, fastavro==1.8.2, idna==3.4, itsdangerous==2.1.2, jmespath==1.0.1, numpy==1.24.4, pandas==2.0.3, pyarrow==13.0.0, pycparser==2.21, python-dateutil==2.8.2, pytz==2023.3.post1, requests==2.31.0, s3transfer==0.6.2, six==1.16.0, tzdata==2023.3, urllib3==1.26.16
     [9/start/145427656 (pid 23882)] 
     [9/start/145427656 (pid 23882)] All packages already cached in s3.
     [9/start/145427656 (pid 23882)] Caching 5 environments and aliases to s3 ... done in 0 seconds.
     [9/start/145427656 (pid 23882)] 
     [9/start/145427656 (pid 23882)] Build environment and aliased using mlp/metaflow/test/build_in_step_example/id_9_9_123
     [9/start/145427656 (pid 23882)] Task finished successfully.
     Creating Conda environment 362c72d3721f8705c1047a970f89b280af781a48 (f2650491e1e71dd0754a5c7bde59578be7986978)...
        Extracting and linking Conda environment ... (conda packages) ... (pypi packages) ... done in 14 seconds.
     [9/end/145427711 (pid 24259)] Task is starting.
     [9/end/145427711 (pid 24259)] Imported itsdangerous and found version 2.1.2
     [9/end/145427711 (pid 24259)] Task finished successfully.
     Done! See the run in the UI at https://mfui.net/BuildInEnvExample/9
```

</CodeOutputBlock>
