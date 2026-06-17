"""Top-level pytest conftest for the metaflow-prebuilt test suite.

Import ``metaflow`` *first*, before any test module imports a
``metaflow_extensions.prebuilt`` submodule.

Why this matters: ``PrebuiltCondaEnvironment`` is a registered Metaflow
environment plugin (``--environment=prebuilt``). Importing any prebuilt
submodule runs ``from metaflow.exception import ...`` at module load, which
triggers metaflow's full initialization, which calls
``resolve_plugins("environment")`` — and that, in turn, imports
``PrebuiltCondaEnvironment``. If a prebuilt submodule happens to be the *first*
thing imported in a process, that module is still mid-import when plugin
resolution re-enters it, raising::

    ValueError: Cannot locate 'PrebuiltCondaEnvironment' class for environment
    plugin at 'metaflow_extensions.prebuilt.plugins.conda.prebuilt_conda_environment'

Forcing metaflow to load fully first (so plugin resolution completes against a
clean, top-level import of the module) avoids the re-entry. This mirrors real
usage, where ``import metaflow`` always runs before any extension submodule is
imported directly. pytest imports this conftest before collecting the test
modules beneath ``tests/``, so the ordering is guaranteed.
"""

import metaflow  # noqa: F401  (imported for its initialization side effect)
