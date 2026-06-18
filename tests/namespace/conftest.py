"""Shared setup for the namespace / nflx_compat shim tests.

Importing metaflow once, up front, fully initializes its plugin registry
(``resolve_plugins`` imports every step-decorator class, including the conda
ones). The shim tests then import ``metaflow_extensions.nflx.plugins.conda.*``
modules directly; doing so before metaflow is initialized would re-enter
metaflow's bootstrap and hit the conda circular-import. Loading metaflow here
makes the suite order-independent (it otherwise only passes when a sibling
test file happens to import metaflow first).
"""

import metaflow  # noqa: F401  (imported for its initialization side effect)
