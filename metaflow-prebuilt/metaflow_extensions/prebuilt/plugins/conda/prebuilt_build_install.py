"""
Build-time conda installer stub.

This module is intentionally empty in the OSS metaflow-prebuilt package.
The real implementation lives alongside the conda stack (e.g. nflx-metaflow's
metaflow_extensions.nflx.plugins.conda.prebuilt_build_install), which
imports the Conda class and related utilities needed to install the env.

Set PrebuiltCondaEnvironment._BUILD_INSTALL_MODULE to the package that
provides the real prebuilt_build_install module.
"""

import sys

if __name__ == "__main__":
    print(
        "ERROR: This is a stub. Configure PrebuiltCondaEnvironment._BUILD_INSTALL_MODULE "
        "to point to a real prebuilt_build_install implementation.",
        file=sys.stderr,
    )
    sys.exit(1)
