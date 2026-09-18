"""Install a released metaflow-functions into a venv, to use it as the reader.

Both compat suites need this: the reader whose compatibility is in question is a
published package, not this working tree, so it has to be installed for real.
"""

import subprocess
import sys

# netflixext is not optional: environment.py imports Conda from it at module scope.
DEPENDENCIES = ["metaflow", "metaflow-netflixext", "fastavro", "psutil"]


def install(version: str, venv_dir) -> str:
    """Path to the python of a venv with that metaflow-functions installed."""
    subprocess.run(
        [sys.executable, "-m", "venv", str(venv_dir)], check=True, capture_output=True
    )
    subprocess.run(
        [
            str(venv_dir / "bin" / "pip"),
            "install",
            "-q",
            *DEPENDENCIES,
            f"metaflow-functions=={version}",
        ],
        check=True,
        capture_output=True,
        timeout=900,
    )
    return str(venv_dir / "bin" / "python")
