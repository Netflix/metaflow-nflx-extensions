import os
import shutil
import subprocess
import sys
from typing import Union, TYPE_CHECKING
from metaflow.util import which

if TYPE_CHECKING:
    import sh

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
SITECUSTOMIZE_FILE = os.path.join(MODULE_DIR, "sitecustomize.py")

# Allow internal extensions to provide their own coveragerc override
try:
    import metaflow_extensions.nflx.plugins.coverage as _nflx_cov

    _nflx_coveragerc = os.path.join(os.path.dirname(_nflx_cov.__file__), "coveragerc.py")
    COVERAGE_RCFILE = _nflx_coveragerc if os.path.exists(_nflx_coveragerc) else os.path.join(MODULE_DIR, "coveragerc.py")
except ImportError:
    COVERAGE_RCFILE = os.path.join(MODULE_DIR, "coveragerc.py")


def get_local_coverage_dir():
    METAFLOW_COVERAGE_S3_PATH = os.environ.get("METAFLOW_COVERAGE_S3_PATH")
    assert METAFLOW_COVERAGE_S3_PATH, "METAFLOW_COVERAGE_S3_PATH is not set"
    group_tag = os.path.basename(METAFLOW_COVERAGE_S3_PATH)
    coverage_dir = os.path.join("/tmp", group_tag)
    os.makedirs(coverage_dir, exist_ok=True)
    return coverage_dir


def get_sitepackages_dir(python_binary: str):
    return (
        subprocess.run(
            [python_binary, "-c", "import site; print(site.getsitepackages()[0])"],
            check=True,
            stdout=subprocess.PIPE,
        )
        .stdout.decode()
        .strip()
    )


def setup_sitecustomize(python_binary: str):
    site_packages_dir = get_sitepackages_dir(python_binary)
    sitecustomize_file = os.path.join(site_packages_dir, "sitecustomize.py")
    local_coverage_dir = get_local_coverage_dir()

    # We also copy the .coveragerc file to the site-packages directory.
    # This keeps the .coveragerc file in the same directory as the sitecustomize.py file
    # so that when the env is saved and restored we will have both.
    coverage_rcfile = os.path.join(site_packages_dir, ".coveragerc")
    if not os.path.exists(coverage_rcfile):
        shutil.copy(COVERAGE_RCFILE, coverage_rcfile)

    # We harden out the environment variables as the fallback so that it will work with
    # subprocesses that are launched without inheriting the environment.
    lines = [
        "import os",
        "import site",
        "site_packages_dir = site.getsitepackages()[0]",
        f"os.environ['COVERAGE_FILE'] = os.environ.get('COVERAGE_FILE') or os.path.join('{local_coverage_dir}', f'.coverage')",
        f"os.environ['COVERAGE_PROCESS_START'] = os.environ.get('COVERAGE_PROCESS_START') or os.path.join(site_packages_dir, '.coveragerc')",
        f"os.environ['COVERAGE_RCFILE'] = os.environ.get('COVERAGE_RCFILE') or os.path.join(site_packages_dir, '.coveragerc')",
        f"os.environ['METAFLOW_COVERAGE_S3_PATH'] = os.environ.get('METAFLOW_COVERAGE_S3_PATH') or '{os.environ['METAFLOW_COVERAGE_S3_PATH']}'",
        "import coverage",
        "coverage.process_startup()",
        "coverage_context = os.environ.get('METAFLOW_COVERAGE_CONTEXT')",
        "if coverage_context and coverage.Coverage.current():",
        "    coverage.Coverage.current().switch_context(coverage_context)",
    ]
    if not os.path.exists(sitecustomize_file):
        with open(sitecustomize_file, "w") as target_file:
            target_file.write("\n".join(lines) + "\n")


def maybe_install_coverage(python_binary: str):
    try:
        subprocess.run(
            [python_binary, "-c", "import coverage"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError:
        subprocess.run([python_binary, "-m", "pip", "install", "coverage"], check=True)


def setup_coverage(sh_python: Union[str, "sh.Command"]):
    sh_python = str(sh_python)
    assert os.environ.get(
        "METAFLOW_COVERAGE_S3_PATH"
    ), "METAFLOW_COVERAGE_S3_PATH is not set"

    python_binaries = set(
        ["python"]
        + os.environ.get("MF_BDI_PYTHONPATH", "").split()
        + os.environ.get("MFPY", "").split()
        + [sh_python]
    )
    for python_binary in python_binaries:
        if not python_binary or not which(python_binary):
            continue
        maybe_install_coverage(python_binary)
        setup_sitecustomize(python_binary)


if __name__ == "__main__":
    sh_python = sys.argv[1] if len(sys.argv) > 1 else ""
    setup_coverage(sh_python)
