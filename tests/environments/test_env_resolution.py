"""
Tests for actual environment resolution using Flow API with @conda/@pypi decorators.

These tests create test flows with environment specifications and use FlowAPI
to resolve the environments, then validate the resolved packages.
"""

import os
import platform
import tempfile
import uuid
from typing import Any, Dict, Optional
import pytest

from metaflow import FlowAPI

from metaflow_extensions.nflx.plugins.conda.parsers import req_parser, yml_parser

# Get the test data directory
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_DIR = os.path.join(TEST_DIR, "env_specs")
FOO_PKG_DIR = os.path.join(TEST_DIR, "foo_pkg")

# Generate a unique ID for this test run to avoid conflicts
TEST_RUN_ID = str(uuid.uuid4())


# Expected packages for each test environment
# Format: "package_name": constraint
#   - ">= X.Y.Z" means minimum version
#   - "<= X.Y.Z" means maximum version
#   - "== X.Y.Z" means exact version
#   - "" (empty string) means any version (package must be present)
#   - None means package must be absent
EXPECTED_PACKAGES = {
    "channel-conda.yml": {
        "comet_ml": ">= 3",
        "itsdangerous": "",
    },
    "conda-multiple-channels.yml": {
        "scikit-learn": ">= 1.0.0",
        "pandas": ">= 1.0.0",
    },
    "conda-with-channel-spec.yml": {
        "numpy": ">= 1.20",
        "pandas": ">= 1.0",
    },
    "conda-with-npconda.yml": {
        "pandas": "",
        "requests": "",
    },
    "conda-with-sys-deps.yml": {
        "pandas": ">= 1.0.0",
    },
    "corner-cases.txt": {
        "foo": "== 1.0.0",
        "outlier-detector": "== 0.0.3",
        "clip": "== 1.0",
    },
    "only-src.txt": {
        "matplotlib": "",
        "outlier-detector": "== 0.0.3",
    },
    "only-src.yml": {
        "matplotlib": "",
        "outlier-detector": "== 0.0.3",
    },
    "pypi-custom-index.txt": {
        "pandas": ">= 1.0.0",
        "requests": ">= 2.20.0",
    },
    "pypi-simple-no-extras.txt": {
        "pandas": ">= 1.0",
        "numpy": ">= 1.20",
        "requests": "",
    },
    "pypi-with-find-links.txt": {
        "pandas": ">= 1.0.0",
    },
    "pypi-with-markers.txt": {
        "pandas": ">= 1.0",
        "numpy": ">= 1.20",
    },
    # req-conda-pkg-no-version.txt - MISSING: No expected packages defined
    "req-conda-pkg-no-version.txt": {
        "pandas": "",
    },
    "simple-conda.yml": {
        "pandas": ">= 1",
        "pip": "",
        "requests": ">= 2.21",
    },
    "pip-no-hash.txt": {"typing-extensions": "4.15.0"},
    # Files below are used in test_resolve_environment_from_file but not in test_resolve_environment_no_crash
    "itsdangerous.txt": {
        "requests": "",
        "itsdangerous": "",
    },
    "itsdangerous.yml": {
        "requests": "",
        "itsdangerous": "",
    },
    # pip-version.txt and pip-version.yml have Python version specific checks
    # handled separately
}

# Python version-specific package expectations
# Format: {env_file: {python_version: {package: constraint}}}
PYTHON_VERSION_SPECIFIC_PACKAGES = {
    "pip-version.txt": {
        "3.7": {
            "importlib-resources": ">= 1",
            "jsonschema": "",
        },
        "3.8": {
            "importlib_resources": ">= 1",
            "jsonschema": "",
        },
        "3.10": {
            "importlib_resources": None,  # Should be absent
            "jsonschema": "",
        },
    },
    "pip-version.yml": {
        "3.7": {
            "importlib-resources": ">= 1",
            "jsonschema": "",
        },
        "3.8": {
            "importlib_resources": ">= 1",
            "jsonschema": "",
        },
        "3.10": {
            "importlib_resources": None,  # Should be absent
            "jsonschema": "",
        },
    },
}


def parse_requirement_file(file_path: str) -> Dict[str, Any]:
    """
    Parse a requirements.txt or environment.yml file to extract dependencies.

    Returns:
        Tuple of (packages dict, extras list)
    """
    packages = {}
    extras = []

    if file_path.endswith(".txt"):
        with open(file_path, "r", encoding="utf-8") as f:
            parsed = req_parser(f.read())
        return parsed

    elif file_path.endswith(".yml") or file_path.endswith(".yaml"):
        with open(file_path, "r", encoding="utf-8") as f:
            parsed = yml_parser(f.read())
        return parsed
    else:
        raise ValueError(f"Unsupported file type: {file_path}")


@pytest.fixture
def test_workspace():
    """Create a temporary workspace for running flows with proper environment variables."""
    # Set environment variables BEFORE creating the workspace
    # This ensures they're active for all metaflow operations
    env_vars = {
        "METAFLOW_CONDA_ENVS_DIRNAME": f"testing/envs_{TEST_RUN_ID}",
        "METAFLOW_CONDA_PACKAGES_DIRNAME": f"testing/packages_{TEST_RUN_ID}",
        "METAFLOW_CONDA_MAGIC_FILE_V2": f"condav2-{TEST_RUN_ID}.cnd",
        "METAFLOW_CONDA_LOCK_TIMEOUT": "7200",
        "METAFLOW_DEBUG_CONDA": "1",
        "METAFLOW_CONDA_USE_REMOTE_LATEST": ":none:",  # Disable remote caching for tests
    }

    # Save old values
    old_env = {}
    for key, value in env_vars.items():
        old_env[key] = os.environ.get(key)
        os.environ[key] = value

    # Add a symlink for the package to build, ignore error if it already exists
    symlink_path = os.path.join("/tmp", "build_foo_pkg")
    try:
        os.symlink(FOO_PKG_DIR, symlink_path)
    except FileExistsError:
        pass

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create .metaflow directory
        metaflow_dir = os.path.join(tmpdir, ".metaflow")
        os.makedirs(metaflow_dir)
        yield tmpdir

    # Restore old environment
    for key, value in old_env.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def create_flow_file_for_requirements(
    workspace: str,
    req_file_path: str,
    python_version: Optional[str] = None,
    is_yml: bool = False,
) -> str:
    """
    Create a flow file with @conda or @pypi decorator using requirements from a file.

    Args:
        workspace: Directory to create the flow file in
        req_file_path: Path to the requirements file
        python_version: Python version to use (optional)
        is_yml: Whether the file is a .yml file (uses @conda) vs .txt (uses @pypi)

    Returns:
        Path to the created flow file
    """
    deco_args = parse_requirement_file(req_file_path)

    if not python_version:
        python_version = '"3.10.*"'
    deco_args["python"] = python_version

    # Generate the flow code
    flow_code = """from metaflow import FlowSpec, conda, pypi, step

class TestFlow(FlowSpec):
"""

    if is_yml:
        flow_code += f"""    @conda({", ".join(f"{k}={v!r}" for k, v in deco_args.items())})
"""
    else:
        # Use @pypi decorator for .txt files
        flow_code += f"""    @pypi({", ".join(f"{k}={v!r}" for k, v in deco_args.items())})
"""

    flow_code += """    @step
    def start(self):
        import pandas  # Test that we can import
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == "__main__":
    TestFlow()
"""

    flow_file = os.path.join(workspace, "test_flow.py")
    print("Flow code:", flow_code)
    with open(flow_file, "w", encoding="utf-8") as f:
        f.write(flow_code)

    return flow_file


def compare_version(version1: str, version2: str) -> int:
    """
    Compare two version strings.

    Returns:
        0 if equal, 1 if version1 > version2, -1 if version1 < version2
    """
    if version1 == version2:
        return 0

    v1_parts = [int(x) for x in version1.split(".") if x.isdigit()]
    v2_parts = [int(x) for x in version2.split(".") if x.isdigit()]

    for i in range(max(len(v1_parts), len(v2_parts))):
        p1 = v1_parts[i] if i < len(v1_parts) else 0
        p2 = v2_parts[i] if i < len(v2_parts) else 0
        if p1 > p2:
            return 1
        elif p1 < p2:
            return -1

    return 0


def validate_resolved_environment(resolved_env, checks: Dict[str, Optional[str]]):
    """
    Validate that a resolved environment contains the expected packages.

    Args:
        resolved_env: The resolved environment from FlowAPI
        checks: Dict mapping package name to constraint
            - ">= X.Y.Z" means minimum version
            - "<= X.Y.Z" means maximum version
            - "== X.Y.Z" means exact version
            - "" (empty string) means any version (package must be present)
            - None means package must be absent
    """
    # Get the list of packages from the resolved environment
    packages = {}
    for pkg in resolved_env.packages:
        packages[pkg.package_name] = pkg.package_version

    errors = []
    for pkg_name, constraint in checks.items():
        if constraint is None:
            # Package should be absent
            if pkg_name in packages:
                errors.append(
                    f"Expected {pkg_name} to be absent but found version {packages[pkg_name]}"
                )
        elif constraint == "":
            # Package should be present with any version
            if pkg_name not in packages:
                errors.append(f"Expected {pkg_name} to be present but not found")
        else:
            # Package should be present with version constraint
            if pkg_name not in packages:
                errors.append(f"Expected {pkg_name} but not found")
            else:
                actual_ver = packages[pkg_name]
                # If version contains '.dev', strip that part (e.g. '1.2.3.dev4' -> '1.2.3')
                if ".dev" in actual_ver:
                    actual_ver = actual_ver.split(".dev")[0]

                # Parse constraint (e.g., ">= 1.2.3", "== 1.0.0", "<= 2.0.0")
                if constraint.startswith(">="):
                    min_ver = constraint[2:].strip()
                    if compare_version(actual_ver, min_ver) < 0:
                        errors.append(
                            f"Expected {pkg_name} >= {min_ver} but found {actual_ver}"
                        )
                elif constraint.startswith("<="):
                    max_ver = constraint[2:].strip()
                    if compare_version(actual_ver, max_ver) > 0:
                        errors.append(
                            f"Expected {pkg_name} <= {max_ver} but found {actual_ver}"
                        )
                elif constraint.startswith("=="):
                    exact_ver = constraint[2:].strip()
                    if actual_ver != exact_ver:
                        errors.append(
                            f"Expected {pkg_name} == {exact_ver} but found {actual_ver}"
                        )

    if errors:
        raise AssertionError("\n".join(errors))


@pytest.mark.parametrize(
    "env_file,python_version",
    [
        ("simple-conda.yml", "3.10"),
        ("itsdangerous.txt", "3.10"),
        ("itsdangerous.yml", "3.10"),
    ],
)
def test_resolve_environment_from_file(test_workspace, env_file, python_version):
    """
    Test resolving environments from requirement files using Flow API.
    """
    file_path = os.path.join(ENV_DIR, env_file)

    # Skip if file doesn't exist
    if not os.path.exists(file_path):
        pytest.skip(f"File {file_path} not found")

    # Create flow file
    current_dir = os.getcwd()
    try:
        os.chdir(test_workspace)

        is_yml = env_file.endswith(".yml")
        flow_file = create_flow_file_for_requirements(
            test_workspace, file_path, python_version, is_yml
        )

        # Use FlowAPI to resolve the environment
        api = FlowAPI(flow_file, environment="conda")

        # Resolve the environment with force=True to ensure we execute resolver code
        result = api.environment().resolve("start", force=True, timeout=2400)

        # Get the resolved environment
        assert "start" in result
        arch_results = result["start"]
        assert len(arch_results) > 0

        # Get the first (and should be only) architecture result
        resolved_env, was_just_resolved = next(iter(arch_results.values()))

        # Validate the structure
        assert resolved_env is not None
        assert hasattr(resolved_env, "packages")

        # Validate against expected packages
        if env_file in EXPECTED_PACKAGES:
            checks = EXPECTED_PACKAGES[env_file]
            validate_resolved_environment(resolved_env, checks)

    finally:
        os.chdir(current_dir)


@pytest.mark.parametrize(
    "env_file",
    [
        "channel-conda.yml",
        "conda_lock_prio.yml",
        "conda-multiple-channels.yml",
        "conda-with-channel-spec.yml",
        "conda-with-npconda.yml",
        "conda-with-sys-deps.yml",
        "corner-cases.txt",
        "only-src.txt",
        "only-src.yml",
        "pypi-custom-index.txt",
        "pypi-simple-no-extras.txt",
        "pypi-with-find-links.txt",
        "pypi-with-markers.txt",
        "req-conda-pkg-no-version.txt",
        "simple-conda.yml",
    ],
)
def test_resolve_environment_no_crash(test_workspace, env_file):
    """
    Test that various environment files can be resolved without crashing.
    """
    file_path = os.path.join(ENV_DIR, env_file)

    # Skip if file doesn't exist
    if not os.path.exists(file_path):
        pytest.skip(f"File {file_path} not found")

    # conda-with-sys-deps.yml uses __glibc virtual package which is Linux-only
    if env_file == "conda-with-sys-deps.yml" and platform.system() != "Linux":
        pytest.skip("conda-with-sys-deps.yml requires Linux (__glibc virtual package)")

    current_dir = os.getcwd()
    try:
        os.chdir(test_workspace)

        is_yml = env_file.endswith(".yml")
        flow_file = create_flow_file_for_requirements(
            test_workspace, file_path, "3.10.*", is_yml
        )

        # Use FlowAPI to resolve the environment
        api = FlowAPI(flow_file, environment="conda")

        # Just verify it resolves without crashing (use force=True to test resolver code)
        result = api.environment().resolve("start", force=True, timeout=2400)
        assert "start" in result

        # Also validate expected packages if available
        if env_file in EXPECTED_PACKAGES:
            arch_results = result["start"]
            resolved_env, _ = next(iter(arch_results.values()))
            checks = EXPECTED_PACKAGES[env_file]
            validate_resolved_environment(resolved_env, checks)

    finally:
        os.chdir(current_dir)
