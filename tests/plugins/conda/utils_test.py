from itertools import chain
from metaflow_extensions.nflx.plugins.conda.utils import (
    filter_packages_by_markers,
)
from metaflow_extensions.nflx.plugins.conda.resolvers.pylock_toml_resolver import (
    PylockTomlResolver,
)

import pytest
import tomli
from io import BytesIO
from metaflow._vendor.packaging.version import InvalidVersion
from contextlib import nullcontext as does_not_raise


def test_filter_packages_by_marker():
    root_obj = tomli.load(
        BytesIO(
            """
lock-version = "1.0"
created-by = "uv"
requires-python = ">=3.10.18"
[[packages]]
name = "numpy"
version = "2.2.6"
marker = "python_full_version < '3.11'"
sdist = { url = "https://pypi.netflix.net/packages/18694941122/numpy-2.2.6.tar.gz", upload-time = 2025-05-17T22:38:04Z, size = 20276440, hashes = { sha256 = "e29554e2bef54a90aa5cc07da6ce955accb83f21ab5de01a62c8478897b264fd" } }
wheels = [
    { url = "https://pypi.netflix.net/packages/18694702540/numpy-2.2.6-cp310-cp310-macosx_10_9_x86_64.whl", upload-time = 2025-05-17T21:27:58Z, size = 21165245, hashes = { sha256 = "b412caa66f72040e6d268491a59f2c43bf03eb6c96dd8f0307829feb7fa2b6fb" } },
    { url = "https://pypi.netflix.net/packages/18694705588/numpy-2.2.6-cp310-cp310-macosx_11_0_arm64.whl", upload-time = 2025-05-17T21:28:21Z, size = 14360048, hashes = { sha256 = "8e41fd67c52b86603a91c1a505ebaef50b3314de0213461c7a6e99c9a3beff90" } },
    { url = "https://pypi.netflix.net/packages/18694705589/numpy-2.2.6-cp310-cp310-macosx_14_0_arm64.whl", upload-time = 2025-05-17T21:28:30Z, size = 5340542, hashes = { sha256 = "37e990a01ae6ec7fe7fa1c26c55ecb672dd98b19c3d0e1d1f326fa13cb38d163" } },
]
""".encode(
                "utf-8"
            )
        )
    )

    numpy_package_obj = root_obj["packages"][0]
    numpy_packages = PylockTomlResolver._toml_package_to_package_specs(
        numpy_package_obj
    )

    assert (
        len(
            filter_packages_by_markers(
                numpy_packages,
                "3.10.1",
                "x86_64",
            )
        )
        == 3
    )
    assert len(filter_packages_by_markers(numpy_packages, "3.9.1", "x86_64")) == 3

    # empty resutl due to marker = "python_full_version < '3.11'"
    assert filter_packages_by_markers(numpy_packages, "3.11.0", "x86_64") == []


@pytest.fixture
def mock_target_env_template():
    return {
        "implementation_name": "cpython",
        "implementation_version": "3.10.19",
        "os_name": "posix",
        "platform_machine": "arm64",
        "platform_release": "TO BE FILLED IN TEST",
        "platform_system": "TO BE FILLED IN TEST",
        "platform_version": "Not used",
        "python_full_version": "TO BE FILLED IN TEST",
        "platform_python_implementation": "CPython",
        "python_version": "TO BE FILLED IN TEST",
        "sys_platform": "TO BE FILLED IN TEST",
    }


multiple_marker_test_case_params = [
    {
        "python_full_version": "3.10.0",
        "env": {
            "platform_system": "Darwin",
            "platform_release": "23.0.0",
            "sys_platform": "darwin",
        },
        "arch": None,
        "expected_packages": [("numpy", "2.2.1")],
        "test_id": "darwin_python_lt_311",
    },
    {
        "python_full_version": "3.12.0",
        "env": {
            "platform_system": "Darwin",
            "platform_release": "23.0.0",
            "sys_platform": "darwin",
        },
        "arch": None,
        "expected_packages": [("numpy", "2.2.2")],
        "test_id": "darwin_python_lt_312",
    },
    {
        "python_full_version": "3.12.0",
        "env": {
            "platform_system": "linux",
            "platform_release": "23.0.0",
            "sys_platform": "linux",
        },
        "arch": None,
        "expected_packages": [],
        "test_id": "linux_python_lt_311_linux_no_match",
    },
    {
        "python_full_version": "3.10.0",
        "env": {
            "platform_system": "darwin",
            "platform_release": "24.0.0",
            "sys_platform": "darwin",
        },
        "arch": None,
        "expected_packages": [("numpy", "2.2.1"), ("numpy", "2.2.3")],
        "test_id": "platform_release_24",
    },
    {
        "python_full_version": "3.13.0",
        "env": {
            "platform_system": "linux",
            "implementation_version": "3.10.19",
            "os_name": "posix",
            "platform_release": "6.5.13netflix-00003-g301fd1c6f367",
            "platform_version": "#1 SMP PREEMPT_DYNAMIC Tue Jun 24 11:58:55 MDT 2025",
            "sys_platform": "linux",
            "platform_machine": "x86_64",
            "implementation_name": "cpython",
            "python_full_version": "3.10.19",
            "platform_python_implementation": "CPython",
            "python_version": "3.10",
        },
        "arch": None,
        "expected_packages": [("numpy", "2.2.4")],
        "test_id": "linux_python_lt_313",
    },
]


@pytest.mark.parametrize(
    "case",
    multiple_marker_test_case_params,
    ids=[d["test_id"] for d in multiple_marker_test_case_params],
)
def test_combined_marker_conditions(case, mock_target_env_template):
    root_obj = tomli.load(
        BytesIO(
            """
lock-version = "1.0"
created-by = "uv"
requires-python = ">=3.10.18"
[[packages]]
name = "numpy"
version = "2.2.1"
marker = "sys_platform == 'darwin' and python_full_version < '3.11'"
wheels = [
    { url = "https://pypi.netflix.net/packages/18694702540/numpy-2.2.1-cp310-cp310-macosx_10_9_x86_64.whl", upload-time = 2025-05-17T21:27:58Z, size = 21165245, hashes = { sha256 = "b412caa66f72040e6d268491a59f2c43bf03eb6c96dd8f0307829feb7fa2b6fb" } },
]
[[packages]]
name = "numpy"
version = "2.2.2"
marker = "sys_platform == 'darwin' and python_full_version >= '3.12'"
wheels = [
    { url = "https://pypi.netflix.net/packages/18694702540/numpy-2.2.2-cp310-cp310-macosx_10_9_x86_64.whl", upload-time = 2025-05-17T21:27:58Z, size = 21165245, hashes = { sha256 = "b412caa66f72040e6d268491a59f2c43bf03eb6c96dd8f0307829feb7fa2b6fb" } },
]
[[packages]]
name = "numpy"
version = "2.2.3"
marker = "sys_platform == 'darwin' and platform_release >= '24'"
wheels = [
    { url = "https://pypi.netflix.net/packages/18694702540/numpy-2.2.3-cp310-cp310-macosx_10_9_x86_64.whl", upload-time = 2025-05-17T21:27:58Z, size = 21165245, hashes = { sha256 = "b412caa66f72040e6d268491a59f2c43bf03eb6c96dd8f0307829feb7fa2b6fb" } },
]
[[packages]]
name = "numpy"
version = "2.2.4"
marker = "sys_platform == 'linux' and python_full_version >= '3.13'"
wheels = [
    { url = "https://pypi.netflix.net/packages/18694702540/numpy-2.2.4-cp310-cp310-macosx_10_9_x86_64.whl", upload-time = 2025-05-17T21:27:58Z, size = 21165245, hashes = { sha256 = "b412caa66f72040e6d268491a59f2c43bf03eb6c96dd8f0307829feb7fa2b6fb" } },
]
        """.encode(
                "utf-8"
            )
        )
    )
    packages_dict = PylockTomlResolver._pylock_toml_root_obj_to_packages(root_obj)

    assert "numpy" in packages_dict
    assert len(packages_dict) == 1
    assert len(packages_dict["numpy"]) == 4
    numpy_packages = packages_dict["numpy"]

    target_env = mock_target_env_template.copy()
    target_env.update(case["env"])

    with case.get("expected_exception", does_not_raise()):
        filtered = filter_packages_by_markers(
            numpy_packages,
            case["python_full_version"],
            case["arch"],
            target_env=target_env,
        )

        assert len(filtered) == len(case["expected_packages"])
        assert case["expected_packages"] == [
            (p.package_name, p.package_version) for p in filtered
        ]
