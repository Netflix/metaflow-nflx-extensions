from io import BytesIO
import itertools

import metaflow

from metaflow_extensions.netflix_ext.plugins.conda.resolvers.pylock_toml_resolver import (
    PylockTomlResolver,
)
from metaflow_extensions.netflix_ext.plugins.conda.conda import Conda
from metaflow_extensions.netflix_ext.plugins.conda.env_descr import (
    CondaPackageSpecification,
    EnvID,
    PackageSpecification,
    PypiPackageSpecification,
    EnvType,
    ResolvedEnvironment,
)

from metaflow_extensions.netflix_ext.plugins.conda.utils import (
    get_best_compatible_packages,
    get_maximum_glibc_version,
    pypi_tags_from_arch,
    dict_to_tstr,
    dict_to_tstr,
)
from metaflow._vendor.packaging.tags import (
    Tag,
)
from metaflow_extensions.netflix_ext.plugins.conda.conda import CondaException


from pathlib import Path
import pytest
import tomli
from typing import Any, List, Tuple, Dict


def echo(*args, **kwargs):
    pass


def supported_tags_set1():
    python_version = "3.10.18"
    architecture = "osx-arm64"
    deps = {"sys": ["__glibc==2.35"]}

    glibc_version = get_maximum_glibc_version(architecture, deps)
    supported_tags = pypi_tags_from_arch(python_version, architecture, glibc_version)

    return supported_tags


test_case_params = [
    (
        {
            "supported_tags": [("cp310", "cp310", "macosx_11_0_arm64")],
            "expected": {},
            "toml_str": """
                lock-version = "1.0"
                created-by = "uv"
                requires-python = ">=3.10.18"
                [[packages]]
                name = "charset-normalizer"
                version = "3.4.3"
                sdist = { url = "https://pypi.netflix.net/packages/18953203074/charset_normalizer-3.4.3.tar.gz", upload-time = 2025-08-09T07:57:28Z, size = 122371, hashes = { sha256 = "6fce4b8500244f6fcb71465d4a4930d132ba9ab8e71a7859e6a5d59851068d14" } }
                wheels = [
                    { url = "https://pypi.netflix.net/packages/18953196839/charset_normalizer-3.4.3-cp312-cp312-manylinux2014_s390x.manylinux_2_17_s390x.manylinux_2_28_s390x.whl", upload-time = 2025-08-09T07:56:13Z, size = 157104, hashes = { sha256 = "3cfb2aad70f2c6debfbcb717f23b7eb55febc0bb23dcffc0f076009da10c6392" } },
                    { url = "https://pypi.netflix.net/packages/18953196840/charset_normalizer-3.4.3-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl", upload-time = 2025-08-09T07:56:14Z, size = 151830, hashes = { sha256 = "1606f4a55c0fd363d754049cdf400175ee96c992b1f8018b993941f221221c5f" } },
                    { url = "https://pypi.netflix.net/packages/18953196841/charset_normalizer-3.4.3-cp312-cp312-musllinux_1_2_aarch64.whl", upload-time = 2025-08-09T07:56:16Z, size = 148854, hashes = { sha256 = "027b776c26d38b7f15b26a5da1044f376455fb3766df8fc38563b4efbc515154" } },
                    { url = "https://pypi.netflix.net/packages/18953196842/charset_normalizer-3.4.3-cp312-cp312-musllinux_1_2_ppc64le.whl", upload-time = 2025-08-09T07:56:17Z, size = 160670, hashes = { sha256 = "42e5088973e56e31e4fa58eb6bd709e42fc03799c11c42929592889a2e54c491" } },
                    { url = "https://pypi.netflix.net/packages/18953196843/charset_normalizer-3.4.3-cp312-cp312-musllinux_1_2_s390x.whl", upload-time = 2025-08-09T07:56:18Z, size = 158501, hashes = { sha256 = "cc34f233c9e71701040d772aa7490318673aa7164a0efe3172b2981218c26d93" } },
                    { url = "https://pypi.netflix.net/packages/18953196844/charset_normalizer-3.4.3-cp312-cp312-musllinux_1_2_x86_64.whl", upload-time = 2025-08-09T07:56:20Z, size = 153173, hashes = { sha256 = "320e8e66157cc4e247d9ddca8e21f427efc7a04bbd0ac8a9faf56583fa543f9f" } }
                ]
            """,
            "check_subset": False,
            "id": "no_match",
        }
    ),
    (
        {
            "supported_tags": [("cp310", "cp310", "macosx_10_9_universal2")],
            "expected": {
                "charset-normalizer": "charset_normalizer-3.4.2-cp310-cp310-macosx_10_9_universal2",
                "numpy": None,  # No numpy package with "macosx_10_9_universal2"
                "idna": None,
            },
            "toml_file": str(Path(__file__).parent / "data" / "sample_pylock1.toml"),
            "check_subset": True,
            "id": "single_tag_multiple_packages_full_toml_file",
        }
    ),
    (
        {
            "supported_tags": [("cp310", "cp310", "macosx_10_9_universal2")],
            "expected": {
                "charset-normalizer": "charset_normalizer-3.4.2-cp310-cp310-macosx_10_9_universal2",
            },
            "toml_file": str(
                Path(__file__).parent / "data" / "simple_toml_no_macosx_11_0_arm64.toml"
            ),
            "check_subset": True,
            "id": "single_tag_multiple_packages_macosx_10_9_universal2",
        }
    ),
    (
        {
            "supported_tags": [("cp311", "cp311", "macosx_10_9_universal2")],
            "expected": {
                "charset-normalizer": "charset_normalizer-3.4.2-cp311-cp311-macosx_10_9_universal2",
            },
            "toml_file": str(
                Path(__file__).parent / "data" / "simple_toml_no_macosx_11_0_arm64.toml"
            ),
            "check_subset": True,
            "id": "single_tag_multiple_packages_cp311",
        }
    ),
    (
        {
            "supported_tags": [("cp310", "cp310", "musllinux_1_2_x86_64")],
            "expected": {
                "charset-normalizer": "charset_normalizer-3.4.2-cp310-cp310-musllinux_1_2_x86_64",
            },
            "toml_file": str(
                Path(__file__).parent / "data" / "simple_toml_no_macosx_11_0_arm64.toml"
            ),
            "check_subset": True,
            "id": "single_tag_multiple_packages_musllinux_1_2_x86_64",
        }
    ),
    (
        {
            "supported_tags": [
                ("cp310", "cp310", "macosx_10_9_universal2"),
                ("py3", "none", "any"),
            ],
            "expected": {
                "charset-normalizer": "charset_normalizer-3.4.2-cp310-cp310-macosx_10_9_universal2",
                "numpy": None,  # No numpy package with "macosx_10_9_universal2"
                "idna": "idna-3.10-py3-none-any",
            },
            "toml_file": str(Path(__file__).parent / "data" / "sample_pylock1.toml"),
            "check_subset": True,
            "id": "multiple_tag_multiple_packages_macosx_10_9_universal2_and_any",
        }
    ),
    (
        {
            "supported_tags": [
                ("cp310", "cp310", "macosx_10_9_universal2"),
                ("py3", "none", "any"),
                ("cp310", "cp310", "macosx_11_0_arm64"),
            ],
            "expected": {
                "charset-normalizer": "charset_normalizer-3.4.2-cp310-cp310-macosx_10_9_universal2",
                "numpy": "numpy-2.2.6-cp310-cp310-macosx_11_0_arm64",
                "idna": "idna-3.10-py3-none-any",
            },
            "toml_file": str(Path(__file__).parent / "data" / "sample_pylock1.toml"),
            "check_subset": True,
            "id": "multiple_tag_multiple_packages_three_tags_three_hits",
        }
    ),
    (
        {
            "supported_tags": [
                ("cp310", "cp310", "win_amd64"),
                ("py3", "none", "any"),
            ],
            "expected": {
                "charset-normalizer": "charset_normalizer-3.4.2-cp310-cp310-win_amd64",
                "numpy": "numpy-2.2.6-cp310-cp310-win_amd64",
                "idna": "idna-3.10-py3-none-any",
            },
            "toml_file": str(Path(__file__).parent / "data" / "sample_pylock1.toml"),
            "check_subset": True,
            "id": "multiple_tag_multiple_packages_windows_platform",
        }
    ),
    (
        {
            "supported_tags": [
                ("cp310", "cp310", "musllinux_1_2_x86_64"),
                ("cp313", "cp313", "musllinux_1_2_x86_64"),
            ],
            "expected": {
                "charset-normalizer": "charset_normalizer-3.4.2-cp310-cp310-musllinux_1_2_x86_64",
                "numpy": "numpy-2.2.6-cp310-cp310-musllinux_1_2_x86_64",
                "idna": None,
            },
            "toml_file": str(Path(__file__).parent / "data" / "sample_pylock1.toml"),
            "check_subset": True,
            "id": "multiple_tag_multiple_packages_windows_platform",
        }
    ),
    (
        {
            "supported_tags_tag_type": supported_tags_set1(),
            "expected": {
                # certifi: there is only one wheel.
                # [[packages]]
                # name = "certifi"
                # version = "2025.8.3"
                # sdist = { url = "https://pypi.netflix.net/packages/18933835518/certifi-2025.8.3.tar.gz", upload-time = 2025-08-03T03:07:47Z, size = 162386, hashes = { sha256 = "e564105f78ded564e3ae7c923924435e1daa7463faeab5bb932bc53ffae63407" } }
                # wheels = [{ url = "https://pypi.netflix.net/packages/18933835517/certifi-2025.8.3-py3-none-any.whl", upload-time = 2025-08-03T03:07:45Z, size = 161216, hashes = { sha256 = "f6c12493cfb1b06ba2ff328595af9350c65d6644968e5d3a2ffd78699af217a5" } }]
                "certifi": "certifi-2025.8.3-py3-none-any",
                # charset-normalizer: see pylock.customer-soo.toml.
                # Example lines:
                # [[packages]]
                #     name = "charset-normalizer"
                #     version = "3.4.3"
                #     sdist = { url = "https://pypi.netflix.net/packages/18953203074/charset_normalizer-3.4.3.tar.gz", upload-time = 2025-08-09T07:57:28Z, size = 122371, hashes = { sha256 = "6fce4b8500244f6fcb71465d4a4930d132ba9ab8e71a7859e6a5d59851068d14" } }
                #     wheels = [
                #     { url = "https://pypi.netflix.net/packages/18953191400/charset_normalizer-3.4.3-cp310-cp310-macosx_10_9_universal2.whl", upload-time = 2025-08-09T07:55:36Z, size = 207695, hashes = { sha256 = "fb7f67a1bfa6e40b438170ebdc8158b78dc465a5a67b6dde178a46987b244a72" } },
                #     { url = "https://pypi.netflix.net/packages/18953191401/charset_normalizer-3.4.3-cp310-cp310-manylinux2014_aarch64.manylinux_2_17_aarch64.manylinux_2_28_aarch64.whl", upload-time = 2025-08-09T07:55:38Z, size = 147153, hashes = { sha256 = "cc9370a2da1ac13f0153780040f465839e6cccb4a1e44810124b4e22483c93fe" } },
                #     { url = "https://pypi.netflix.net/packages/18953191402/charset_normalizer-3.4.3-cp310-cp310-manylinux2014_ppc64le.manylinux_2_17_ppc64le.manylinux_2_28_ppc64le.whl", upload-time = 2025-08-09T07:55:40Z, size = 160428, hashes = { sha256 = "07a0eae9e2787b586e129fdcbe1af6997f8d0e5abaa0bc98c0e20e124d67e601" } },
                #     { url = "https://pypi.netflix.net/packages/18953191403/charset_normalizer-3.4.3-cp310-cp310-manylinux2014_s390x.manylinux_2_17_s390x.manylinux_2_28_s390x.whl", upload-time = 2025-08-09T07:55:41Z, size = 157627, hashes = { sha256 = "74d77e25adda8581ffc1c720f1c81ca082921329452eba58b16233ab1842141c" } },
                #     { url = "https://pypi.netflix.net/packages/18953191404/charset_normalizer-3.4.3-cp310-cp310-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl", upload-time = 2025-08-09T07:55:43Z, size = 152388, hashes = { sha256 = "d0e909868420b7049dafd3a31d45125b31143eec59235311fc4c57ea26a4acd2" } },
                #     { url = "https://pypi.netflix.net/packages/18953191405/charset_normalizer-3.4.3-cp310-cp310-musllinux_1_2_aarch64.whl", upload-time = 2025-08-09T07:55:44Z, size = 150077, hashes = { sha256 = "c6f162aabe9a91a309510d74eeb6507fab5fff92337a15acbe77753d88d9dcf0" } },
                #     { url = "https://pypi.netflix.net/packages/18953191406/charset_normalizer-3.4.3-cp310-cp310-musllinux_1_2_ppc64le.whl", upload-time = 2025-08-09T07:55:46Z, size = 161631, hashes = { sha256 = "4ca4c094de7771a98d7fbd67d9e5dbf1eb73efa4f744a730437d8a3a5cf994f0" } },
                #     { url = "https://pypi.netflix.net/packages/18953191407/charset_normalizer-3.4.3-cp310-cp310-musllinux_1_2_s390x.whl", upload-time = 2025-08-09T07:55:47Z, size = 159210, hashes = { sha256 = "02425242e96bcf29a49711b0ca9f37e451da7c70562bc10e8ed992a5a7a25cc0" } },
                # ...
                "charset-normalizer": "charset_normalizer-3.4.2-cp310-cp310-macosx_10_9_universal2",
                # There is only one wheel in idna
                "idna": "idna-3.10-py3-none-any",
                # The first cp310 and arm64 numpy listed in pylock.customer-soo.toml
                "numpy": "numpy-2.2.6-cp310-cp310-macosx_11_0_arm64",
                # Similar to numpy.
                "pandas": "pandas-2.3.1-cp310-cp310-macosx_11_0_arm64",
                # Only one wheel.
                "python-dateutil": "python_dateutil-2.9.0.post0-py2.py3-none-any",
                # Only one wheel.
                "pytz": "pytz-2025.2-py2.py3-none-any",
                # Only one wheel.
                "requests": "requests-2.32.4-py3-none-any",
                # Only one wheel.
                "six": "six-1.17.0-py2.py3-none-any",
                # Only one wheel.
                "tzdata": "tzdata-2025.2-py2.py3-none-any",
                # Only one wheel.
                "urllib3": "urllib3-2.5.0-py3-none-any",
            },
            "toml_file": str(Path(__file__).parent / "data" / "sample_pylock1.toml"),
            "check_subset": True,
            "id": "end_to_end_matching",
        }
    ),
    (
        {
            "toml_str": """
                    lock-version = "1.0"
                    created-by = "uv"
                    requires-python = ">=3.10.18"
                    [[packages]]
                    name = "charset-normalizer"
                    version = "3.4.3"
                    sdist = { url = "https://pypi.netflix.net/packages/18953203074/charset_normalizer-3.4.3.tar.gz", upload-time = 2025-08-09T07:57:28Z, size = 122371, hashes = { sha256 = "6fce4b8500244f6fcb71465d4a4930d132ba9ab8e71a7859e6a5d59851068d14" } }
                """,
            "supported_tags": [("cp310", "cp310", "macosx_10_9_universal2")],
            "expected": {},
            "check_subset": True,
            "id": "no_wheel_in_package",
            "expected_conda_exception": "We just encountered a package definition that contains no wheels",
        }
    ),
    (
        {
            "toml_str": """
                lock-version = "1.0"
                created-by = "uv"
                requires-python = ">=3.10.18"
                [[packages]]
                name = "charset-normalizer"
                version = "3.4.3"
                sdist = { url = "https://pypi.netflix.net/packages/18953203074/charset_normalizer-3.4.3.tar.gz", upload-time = 2025-08-09T07:57:28Z, size = 122371, hashes = { sha256 = "6fce4b8500244f6fcb71465d4a4930d132ba9ab8e71a7859e6a5d59851068d14" } }
                vcs = { foo= "foo"}
            """,
            "supported_tags": [("cp310", "cp310", "macosx_10_9_universal2")],
            "expected": {},
            "check_subset": True,
            "id": "other_package_formats_than_sdist_wheels",
            "expected_conda_exception": "Currently we only support wheel packages",
        }
    ),
    (
        {
            "toml_str": """
                lock-version = "1.0"
                created-by = "uv"
                requires-python = ">=3.10.18"
                [[packages]]
                name = "charset-normalizer"
                version = "3.4.3"
                sdist = { url = "https://pypi.netflix.net/packages/18953203074/charset_normalizer-3.4.3.tar.gz", upload-time = 2025-08-09T07:57:28Z, size = 122371, hashes = { sha256 = "6fce4b8500244f6fcb71465d4a4930d132ba9ab8e71a7859e6a5d59851068d14" } }
                wheels = [ { url = "https://pypi.netflix.net/packages/18653672681/charset_normalizer-3.4.2-cp310-cp310-macosx_10_9_universal2.whl", upload-time = 2025-05-02T08:31:46Z, size = 201818 }, ]
            """,
            "supported_tags": [("cp310", "cp310", "macosx_10_9_universal2")],
            "expected": {},
            "check_subset": True,
            "id": "no_hash_in_wheel",
            "expected_conda_exception": "We encountered a wheel package that's missing an url or a hash.",
        }
    ),
]


@pytest.mark.parametrize(
    "case",
    test_case_params,
    ids=[tuple["id"] for tuple in test_case_params],
)
def test_package_matching(case):
    expected_exception_msg = case.get("expected_conda_exception", None)
    if expected_exception_msg:
        with pytest.raises(CondaException, match=expected_exception_msg):
            check_package_matching_may_throw_exception(case)
    else:
        check_package_matching_may_throw_exception(case)


def check_package_matching_may_throw_exception(case):
    root_obj = (
        PylockTomlResolver._read_toml(case["toml_file"])
        if case.get("toml_file", None)
        else (tomli.loads(case["toml_str"]))
    )
    grouped = PylockTomlResolver._pylock_toml_root_obj_to_packages(root_obj)
    if "supported_tags_tag_type" in case:
        tags = case["supported_tags_tag_type"]
    else:
        tags = create_supported_tags(case["supported_tags"])
    best = get_best_compatible_packages(grouped, tags)
    best_filenames = {k: (v.filename if v else None) for k, v in best.items()}

    if case["check_subset"]:
        for k, v in case["expected"].items():
            if v is None:
                assert k not in best_filenames or best_filenames[k] is None
            assert best_filenames.get(k) == v
    else:
        assert best_filenames == case["expected"]


def test_resolve_one_package():
    data_path = Path(__file__).parent / "data" / "sample_pylock1.toml"
    obj = PylockTomlResolver._read_toml(data_path)

    """
    Content of the first package:
    [[packages]]
    name = "certifi"
    version = "2025.8.3"
    sdist = { url = "https://pypi.netflix.net/packages/18933835518/certifi-2025.8.3.tar.gz", upload-time = 2025-08-03T03:07:47Z, size = 162386, hashes = { sha256 = "e564105f78ded564e3ae7c923924435e1daa7463faeab5bb932bc53ffae63407" } }
    wheels = [{ url = "https://pypi.netflix.net/packages/18933835517/certifi-2025.8.3-py3-none-any.whl", upload-time = 2025-08-03T03:07:45Z, size = 161216, hashes = { sha256 = "f6c12493cfb1b06ba2ff328595af9350c65d6644968e5d3a2ffd78699af217a5" } }]
    """
    first_package_obj = obj["packages"][0]
    assert len(first_package_obj["wheels"]) == 1
    output_packages = PylockTomlResolver._toml_package_to_package_specs(
        first_package_obj
    )
    assert len(output_packages) == 1

    assert output_packages[0].filename == "certifi-2025.8.3-py3-none-any"
    assert output_packages[0].package_name == "certifi"
    assert output_packages[0].package_version == "2025.8.3"
    assert (
        output_packages[0].url
        == "https://pypi.netflix.net/packages/18933835517/certifi-2025.8.3-py3-none-any.whl"
    )


def test_one_to_multi_package_resolution():
    data_path = Path(__file__).parent / "data" / "sample_pylock1.toml"
    obj = PylockTomlResolver._read_toml(data_path)

    """
    Content of the second package:

    [[packages]]
    name = "charset-normalizer"
    version = "3.4.2"
    sdist = { url = "https://pypi.netflix.net/packages/18653687935/charset_normalizer-3.4.2.tar.gz", upload-time = 2025-05-02T08:34:42Z, size = 126367, hashes = { sha256 = "5baececa9ecba31eff645232d59845c07aa030f0c81ee70184a90d35099a0e63" } }
    wheels = [
        { url = "https://pypi.netflix.net/packages/18653672681/charset_normalizer-3.4.2-cp310-cp310-macosx_10_9_universal2.whl", upload-time = 2025-05-02T08:31:46Z, size = 201818, hashes = { sha256 = "7c48ed483eb946e6c04ccbe02c6b4d1d48e51944b6db70f697e089c193404941" } },
        { url = "https://pypi.netflix.net/packages/18653672682/charset_normalizer-3.4.2-cp310-cp310-manylinux_2_17_aarch64.manylinux2014_aarch64.whl", upload-time = 2025-05-02T08:31:48Z, size = 144649, hashes = { sha256 = "b2d318c11350e10662026ad0eb71bb51c7812fc8590825304ae0bdd4ac283acd" } },
        { url = "https://pypi.netflix.net/packages/18653672683/charset_normalizer-3.4.2-cp310-cp310-manylinux_2_17_ppc64le.manylinux2014_ppc64le.whl", upload-time = 2025-05-02T08:31:50Z, size = 155045, hashes = { sha256 = "9cbfacf36cb0ec2897ce0ebc5d08ca44213af24265bd56eca54bee7923c48fd6" } },
        { url = "https://pypi.netflix.net/packages/18653672684/charset_normalizer-3.4.2-cp310-cp310-manylinux_2_17_s390x.manylinux2014_s390x.whl", upload-time = 2025-05-02T08:31:52Z, size = 147356, hashes = { sha256 = "18dd2e350387c87dabe711b86f83c9c78af772c748904d372ade190b5c7c9d4d" } },
        ... (91 wheels in total)
    """
    package_obj = obj["packages"][1]
    assert len(package_obj["wheels"]) == 91
    output_packages = PylockTomlResolver._toml_package_to_package_specs(package_obj)
    assert len(output_packages) == 91

    # 90-th element of wheels:
    #    { url = "https://pypi.netflix.net/packages/18653687934/charset_normalizer-3.4.2-py3-none-any.whl", upload-time = 2025-05-02T08:34:40Z, size = 52626, hashes = { sha256 = "7f56930ab0abd1c45cd15be65cc741c28b1c9a34876ce8c17a2fa107810c0af0" } },

    assert output_packages[90].filename == "charset_normalizer-3.4.2-py3-none-any"
    assert output_packages[90].package_name == "charset-normalizer"
    assert output_packages[90].package_version == "3.4.2"
    assert (
        output_packages[90].url
        == "https://pypi.netflix.net/packages/18653687934/charset_normalizer-3.4.2-py3-none-any.whl"
    )


def test_parsing_root_toml_obj():
    data_path = Path(__file__).parent / "data" / "sample_pylock1.toml"
    obj = PylockTomlResolver._read_toml(data_path)

    packages_dict = PylockTomlResolver._pylock_toml_root_obj_to_packages(obj)
    counts = 0
    counts = sum(len(p["wheels"]) for p in obj["packages"])
    wheels_count = sum(len(l) for l in packages_dict.values())

    assert wheels_count == counts

    pandas_list = packages_dict["pandas"]
    assert len(pandas_list) == 41
    assert (
        pandas_list[0].url
        == "https://pypi.netflix.net/packages/18851530093/pandas-2.3.1-cp310-cp310-macosx_10_9_x86_64.whl"
    )
    assert (
        pandas_list[40].url
        == "https://pypi.netflix.net/packages/18851544151/pandas-2.3.1-cp39-cp39-win_amd64.whl"
    )


def create_supported_tags(
    # tuple: (interpreter, abi, platform)
    tuple_list: List[Tuple[str, str, str]],
) -> List[Tag]:
    output: List[Tag] = []
    for python_version, architecture, platform in tuple_list:
        output.append(Tag(python_version, architecture, platform))

    return output


def parse_toml_str(toml_str: str) -> Dict[str, List[PypiPackageSpecification]]:
    package_obj = tomli.loads(toml_str)
    return PylockTomlResolver._pylock_toml_root_obj_to_packages(package_obj)


@pytest.fixture
def deps():
    return {
        "conda": [
            "python==>=3.9,<3.10",
            "pip",
            "wheel",
            "tomli",
            "setuptools",
            "uv==<=0.7.8",
        ],
        "sys": [],
    }


# Differences between test_translate_pylock_to_resolved_env() and
# test_pylock_toml_to_resolved_env():
# - test_translate_pylock_to_resolved_env() calls _translate_pylock_toml_to_resolved_env().
# - test_translate_pylock_to_resolved_env() avoids executing code paths in PylockTomlResolver
#   that rely on external methods such as get_python_full_version_from_builder_envs()
#   and pypi_tags_from_arch(), making it more atomic.
# - test_pylock_toml_to_resolved_env() calls PylockTomlResolver.resolve(), which introduces
#   additional dependencies, including a Conda instance for the PylockTomlResolver constructor,
#   get_python_full_version_from_builder_envs(), and pypi_tags_from_arch(). As a result, this test
#   is more end-to-end but less isolated, and requires mocks such as
#   mock_get_python_full_version_from_builder_envs() and arch_id().
# - test_pylock_toml_to_resolved_env() also calls pypi_tags_from_arch(), which makes the list
#   of supported tags substantially longer, covering more combinations of architectures and platforms.
#   This results in a longer _all_packages list in the resolved environment.
def test_translate_pylock_to_resolved_env(deps):
    root_obj = PylockTomlResolver._read_toml(
        str(Path(__file__).parent / "data" / "sample_pylock1.toml"),
    )

    resolved_env = PylockTomlResolver._translate_pylock_toml_to_resolved_env(
        toml_root_obj=root_obj,
        env_type=EnvType.MIXED,
        deps=deps,
        base_packages=[],
        sources={},
        extras={},
        architecture="osx-arm64",
        supported_tags=create_supported_tags(
            [
                ("cp310", "cp310", "macosx_10_9_universal2"),
                ("cp310", "cp310", "macosx_11_0_arm64"),
            ]
        ),
    )

    user_dependencies = sorted(dict_to_tstr(deps), key=lambda t: t.value)
    assert str(resolved_env._user_dependencies) == str(user_dependencies)
    assert resolved_env.env_type == EnvType.MIXED
    assert resolved_env.co_resolved_archs == ["osx-arm64"]
    assert len(resolved_env._all_packages) == 3

    # charset-normalizer had file matching "macosx_10_9_universal2"
    assert (
        resolved_env._all_packages[0].url
        == "https://pypi.netflix.net/packages/18653672681/charset_normalizer-3.4.2-cp310-cp310-macosx_10_9_universal2.whl"
    )

    # numpy had file matching "macosx_11_0_arm64"
    assert (
        resolved_env._all_packages[1].url
        == "https://pypi.netflix.net/packages/18694705588/numpy-2.2.6-cp310-cp310-macosx_11_0_arm64.whl"
    )

    # pandas had file matching "macosx_11_0_arm64"
    assert (
        resolved_env._all_packages[2].url
        == "https://pypi.netflix.net/packages/18851530094/pandas-2.3.1-cp310-cp310-macosx_11_0_arm64.whl"
    )


def test_base_packages_extended_to_packages(deps):
    root_obj = PylockTomlResolver._read_toml(
        str(Path(__file__).parent / "data" / "sample_pylock1.toml"),
    )
    base_packages = [
        CondaPackageSpecification(
            filename="python-3.10.18-h6cefb37_0_cpython",
            url="https://conda.anaconda.org/conda-forge/osx-arm64/python-3.10.18-h6cefb37_0_cpython.conda",
        )
    ]

    resolved_env = PylockTomlResolver._translate_pylock_toml_to_resolved_env(
        toml_root_obj=root_obj,
        env_type=EnvType.MIXED,
        deps=deps,
        base_packages=base_packages,
        sources={},
        extras={},
        architecture="osx-arm64",
        supported_tags=create_supported_tags(
            [
                ("cp310", "cp310", "macosx_10_9_universal2"),
                ("cp310", "cp310", "macosx_11_0_arm64"),
            ]
        ),
    )

    assert len(resolved_env._all_packages) == 4
    assert "python-3.10.18-h6cefb37_0_cpython" in [
        p.filename for p in resolved_env._all_packages
    ]


def test_pylock_toml_to_resolved_env(deps, mocker):
    builder_deps = deps
    builder_sources = {"conda": ["conda-forge"]}
    builder_env_id = EnvID(
        ResolvedEnvironment.get_req_id(builder_deps, builder_sources, {}),  # type: ignore
        "_default",
        "osx-arm64",
    )
    mocker.patch(
        "metaflow_extensions.netflix_ext.plugins.conda.resolvers.pylock_toml_resolver.get_python_full_version_from_builder_envs",
        return_value="3.10.1",
    )
    mocker.patch(
        "metaflow_extensions.netflix_ext.plugins.conda.utils.arch_id",
        return_value="osx-arm64",
    )

    builder_env = ResolvedEnvironment(
        user_dependencies=builder_deps,
        user_sources=builder_sources,
        user_extra_args={},
        arch="osx-arm64",
        env_id=builder_env_id,
        all_packages=[],
    )

    resolver = PylockTomlResolver(conda=Conda(echo, "local"))
    (resolved_env, _) = resolver.resolve(
        env_type=EnvType.MIXED,
        # In a real scenario (with this PR), only extras["pylock_toml_path"] should be passed to the resolver.
        # However, for unit testing and to verify that information provided to the resolver is correctly
        # propagated to the created ResolvedEnvironment, we supply a non-empty deps and check that it appears in the output.
        deps=deps,
        sources={},
        extras={},
        architecture="osx-arm64",
        builder_envs=[builder_env],  # type: ignore
        base_env={},  # type: ignore
        file_paths={
            "pylock_toml": [
                str(Path(__file__).parent / "data" / "sample_pylock1.toml")
            ],
        },
    )

    user_dependencies = sorted(dict_to_tstr(deps), key=lambda t: t.value)

    assert str(resolved_env._user_dependencies) == str(user_dependencies)
    assert resolved_env.env_type == EnvType.MIXED
    assert resolved_env.co_resolved_archs == ["osx-arm64"]

    # Tested resolve() method calls pypi_tags_from_arch(), the supported tags
    # list is substantially longer, including more combinations of architectures and platforms. This results
    # in a longer list of _all_packages in the resolved environment.
    assert len(resolved_env._all_packages) == 11

    # charset-normalizer had file matching "macosx_10_9_universal2"
    assert (
        resolved_env._all_packages[0].url
        == "https://pypi.netflix.net/packages/18933835517/certifi-2025.8.3-py3-none-any.whl"
    )

    # numpy had file matching "macosx_11_0_arm64"
    assert (
        resolved_env._all_packages[1].url
        == "https://pypi.netflix.net/packages/18653672681/charset_normalizer-3.4.2-cp310-cp310-macosx_10_9_universal2.whl"
    )

    # pandas had file matching "macosx_11_0_arm64"
    assert (
        resolved_env._all_packages[10].url
        == "https://pypi.netflix.net/packages/18797204827/urllib3-2.5.0-py3-none-any.whl"
    )


def test_translate_pylock_to_resolved_env_positional_args(deps):
    root_obj = PylockTomlResolver._read_toml(
        str(Path(__file__).parent / "data" / "sample_pylock1.toml"),
    )

    resolved_env = PylockTomlResolver._translate_pylock_toml_to_resolved_env(
        root_obj,
        EnvType.MIXED,
        deps,
        [],
        {},
        {},
        "osx-arm64",
        create_supported_tags(
            [
                ("cp310", "cp310", "macosx_10_9_universal2"),
                ("cp310", "cp310", "macosx_11_0_arm64"),
            ]
        ),
    )

    user_dependencies = sorted(dict_to_tstr(deps), key=lambda t: t.value)
    assert str(resolved_env._user_dependencies) == str(user_dependencies)
    assert resolved_env.env_type == EnvType.MIXED
    assert resolved_env.co_resolved_archs == ["osx-arm64"]
    assert len(resolved_env._all_packages) == 3

    # charset-normalizer had file matching "macosx_10_9_universal2"
    assert (
        resolved_env._all_packages[0].url
        == "https://pypi.netflix.net/packages/18653672681/charset_normalizer-3.4.2-cp310-cp310-macosx_10_9_universal2.whl"
    )

    # numpy had file matching "macosx_11_0_arm64"
    assert (
        resolved_env._all_packages[1].url
        == "https://pypi.netflix.net/packages/18694705588/numpy-2.2.6-cp310-cp310-macosx_11_0_arm64.whl"
    )

    # pandas had file matching "macosx_11_0_arm64"
    assert (
        resolved_env._all_packages[2].url
        == "https://pypi.netflix.net/packages/18851530094/pandas-2.3.1-cp310-cp310-macosx_11_0_arm64.whl"
    )


def test_pylock_toml_to_resolved_env_with_python_version(deps, mocker):
    builder_deps = deps
    builder_sources = {"conda": "conda-forge"}
    builder_env_id = EnvID(
        ResolvedEnvironment.get_req_id(builder_deps, builder_sources, {}),  # type: ignore
        "_default",
        "osx-arm64",
    )
    mocker.patch(
        "metaflow_extensions.netflix_ext.plugins.conda.resolvers.pylock_toml_resolver.get_python_full_version_from_builder_envs",
        return_value="3.10.1",
    )
    mocker.patch(
        "metaflow_extensions.netflix_ext.plugins.conda.utils.arch_id",
        return_value="osx-arm64",
    )

    builder_env = ResolvedEnvironment(
        user_dependencies=builder_deps,
        user_sources=builder_sources,
        user_extra_args={},
        arch="osx-arm64",
        env_id=builder_env_id,
        all_packages=[],
    )

    resolver = PylockTomlResolver(conda=Conda(echo, "local"))
    (resolved_env, _) = resolver.resolve(
        env_type=EnvType.MIXED,
        # In a real scenario (with this PR), only extras["pylock_toml_path"] should be passed to the resolver.
        # However, for unit testing and to verify that information provided to the resolver is correctly
        # propagated to the created ResolvedEnvironment, we supply a non-empty deps and check that it appears in the output.
        python_version_requested="3.10.1",
        deps=deps,
        sources={},
        extras={},
        architecture="osx-arm64",
        builder_envs=[builder_env],  # type: ignore
        base_env={},  # type: ignore
        file_paths={
            "pylock_toml": [
                str(Path(__file__).parent / "data" / "sample_pylock1.toml")
            ],
        },
    )

    user_dependencies = sorted(dict_to_tstr(deps), key=lambda t: t.value)

    assert str(resolved_env._user_dependencies) == str(user_dependencies)
    assert resolved_env.env_type == EnvType.MIXED
    assert resolved_env.co_resolved_archs == ["osx-arm64"]

    # Tested resolve() method calls pypi_tags_from_arch(), the supported tags
    # list is substantially longer, including more combinations of architectures and platforms. This results
    # in a longer list of _all_packages in the resolved environment.
    assert len(resolved_env._all_packages) == 11

    # charset-normalizer had file matching "macosx_10_9_universal2"
    assert (
        resolved_env._all_packages[0].url
        == "https://pypi.netflix.net/packages/18933835517/certifi-2025.8.3-py3-none-any.whl"
    )

    # numpy had file matching "macosx_11_0_arm64"
    assert (
        resolved_env._all_packages[1].url
        == "https://pypi.netflix.net/packages/18653672681/charset_normalizer-3.4.2-cp310-cp310-macosx_10_9_universal2.whl"
    )

    # pandas had file matching "macosx_11_0_arm64"
    assert (
        resolved_env._all_packages[10].url
        == "https://pypi.netflix.net/packages/18797204827/urllib3-2.5.0-py3-none-any.whl"
    )


def test_resolve_marker():
    """
    numpy package of this sample_pylock1.toml contains a "marker" line:

    [[packages]]
    name = "numpy"
    version = "2.2.6"
    marker = "python_full_version < '3.11'"
    sdist = { url = "https://pypi.netflix.net/packages/18694941122/numpy-2.2.6.tar.gz", upload-time = 2025-05-17T22:38:04Z, size = 20276440, hashes = { sha256 = "e29554e2bef54a90aa5cc07da6ce955accb83f21ab5de01a62c8478897b264fd" } }
    wheels = [
    """
    root_obj = PylockTomlResolver._read_toml(
        str(Path(__file__).parent / "data" / "sample_pylock1.toml"),
    )

    # Test resolving one package "numpy"
    numpy_package_obj = [p for p in root_obj["packages"] if p["name"] == "numpy"][0]

    numpy_packages = PylockTomlResolver._toml_package_to_package_specs(
        numpy_package_obj
    )
    assert numpy_packages
    for pkg in numpy_packages:
        assert pkg.environment_marker == "python_full_version < '3.11'"

    # Test resolving the full toml file.
    pkgs_dict = PylockTomlResolver._pylock_toml_root_obj_to_packages(root_obj)
    assert "numpy" in pkgs_dict
    for pkg in pkgs_dict["numpy"]:
        assert pkg.environment_marker == "python_full_version < '3.11'"


@pytest.fixture
def mock_target_env():
    # TODO: run this on workbench (x64) as well.
    return {
        "implementation_name": "cpython",
        "implementation_version": "3.10.19",
        "os_name": "posix",
        "platform_machine": "arm64",
        "platform_release": "25.1.0",
        "platform_system": "Darwin",
        "platform_version": "Darwin Kernel Version 25.1.0: Mon Oct 20 19:34:05 PDT 2025; root:xnu-12377.41.6~2/RELEASE_ARM64_T6041",
        "python_full_version": "3.10.19",
        "platform_python_implementation": "CPython",
        "python_version": "3.10",
        "sys_platform": "darwin",
    }


def test_filter_packages(mock_target_env):
    root_obj = PylockTomlResolver._read_toml(
        str(Path(__file__).parent / "data" / "sample_pylock1.toml"),
    )
    pkgs_dict = PylockTomlResolver._pylock_toml_root_obj_to_packages(root_obj)

    # Filter packages with marker that is satisfied by python version 3.10
    filtered_pkgs = PylockTomlResolver._filter_packages(
        pkgs_dict,
        python_full_version="3.10.10",
        architecture="x86-64",
        target_env=mock_target_env,
    )
    assert "numpy" in filtered_pkgs
    assert len(filtered_pkgs["numpy"]) == len(pkgs_dict["numpy"])
    assert len(pkgs_dict) == 11

    # Filter packages with marker that is NOT satisfied by python version 3.11
    filtered_pkgs = PylockTomlResolver._filter_packages(
        pkgs_dict,
        python_full_version="3.11.10",
        architecture="x86-64",
        target_env=mock_target_env,
    )
    assert "numpy" not in filtered_pkgs
    assert len(filtered_pkgs) == 10


def test_multiple_occurrences_of_same_package():
    # package "numpy" appeared twice, with different markers
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
[[packages]]
name = "numpy"
version = "2.3.4"
marker = "python_full_version >= '3.11'"
index = "https://pypi.netflix.net/simple"
sdist = { url = "https://pypi.netflix.net/packages/19205160721/numpy-2.3.4.tar.gz", upload-time = 2025-10-15T16:18:11Z, size = 20582187, hashes = { sha256 = "a7d018bfedb375a8d979ac758b120ba846a7fe764911a64465fd87b8729f4a6a" } }
wheels = [
    { url = "https://pypi.netflix.net/packages/19205112240/numpy-2.3.4-cp311-cp311-macosx_10_9_x86_64.whl", upload-time = 2025-10-15T16:15:19Z, size = 21259519, hashes = { sha256 = "e78aecd2800b32e8347ce49316d3eaf04aed849cd5b38e0af39f829a4e59f5eb" } },
    { url = "https://pypi.netflix.net/packages/19205112241/numpy-2.3.4-cp311-cp311-macosx_11_0_arm64.whl", upload-time = 2025-10-15T16:15:23Z, size = 14452796, hashes = { sha256 = "7fd09cc5d65bda1e79432859c40978010622112e9194e581e3415a3eccc7f43f" } },
    { url = "https://pypi.netflix.net/packages/19205112242/numpy-2.3.4-cp311-cp311-macosx_14_0_arm64.whl", upload-time = 2025-10-15T16:15:25Z, size = 5381639, hashes = { sha256 = "1b219560ae2c1de48ead517d085bc2d05b9433f8e49d0955c82e8cd37bd7bf36" } },
    { url = "https://pypi.netflix.net/packages/19205112243/numpy-2.3.4-cp311-cp311-macosx_14_0_x86_64.whl", upload-time = 2025-10-15T16:15:27Z, size = 6914296, hashes = { sha256 = "bafa7d87d4c99752d07815ed7a2c0964f8ab311eb8168f41b910bd01d15b6032" } },
]
""".encode(
                "utf-8"
            )
        )
    )
    packages_dict = PylockTomlResolver._pylock_toml_root_obj_to_packages(root_obj)

    assert "numpy" in packages_dict
    assert len(packages_dict["numpy"]) == 7

    grouped = dict(
        (k, list(v))
        for k, v in itertools.groupby(
            packages_dict["numpy"], key=lambda pkg: pkg.environment_marker
        )
    )
    assert grouped["python_full_version < '3.11'"][0].package_version == "2.2.6"
    assert len(grouped["python_full_version < '3.11'"]) == 3
    assert grouped["python_full_version >= '3.11'"][0].package_version == "2.3.4"
    assert len(grouped["python_full_version >= '3.11'"]) == 4
