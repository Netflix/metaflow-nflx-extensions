import os
import sys
from pathlib import Path
import pytest
from contextlib import nullcontext as does_not_raise
import tempfile

from metaflow import resolved_uv as ResolvedUVEnvFlowDecorator
from metaflow_extensions.nflx.plugins.conda.conda_flow_mutator import (
    ResolvedEnvironmentBaseFlowMutator,
    ResolvedReqFlowDecorator,
)

from metaflow_extensions.nflx.plugins.conda.resolvers.pylock_toml_resolver import (
    PylockTomlResolver,
)
from metaflow_extensions.nflx.plugins.conda.utils import compute_file_hash


this_dir = str(Path(__file__).parent)


def compare_dicts(dict1, dict2):
    assert len(dict1) == len(dict2)
    for key, value in dict1.items():
        assert key in dict2
        assert sorted(value.split(",")) == sorted(dict2[key].split(","))


def test_parse_requirements_in():
    requirements_in = os.path.join(
        this_dir, "uv_decorator_envs/req_txt_comprehensive/requirements.in"
    )
    user_deps, user_sources = ResolvedReqFlowDecorator._parse_requirements_in(
        requirements_in
    )

    expected = {
        "python": ">=3.10,<3.12",
        "numpy": ">=1.21.2,<3",
        "pandas": ">=1.4,<3",
        "scikit-learn": ">=1.6,<2",
        "joblib": "<2",
        "protobuf": ">=3",
        "torch": "<3",
        "pillow": ">=9,<12",
        "typing-extensions": ">=4.12,<5",
        "metaflow-netflixext": "<3",
    }
    print(user_deps)

    compare_dicts(user_deps, expected)
    assert user_sources == []
    assert "python" in user_deps


@pytest.mark.skipif(
    sys.version_info < (3, 9),
    reason="Fixture requirements.txt was compiled for Python 3.10 (numpy>=1.26 requires Python >=3.9)",
)
def test_handle_req_txt_scenario():
    requirements_txt_path = os.path.join(
        this_dir, "uv_decorator_envs/req_txt_simple_public/requirements.txt"
    )
    requirements_in_path = os.path.join(
        this_dir, "uv_decorator_envs/req_txt_simple_public/requirements.in"
    )

    tmp_toml_file_path, user_deps, user_sources = (
        ResolvedReqFlowDecorator._handle_req_txt_scenario(
            requirements_txt_path, requirements_in_path
        )
    )

    # Verify temp file was created
    assert Path.exists(
        Path(tmp_toml_file_path)
    ), "Temporary pylock.toml file should exist"

    # Read and validate the generated pylock.toml
    with open(tmp_toml_file_path, "r") as f:
        content = f.read()
        # Should contain the packages we expect
        assert "numpy" in content
        assert "pandas" in content

    # Verify user_deps match what we parsed from requirements.in
    assert "numpy" in user_deps
    assert sorted(user_deps["numpy"].split(",")) == sorted(">=1.21.2,<2".split(","))
    assert "pandas" in user_deps

    # Verify user_sources (should be empty for this simple case)
    assert user_sources == []

    # Verify only user dependencies are in user_deps, not transitive ones
    assert (
        "pytz" not in user_deps
    ), "pytz is a transitive dependency (via pandas), should not be in user_deps"


test_locate_directory_test_cases = [
    {
        "expected_path": "sub1/sub2",
        "start_path": "sub1/sub2/sub3",
        "validate_all_files_in_same_dir": False,
        "env_file_list": ["uv.lock", "pyproject.toml"],
        "create_files": [
            "sub1/pyproject.toml",
            "sub1/sub2/pyproject.toml",
            "sub1/sub2/uv.lock",
        ],
        "expected_exception": does_not_raise(),
        "id": "pyproject_toml_uv_lock_both_present_in_different_levels_uv_wins",
    },
    {
        "expected_path": "sub1/sub2",
        "start_path": "sub1/sub2/sub3/sub4",
        "env_file_list": ["uv.lock", "pyproject.toml"],
        "create_files": [
            "sub1/requirements.txt",
            "sub1/requirements.in",
            "sub1/sub2/uv.lock",
            "sub1/sub2/pyproject.toml",
        ],
        "expected_exception": does_not_raise(),
        "id": "all_four_files_exist",
    },
    {
        "expected_path": "sub1/sub2",
        "start_path": "sub1/sub2/sub3/sub4",
        "env_file_list": ["uv.lock", "pyproject.toml"],
        "create_files": [
            "sub1/sub2/uv.lock",
            "sub1/sub2/requirements.txt",
        ],
        "expected_exception": pytest.raises(
            FileNotFoundError,
            match="Found only partial files in the same directory",
        ),
        "id": "only_uv_no_pyproject_toml",
    },
    {
        "expected_path": "sub1/sub2",
        "start_path": "sub1/sub2/",
        "env_file_list": ["requirements.in", "requirements.txt"],
        "create_files": [
            "sub1/sub2/requirements.in",
            "sub1/sub2/requirements.txt",
        ],
        "expected_exception": does_not_raise(),
        "id": "req_in_txt_both_exist",
    },
    {
        "expected_path": "sub1/sub2",
        "start_path": "sub1/sub2/sub3/sub4",
        "env_file_list": ["requirements.in", "requirements.txt"],
        "create_files": [
            "sub1/sub2/requirements.in",
            "sub1/sub2/requirements.txt",
        ],
        "expected_exception": does_not_raise(),
        "id": "req_in_txt_both_exist_start_path_deeper",
    },
    {
        "expected_path": "sub1/sub2",
        "start_path": "sub1/sub2/sub3/sub4",
        "env_file_list": ["requirements.in", "requirements.txt"],
        "create_files": [
            "sub1/sub2/requirements.txt",
        ],
        "expected_exception": pytest.raises(
            FileNotFoundError,
            match="Found only partial files in the same directory",
        ),
        "id": "has_only_requirements_txt_no_req_in",
    },
]


@pytest.mark.parametrize(
    "case_params",
    test_locate_directory_test_cases,
    ids=[d["id"] for d in test_locate_directory_test_cases],
)
def test_locate_folder_first_contains_any_file_in_list(case_params):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        # Create the directory structure and files specified in the test case
        start_path = Path(temp_dir_path / case_params["start_path"])
        start_path.mkdir(parents=True, exist_ok=True)

        for file_path in case_params["create_files"]:
            full_path = temp_dir_path / file_path
            full_path.parent.mkdir(
                parents=True, exist_ok=True
            )  # Create parent directories
            full_path.touch()  # Create the file

        with case_params["expected_exception"]:
            found_path = ResolvedEnvironmentBaseFlowMutator._locate_first_folder_contains_any_file_from_list(
                case_params["env_file_list"],
                search_start_path=str(start_path),
            )

            assert Path(found_path) == temp_dir_path / case_params["expected_path"]


def test_verifying_autogen():
    assert not ResolvedReqFlowDecorator._contains_autogen_hint_in_requirements_txt(
        os.path.join(this_dir, "uv_decorator_envs/req_txt_no_autogen/requirements.txt")
    )
    assert ResolvedReqFlowDecorator._contains_autogen_hint_in_requirements_txt(
        os.path.join(this_dir, "uv_decorator_envs/req_txt_short/requirements.txt")
    )
