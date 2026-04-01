import metaflow

from metaflow_extensions.nflx.plugins.conda.conda import Conda
from metaflow_extensions.nflx.plugins.conda.conda_common_decorator import (
    StepRequirement,
)
from metaflow_extensions.nflx.plugins.conda.conda_environment import (
    CondaEnvironment,
)
from metaflow_extensions.nflx.plugins.conda.env_descr import EnvType

import pytest

env_type_test_cases = [
    # SCENARIO GROUP 1: from env is None --> determined by final_req's packages
    # SCENARIO 1.1: nothing in the package --> defaults to Conda
    (
        {
            "from_env_is_none": True,
            "final_req_packages": {},
            "expected": EnvType.CONDA_ONLY,
            "id": "from_env None, no package, returns CONDA_ONLY",
        }
    ),
    # SCENARIO 1.2: "conda" in _packages
    (
        {
            "from_env_is_none": True,
            "final_req_packages": {"conda": ["package1"]},
            "expected": EnvType.CONDA_ONLY,
            "id": "from_env None, package has conda",
        }
    ),
    # SCENARIO 1.3: both "pypi" and "conda" in _packages
    (
        {
            "from_env_is_none": True,
            "final_req_packages": {"pypi": ["package1"], "conda": ["package2"]},
            "expected": EnvType.MIXED,
            "id": "from_env None, both pypi and conda packages present",
        }
    ),
    # SCENARIO GROUP 2: has from_env
    # SCENARIO 2.1: no packages or sources
    (
        {
            "from_env_is_none": False,
            "from_env_env_type": EnvType.CONDA_ONLY,
            "final_req_packages": {},
            "expected": EnvType.CONDA_ONLY,
            "id": "from_env not None, no package, no source",
        }
    ),
    (
        {
            "from_env_is_none": False,
            "from_env_env_type": EnvType.PYPI_ONLY,
            "final_req_packages": {},
            "expected": EnvType.PYPI_ONLY,
            "id": "from_env not None, no package, no source",
        }
    ),
    # SCENARIO 2.2: package-or-source and from_env are with the same env_type.
    (
        {
            "from_env_is_none": False,
            "from_env_env_type": EnvType.CONDA_ONLY,
            "final_req_packages": {"conda": ["package1"]},
            "expected": EnvType.CONDA_ONLY,
            "id": "package and from_env are with the same env_type.",
        }
    ),
    (
        {
            "from_env_is_none": False,
            "from_env_env_type": EnvType.PYPI_ONLY,
            "final_req_packages": {"pypi": ["package1"]},
            "expected": EnvType.PYPI_ONLY,
            "id": "package and from_env are with the same env_type.",
        }
    ),
    # SCENARIO 2.3: package-or-source and from_env are not with the same env_type
    (
        {
            "from_env_is_none": False,
            "from_env_env_type": EnvType.CONDA_ONLY,
            "final_req_packages": {"pypi": ["package1"]},
            "expected": EnvType.MIXED,
            "id": "package-or-source and from_env are not with the same env_type",
        }
    ),
    (
        {
            "from_env_is_none": False,
            "from_env_env_type": EnvType.PYPI_ONLY,
            "final_req_packages": {"conda": ["package1"]},
            "expected": EnvType.MIXED,
            "id": "package-or-source and from_env are not with the same env_type",
        }
    ),
]


class MockFromEnv:
    def __init__(self, env_type):
        self.env_type = env_type


@pytest.mark.parametrize(
    "case_params",
    env_type_test_cases,
    ids=[tuple_dict["id"] for tuple_dict in env_type_test_cases],
)
def test_determine_env_type_for_step(case_params):
    if case_params["from_env_is_none"]:
        from_env = None
    else:
        from_env = MockFromEnv(env_type=case_params["from_env_env_type"])

    final_req = StepRequirement()
    final_req._packages = case_params["final_req_packages"]

    assert (
        CondaEnvironment._determine_env_type_for_step(from_env, final_req)
        == case_params["expected"]
    )
