from types import SimpleNamespace

import metaflow  # noqa: F401 -- initialize plugins before importing utilities
import pytest

from metaflow_extensions.netflixext.plugins.conda.env_descr import (
    CondaPackageSpecification,
    EnvType,
    PypiPackageSpecification,
    ResolvedEnvironment,
)
from metaflow_extensions.netflixext.plugins.conda.envsresolver import EnvsResolver


@pytest.mark.parametrize(
    "dependency, installed, compatible",
    [
        ("example-helper-package", True, True),
        ("example-helper-package", False, False),
        ('example-helper-package>=2; python_version >= "3.10"', True, True),
        ('example-helper-package>=3; python_version >= "3.10"', True, False),
        ('example-helper-package>=2; python_version >= "3.10"', False, False),
        ('example-helper-package>=3; python_version < "3.10"', False, True),
    ],
)
def test_named_base_checks_requirements_against_its_python(
    dependency, installed, compatible, monkeypatch
):
    monkeypatch.setattr(
        "metaflow_extensions.netflixext.plugins.conda.envsresolver.CONDA_SYS_DEFAULT_PACKAGES",
        {},
    )
    packages = [
        CondaPackageSpecification(
            "python-3.10.21-0", "https://example.org/python-3.10.21-0.conda"
        )
    ]
    if installed:
        packages.append(
            PypiPackageSpecification(
                "example_helper_package-2.1.2-py3-none-any",
                "https://example.org/example_helper_package-2.1.2-py3-none-any.whl",
            )
        )
    base = ResolvedEnvironment(
        {}, {}, {}, "linux-64", all_packages=packages, env_type=EnvType.PYPI_ONLY
    )
    result = EnvsResolver.extract_info_from_base(
        SimpleNamespace(default_conda_channels=[], default_pypi_sources=[]),
        base,
        {"pypi": [dependency]},
        {},
        {},
        "linux-64",
    )
    assert result[-1] is compatible
