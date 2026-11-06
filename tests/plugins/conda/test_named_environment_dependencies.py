"""Preserve package constraints when reusing or extending named environments."""

from unittest.mock import Mock

import pytest
import metaflow  # Initialize extension configuration before importing the resolver.

from metaflow_extensions.netflixext.plugins.conda import envsresolver
from metaflow_extensions.netflixext.plugins.conda.env_descr import (
    CondaPackageSpecification,
    EnvType,
    PypiPackageSpecification,
    ResolvedEnvironment,
)
from metaflow_extensions.netflixext.plugins.conda.envsresolver import EnvsResolver
from metaflow_extensions.netflixext.plugins.conda.utils import (
    get_requirement,
    split_into_dict,
)


@pytest.fixture
def conda(monkeypatch):
    monkeypatch.setattr(envsresolver, "CONDA_SYS_DEFAULT_PACKAGES", {})
    return Mock(
        default_conda_channels=["conda-forge"],
        default_pypi_sources=["https://pypi.org/simple"],
    )


@pytest.fixture
def make_base():
    def make(arch="linux-64", pandas_name="pandas", mixed=True):
        packages = []
        for name, version in (
            ("python", "3.10.18"),
            ("pandas", "1.5.3"),
            ("numpy", "1.26.4"),
        ):
            filename = "%s-%s-build_0" % (name, version)
            packages.append(
                CondaPackageSpecification(
                    filename,
                    "https://conda.anaconda.org/conda-forge/%s/%s.conda"
                    % (arch, filename),
                    hashes={".conda": "0" * 32},
                )
            )
        deps = {"conda": ["python==3.10.*", "%s==1.5.3" % pandas_name, "numpy==1.26.4"]}
        sources = {"conda": ["conda-forge"]}
        if mixed:
            deps["pypi"] = ["requests==2.32.3"]
            sources["pypi"] = ["https://pypi.org/simple"]
            packages.append(
                PypiPackageSpecification(
                    "requests-2.32.3-py3-none-any",
                    "https://example.org/requests-2.32.3-py3-none-any.whl",
                    hashes={".whl": "1" * 64},
                )
            )
        return ResolvedEnvironment(
            deps,
            sources,
            {},
            arch=arch,
            all_packages=packages,
            env_type=EnvType.MIXED if mixed else EnvType.CONDA_ONLY,
        )

    return make


@pytest.mark.parametrize("arch", ["linux-64", "osx-64", "osx-arm64"])
@pytest.mark.parametrize("mixed", [False, True])
def test_compatible_conda_requirements_reuse_base_artifacts(
    conda, make_base, arch, mixed
):
    base = make_base(arch=arch, mixed=mixed)
    incoming = {"conda": ["python==3.10.*", "pandas==>=0.24.0"]}
    if mixed:
        incoming["pypi"] = ["requests==>=2.21.0"]

    result = EnvsResolver.find_resolved_environment(
        conda, arch, incoming, {}, {}, base_env=base
    )

    assert result[6] is True
    resolved = result[7]
    assert resolved.env_id.req_id != base.env_id.req_id
    assert resolved.env_id.full_id == base.env_id.full_id
    assert list(resolved.packages) == list(base.packages)
    conda.environment.assert_not_called()


@pytest.mark.parametrize(
    "requirement,compatible",
    [
        ("pandas==>=1.0", True),
        ("conda-forge::pandas==>=1.0", True),
        ("other-channel::pandas==>=1.0", False),
        ("pandas==>=2.0", False),
        ("missing-package", False),
    ],
)
def test_base_compatibility_respects_conda_versions_and_channels(
    conda, make_base, requirement, compatible
):
    result = EnvsResolver.extract_info_from_base(
        conda, make_base(), {"conda": [requirement]}, {}, {}, "linux-64"
    )
    assert result[6] is compatible


@pytest.mark.parametrize("pandas_name", ["pandas", "conda-forge::pandas"])
@pytest.mark.parametrize("minimum", [">=0.24.0", ">=2.0"])
def test_derivation_combines_qualified_and_unqualified_conda_constraints(
    conda, make_base, pandas_name, minimum
):
    base = make_base(pandas_name=pandas_name)
    original_deps = list(base.deps)
    incoming = {
        "conda": ["pandas==%s" % minimum],
        "pypi": ["new-package"],
    }
    result = EnvsResolver.extract_info_from_base(
        conda, base, incoming, {}, {}, "linux-64"
    )

    assert result[6] is False
    for deps in (result[3], result[4]):
        pandas_reqs = [
            dep for dep in deps["conda"] if get_requirement(dep).name == "pandas"
        ]
        assert len(pandas_reqs) == 1
        req = get_requirement(pandas_reqs[0])
        assert str(req.specifier) == str(
            get_requirement("pandas==1.5.3,%s" % minimum).specifier
        )
        assert not req.specifier.contains("2.3.3")
        assert req.specifier.contains("1.5.3") is (minimum == ">=0.24.0")
    resolved_deps = split_into_dict(result[4]["conda"])
    assert "pandas" not in resolved_deps
    assert "conda-forge::pandas" in resolved_deps
    assert resolved_deps["conda-forge::numpy"] == "1.26.4"
    assert list(base.deps) == original_deps
    assert incoming["conda"] == ["pandas==%s" % minimum]


def test_derivation_preserves_distinct_explicit_channels(conda, make_base):
    result = EnvsResolver.extract_info_from_base(
        conda,
        make_base(pandas_name="conda-forge::pandas"),
        {"conda": ["other-channel::pandas==>=2.0"]},
        {},
        {},
        "linux-64",
    )
    assert result[6] is False
    deps = split_into_dict(result[4]["conda"])
    assert deps["conda-forge::pandas"] == "1.5.3"
    assert deps["other-channel::pandas"] == ">=2.0"
