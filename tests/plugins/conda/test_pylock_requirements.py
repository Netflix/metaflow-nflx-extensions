from unittest.mock import Mock

import metaflow  # noqa: F401 -- initialize plugins before importing utilities
import pytest

from metaflow_extensions.netflixext.plugins.conda.env_descr import (
    CondaPackageSpecification,
    EnvType,
    ResolvedEnvironment,
)
from metaflow_extensions.netflixext.plugins.conda.resolvers.pylock_toml_resolver import (
    PylockTomlResolver,
)
from metaflow_extensions.netflixext.plugins.conda.utils import CondaException


@pytest.fixture
def resolve_locked_environment(tmp_path):
    def resolve(version, requirements, marker=None):
        lock = tmp_path / "pylock.toml"
        content = 'lock-version = "1.0"\ncreated-by = "uv"\n'
        if version:
            content += (
                '[[packages]]\nname = "nflx-pyiceberg"\n'
                f'version = "{version}"\n'
                + (f"marker = '{marker}'\n" if marker else "")
                + 'wheels = [{url = "https://example.com/nflx_pyiceberg-'
                + version
                + '-py3-none-any.whl", hashes = {sha256 = "abc"}}]\n'
            )
        lock.write_text(content)
        python = CondaPackageSpecification(
            "python-3.10.14-h123_0", "https://example.com/python-3.10.14-h123_0.conda"
        )
        builder = ResolvedEnvironment(
            user_dependencies={"conda": ["python==3.10.14"]},
            user_sources={},
            user_extra_args={},
            arch="linux-64",
            all_packages=[python],
        )
        return PylockTomlResolver(Mock()).resolve(
            env_type=EnvType.PYPI_ONLY,
            python_version_requested="python==3.10.14",
            deps={"pypi": requirements, "sys": ["__glibc==2.28"]},
            sources={},
            extras={},
            architecture="linux-64",
            builder_envs=[builder],
            file_paths={"pylock_toml": [str(lock)]},
        )[0]

    return resolve


@pytest.mark.parametrize("version", [None, "0.11.101"])
def test_pylock_rejects_missing_or_incompatible_required_package(
    resolve_locked_environment, version
):
    with pytest.raises(CondaException, match="regenerate the lockfile") as error:
        resolve_locked_environment(version, ["nflx-pyiceberg==>=0.11.102"])
    assert "nflx-pyiceberg>=0.11.102" in str(error.value)
    assert (version or "missing") in str(error.value)


def test_pylock_accepts_compatible_required_package(resolve_locked_environment):
    env = resolve_locked_environment("0.11.102", ["Nflx_PyIceberg==>=0.11.102"])
    assert any(
        p.package_version == "0.11.102" for p in env.packages if p.TYPE == "pypi"
    )


def test_pylock_rejects_conflicting_user_pin_with_extras(resolve_locked_environment):
    with pytest.raises(CondaException, match="does not satisfy"):
        resolve_locked_environment(
            "0.11.102",
            [
                "nflx-pyiceberg[pyarrow]==0.11.101",
                "nflx-pyiceberg==>=0.11.102",
            ],
        )


def test_pylock_filters_requirement_markers_against_target_python(
    resolve_locked_environment,
):
    env = resolve_locked_environment(
        None, ['nflx-pyiceberg<0.11.102; python_version < "3.10"']
    )
    assert all(p.TYPE != "pypi" for p in env.packages)


def test_pylock_rejects_required_package_filtered_out_of_lock(
    resolve_locked_environment,
):
    with pytest.raises(CondaException, match="locked version: missing"):
        resolve_locked_environment(
            "0.11.102", ["nflx-pyiceberg==>=0.11.102"], marker='python_version < "3.10"'
        )
