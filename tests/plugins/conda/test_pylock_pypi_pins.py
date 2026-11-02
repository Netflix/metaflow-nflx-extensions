from contextlib import nullcontext
from types import SimpleNamespace

import pytest
import metaflow  # noqa: F401 -- initialize plugins before importing utilities

from metaflow_extensions.netflixext.plugins.conda.env_descr import (
    CondaPackageSpecification,
    EnvID,
    PypiPackageSpecification,
)
from metaflow_extensions.netflixext.plugins.conda.envsresolver import EnvsResolver
from metaflow_extensions.netflixext.plugins.conda.utils import (
    CondaException,
    validate_pypi_pins,
)


def _package(name, version):
    filename = "%s-%s-py3-none-any" % (name, version)
    return PypiPackageSpecification(filename, "https://example.org/%s.whl" % filename)


@pytest.mark.parametrize(
    "pins, version, error",
    [
        ({"nflx-pyiceberg": ">=0.11.102", "nflx-python-eureq": ""}, version, None)
        for version in ("0.11.102", "0.11.104", "0.12.0rc1")
    ]
    + [
        ({"nflx-pyiceberg": ">=0.11.102"}, None, "locked version: missing"),
        ({"nflx-pyiceberg": ">=0.11.102"}, "0.11.101", "locked version: 0.11.101"),
        ({"missing-package": ""}, None, "missing-package"),
        ({}, None, None),
    ],
)
def test_locked_packages_satisfy_hook_requirements(pins, version, error):
    # Final package sets include transitive dependencies, regardless of their origin.
    packages = [_package("nflx_python_eureq", "1.0")]
    if version:
        packages.append(_package("nflx_pyiceberg", version))
    with (
        pytest.raises(CondaException, match=error) if error else nullcontext()
    ) as result:
        validate_pypi_pins(pins, packages, "3.10.21", "linux-64")
    if error:
        assert "regenerate the UV/pylock lockfile" in str(result.value)


@pytest.mark.parametrize(
    "python, arch, error",
    [
        ("3.10.21", "osx-arm64", None),
        ("3.10.21", "linux-64", "linux-only"),
        ("3.10.22", "osx-arm64", "new-python"),
    ],
)
def test_locked_validation_evaluates_target_markers(python, arch, error, monkeypatch):
    monkeypatch.setattr(
        "metaflow_extensions.netflixext.plugins.conda.utils.CONDA_SYS_MARKERS",
        {
            "osx-arm64": {"sys_platform": "darwin"},
            "linux-64": {"sys_platform": "linux"},
        },
    )
    pins = {
        'linux-only; sys_platform == "linux"': "",
        'new-python; python_full_version >= "3.10.22"': "",
    }
    with pytest.raises(CondaException, match=error) if error else nullcontext():
        validate_pypi_pins(pins, [], python, arch)


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize(
    "locked, version, error",
    [
        (True, None, "nflx-pyiceberg"),
        (True, "0.11.101", "nflx-pyiceberg"),
        (True, "0.11.104", None),
        (False, None, None),
    ],
)
def test_resolution_validates_only_frozen_hook_requirements(
    cached, locked, version, error, mocker
):
    env_id = EnvID("request", "full", "linux-64")
    packages = [
        CondaPackageSpecification(
            "python-3.10.21-0", "https://example.org/python-3.10.21-0.conda"
        )
    ]
    if version:
        packages.append(_package("nflx_pyiceberg", version))
    resolved = SimpleNamespace(
        env_id=env_id, packages=packages, is_cached=lambda formats: True
    )
    resolver = EnvsResolver(SimpleNamespace(_datastore_type="s3"))
    resolver._non_step_envs = True
    req = {
        "file_paths": {"pylock_toml": ["fixture.toml"]} if locked else {},
        "resolved": resolved if cached else None,
        "need_caching": False,
        "conda_format": ["_any"],
    }
    resolver._requested_envs[env_id] = req

    def resolve(echo, env_ids):
        assert env_ids == [env_id]
        req["resolved"] = resolved

    resolution = mocker.patch.object(
        resolver, "_resolve_environments", side_effect=resolve
    )
    hook = mocker.patch(
        "metaflow_extensions.netflixext.plugins.conda.envsresolver.get_pinned_pypi_libs",
        return_value={"nflx-pyiceberg": ">=0.11.102"},
    )
    with pytest.raises(CondaException, match=error) if error else nullcontext():
        resolver.resolve_environments(lambda *args: None)
    if locked:
        hook.assert_called_once_with("3.10.21", "s3")
    else:
        hook.assert_not_called()
    assert resolution.call_count == (0 if cached else 1)
