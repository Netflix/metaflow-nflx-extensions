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


@pytest.mark.parametrize("version", ["0.11.102", "0.11.104", "0.12.0rc1"])
def test_locked_transitive_packages_satisfy_extension_pins(version):
    # No distinction between direct and transitive dependencies in the final set.
    validate_pypi_pins(
        {"nflx-pyiceberg": ">=0.11.102", "nflx-python-eureq": ""},
        [_package("nflx_pyiceberg", version), _package("nflx_python_eureq", "1.0")],
        "3.10.21",
        "linux-64",
    )


@pytest.mark.parametrize(
    "packages, expected",
    [
        ([], "locked version: missing"),
        ([_package("nflx_pyiceberg", "0.11.101")], "locked version: 0.11.101"),
    ],
)
def test_locked_missing_or_outdated_extension_package_fails(packages, expected):
    with pytest.raises(CondaException, match=expected) as error:
        validate_pypi_pins(
            {"nflx-pyiceberg": ">=0.11.102"}, packages, "3.10.21", "linux-64"
        )
    assert "regenerate the UV/pylock lockfile" in str(error.value)


def test_locked_unversioned_extension_package_is_required():
    with pytest.raises(CondaException, match="nflx-python-eureq"):
        validate_pypi_pins({"nflx-python-eureq": ""}, [], "3.10.21", "linux-64")


def test_locked_validation_evaluates_target_python_and_platform_markers(monkeypatch):
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
    validate_pypi_pins(pins, [], "3.10.21", "osx-arm64")
    with pytest.raises(CondaException, match="linux-only"):
        validate_pypi_pins(pins, [], "3.10.21", "linux-64")
    with pytest.raises(CondaException, match="new-python"):
        validate_pypi_pins(pins, [], "3.10.22", "osx-arm64")


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("version", [None, "0.11.101", "0.11.104"])
def test_fresh_and_cached_locked_environments_validate_hook_pins(
    cached, version, mocker
):
    module = "metaflow_extensions.netflixext.plugins.conda.envsresolver"
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
        "file_paths": {"pylock_toml": ["fixture.toml"]},
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
        module + ".get_pinned_pypi_libs", return_value={"nflx-pyiceberg": ">=0.11.102"}
    )
    if version == "0.11.104":
        resolver.resolve_environments(lambda *args: None)
    else:
        with pytest.raises(CondaException, match="nflx-pyiceberg"):
            resolver.resolve_environments(lambda *args: None)
    hook.assert_called_once_with("3.10.21", "s3")
    assert resolution.call_count == (0 if cached else 1)


def test_ordinary_resolution_does_not_apply_frozen_lock_validation(mocker):
    resolver = EnvsResolver(SimpleNamespace())
    resolver._non_step_envs = True
    resolver._requested_envs[EnvID("request", "full", "linux-64")] = {
        "resolved": SimpleNamespace(is_cached=lambda formats: True),
        "need_caching": False,
        "conda_format": ["_any"],
    }
    hook = mocker.patch(
        "metaflow_extensions.netflixext.plugins.conda.envsresolver.get_pinned_pypi_libs"
    )
    resolver.resolve_environments(lambda *args: None)
    hook.assert_not_called()


def test_no_pypi_hook_pins_preserves_existing_lock_behavior():
    validate_pypi_pins({}, [], "3.10.21", "linux-64")
