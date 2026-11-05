from contextlib import nullcontext
from types import SimpleNamespace

import pytest
import metaflow  # noqa: F401 -- initialize plugins before importing utilities

from metaflow_extensions.netflixext.plugins.conda.env_descr import (
    CondaPackageSpecification,
    EnvID,
    EnvType,
    PypiPackageSpecification,
)
from metaflow_extensions.netflixext.plugins.conda.envsresolver import EnvsResolver
from metaflow_extensions.netflixext.plugins.conda.utils import (
    CondaException,
    validate_pypi_pins,
)


def _package(name, version, kind="pypi"):
    if kind == "conda":
        filename = "%s-%s-0" % (name.replace("_", "-"), version)
        return CondaPackageSpecification(
            filename, "https://example.org/%s.conda" % filename
        )
    filename = "%s-%s-py3-none-any" % (name, version)
    return PypiPackageSpecification(filename, "https://example.org/%s.whl" % filename)


@pytest.mark.parametrize(
    "pins, version, error",
    [
        ({"example-package": ">=1.2.0", "example-helper-package": ""}, version, None)
        for version in ("1.2.0", "1.2.1", "1.3.0rc1")
    ]
    + [
        ({"example-package": ">=1.2.0"}, None, "version: missing"),
        ({"example-package": ">=1.2.0"}, "1.1.0", "version: 1.1.0"),
        ({"missing-package": ""}, None, "missing-package"),
        ({}, None, None),
    ],
)
@pytest.mark.parametrize("kind", ["conda", "pypi"])
def test_final_packages_satisfy_hook_requirements(pins, version, error, kind):
    # Final package sets include transitive dependencies, regardless of their origin.
    packages = [_package("example_helper_package", "1.0", kind)]
    if version:
        packages.append(_package("example_package", version, kind))
    with (
        pytest.raises(CondaException, match=error) if error else nullcontext()
    ) as result:
        validate_pypi_pins(pins, packages, "3.10.21", "linux-64")
    if error:
        assert "resolve again" in str(result.value)


@pytest.mark.parametrize(
    "conda_version, pypi_version, error",
    [("1.1.0", "1.2.1", None), ("1.2.1", "1.1.0", "version: 1.1.0")],
    ids=["compatible-pypi", "incompatible-pypi"],
)
def test_pypi_version_takes_precedence_over_conda_record(
    conda_version, pypi_version, error
):
    # Record order must not affect precedence; packages may be an iterator.
    packages = iter(
        [
            _package("example_package", pypi_version),
            _package("example_package", conda_version, "conda"),
        ]
    )
    with pytest.raises(CondaException, match=error) if error else nullcontext():
        validate_pypi_pins(
            {"example-package": ">=1.2.0"}, packages, "3.10.21", "linux-64"
        )


@pytest.mark.parametrize(
    "python, arch, error",
    [
        ("3.10.21", "osx-arm64", None),
        ("3.10.21", "linux-64", "linux-only"),
        ("3.10.22", "osx-arm64", "new-python"),
    ],
)
def test_pin_validation_evaluates_target_markers(python, arch, error, monkeypatch):
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
@pytest.mark.parametrize("locked", [False, True])
@pytest.mark.parametrize(
    "version, error",
    [(None, "example-package"), ("1.1.0", "example-package"), ("1.2.1", None)],
)
def test_resolution_validates_fresh_and_reused_hook_requirements(
    cached, locked, version, error, mocker
):
    env_id = EnvID("request", "full", "linux-64")
    packages = [
        CondaPackageSpecification(
            "python-3.10.21-0", "https://example.org/python-3.10.21-0.conda"
        )
    ]
    if version:
        packages.append(_package("example_package", version))
    resolved = SimpleNamespace(
        env_id=env_id, packages=packages, is_cached=lambda formats: True
    )
    resolver = EnvsResolver(SimpleNamespace(_datastore_type="s3"))
    resolver._non_step_envs = True
    req = {
        "file_paths": {"pylock_toml": ["fixture.toml"]} if locked else {},
        "resolved": resolved if cached else None,
        "env_type": EnvType.PYPI_ONLY,
        "enforce_pypi_pins": True,
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
        return_value={"example-package": ">=1.2.0"},
    )
    with (
        pytest.raises(CondaException, match=error) if error else nullcontext()
    ) as result:
        resolver.resolve_environments(lambda *args: None)
    if error:
        remedy = "regenerate the UV/pylock lockfile" if locked else "resolve again"
        assert remedy in str(result.value)
    hook.assert_called_once_with("3.10.21", "s3")
    assert resolution.call_count == (0 if cached else 1)


@pytest.mark.parametrize(
    "env_type, enforce",
    [(EnvType.CONDA_ONLY, True), (EnvType.PYPI_ONLY, False)],
    ids=["conda-only", "dependencies-skipped"],
)
def test_resolution_does_not_enforce_pypi_pins_when_excluded(env_type, enforce, mocker):
    env_id = EnvID("request", "full", "linux-64")
    resolver = EnvsResolver(SimpleNamespace(_datastore_type="s3"))
    resolver._non_step_envs = True
    resolver._requested_envs[env_id] = {
        "resolved": SimpleNamespace(env_id=env_id, is_cached=lambda formats: True),
        "env_type": env_type,
        "enforce_pypi_pins": enforce,
        "need_caching": False,
        "conda_format": ["_any"],
    }
    hook = mocker.patch(
        "metaflow_extensions.netflixext.plugins.conda.envsresolver.get_pinned_pypi_libs"
    )
    resolver.resolve_environments(lambda *args: None)
    hook.assert_not_called()
