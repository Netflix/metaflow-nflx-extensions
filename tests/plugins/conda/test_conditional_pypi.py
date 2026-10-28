from types import SimpleNamespace

import metaflow
import pytest
from metaflow._vendor.packaging.requirements import Requirement

from metaflow_extensions.netflixext.plugins.conda.conditional_pypi import (
    conditional_requirements,
    constrained_dependencies,
    pypi_requirement,
    validate_conditional_requirements,
)
from metaflow_extensions.netflixext.plugins.conda.env_descr import (
    CondaPackageSpecification,
    EnvID,
    EnvType,
    PypiPackageSpecification,
    ResolvedEnvironment,
)
from metaflow_extensions.netflixext.plugins.conda.envsresolver import EnvsResolver
from metaflow_extensions.netflixext.plugins.conda.utils import CondaException


@pytest.fixture
def policy(mocker):
    hook = mocker.Mock(
        return_value={
            "example-native": {"example-native": ">=2", "example-adapter": ""}
        }
    )
    mocker.patch(
        "metaflow.extension_support.get_modules",
        return_value=[
            SimpleNamespace(module=SimpleNamespace(get_conditional_pypi_libs=hook))
        ],
    )
    return hook


@pytest.fixture
def environment():
    def make(versions=None, deps=None, env_type=EnvType.PYPI_ONLY):
        packages = [
            CondaPackageSpecification(
                "python-3.10.18-h123_0",
                "https://conda.example/linux-64/python-3.10.18-h123_0.conda",
            )
        ]
        for name, version in (versions or {}).items():
            filename = "%s-%s-py3-none-any" % (name.replace("-", "_"), version)
            packages.append(
                PypiPackageSpecification(
                    filename, "https://packages.example/%s.whl" % filename
                )
            )
        return ResolvedEnvironment(
            deps or {"conda": ["python==3.10.18"], "pypi": ["example-parent"]},
            {},
            {},
            "linux-64",
            all_packages=packages,
            env_type=env_type,
        )

    return make


@pytest.fixture
def resolve(mocker, environment):
    def run(results, deps=None, types=None):
        deps = deps or {"conda": ["python==3.10.18"], "pypi": ["example-parent"]}
        conda = SimpleNamespace(_datastore_type="s3")
        backend = mocker.Mock(TYPES=types or ["pip"])
        backend.resolve.side_effect = [(result, []) for result in results]
        mocker.patch.object(
            EnvsResolver, "get_resolver_cls", return_value=lambda conda: backend
        )
        resolver = EnvsResolver(conda)
        description = {
            "id": EnvID(
                ResolvedEnvironment.get_req_id(deps, {}, {}), "_default", "linux-64"
            ),
            "env_type": results[0].env_type,
            "python_version_requested": "python==3.10.18",
            "deps": deps,
            "user_deps": deps,
            "sources": {},
            "extras": {},
            "base": None,
            "file_paths": {},
            "full_id_unique_keys": {"lock": "fingerprint"},
            "base_accurate": False,
        }
        result = resolver._resolve(description, {})[1]
        return result, backend, description

    return run


def test_absent_native_package_keeps_graph_and_request_identity(
    policy, environment, resolve
):
    original = environment({"example-parent": "1"})
    result, backend, _ = resolve([original])
    assert result is original
    assert backend.resolve.call_count == 1
    assert result.env_id == original.env_id


@pytest.mark.parametrize(
    "env_type", [EnvType.PYPI_ONLY, EnvType.MIXED], ids=["pypi", "mixed"]
)
def test_transitive_native_package_activates_version_and_companion_requirements(
    policy, environment, resolve, env_type
):
    initial = environment(
        {"example-parent": "1", "example-native": "1"}, env_type=env_type
    )
    upgraded = environment(
        {"example-parent": "1", "example-native": "2", "example-adapter": "1"},
        env_type=env_type,
    )
    result, backend, description = resolve([initial, upgraded])
    solved = backend.resolve.call_args.args[2]["pypi"]
    assert "example-native==>=2" in solved
    assert "example-adapter" in solved
    assert backend.resolve.call_count == 2
    assert result.env_id.req_id == description["id"].req_id
    assert result.to_dict()["full_id_unique_keys"] == {"lock": "fingerprint"}
    assert "example-native" not in [str(dep) for dep in result.deps]
    policy.assert_called_with("3.10.18", "s3")


def test_compatible_native_graph_requires_no_second_resolution(
    policy, environment, resolve
):
    initial = environment({"example-native": "2", "example-adapter": "1"})
    result, backend, _ = resolve([initial])
    assert backend.resolve.call_count == 1
    assert {p.package_name for p in result.packages} == {
        p.package_name for p in initial.packages
    }


def test_direct_user_pin_is_intersected_without_overriding(policy, environment):
    original = environment({"example-native": "1"})
    deps = {
        "pypi": ["example-native[feature]==1", "other-package; python_version < '3.9'"]
    }
    merged = constrained_dependencies(
        deps, conditional_requirements(original, "s3"), original
    )
    req = next(
        pypi_requirement(value)
        for value in merged["pypi"]
        if value.startswith("example-native")
    )
    assert req.extras == {"feature"}
    assert not req.specifier.contains("1")
    assert not req.specifier.contains("2")
    assert "other-package; python_version < '3.9'" in merged["pypi"]
    assert deps["pypi"][0] == "example-native[feature]==1"


def test_url_requirements_are_preserved_and_incompatible_versions_fail(
    policy, environment, resolve
):
    initial = environment({"example-native": "1"})
    deps = {"pypi": ["example-native @ https://packages.example/native.whl"]}
    with pytest.raises(CondaException, match="Unable to satisfy conditional"):
        resolve([initial, initial], deps=deps)


@pytest.mark.parametrize(
    "versions,valid",
    [
        ({}, True),
        ({"example-native": "2", "example-adapter": "1"}, True),
        ({"example-native": "1", "example-adapter": "1"}, False),
        ({"example-native": "2"}, False),
    ],
    ids=["absent", "compatible", "old-native", "missing-adapter"],
)
def test_locks_validate_only_requirements_of_selected_triggers(
    policy, environment, resolve, versions, valid
):
    locked = environment(versions)
    if valid:
        _, backend, _ = resolve([locked], types=["pylock_toml"])
        assert backend.resolve.call_count == 1
    else:
        with pytest.raises(CondaException, match="regenerate.*lockfile"):
            resolve([locked], types=["pylock_toml"])


@pytest.mark.parametrize(
    "versions,usable",
    [
        ({"example-parent": "1"}, True),
        ({"example-native": "2", "example-adapter": "1"}, True),
        ({"example-native": "1"}, False),
    ],
    ids=["absent", "compatible", "incompatible"],
)
def test_cache_lookup_rechecks_conditional_policy(
    policy, environment, mocker, versions, usable
):
    cached = environment(versions)
    conda = SimpleNamespace(
        _datastore_type="s3", environment=mocker.Mock(return_value=cached)
    )
    found = EnvsResolver.find_resolved_environment(
        conda,
        "linux-64",
        {"conda": ["python==3.10.18"], "pypi": ["example-parent"]},
        {},
        {},
    )[-1]
    assert (found is cached) == usable


def test_conda_only_environment_never_calls_pypi_hooks(policy, environment):
    validate_conditional_requirements(environment(env_type=EnvType.CONDA_ONLY), "s3")
    policy.assert_not_called()


def test_multiple_extensions_preserve_all_constraints(environment, mocker):
    modules = [
        SimpleNamespace(
            module=SimpleNamespace(
                get_conditional_pypi_libs=lambda python, datastore: {
                    "EXAMPLE_native": {"example-native": ">=2"}
                }
            )
        ),
        SimpleNamespace(
            module=SimpleNamespace(
                get_conditional_pypi_libs=lambda python, datastore: {
                    "example-native": {"example-native": "<3"}
                }
            )
        ),
    ]
    mocker.patch("metaflow.extension_support.get_modules", return_value=modules)
    reqs = conditional_requirements(environment({"example-native": "2"}), "s3")
    assert {str(req) for req in reqs} == {"example-native>=2", "example-native<3"}


def test_conditional_merge_preserves_arbitrary_equality(environment):
    deps = {"pypi": ["example-native===2"]}
    merged = constrained_dependencies(
        deps, [Requirement("example-native>=2")], environment({"example-native": "2"})
    )
    req = pypi_requirement(merged["pypi"][0])
    assert "===2" in str(req.specifier)


@pytest.mark.parametrize("native", [False, True], ids=["absent", "incompatible"])
def test_named_base_is_checked_before_reuse(policy, environment, native):
    base = environment({"example-native": "1"} if native else {"example-parent": "1"})
    conda = SimpleNamespace(
        _datastore_type="s3", default_conda_channels=[], default_pypi_sources=[]
    )
    if native:
        with pytest.raises(CondaException, match="conditional requirements"):
            EnvsResolver.extract_info_from_base(conda, base, {}, {}, {}, "linux-64")
    else:
        EnvsResolver.extract_info_from_base(conda, base, {}, {}, {}, "linux-64")


@pytest.mark.parametrize(
    "requested_package,expected",
    [
        ("example-parent", {"example-parent", "example-native", "example-adapter"}),
        ("example-unrelated", {"example-unrelated"}),
        ("example-native<2", None),
    ],
    ids=["transitive-native", "unrelated", "conflicting-user-pin"],
)
def test_real_pip_resolution_applies_only_selected_conditional_bundles(
    policy, environment, mocker, tmp_path, requested_package, expected
):
    import os
    import subprocess
    import sys
    import zipfile

    from metaflow_extensions.netflixext.plugins.conda.resolvers.pip_resolver import (
        PipResolver,
    )
    from metaflow_extensions.netflixext.plugins.conda.utils import get_glibc_version

    for name, version, dependencies in [
        ("example-parent", "1", ["example-native"]),
        ("example-native", "2", []),
        ("example-native", "1", []),
        ("example-adapter", "1", []),
        ("example-unrelated", "1", []),
    ]:
        dist = name.replace("-", "_")
        metadata_dir = "%s-%s.dist-info" % (dist, version)
        with zipfile.ZipFile(
            tmp_path / ("%s-%s-py3-none-any.whl" % (dist, version)), "w"
        ) as wheel:
            metadata = "Metadata-Version: 2.1\nName: %s\nVersion: %s\n" % (
                name,
                version,
            )
            metadata += "".join("Requires-Dist: %s\n" % dep for dep in dependencies)
            wheel.writestr(metadata_dir + "/METADATA", metadata)
            wheel.writestr(
                metadata_dir + "/WHEEL",
                "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
            )
            wheel.writestr(metadata_dir + "/RECORD", "")

    def call_binary(args, binary, addl_env):
        return subprocess.check_output(
            [binary] + args, env=dict(os.environ, **addl_env), stderr=subprocess.STDOUT
        )

    conda = SimpleNamespace(
        _datastore_type="s3",
        create_builder_env=lambda env: sys.prefix,
        call_binary=call_binary,
    )
    mocker.patch.object(EnvsResolver, "get_resolver_cls", return_value=PipResolver)
    builder = environment()
    deps = {
        "conda": ["python==3.10.18"],
        "pypi": [requested_package],
        "sys": ["__glibc==" + get_glibc_version()],
    }
    sources = {"pypi": ["https://pypi.netflix.net/simple"]}
    extras = {"pypi": ["--no-index", "--find-links " + str(tmp_path)]}
    requested = EnvID(
        ResolvedEnvironment.get_req_id(deps, sources, extras), "_default", "linux-64"
    )
    description = {
        "id": requested,
        "env_type": EnvType.PYPI_ONLY,
        "python_version_requested": "python==3.10.18",
        "deps": deps,
        "user_deps": deps,
        "sources": sources,
        "extras": extras,
        "base": None,
        "file_paths": {},
        "full_id_unique_keys": {},
        "base_accurate": False,
    }
    resolver = EnvsResolver(conda)
    resolver._builder_envs[builder.env_id] = {"resolved": builder}
    if expected is None:
        with pytest.raises(subprocess.CalledProcessError) as error:
            resolver._resolve(description, {requested.req_id: [builder.env_id]})
        assert b"example-native<2,>=2" in error.value.output
        return
    result = resolver._resolve(description, {requested.req_id: [builder.env_id]})[1]
    assert {p.package_name for p in result.packages if p.TYPE == "pypi"} == expected
    assert result.env_id.req_id == requested.req_id
