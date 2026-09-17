"""
Tests for consuming conda-lock lockfiles (`conda-lock.yml`).

See https://github.com/Netflix/metaflow-nflx-extensions/issues/34. The fixture in
data/sample_conda_lock.yml is real `conda-lock lock` output, trimmed.
"""

from pathlib import Path

import pytest

from metaflow.metaflow_environment import InvalidEnvironmentException

from metaflow_extensions.netflixext.plugins.conda.conda_lock_file import (
    resolved_environments_from_conda_lock,
)
from metaflow_extensions.netflixext.plugins.conda.env_descr import (
    CondaPackageSpecification,
    EnvType,
    PypiPackageSpecification,
)
from metaflow_extensions.netflixext.plugins.conda.parsers import parse_conda_lock_yml


DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def lock_content() -> str:
    return (DATA_DIR / "sample_conda_lock.yml").read_text()


# ---------------------------------------------------------------------------
# parse_conda_lock_yml
# ---------------------------------------------------------------------------


def test_parse_metadata(lock_content):
    lock = parse_conda_lock_yml(lock_content)

    assert lock.version == 1
    assert lock.platforms == ["linux-64", "osx-arm64"]
    assert lock.channels == ["conda-forge"]
    assert lock.sources == ["environment.yml"]
    assert sorted(lock.content_hash) == ["linux-64", "osx-arm64"]


def test_parse_packages_normalizes_manager(lock_content):
    lock = parse_conda_lock_yml(lock_content)

    # conda-lock writes "pip"; the rest of the codebase calls it "pypi".
    assert {p.manager for p in lock.packages} == {"conda", "pypi"}
    itsdangerous = next(p for p in lock.packages if p.name == "itsdangerous")
    assert itsdangerous.manager == "pypi"
    assert itsdangerous.version == "2.2.0"
    assert itsdangerous.url.endswith(".whl")
    assert "sha256" in itsdangerous.hashes


def test_parse_keeps_per_platform_entries(lock_content):
    lock = parse_conda_lock_yml(lock_content)

    pythons = {p.platform: p.version for p in lock.packages if p.name == "python"}
    # The same package resolves to different versions per platform; both are kept.
    assert pythons["linux-64"] == "3.12.14"
    assert pythons["osx-arm64"] == "3.12.2"


def test_packages_for_skips_dev_by_default(lock_content):
    lock = parse_conda_lock_yml(lock_content)

    names = {p.name for p in lock.packages_for("linux-64")}
    assert "pytest" not in names, "dev category must be skipped by default"

    with_dev = {p.name for p in lock.packages_for("linux-64", include_dev=True)}
    assert "pytest" in with_dev


@pytest.mark.parametrize(
    "content,expected",
    [
        ("name: myenv\ndependencies:\n  - python=3.12\n", "Unsupported conda-lock"),
        ("version: 99\nmetadata: {}\npackage: []\n", "Unsupported conda-lock"),
        ("version: 1\nmetadata: {}\npackage: []\n", "does not list any platform"),
        ("[]\n", "must be a YAML mapping"),
    ],
)
def test_parse_rejects_bad_input(content, expected):
    with pytest.raises(InvalidEnvironmentException, match=expected):
        parse_conda_lock_yml(content)


def test_parse_rejects_unknown_manager():
    content = """
version: 1
metadata:
  platforms: [linux-64]
package:
- name: foo
  version: '1.0'
  manager: poetry
  platform: linux-64
  url: https://example.com/foo-1.0.conda
"""
    with pytest.raises(InvalidEnvironmentException, match="unsupported manager"):
        parse_conda_lock_yml(content)


def test_parse_rejects_package_for_unlisted_platform():
    content = """
version: 1
metadata:
  platforms: [linux-64]
package:
- name: foo
  version: '1.0'
  manager: conda
  platform: win-64
  url: https://example.com/foo-1.0.conda
  hash:
    md5: deadbeef
"""
    with pytest.raises(InvalidEnvironmentException, match="not listed in"):
        parse_conda_lock_yml(content)


def test_parse_rejects_package_missing_url():
    content = """
version: 1
metadata:
  platforms: [linux-64]
package:
- name: foo
  version: '1.0'
  manager: conda
  platform: linux-64
"""
    with pytest.raises(InvalidEnvironmentException, match="missing a 'url'"):
        parse_conda_lock_yml(content)


# ---------------------------------------------------------------------------
# resolved_environments_from_conda_lock
# ---------------------------------------------------------------------------


def test_builds_one_environment_per_platform(lock_content):
    envs = resolved_environments_from_conda_lock(lock_content)

    assert sorted(envs) == ["linux-64", "osx-arm64"]
    for arch, env in envs.items():
        assert env.env_id.arch == arch


def test_environments_share_req_id_and_full_id(lock_content):
    envs = resolved_environments_from_conda_lock(lock_content)

    # One lockfile means one requirement, and the platforms are co-resolved by
    # construction, so they must agree on both ids.
    assert len({e.env_id.req_id for e in envs.values()}) == 1
    assert len({e.env_id.full_id for e in envs.values()}) == 1
    for env in envs.values():
        assert sorted(env.co_resolved_archs) == ["linux-64", "osx-arm64"]


def test_packages_keep_lockfile_url_and_hash(lock_content):
    lock = parse_conda_lock_yml(lock_content)
    envs = resolved_environments_from_conda_lock(lock_content)

    packages = {p.package_name: p for p in envs["linux-64"].packages}

    python_lock = next(
        p for p in lock.packages if p.name == "python" and p.platform == "linux-64"
    )
    python = packages["python"]
    assert isinstance(python, CondaPackageSpecification)
    assert python.url == python_lock.url
    assert python.package_version == "3.12.14"
    # Conda packages are addressed by md5 everywhere in this codebase.
    assert python.pkg_hash(python.url_format) == python_lock.hashes["md5"]

    itsdangerous_lock = next(p for p in lock.packages if p.name == "itsdangerous")
    itsdangerous = packages["itsdangerous"]
    assert isinstance(itsdangerous, PypiPackageSpecification)
    assert itsdangerous.url == itsdangerous_lock.url
    assert itsdangerous.pkg_hash(".whl") == itsdangerous_lock.hashes["sha256"]


def test_env_type_is_mixed_when_pypi_present(lock_content):
    envs = resolved_environments_from_conda_lock(lock_content)

    assert envs["linux-64"].env_type == EnvType.MIXED


def test_same_package_set_gives_the_same_environment(lock_content):
    envs = resolved_environments_from_conda_lock(lock_content)
    # Same packages, different lockfile metadata.
    other = resolved_environments_from_conda_lock(
        lock_content.replace("  - environment.yml", "  - other.yml")
    )

    # Environments are identified by what they pin, so an identical package set reuses
    # the same environment (and therefore the same cache entry).
    assert envs["linux-64"].env_id.full_id == other["linux-64"].env_id.full_id


def test_different_package_set_gives_a_different_environment(lock_content):
    lock = parse_conda_lock_yml(lock_content)
    click_md5 = next(
        p for p in lock.packages if p.name == "click" and p.platform == "linux-64"
    ).hashes["md5"]

    envs = resolved_environments_from_conda_lock(lock_content)
    repinned = resolved_environments_from_conda_lock(
        lock_content.replace(click_md5, "0" * len(click_md5))
    )

    assert envs["linux-64"].env_id.full_id != repinned["linux-64"].env_id.full_id


def test_platform_restriction(lock_content):
    envs = resolved_environments_from_conda_lock(lock_content, platforms=["linux-64"])

    assert sorted(envs) == ["linux-64"]


def test_unknown_platform_is_rejected(lock_content):
    with pytest.raises(InvalidEnvironmentException, match="does not contain platform"):
        resolved_environments_from_conda_lock(lock_content, platforms=["win-64"])


def test_dev_packages_excluded_by_default(lock_content):
    envs = resolved_environments_from_conda_lock(lock_content)
    with_dev = resolved_environments_from_conda_lock(lock_content, include_dev=True)

    assert "pytest" not in {p.package_name for p in envs["linux-64"].packages}
    assert "pytest" in {p.package_name for p in with_dev["linux-64"].packages}
    # Different package sets must give different environments.
    assert envs["linux-64"].env_id.full_id != with_dev["linux-64"].env_id.full_id


def test_user_dependencies_are_lockfile_roots(lock_content):
    envs = resolved_environments_from_conda_lock(lock_content)

    deps = {str(d) for d in envs["linux-64"].deps}
    # click and itsdangerous are roots: nothing in the lockfile depends on them.
    assert "conda::click==8.5.0" in deps
    assert "pypi::itsdangerous==2.2.0" in deps
    # python is depended on by the others, so it is not a root.
    assert not any(d.startswith("conda::python==") for d in deps)
    # The deps are computed from the whole lockfile so every platform agrees.
    assert deps == {str(d) for d in envs["osx-arm64"].deps}


def test_vcs_pypi_dependency_is_rejected():
    content = """
version: 1
metadata:
  platforms: [linux-64]
package:
- name: foo
  version: '1.0'
  manager: pip
  platform: linux-64
  url: git+https://github.com/example/foo@abcdef
  hash:
    sha256: deadbeef
"""
    with pytest.raises(InvalidEnvironmentException, match="VCS dependency"):
        resolved_environments_from_conda_lock(content)


def test_pypi_sdist_is_rejected():
    content = """
version: 1
metadata:
  platforms: [linux-64]
package:
- name: foo
  version: '1.0'
  manager: pip
  platform: linux-64
  url: https://files.pythonhosted.org/packages/ab/foo-1.0.tar.gz
  hash:
    sha256: deadbeef
"""
    with pytest.raises(InvalidEnvironmentException, match="source distribution"):
        resolved_environments_from_conda_lock(content)


def test_conda_package_without_md5_is_rejected():
    content = """
version: 1
metadata:
  platforms: [linux-64]
package:
- name: foo
  version: '1.0'
  manager: conda
  platform: linux-64
  url: https://conda.anaconda.org/conda-forge/linux-64/foo-1.0-h1.conda
  hash:
    sha256: deadbeef
"""
    with pytest.raises(InvalidEnvironmentException, match="no md5 hash"):
        resolved_environments_from_conda_lock(content)


# ---------------------------------------------------------------------------
# `environment resolve --lockfile` CLI
# ---------------------------------------------------------------------------


@pytest.fixture
def lock_cli():
    """Click runner plus the env vars the other CLI tests use to stay self-contained."""
    import uuid

    from metaflow._vendor.click.testing import CliRunner

    run_id = str(uuid.uuid4())
    return CliRunner(), {
        "METAFLOW_CONDA_ENVS_DIRNAME": "testing/envs_%s" % run_id,
        "METAFLOW_CONDA_PACKAGES_DIRNAME": "testing/packages_%s" % run_id,
        "METAFLOW_CONDA_MAGIC_FILE_V2": "condav2-%s.cnd" % run_id,
        "METAFLOW_CONDA_LOCK_TIMEOUT": "7200",
    }


def _invoke(runner, env_vars, args):
    from metaflow_extensions.netflixext.cmd.environment.environment_cmd import (
        environment,
    )

    return runner.invoke(environment, args, env=env_vars, catch_exceptions=False)


def test_cli_resolves_every_platform_of_the_lockfile(lock_cli):
    runner, env_vars = lock_cli
    result = _invoke(
        runner,
        env_vars,
        ["resolve", "--lockfile", str(DATA_DIR / "sample_conda_lock.yml"), "--dry-run"],
    )

    assert result.exit_code == 0, result.output
    assert "### Environment for architecture linux-64" in result.output
    assert "### Environment for architecture osx-arm64" in result.output
    assert "Dry-run -- not caching or aliasing" in result.output


def test_cli_arch_restricts_platforms(lock_cli):
    runner, env_vars = lock_cli
    result = _invoke(
        runner,
        env_vars,
        [
            "resolve",
            "--lockfile",
            str(DATA_DIR / "sample_conda_lock.yml"),
            "--arch",
            "linux-64",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "### Environment for architecture linux-64" in result.output
    assert "osx-arm64" not in result.output


def test_cli_rejects_options_that_imply_resolution(lock_cli, tmp_path):
    runner, env_vars = lock_cli
    env_yml = tmp_path / "environment.yml"
    env_yml.write_text("dependencies:\n  - python=3.12\n")

    result = _invoke(
        runner,
        env_vars,
        [
            "resolve",
            "--lockfile",
            str(DATA_DIR / "sample_conda_lock.yml"),
            "-f",
            str(env_yml),
            "--dry-run",
        ],
    )

    assert result.exit_code != 0
    assert "Cannot specify -f/--file with --lockfile" in result.output


def test_cli_include_dev_requires_lockfile(lock_cli, tmp_path):
    runner, env_vars = lock_cli
    env_yml = tmp_path / "environment.yml"
    env_yml.write_text("dependencies:\n  - python=3.12\n")

    result = _invoke(
        runner, env_vars, ["resolve", "-f", str(env_yml), "--include-dev", "--dry-run"]
    )

    assert result.exit_code != 0
    assert "only meaningful with --lockfile" in result.output
