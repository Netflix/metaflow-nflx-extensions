"""
Tests for looking up lock-pinned environments (@resolved_uv, @resolved_req) on a
host with no local manifest.

Environments are stored with Conda.cache_environments into Metaflow's
LocalStorage, then looked up by a new Conda, as a scheduled run on a fresh
container would. Nothing is resolved or downloaded.
"""

from datetime import datetime, timedelta

# Import Metaflow first so its extension plugin registry is initialized before
# importing plugin modules directly.
import metaflow
import metaflow.metaflow_config as mf_config
from metaflow.plugins.datastores.local_storage import LocalStorage
import metaflow_extensions.netflixext.plugins.conda.conda as conda_module
from metaflow_extensions.netflixext.plugins.conda.conda import Conda
from metaflow_extensions.netflixext.plugins.conda.envsresolver import EnvsResolver
from metaflow_extensions.netflixext.plugins.conda.env_descr import (
    CachedEnvironmentInfo,
    EnvID,
    EnvType,
    PypiCachePackage,
    PypiPackageSpecification,
    ResolvedEnvironment,
)

import pytest

ARCH = "linux-64"
DEPS = {"pypi": ["numpy==1.26.0"]}
REQ_ID = ResolvedEnvironment.get_req_id(DEPS)
KEYS_A = {"requirements_txt_content_hash": "aaaa"}
KEYS_B = {"requirements_txt_content_hash": "bbbb"}


def _make_env(numpy_patch, keys, resolved_on):
    filename = "numpy-1.26.%s-cp311-cp311-manylinux_2_17_x86_64" % numpy_patch
    url = "https://example.com/%s.whl" % filename
    pkg = PypiPackageSpecification(filename, url, url_format=".whl")
    # Environments stored remotely have their packages cached as well
    pkg.add_cached_version(
        ".whl",
        PypiCachePackage(
            PypiCachePackage.make_cache_url(url, filename, ".whl", "0" * 64)
        ),
    )
    env = ResolvedEnvironment(
        DEPS,
        None,
        None,
        arch=ARCH,
        all_packages=[pkg],
        resolved_on=resolved_on,
        resolved_by="someone",
        env_type=EnvType.PYPI_ONLY,
        full_id_unique_keys=keys,
    )
    # Force computation of the full_id
    env.env_id
    return env


@pytest.fixture
def conda_root(tmp_path, monkeypatch):
    # Point the local datastore and the conda cache at a temporary directory and
    # enable caching for the local datastore, as CONDA_TEST does in CI.
    monkeypatch.setattr(LocalStorage, "SYSROOT_VAR", str(tmp_path / "manifest"))
    monkeypatch.setattr(mf_config, "CONDA_LOCALROOT", str(tmp_path / "conda"))
    monkeypatch.setattr(conda_module, "CONDA_TEST", True)
    # Packages are already cached so no conda binary is needed to store envs.
    monkeypatch.setattr(Conda, "_find_conda_binary", lambda self: None)
    return tmp_path


def _make_conda():
    # A new Conda starts with an empty local manifest, like a fresh host.
    return Conda(echo=lambda *args, **kwargs: None, datastore_type="local")


def _store_envs(*envs):
    # Store envs the way a flow run does after resolving them.
    for env in envs:
        env.dirty = True
    _make_conda().cache_environments(list(envs))


@pytest.fixture
def stored_envs(conda_root):
    now = datetime.now()
    # Older env resolved from lock A, newer env resolved from lock B
    env_a = _make_env("0", KEYS_A, now - timedelta(days=2))
    env_b = _make_env("1", KEYS_B, now - timedelta(days=1))
    _store_envs(env_a, env_b)
    return env_a, env_b


def _default_lookup(conda, keys=None, **kwargs):
    return conda.environment(
        EnvID(REQ_ID, "_default", ARCH), full_id_unique_keys=keys, **kwargs
    )


def test_lock_pinned_default_found_remotely_under_none(stored_envs):
    # A fresh host finds the env for the same lock even though a newer env exists.
    env_a, _ = stored_envs
    env = _default_lookup(_make_conda(), KEYS_A, use_latest=":none:")
    assert env is not None
    assert env.env_id == env_a.env_id


def test_lock_pinned_any_picks_matching_keys_not_latest(stored_envs):
    # With ":any:" the newest env with matching keys wins, not the newest env overall.
    env_a, _ = stored_envs
    env = _default_lookup(_make_conda(), KEYS_A, use_latest=":any:")
    assert env is not None
    assert env.env_id == env_a.env_id


def test_lock_pinned_picks_newest_of_several_matching_envs(conda_root):
    # Builder drift leaves several envs for one lock; the newest one is reused.
    now = datetime.now()
    older = _make_env("0", KEYS_A, now - timedelta(days=3))
    newest = _make_env("2", KEYS_A, now - timedelta(days=1))
    other_lock = _make_env("1", KEYS_B, now)
    _store_envs(older, newest, other_lock)
    assert len({older.env_id.full_id, newest.env_id.full_id}) == 2
    env = _default_lookup(_make_conda(), KEYS_A, use_latest=":none:")
    assert env is not None
    assert env.env_id == newest.env_id


@pytest.mark.usefixtures("stored_envs")
def test_lock_pinned_no_matching_keys_returns_none():
    # A changed lock file matches nothing, so the caller resolves a new env.
    keys = {"requirements_txt_content_hash": "cccc"}
    assert _default_lookup(_make_conda(), keys, use_latest=":none:") is None


@pytest.mark.usefixtures("stored_envs")
def test_not_lock_pinned_default_stays_local_only_under_none():
    # Envs without a lock hash (@pypi, @conda) keep the local-only default.
    assert _default_lookup(_make_conda(), use_latest=":none:") is None


@pytest.mark.usefixtures("stored_envs")
def test_lock_pinned_local_only_does_not_go_remote():
    # Runtime lookups pass local_only=True with the lock keys and must stay local.
    env = _default_lookup(_make_conda(), KEYS_A, local_only=True, use_latest=":none:")
    assert env is None


@pytest.mark.xfail(
    strict=True,
    reason="Existing bug: a comma-separated user list never matches because "
    "conda.py compares the (EnvID, env) tuple to the usernames",
)
def test_lock_pinned_user_list_matches_resolver(stored_envs):
    # CONDA_USE_REMOTE_LATEST="user1,user2" should accept envs resolved by those users.
    env_a, _ = stored_envs
    env = _default_lookup(_make_conda(), KEYS_A, use_latest="someone")
    assert env is not None
    assert env.env_id == env_a.env_id


def test_resolver_reuses_lock_pinned_env_on_fresh_host(stored_envs):
    # A fresh host's resolver reuses the stored env: nothing to resolve or cache.
    env_a, _ = stored_envs
    resolver = EnvsResolver(_make_conda())
    resolver.add_environment(
        ARCH,
        DEPS,
        {},
        {},
        {"pylock_toml": ["/unused/pylock.toml"]},
        "start",
        use_latest=":none:",
        full_id_unique_keys=KEYS_A,
    )
    (req,) = resolver._requested_envs.values()
    assert req["resolved"] is not None
    assert req["resolved"].env_id == env_a.env_id
    assert not req["need_caching"]
    assert list(resolver.need_caching_environments()) == []
