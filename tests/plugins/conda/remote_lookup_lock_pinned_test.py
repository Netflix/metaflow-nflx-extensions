import json
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timedelta

# Import Metaflow first so its extension plugin registry is initialized before
# importing plugin modules directly.
import metaflow
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


class _FakeStorage:
    """In-memory stand-in for the datastore storage used by Conda."""

    def __init__(self):
        self.objects = {}

    def put_env(self, env):
        path = Conda.get_datastore_path_to_env(env.env_id)
        self.objects[path] = json.dumps(env.to_dict()).encode("utf-8")

    def list_content(self, paths):
        result = set()
        for prefix in paths:
            prefix = prefix.rstrip("/") + "/"
            for key in self.objects:
                if key.startswith(prefix):
                    child = key[len(prefix) :].split("/", 1)[0]
                    result.add((prefix + child, False))
        return list(result)

    @contextmanager
    def load_bytes(self, keys):
        with tempfile.TemporaryDirectory() as td:
            loaded = []
            for idx, key in enumerate(keys):
                if key in self.objects:
                    path = os.path.join(td, str(idx))
                    with open(path, "wb") as f:
                        f.write(self.objects[key])
                    loaded.append((key, path, None))
                else:
                    loaded.append((key, None, None))
            yield loaded


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


def _make_conda(storage):
    conda = Conda.__new__(Conda)
    conda._cached_environment = CachedEnvironmentInfo()
    conda._storage = storage
    return conda


@pytest.fixture
def storage():
    now = datetime.now()
    s = _FakeStorage()
    # Older env resolved from lock A, newer env resolved from lock B
    env_a = _make_env("0", KEYS_A, now - timedelta(days=2))
    env_b = _make_env("1", KEYS_B, now - timedelta(days=1))
    s.put_env(env_a)
    s.put_env(env_b)
    return s, env_a, env_b


def _default_lookup(conda, keys=None, **kwargs):
    return conda.environment(
        EnvID(REQ_ID, "_default", ARCH), full_id_unique_keys=keys, **kwargs
    )


def test_lock_pinned_default_found_remotely_under_none(storage):
    # A fresh host finds the env for the same lock even though a newer env exists.
    s, env_a, _ = storage
    env = _default_lookup(_make_conda(s), KEYS_A, use_latest=":none:")
    assert env is not None
    assert env.env_id == env_a.env_id


def test_lock_pinned_any_picks_matching_keys_not_latest(storage):
    # With ":any:" the newest env with matching keys wins, not the newest env overall.
    s, env_a, _ = storage
    env = _default_lookup(_make_conda(s), KEYS_A, use_latest=":any:")
    assert env is not None
    assert env.env_id == env_a.env_id


def test_lock_pinned_picks_newest_of_several_matching_envs():
    # Builder drift leaves several envs for one lock; the newest one is reused.
    now = datetime.now()
    s = _FakeStorage()
    older = _make_env("0", KEYS_A, now - timedelta(days=3))
    newest = _make_env("2", KEYS_A, now - timedelta(days=1))
    other_lock = _make_env("1", KEYS_B, now)
    for env in (older, newest, other_lock):
        s.put_env(env)
    assert len({older.env_id.full_id, newest.env_id.full_id}) == 2
    env = _default_lookup(_make_conda(s), KEYS_A, use_latest=":none:")
    assert env is not None
    assert env.env_id == newest.env_id


def test_lock_pinned_no_matching_keys_returns_none(storage):
    # A changed lock file matches nothing, so the caller resolves a new env.
    s, _, _ = storage
    keys = {"requirements_txt_content_hash": "cccc"}
    assert _default_lookup(_make_conda(s), keys, use_latest=":none:") is None


def test_not_lock_pinned_default_stays_local_only_under_none(storage):
    # Envs without a lock hash (@pypi, @conda) keep the local-only default.
    s, _, _ = storage
    assert _default_lookup(_make_conda(s), use_latest=":none:") is None


def test_lock_pinned_local_only_does_not_go_remote(storage):
    # Runtime lookups pass local_only=True with the lock keys and must stay local.
    s, _, _ = storage
    env = _default_lookup(_make_conda(s), KEYS_A, local_only=True, use_latest=":none:")
    assert env is None


@pytest.mark.xfail(
    strict=True,
    reason="Existing bug: a comma-separated user list never matches because "
    "conda.py compares the (EnvID, env) tuple to the usernames",
)
def test_lock_pinned_user_list_matches_resolver(storage):
    # CONDA_USE_REMOTE_LATEST="user1,user2" should accept envs resolved by those users.
    s, env_a, _ = storage
    env = _default_lookup(_make_conda(s), KEYS_A, use_latest="someone")
    assert env is not None
    assert env.env_id == env_a.env_id


def test_resolver_reuses_lock_pinned_env_on_fresh_host(storage):
    # A fresh host's resolver reuses the stored env: nothing to resolve or cache.
    s, env_a, _ = storage
    resolver = EnvsResolver(_make_conda(s))
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
