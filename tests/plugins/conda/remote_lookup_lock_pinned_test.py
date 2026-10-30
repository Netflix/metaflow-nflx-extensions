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


def test_lock_pinned_default_found_remotely_under_none(storage):
    s, env_a, _ = storage
    conda = _make_conda(s)
    env = conda.environment(
        EnvID(REQ_ID, "_default", ARCH),
        use_latest=":none:",
        full_id_unique_keys=KEYS_A,
    )
    assert env is not None
    assert env.env_id == env_a.env_id


def test_lock_pinned_picks_matching_keys_not_latest(storage):
    s, env_a, env_b = storage
    # env_b is the most recent but was resolved from a different lock
    assert env_b.resolved_on > env_a.resolved_on
    for use_latest in (":none:", ":any:"):
        conda = _make_conda(s)
        env = conda.environment(
            EnvID(REQ_ID, "_default", ARCH),
            use_latest=use_latest,
            full_id_unique_keys=KEYS_A,
        )
        assert env is not None
        assert env.env_id == env_a.env_id


def test_lock_pinned_no_matching_keys_returns_none(storage):
    s, _, _ = storage
    conda = _make_conda(s)
    env = conda.environment(
        EnvID(REQ_ID, "_default", ARCH),
        use_latest=":none:",
        full_id_unique_keys={"requirements_txt_content_hash": "cccc"},
    )
    assert env is None


def test_not_lock_pinned_default_stays_local_only_under_none(storage):
    s, _, _ = storage
    conda = _make_conda(s)
    env = conda.environment(EnvID(REQ_ID, "_default", ARCH), use_latest=":none:")
    assert env is None


def test_lock_pinned_local_only_does_not_go_remote(storage):
    s, _, _ = storage
    conda = _make_conda(s)
    env = conda.environment(
        EnvID(REQ_ID, "_default", ARCH),
        local_only=True,
        use_latest=":none:",
        full_id_unique_keys=KEYS_A,
    )
    assert env is None


@pytest.mark.parametrize(
    "keys, expect_found",
    [(KEYS_A, True), ({"requirements_txt_content_hash": "cccc"}, False), (None, False)],
)
def test_resolver_add_environment_fresh_host(storage, keys, expect_found):
    # Mirrors what a flow run does on a host with an empty local manifest (for
    # example a scheduled run in a fresh container): add the step's environment
    # to the resolver with the default use_latest (":none:").
    s, env_a, _ = storage
    conda = _make_conda(s)
    resolver = EnvsResolver(conda)
    resolver.add_environment(
        ARCH,
        DEPS,
        {},
        {},
        {"pylock_toml": ["/unused/pylock.toml"]} if keys else {},
        "start",
        use_latest=":none:",
        full_id_unique_keys=keys,
    )
    (req,) = resolver._requested_envs.values()
    if expect_found:
        # The lock-pinned environment is reused as is: nothing to resolve or cache
        # so the builder environment is never solved and the full_id is unchanged.
        assert req["resolved"] is not None
        assert req["resolved"].env_id == env_a.env_id
        assert not req["need_caching"]
        assert list(resolver.need_caching_environments()) == []
    else:
        assert req["resolved"] is None
        assert req["need_caching"]
