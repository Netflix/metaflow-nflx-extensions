"""
Unit tests for the CacheBackend ABC, NullCache, and Cache public class.
No external services required.
"""

import pytest


class TestNullCache:
    def test_get_raises_cache_not_found(self):
        from metaflow_extensions.fastdata_ext.plugins.datatools.cache import (
            NullCache,
            CacheError,
        )

        nc = NullCache()
        with pytest.raises(CacheError):
            nc.get("any_key")

    def test_put_is_noop(self):
        from metaflow_extensions.fastdata_ext.plugins.datatools.cache import NullCache

        nc = NullCache()
        nc.put("key", "value")  # should not raise

    def test_delete_is_noop(self):
        from metaflow_extensions.fastdata_ext.plugins.datatools.cache import NullCache

        nc = NullCache()
        nc.delete("key")  # should not raise

    def test_close_is_noop(self):
        from metaflow_extensions.fastdata_ext.plugins.datatools.cache import NullCache

        nc = NullCache()
        nc.close()  # should not raise


class TestCache:
    def test_default_backend_is_null_cache(self):
        from metaflow_extensions.fastdata_ext.plugins.datatools.cache import (
            Cache,
            NullCache,
        )

        c = Cache()
        assert isinstance(c._backend, NullCache)

    def test_custom_backend(self):
        from metaflow_extensions.fastdata_ext.plugins.datatools.cache import (
            Cache,
            CacheBackend,
            CacheError,
        )
        from unittest.mock import MagicMock

        backend = MagicMock(spec=CacheBackend)
        backend.get.return_value = "cached_value"
        c = Cache(backend=backend)
        assert c.get("key") == "cached_value"

    def test_context_manager(self):
        from metaflow_extensions.fastdata_ext.plugins.datatools.cache import Cache

        with Cache() as c:
            assert c is not None

    def test_get_miss_raises_cache_error(self):
        from metaflow_extensions.fastdata_ext.plugins.datatools.cache import (
            Cache,
            CacheError,
        )

        c = Cache()  # NullCache backend
        with pytest.raises(CacheError):
            c.get("missing_key")

    def test_put_and_get_with_real_backend(self):
        """Test with a simple in-memory backend."""
        from metaflow_extensions.fastdata_ext.plugins.datatools.cache import (
            Cache,
            CacheBackend,
            CacheError,
        )

        class InMemoryCache(CacheBackend):
            def __init__(self):
                self._store = {}

            def get(self, key):
                if key not in self._store:
                    raise CacheError("miss")
                return self._store[key]

            def put(self, key, value, ttl=None):
                self._store[key] = value

            def delete(self, key):
                self._store.pop(key, None)

        c = Cache(backend=InMemoryCache())
        c.put("k", 42)
        assert c.get("k") == 42
        c.delete("k")
        with pytest.raises(CacheError):
            c.get("k")


class TestCacheBackendABC:
    def test_cannot_instantiate_directly(self):
        from metaflow_extensions.fastdata_ext.plugins.datatools.cache import (
            CacheBackend,
        )

        with pytest.raises(TypeError):
            CacheBackend()

    def test_close_has_default_noop(self):
        from metaflow_extensions.fastdata_ext.plugins.datatools.cache import (
            CacheBackend,
            CacheError,
        )

        class MinimalCache(CacheBackend):
            def get(self, key):
                raise CacheError("miss")

            def put(self, key, value, ttl=None):
                pass

            def delete(self, key):
                pass

        mc = MinimalCache()
        mc.close()  # default no-op should not raise
