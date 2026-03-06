"""
Cache abstraction for metaflow-fastdata.

Provides:
  - CacheBackend: abstract base class for cache backends
  - NullCache: default no-op cache (OSS default)
  - Cache: public API class (uses configured backend, defaults to NullCache)
  - CacheError, CacheNotFound: exception classes

Internal implementations (e.g. DGW KV) are registered via mfextinit overrides
in Netflix-internal packages.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

from .exceptions import CacheError, CacheNotFound


class CacheBackend(ABC):
    """Abstract base class for cache backends."""

    @abstractmethod
    def get(self, key: str) -> Any:
        """Retrieve a value by key. Raise CacheNotFound if not present."""
        ...

    @abstractmethod
    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Store a value with an optional TTL (seconds)."""
        ...

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete a key from the cache."""
        ...

    def close(self) -> None:
        """Optional cleanup hook."""
        pass


class NullCache(CacheBackend):
    """No-op cache backend — the OSS default.

    All gets miss; all puts/deletes are no-ops.
    """

    def get(self, key: str) -> Any:
        raise CacheNotFound("NullCache: key '%s' not found" % key)

    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        pass

    def delete(self, key: str) -> None:
        pass


def _get_default_backend() -> CacheBackend:
    """Return the configured cache backend instance."""
    try:
        from metaflow.metaflow_config import DEFAULT_CACHE_BACKEND  # type: ignore
        backend_name = DEFAULT_CACHE_BACKEND
    except (ImportError, AttributeError):
        backend_name = "null"

    if backend_name == "null" or backend_name is None:
        return NullCache()
    raise ValueError("Unknown cache backend: %s" % backend_name)


class Cache:
    """Public-facing cache API.

    Delegates to a CacheBackend instance.  The default backend is NullCache
    (a no-op).  Netflix-internal packages register DGWKVCache via mfextinit.

    Usage::

        cache = Cache()
        try:
            value = cache.get("my_key")
        except CacheNotFound:
            value = compute_value()
            cache.put("my_key", value, ttl=3600)
    """

    def __init__(self, backend: Optional[CacheBackend] = None):
        self._backend = backend or _get_default_backend()

    def get(self, key: str) -> Any:
        return self._backend.get(key)

    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        self._backend.put(key, value, ttl=ttl)

    def delete(self, key: str) -> None:
        self._backend.delete(key)

    def close(self) -> None:
        self._backend.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
