"""
Regression tests for credential stripping at the actual call sites.

These tests verify that cache path construction and package URL normalization
do NOT embed credentials (user:token@host) even when the input URL contains them.
Unlike test_strip_url_credentials.py (which tests the helpers in isolation),
these tests exercise the real call sites — so a future refactor that stops
calling _safe_netloc would cause these tests to fail.
"""

import metaflow  # must be imported first to initialize the extension system

from metaflow_extensions.nflx.plugins.conda.env_descr import (
    CondaCachePackage,
    PypiCachePackage,
    PypiPackageSpecification,
)


class TestMakePartialCacheUrlStripsCredentials:
    """make_partial_cache_url must not embed credentials in the returned path."""

    def test_conda_cache_url_strips_user_and_token(self):
        url = "https://user:token@repo.anaconda.com/pkgs/main/linux-64/numpy-1.24.0-py39h12345678_0.conda"
        result = CondaCachePackage.make_partial_cache_url(url)
        assert "user" not in result
        assert "token" not in result
        assert "repo.anaconda.com" in result

    def test_pypi_cache_url_strips_user_and_token(self):
        url = "https://user:secret@private-pypi.example.com/packages/numpy-1.24.0-cp311-cp311-manylinux2014_x86_64.whl"
        result = PypiCachePackage.make_partial_cache_url(url)
        assert "user" not in result
        assert "secret" not in result
        assert "private-pypi.example.com" in result

    def test_conda_cache_url_preserves_host_and_path(self):
        url = "https://user:token@conda.example.com:8080/channel/linux-64/pkg-1.0-py39_0.tar.bz2"
        result = CondaCachePackage.make_partial_cache_url(url)
        assert "conda.example.com:8080" in result
        assert "channel/linux-64/pkg-1.0-py39_0.tar.bz2" in result

    def test_url_without_credentials_unchanged_behavior(self):
        url = "https://repo.anaconda.com/pkgs/main/linux-64/numpy-1.24.0-py39h12345678_0.conda"
        result = CondaCachePackage.make_partial_cache_url(url)
        assert "repo.anaconda.com" in result


class TestPackageSpecificationFakeurlStripsCredentials:
    """PackageSpecification.__init__ with is_real_url=False must not embed credentials in the FAKEURL."""

    def test_pypi_fakeurl_strips_credentials(self):
        spec = PypiPackageSpecification(
            filename="numpy-1.24.0-cp311-cp311-manylinux2014_x86_64",
            url="https://user:secret@private-pypi.example.com/packages/numpy-1.24.0-cp311-cp311-manylinux2014_x86_64.whl",
            is_real_url=False,
        )
        assert "user" not in spec._url, "credential 'user' found in URL: %s" % spec._url
        assert "secret" not in spec._url, "credential 'secret' found in URL: %s" % spec._url
        assert "private-pypi.example.com" in spec._url

    def test_pypi_fakeurl_real_url_path_passes_through(self):
        # Real URLs are not modified; credentials would still be present if provided.
        # This test just verifies is_real_url=True does not strip the FAKEURL_PATHCOMPONENT.
        spec = PypiPackageSpecification(
            filename="numpy-1.24.0-cp311-cp311-manylinux2014_x86_64",
            url="https://private-pypi.example.com/packages/numpy-1.24.0-cp311-cp311-manylinux2014_x86_64.whl",
            is_real_url=True,
        )
        assert "_fake" not in spec._url
        assert "private-pypi.example.com" in spec._url
