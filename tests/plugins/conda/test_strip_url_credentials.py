"""Tests for strip_url_credentials and safe_netloc helpers in utils.py."""

import sys
import os

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "metaflow-netflixext",
    ),
)

from urllib.parse import urlparse
from metaflow_extensions.nflx.plugins.conda.utils import (
    strip_url_credentials,
    _safe_netloc,
)


class TestStripUrlCredentials:
    def test_no_credentials(self):
        url = "https://pypi.org/simple/"
        assert strip_url_credentials(url) == url

    def test_user_and_password(self):
        url = "https://user:token@private-pypi.example.com/simple/"
        assert strip_url_credentials(url) == "https://private-pypi.example.com/simple/"

    def test_user_only(self):
        url = "https://user@private-pypi.example.com/simple/"
        assert strip_url_credentials(url) == "https://private-pypi.example.com/simple/"

    def test_preserves_port(self):
        url = "https://user:token@private-pypi.example.com:8080/simple/"
        assert (
            strip_url_credentials(url)
            == "https://private-pypi.example.com:8080/simple/"
        )

    def test_preserves_query_and_fragment(self):
        url = "https://user:pass@host.com/path?q=1#frag"
        assert strip_url_credentials(url) == "https://host.com/path?q=1#frag"

    def test_file_url_unchanged(self):
        url = "file:///tmp/packages/foo-1.0.tar.gz"
        assert strip_url_credentials(url) == url

    def test_empty_string(self):
        assert strip_url_credentials("") == ""

    def test_git_plus_https(self):
        url = "git+https://token:x-oauth@github.com/org/repo.git@abc123"
        result = strip_url_credentials(url)
        assert "token" not in result
        assert "x-oauth" not in result
        assert "github.com" in result


class TestSafeNetloc:
    def test_simple_host(self):
        parsed = urlparse("https://pypi.org/simple/")
        assert _safe_netloc(parsed) == "pypi.org"

    def test_host_with_port(self):
        parsed = urlparse("https://pypi.org:8080/simple/")
        assert _safe_netloc(parsed) == "pypi.org:8080"

    def test_credentials_stripped(self):
        parsed = urlparse("https://user:token@private.com/simple/")
        assert _safe_netloc(parsed) == "private.com"
        assert "user" not in _safe_netloc(parsed)
        assert "token" not in _safe_netloc(parsed)

    def test_credentials_with_port_stripped(self):
        parsed = urlparse("https://user:token@private.com:443/simple/")
        assert _safe_netloc(parsed) == "private.com:443"
