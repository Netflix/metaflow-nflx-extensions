import json
import os

import pytest
import requests
import metaflow  # noqa: F401 -- initialize plugins before importing utilities

from metaflow.exception import MetaflowException
from metaflow_extensions.netflixext.plugins.conda import pypi_conda_mapping as mapping


_RECORDS = {
    "graphviz": {"conda_name": "python-graphviz"},
    "Num_Py": {"conda_name": "Num_Py"},
    "legacy": {"conda_forge": "python-legacy"},
}
_EXPECTED = {
    "graphviz": "python-graphviz",
    "num-py": "num-py",
    "legacy": "python-legacy",
}


@pytest.fixture(autouse=True)
def isolated_mapping_cache(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    mapping.get_pypi_to_conda_mapping.cache_clear()
    yield
    mapping.get_pypi_to_conda_mapping.cache_clear()


@pytest.fixture
def download(mocker):
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps(_RECORDS).encode()
    return mocker.patch.object(mapping.requests, "get", return_value=response)


def _write_cache(tmp_path, content=None, stale=False):
    cache_path = tmp_path / "metaflow" / "pypi-conda-mapping.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(
        content if content is not None else json.dumps(_RECORDS).encode()
    )
    if stale:
        os.utime(cache_path, (1, 1))
    return cache_path


def test_mapping_download_is_normalized_and_persisted(tmp_path, download):
    assert mapping.get_pypi_to_conda_mapping() == _EXPECTED
    download.assert_called_once_with(mapping._MAPPING_URL, timeout=(3.05, 10))
    cache_path = tmp_path / "metaflow" / "pypi-conda-mapping.json"
    assert json.loads(cache_path.read_bytes()) == _RECORDS


def test_fresh_disk_cache_does_not_require_network(tmp_path, download):
    _write_cache(tmp_path)
    assert mapping.get_pypi_to_conda_mapping() == _EXPECTED
    download.assert_not_called()


def test_loaded_mapping_is_reused_in_process(tmp_path, download):
    mapping.get_pypi_to_conda_mapping()
    _write_cache(tmp_path, stale=True)
    assert mapping.get_pypi_to_conda_mapping() == _EXPECTED
    assert download.call_count == 1


def test_expired_cache_is_refreshed(tmp_path, download):
    cache_path = _write_cache(tmp_path, b'{"old": {"conda_name": "old"}}', stale=True)
    assert mapping.get_pypi_to_conda_mapping() == _EXPECTED
    assert json.loads(cache_path.read_bytes()) == _RECORDS
    download.assert_called_once()


@pytest.mark.parametrize(
    "error", [requests.Timeout("timed out"), requests.HTTPError("404")]
)
def test_refresh_failure_preserves_and_uses_stale_cache(tmp_path, download, error):
    cache_path = _write_cache(tmp_path, stale=True)
    original = cache_path.read_bytes()
    if isinstance(error, requests.HTTPError):
        download.return_value.status_code = 404
    else:
        download.side_effect = error
    assert mapping.get_pypi_to_conda_mapping() == _EXPECTED
    assert cache_path.read_bytes() == original
    download.assert_called_once()


@pytest.mark.parametrize("cached_content", [None, b"invalid JSON"])
def test_download_failure_without_valid_cache_reports_recovery(
    tmp_path, download, cached_content
):
    if cached_content is not None:
        _write_cache(tmp_path, cached_content)
    download.side_effect = requests.ConnectionError("unavailable")
    with pytest.raises(
        MetaflowException, match="Check network access and retry"
    ) as error:
        mapping.get_pypi_to_conda_mapping()
    assert mapping._MAPPING_URL in str(error.value)
    assert str(tmp_path) in str(error.value)


@pytest.mark.parametrize(
    "content",
    [
        b"invalid JSON",
        b"[]",
        b"{}",
        b'{"graphviz": null}',
        b'{"graphviz": {"conda_name": 1}}',
    ],
)
def test_invalid_download_does_not_overwrite_stale_cache(tmp_path, download, content):
    cache_path = _write_cache(tmp_path, stale=True)
    original = cache_path.read_bytes()
    download.return_value._content = content
    assert mapping.get_pypi_to_conda_mapping() == _EXPECTED
    assert cache_path.read_bytes() == original


def test_invalid_download_without_cache_fails_validation(download):
    download.return_value._content = b"{}"
    with pytest.raises(MetaflowException, match="no valid local cache"):
        mapping.get_pypi_to_conda_mapping()


def test_corrupt_cache_is_replaced_by_valid_download(tmp_path, download):
    cache_path = _write_cache(tmp_path, b"invalid JSON")
    assert mapping.get_pypi_to_conda_mapping() == _EXPECTED
    assert json.loads(cache_path.read_bytes()) == _RECORDS


def test_cache_write_failure_preserves_old_file_and_allows_validation(
    tmp_path, download, mocker
):
    cache_path = _write_cache(tmp_path, b'{"old": {"conda_name": "old"}}', stale=True)
    original = cache_path.read_bytes()
    mocker.patch.object(mapping.os, "replace", side_effect=OSError("read-only cache"))
    assert mapping.get_pypi_to_conda_mapping() == _EXPECTED
    assert cache_path.read_bytes() == original
    assert list(cache_path.parent.iterdir()) == [cache_path]
