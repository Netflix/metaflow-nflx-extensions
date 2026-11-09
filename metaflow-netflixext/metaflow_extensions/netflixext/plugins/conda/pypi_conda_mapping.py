"""Resolve Python distribution names using conda-forge's upstream mapping."""

import json
import os
import tempfile
import time
from functools import lru_cache
from pathlib import Path
from typing import Dict

import requests

from metaflow._vendor.packaging.utils import canonicalize_name
from metaflow.debug import debug
from metaflow.exception import MetaflowException


_MAPPING_URL = (
    "https://raw.githubusercontent.com/conda-forge/conda-forge-bot-data/"
    "main/mappings/pypi/grayskull_pypi_mapping.json"
)
_CACHE_MAX_AGE = 24 * 60 * 60


def _parse_mapping(content: bytes) -> Dict[str, str]:
    records = json.loads(content)
    if not isinstance(records, dict) or not records:
        raise ValueError("Expected a nonempty PyPI-to-Conda mapping")
    mapping = {}
    for name, record in records.items():
        if not isinstance(record, dict):
            raise ValueError("Invalid mapping entry for %s" % name)
        conda_name = record.get("conda_name") or record.get("conda_forge")
        if not isinstance(conda_name, str) or not conda_name.strip():
            raise ValueError("Missing Conda name for %s" % name)
        mapping[canonicalize_name(name)] = canonicalize_name(conda_name)
    return mapping


@lru_cache(maxsize=1)
def get_pypi_to_conda_mapping() -> Dict[str, str]:
    """Load once per process, refreshing the disk cache at most daily.

    Keep the last valid mapping indefinitely for use when a refresh fails. This
    cache is independent of conda-lock, which need not be installed or imported.
    """
    cache_root = Path(os.environ.get("XDG_CACHE_HOME", "~/.cache")).expanduser()
    cache_path = cache_root / "metaflow" / "pypi-conda-mapping.json"
    cached = None
    try:
        cached = _parse_mapping(cache_path.read_bytes())
        age = time.time() - cache_path.stat().st_mtime
        if 0 <= age < _CACHE_MAX_AGE:
            return cached
    except (OSError, ValueError) as e:
        debug.conda_exec("Cannot read cached PyPI-to-Conda mapping: %s" % e)

    try:
        response = requests.get(_MAPPING_URL, timeout=(3.05, 10))
        response.raise_for_status()
        content = response.content
        mapping = _parse_mapping(content)
    except (requests.RequestException, ValueError) as e:
        if cached is not None:
            debug.conda_exec(
                "Using cached PyPI-to-Conda mapping after refresh failed: %s" % e
            )
            return cached
        raise MetaflowException(
            "Cannot validate extension PyPI requirements against Conda packages: "
            "the PyPI-to-Conda name mapping could not be loaded from %s, and no "
            "valid local cache is available at %s. Check network access and retry. "
            "Original error: %s" % (_MAPPING_URL, cache_path, e)
        ) from e

    # A failed or interrupted write must not replace a valid cached mapping.
    temporary_path = None
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=cache_path.parent, delete=False) as f:
            temporary_path = f.name
            f.write(content)
        os.replace(temporary_path, cache_path)
    except OSError as e:
        # Successful validation does not require a writable cache directory.
        debug.conda_exec("Cannot cache PyPI-to-Conda mapping: %s" % e)
    finally:
        if temporary_path is not None:
            try:
                os.unlink(temporary_path)
            except FileNotFoundError:
                pass  # The successful atomic replacement already removed it.
            except OSError as e:
                debug.conda_exec("Cannot remove temporary mapping cache: %s" % e)
    return mapping
