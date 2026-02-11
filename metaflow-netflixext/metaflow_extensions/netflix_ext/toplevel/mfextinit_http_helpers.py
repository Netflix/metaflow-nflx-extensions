"""HTTP helpers for Netflix extensions."""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def create_http_adapter_with_retry(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 503, 504),
    **kwargs
):
    """
    Create an HTTP adapter with retry logic.

    Parameters
    ----------
    retries : int
        Number of retries
    backoff_factor : float
        Backoff factor for retries
    status_forcelist : tuple
        HTTP status codes to retry on
    **kwargs : dict
        Additional keyword arguments for the adapter

    Returns
    -------
    HTTPAdapter
        HTTP adapter with retry logic
    """
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        **kwargs
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    return adapter
