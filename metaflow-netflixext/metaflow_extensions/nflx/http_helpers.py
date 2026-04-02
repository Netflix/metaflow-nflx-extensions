"""
HTTP utilities for Metaflow Netflix extensions.

This module provides utilities for making robust HTTP requests with proper
retry logic for transient failures.
"""

import requests
from urllib3 import Retry


# Standard retry configuration for HTTP requests
# Retries on connection errors and transient server errors (408, 425, 429, 5xx)
DEFAULT_STATUS_FORCELIST = [408, 425, 429, 500, 502, 503, 504]
DEFAULT_RETRY_TOTAL = 5
DEFAULT_RETRY_BACKOFF_FACTOR = 0.1


def create_http_adapter_with_retry(
    total=DEFAULT_RETRY_TOTAL,
    backoff_factor=DEFAULT_RETRY_BACKOFF_FACTOR,
    status_forcelist=None,
    pool_connections=10,
    pool_maxsize=10,
):
    """
    Create an HTTPAdapter with retry logic for transient failures.

    This adapter will automatically retry requests on:
    - Connection errors (connection refused, connection reset, etc.)
    - Read timeouts
    - HTTP status codes in status_forcelist (default: 408, 425, 429, 500, 502, 503, 504)

    Args:
        total: Total number of retry attempts (default: 5)
        backoff_factor: Backoff factor for exponential backoff between retries (default: 0.1)
                       Retry delays: {backoff_factor} * (2 ^ (retry_number - 1)) seconds
        status_forcelist: List of HTTP status codes to retry on (default: [408, 425, 429, 500, 502, 503, 504])
        pool_connections: Number of connection pools to cache (default: 10)
        pool_maxsize: Maximum number of connections to save in the pool (default: 10)

    Returns:
        requests.adapters.HTTPAdapter configured with retry logic

    Example:
        >>> session = requests.Session()
        >>> adapter = create_http_adapter_with_retry()
        >>> session.mount("https://", adapter)
        >>> session.mount("http://", adapter)
        >>> response = session.get("https://api.example.com/data")
    """
    if status_forcelist is None:
        status_forcelist = DEFAULT_STATUS_FORCELIST

    retry = Retry(
        total=total,
        read=total,  # Retry on read timeouts
        connect=total,  # Retry on connection timeouts
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )

    adapter = requests.adapters.HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=retry,
    )

    return adapter


def create_session_with_retry(
    total=DEFAULT_RETRY_TOTAL,
    backoff_factor=DEFAULT_RETRY_BACKOFF_FACTOR,
    status_forcelist=None,
    pool_connections=10,
    pool_maxsize=10,
):
    """
    Create a requests.Session with retry logic pre-configured.

    This is a convenience function that creates a session and mounts
    the retry adapter for both HTTP and HTTPS.

    Args:
        total: Total number of retry attempts (default: 5)
        backoff_factor: Backoff factor for exponential backoff (default: 0.1)
        status_forcelist: List of HTTP status codes to retry on (default: [408, 425, 429, 500, 502, 503, 504])
        pool_connections: Number of connection pools to cache (default: 10)
        pool_maxsize: Maximum number of connections in the pool (default: 10)

    Returns:
        requests.Session configured with retry logic

    Example:
        >>> session = create_session_with_retry()
        >>> response = session.get("https://api.example.com/data")
    """
    session = requests.Session()
    adapter = create_http_adapter_with_retry(
        total=total,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
