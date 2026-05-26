"""
Test client for the FastAPI Metaflow Function server.

Usage:
    python client.py                          # default localhost:8000
    python client.py http://my-host:8000      # custom URL
"""

import sys
import httpx

BASE_URL = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8000"


def check_health():
    resp = httpx.get(f"{BASE_URL}/healthz", timeout=10)
    resp.raise_for_status()
    print("Health:", resp.json())


def predict(features: list[float]) -> dict:
    resp = httpx.post(
        f"{BASE_URL}/predict",
        json={"features": features},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    check_health()

    test_cases = [
        [1.0, 2.0, 3.0],
        [0.0, 0.0, 0.0],
        [-1.0, 0.5, 2.0],
    ]

    for features in test_cases:
        result = predict(features)
        print(f"features={features}  ->  prediction={result['prediction']:.4f}")
