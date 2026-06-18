"""
Test client for the Modal Metaflow Function endpoint.

Usage:
    python client.py <modal-endpoint-url>

The URL is printed by `modal serve` or `modal deploy`.
Example:
    python client.py https://your-org--metaflow-functions-demo-functionserver-predict.modal.run
"""

import sys
import httpx

if len(sys.argv) < 2:
    print("Usage: python client.py <modal-endpoint-url>")
    sys.exit(1)

URL = sys.argv[1].rstrip("/")

TEST_CASES = [
    [1.0, 2.0, 3.0],
    [0.0, 0.0, 0.0],
    [-1.0, 0.5, 2.0],
]

print(f"Hitting: {URL}")
for features in TEST_CASES:
    resp = httpx.post(URL, json={"features": features}, timeout=60)
    resp.raise_for_status()
    result = resp.json()
    if "error" in result:
        print(f"  ERROR: {result['error']}")
    else:
        print(f"  features={features}  ->  prediction={result['prediction']:.4f}")
