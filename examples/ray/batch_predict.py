"""
Ray backend example — concurrent batch inference with Metaflow Functions.

Loads a function once, then fans out N predictions in parallel across Ray actors.
This is the unique capability Ray adds: the same actor handles multiple requests
concurrently without spinning up N separate conda subprocesses.

Required env vars:
    METAFLOW_FUNCTION_REFERENCE   — S3 path to the json function spec
    METAFLOW_DEFAULT_DATASTORE    — set to "s3" for S3-backed functions
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION

Optional:
    METAFLOW_FUNCTION_RAY_ADDRESS         — connect to existing cluster (default: local)
    METAFLOW_FUNCTION_RAY_OBJECT_STORE_MEMORY — object store bytes (default: 256MB)

Run:
    METAFLOW_FUNCTION_REFERENCE="s3://..." python batch_predict.py
    METAFLOW_FUNCTION_REFERENCE="s3://..." python batch_predict.py --concurrent 20
"""

import argparse
import asyncio
import os
import time

from metaflow import function_from_json, close_function


def load_function():
    reference = os.environ.get("METAFLOW_FUNCTION_REFERENCE")
    if not reference:
        raise RuntimeError("METAFLOW_FUNCTION_REFERENCE env var is required")

    # backend="ray" — Ray actor pool; actor runs in the correct conda env
    # start_runtime=True — actor started immediately so first call has no cold-start
    fn = function_from_json(
        reference,
        start_runtime=True,
        use_proxy=True,
        backend="ray",
    )
    print(f"Function loaded: {reference}")
    return fn


# ---------------------------------------------------------------------------
# Sync batch — sequential baseline
# ---------------------------------------------------------------------------

def batch_predict_sync(fn, inputs: list) -> list:
    """Run N predictions sequentially. Baseline for comparison."""
    return [fn(item) for item in inputs]


# ---------------------------------------------------------------------------
# Async batch — concurrent via call_async
# ---------------------------------------------------------------------------

async def batch_predict_async(fn, inputs: list) -> list:
    """
    Fan out N predictions concurrently.

    fn.call_async(data) returns an awaitable that runs ray.get in a thread
    pool executor, so the event loop yields between calls and all N tasks
    make progress in parallel.
    """
    tasks = [asyncio.create_task(fn.call_async(item)) for item in inputs]
    return await asyncio.gather(*tasks)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Metaflow Functions Ray batch example")
    parser.add_argument(
        "--concurrent", type=int, default=10,
        help="Number of concurrent predictions to run (default: 10)"
    )
    parser.add_argument(
        "--compare-sync", action="store_true",
        help="Also run sequential baseline and print speedup"
    )
    args = parser.parse_args()

    fn = load_function()

    # Build N input payloads
    inputs = [
        {"features": [float(i), float(i % 3), float(i % 5)]}
        for i in range(args.concurrent)
    ]

    try:
        # --- async concurrent ---
        print(f"\nRunning {args.concurrent} predictions concurrently (Ray)...")
        t0 = time.perf_counter()
        results_async = asyncio.run(batch_predict_async(fn, inputs))
        elapsed_async = time.perf_counter() - t0

        for i, result in enumerate(results_async):
            print(f"  [{i:02d}] features={inputs[i]['features']}  "
                  f"->  prediction={result['prediction']:.4f}")
        print(f"Concurrent elapsed: {elapsed_async:.2f}s")

        # --- sync sequential (optional baseline) ---
        if args.compare_sync:
            print(f"\nRunning {args.concurrent} predictions sequentially (baseline)...")
            t0 = time.perf_counter()
            results_sync = batch_predict_sync(fn, inputs)
            elapsed_sync = time.perf_counter() - t0
            print(f"Sequential elapsed: {elapsed_sync:.2f}s")
            print(f"Speedup: {elapsed_sync / elapsed_async:.1f}x")

    finally:
        close_function(fn)


if __name__ == "__main__":
    main()
