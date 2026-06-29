"""
FastAPI server that serves a relocatable Metaflow Function.

The function is loaded ONCE at startup via the lifespan hook.
Inference is offloaded to a thread pool to avoid blocking the async event loop.

Required env vars:
    METAFLOW_FUNCTION_REFERENCE   — S3 (or local) path to the .json spec
    METAFLOW_DEFAULT_DATASTORE    — set to "s3" for S3-backed functions
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION  — if S3

Run:
    METAFLOW_FUNCTION_REFERENCE="s3://..." uvicorn server:app --port 8000
"""

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from metaflow import function_from_json, close_function

_executor = ThreadPoolExecutor(max_workers=int(os.getenv("NUM_WORKERS", "4")))
_state: dict[str, Any] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    reference = os.environ.get("METAFLOW_FUNCTION_REFERENCE")
    if not reference:
        raise RuntimeError("METAFLOW_FUNCTION_REFERENCE env var is required")

    # use_proxy=True  — parent process never imports user code or their deps
    # backend="memory" — subprocess runs in the correct conda environment
    # start_runtime=True — subprocess started immediately; first call won't cold-start
    _state["fn"] = function_from_json(
        reference,
        start_runtime=True,
        use_proxy=True,
        backend="memory",
    )
    print(f"Function loaded from: {reference}")

    yield

    close_function(_state.pop("fn"))
    _executor.shutdown(wait=False)


app = FastAPI(title="Metaflow Function Server", lifespan=lifespan)


class PredictRequest(BaseModel):
    features: list[float]


class PredictResponse(BaseModel):
    prediction: float
    n_features: int


@app.get("/healthz")
async def healthz():
    if "fn" not in _state:
        raise HTTPException(status_code=503, detail="Function not ready")
    return {"status": "ok"}


@app.post("/predict", response_model=PredictResponse)
async def predict(req: PredictRequest):
    fn = _state.get("fn")
    if fn is None:
        raise HTTPException(status_code=503, detail="Function not ready")

    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            _executor, fn, {"features": req.features}
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return result
