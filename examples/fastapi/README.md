# FastAPI Serving Example

Serves a Metaflow Function as a REST API. The function loads **once** at startup (no per-request reload). Inference runs in a thread pool to avoid blocking the async event loop.

---

## Prerequisites

1. Run the training flow first — see `examples/shared/README.md`
2. Have the JSON function reference (S3 path) from that run

```bash
pip install fastapi uvicorn httpx
```

---

## Start the server

```bash
cd examples/fastapi

export METAFLOW_FUNCTION_REFERENCE="s3://your-bucket/functions/.../uuid.json"
export METAFLOW_DEFAULT_DATASTORE=s3   # needed for S3 spec download
export AWS_DEFAULT_REGION=us-east-1

# We use port 8080 to avoid conflicts with macOS Control Center/AirPlay which often silently occupies port 8000
uvicorn server:app --host 0.0.0.0 --port 8080
```

Expected startup output:

```
Function loaded from: s3://your-bucket/functions/.../uuid.json
INFO:     Application startup complete.
```

---

## Call the server

```bash
# With the client script (ensure you pass the correct port URL)
python client.py http://localhost:8080

# Or with curl
curl -s -X POST http://localhost:8080/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [1.0, 2.0, 3.0]}' | python -m json.tool
```

Expected response:

```json
{
  "prediction": 4.6123,
  "n_features": 3
}
```

---

## API

| Endpoint | Method | Request | Response |
|---|---|---|---|
| `/healthz` | GET | — | `{"status": "ok"}` |
| `/predict` | POST | `{"features": [f1, f2, f3]}` | `{"prediction": float, "n_features": int}` |

---

## Configuration

| Env var | Default | Description |
|---|---|---|
| `METAFLOW_FUNCTION_REFERENCE` | required | S3 or local path to function JSON spec |
| `NUM_WORKERS` | `4` | Thread pool size for concurrent inference |
| `METAFLOW_DEFAULT_DATASTORE` | — | Set to `s3` for S3-backed functions |

---

## How it works

```
uvicorn starts
  └─ lifespan() runs
       └─ function_from_json(reference, use_proxy=True, backend="memory")
            └─ downloads spec JSON from S3
            └─ starts memory backend subprocess in the correct conda env
            └─ subprocess extracts code ZIP, loads user function

POST /predict {"features": [...]}
  └─ run_in_executor(fn, {"features": [...]})   # offload CPU to thread pool
       └─ IPC to subprocess → json_predict(data, params)
            └─ params.coef, params.intercept read from task artifacts
       └─ returns {"prediction": float, "n_features": int}

server shutdown
  └─ close_function(fn)   # terminates subprocess + cleans temp dirs
```

## Swapping to a different trained model

Re-run the training flow, get the new reference, restart the server with the new `METAFLOW_FUNCTION_REFERENCE`. No code changes needed.