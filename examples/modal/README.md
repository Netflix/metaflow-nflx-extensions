# Modal Serverless Example

Deploys a Metaflow Function as a serverless HTTPS endpoint on Modal. The function loads **once** per container in `@modal.enter()` and is called in-process — Modal's container provides the env isolation instead of an inner conda subprocess.

---

## Prerequisites

1. Run the training flow — see `examples/shared/README.md`
2. Modal account: https://modal.com (free tier available)

```bash
pip install modal httpx
modal setup    # authenticate once
```

---

## Step 1 — Create a Modal Secret with AWS + function reference

```bash
# Get the JSON function reference from your training run
cd examples/shared

export REF=$(python3 -c "
from metaflow import Flow
run = Flow('PredictionTrainingFlow').latest_run
print(run['bind_functions'].task.data.json_predict_fn.reference)
")
echo $REF

modal secret create metaflow-aws \
    AWS_ACCESS_KEY_ID=minioadmin \
    AWS_SECRET_ACCESS_KEY=minioadmin \
    AWS_DEFAULT_REGION=us-east-1 \
    AWS_ENDPOINT_URL=http://host.docker.internal:9000 \
    METAFLOW_DEFAULT_DATASTORE=s3 \
    METAFLOW_FUNCTION_RUNTIME_PATH=/tmp/mf_functions \
    METAFLOW_FUNCTION_REFERENCE="$REF"
```

> **MinIO note:** Modal containers can't reach `localhost:9000` on your machine.
> Use `host.docker.internal:9000` (Mac/Windows) or your machine's LAN IP.
> For real AWS, omit `AWS_ENDPOINT_URL` and use real credentials.

---

## Step 2 — Deploy

```bash
cd examples/modal

# Dev mode — ephemeral URL, live reload, logs in terminal
modal serve serve.py

# Production — stable HTTPS URL, survives restarts
modal deploy serve.py
```

Modal prints the endpoint URL on deploy:
```
✓ Created web endpoint https://your-org--metaflow-functions-demo-...modal.run
```

---

## Step 3 — Call the endpoint

```bash
# With client script
python client.py https://your-org--metaflow-functions-demo-....modal.run

# With curl
curl -X POST https://your-org--metaflow-functions-demo-....modal.run/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [1.0, 2.0, 3.0]}'
```

Expected response:
```json
{"prediction": 4.4712, "n_features": 3}
```

---

## GPU workloads

To target a GPU, uncomment in `serve.py`:

```python
@app.cls(
    image=image,
    secrets=[...],
    gpu="T4",         # or "A10G", "A100", "H100"
    min_containers=1, # keep warm — GPU cold start is expensive
    ...
)
```

The function's user code runs in-process in the GPU container. Add GPU packages (e.g. `torch`) to the `image` definition.

---

## Keep-warm vs cold start

| `min_containers` | Behavior | Cost |
|---|---|---|
| `0` (default) | Cold starts (~5–15s first request) | Pay only when called |
| `1` | Always warm, instant responses | Continuous container billing |

---

## How it differs from FastAPI / gRPC

| | FastAPI / gRPC | Modal |
|---|---|---|
| Backend | `memory` (conda subprocess) | `local` (in-process) |
| Env isolation | conda env in subprocess | Modal container image |
| `use_proxy` | `True` | `False` (load immediately at `@modal.enter()`) |
| Scaling | manual (uvicorn workers) | automatic (Modal autoscales) |
| Infrastructure | you manage the server | Modal manages everything |

The same `.reference` artifact works across all three — only the backend and deployment target change.

---

## Swapping the model

Re-run the training flow, update `METAFLOW_FUNCTION_REFERENCE` in the Modal Secret:

```bash
modal secret create metaflow-aws --force \
    ... \
    METAFLOW_FUNCTION_REFERENCE="s3://new-reference.json"
```

Then `modal deploy` again. No code changes.
