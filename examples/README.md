# Metaflow Functions — Deployment Examples

These examples show end-to-end usage of `metaflow-functions`: train a model in a Metaflow flow, export the function, then serve it from an external system.

---

## Concept

```
Metaflow Flow (training)          External Server (serving)
─────────────────────────         ─────────────────────────
@conda step                       function_from_json(reference)
  train model                       └─ downloads spec JSON
  self.coef = [...]                 └─ starts subprocess in
  self.intercept = 0.04                 correct conda env
      ↓
  bind_functions step           POST /predict {"features": [...]}
    JsonFunction(fn, task)  →     └─ fn({"features": [...]})
    fn.reference = "s3://..."          └─ {"prediction": 4.61}
```

The server needs **no knowledge** of the model code, dependencies, or parameters — all of that is captured in the function reference.

---

## Step 0: Run the training flow

All examples share the same training flow.

```bash
cd examples/shared
python training_flow.py run
```

Then export the reference:

```bash
export METAFLOW_FUNCTION_REFERENCE=$(python -c "
from metaflow import Flow
print(Flow('PredictionTrainingFlow').latest_run['bind_functions'].task.data.json_predict_fn.reference)
")
```

See [shared/README.md](shared/README.md) for details.

---

## Step 1: Pick a serving target

| Example | Protocol | Best for |
|---|---|---|
| [fastapi/](fastapi/) | REST (HTTP/JSON) | Web APIs, microservices |
| [grpc/](grpc/) | gRPC (protobuf) | Low-latency, typed RPC |
| [modal/](modal/) | HTTPS (serverless) | GPU workloads, no infra |

---

## Which backend to use

| Backend | When |
|---|---|
| `memory` (default) | Always. Subprocess runs in the correct conda env. |
| `local` | Testing only — runs in-process, no env isolation. |
| `ray` | Existing Ray cluster, distributed fan-out. |

Set via `METAFLOW_FUNCTION_BACKEND` env var or `backend=` param on `function_from_json`.

---

## Prerequisites (all examples)

- Metaflow configured with S3 datastore (`METAFLOW_DEFAULT_DATASTORE=s3`)
- AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`)
- conda/micromamba for environment isolation (`METAFLOW_CONDA_DEPENDENCY_RESOLVER=micromamba`)
- Python >= 3.10

```bash
pip install metaflow metaflow-nflx-extensions
```
