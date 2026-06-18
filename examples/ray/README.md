# Ray Backend Example

Runs concurrent batch inference using the Ray backend. Ray actors handle multiple
predictions in parallel — the same actor stays warm across calls, unlike spawning
N separate conda subprocesses.

---

## What Ray adds vs FastAPI / gRPC

| | FastAPI / gRPC | Ray |
|---|---|---|
| Concurrency model | Thread pool (FastAPI) / asyncio (gRPC) | Ray actor pool |
| Env isolation | conda subprocess per function | Ray runtime_env per actor |
| Scaling | vertical (more threads/workers) | horizontal (Ray cluster) |
| Best for | serving one request at a time | fan-out batch inference |

---

## Prerequisites

1. Run the training flow — see `examples/shared/README.md`
2. Have the JSON function reference from that run

```bash
pip install ray
```

---

## Get the reference

```bash
cd examples/shared

export METAFLOW_FUNCTION_REFERENCE=$(python3 -c "
from metaflow import Flow
run = Flow('PredictionTrainingFlow').latest_run
print(run['bind_functions'].task.data.json_predict_fn.reference)
")

echo $METAFLOW_FUNCTION_REFERENCE
```

---

## Run batch inference

```bash
cd examples/ray

export METAFLOW_DEFAULT_DATASTORE=s3
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:9000   # remove for real AWS

# 10 concurrent predictions (default)
python batch_predict.py

# 50 concurrent, plus sequential baseline for speedup comparison
python batch_predict.py --concurrent 50 --compare-sync
```

Expected output:

```
Function loaded: s3://metaflow-test/functions/...
Running 10 predictions concurrently (Ray)...
  [00] features=[0.0, 0.0, 0.0]  ->  prediction=0.0401
  [01] features=[1.0, 1.0, 1.0]  ->  prediction=2.5401
  ...
Concurrent elapsed: 0.43s
```

---

## Resource allocation

Specify CPU/GPU/memory at bind time so Ray allocates correctly:

```python
# In training_flow.py bind_functions step:
self.json_predict_fn = JsonFunction(
    json_predict,
    task=start_task,
    resources={"num_cpus": 2, "num_gpus": 1},
)
```

The Ray actor for this function will request 2 CPUs and 1 GPU from the cluster.
Omitting `resources` lets Ray use defaults (1 CPU, no GPU).

---

## Connecting to an existing Ray cluster

```bash
export METAFLOW_FUNCTION_RAY_ADDRESS="ray://my-cluster-head:10001"
python batch_predict.py
```

Without `METAFLOW_FUNCTION_RAY_ADDRESS`, a local single-node cluster starts automatically.

---

## How it works

```
batch_predict.py
  └─ function_from_json(reference, backend="ray", start_runtime=True)
       └─ _get_or_create_actor():
            ├─ runtime_env={"conda": /path/to/envs/metaflow_abc}
            ├─ num_cpus / num_gpus from system_metadata["resources"]
            └─ FunctionActorClass.remote(reference)
                 └─ actor.__init__: extracts code ZIP, loads function in-process

asyncio.gather(*[fn.call_async(item) for item in inputs])
  └─ each call_async: actor.execute.remote(data)
       └─ await loop.run_in_executor(None, ray.get, result_ref)
            └─ event loop free while Ray task runs in actor
```
