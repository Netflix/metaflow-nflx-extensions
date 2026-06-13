# Shared Training Flow

Trains a scikit-learn `LinearRegression` on synthetic 3-feature data and exports two relocatable Metaflow Functions:

| Artifact | Type | I/O | Used by |
|---|---|---|---|
| `json_predict_fn` | `JsonFunction` | `dict → dict` | FastAPI example |
| `avro_predict_fn` | `AvroFunction` | `str → str` | gRPC example |

---

## Prerequisites

- Metaflow configured with an S3 datastore (`METAFLOW_DEFAULT_DATASTORE=s3`)
- AWS credentials available
- conda/micromamba available (`METAFLOW_CONDA_DEPENDENCY_RESOLVER=micromamba`)
- `pip install metaflow metaflow-nflx-extensions`

---

## Run the flow

```bash
cd examples/shared
python training_flow.py run
```

Expected output (last lines):

```
JSON ref: s3://your-bucket/functions/.../metadata/ab/abc123.json
Avro ref: s3://your-bucket/functions/.../metadata/cd/cde456.json
```

---

## Get the references after the run

```bash
python -c "
from metaflow import Flow
run = Flow('PredictionTrainingFlow').latest_run
task = run['bind_functions'].task
print('JSON ref:', task.data.json_predict_fn.reference)
print('Avro ref:', task.data.avro_predict_fn.reference)
"
```

Set the reference as an env var for the serving examples:

```bash
export METAFLOW_FUNCTION_REFERENCE=$(python -c "
from metaflow import Flow
run = Flow('PredictionTrainingFlow').latest_run
print(Flow('PredictionTrainingFlow').latest_run['bind_functions'].task.data.json_predict_fn.reference)
")
```

---

## How it works

```
start (@conda)              bind_functions (@step)
  train sklearn model   →     JsonFunction(json_predict, task=start_task)
  self.coef = [...]           AvroFunction(avro_predict, task=start_task)
  self.intercept = 0.04         ↓
                              self.json_predict_fn.reference = "s3://..."
```

The `@conda` decorator on `start` is **required** — the step must have an environment for the function to be relocatable. `bind_functions` has no `@conda` because it only reads artifacts, not trains.
