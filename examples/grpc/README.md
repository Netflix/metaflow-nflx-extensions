# gRPC Serving Example

Serves a Metaflow Function as a gRPC service. The function loads **once** when the servicer starts. Uses `grpc.aio` + `fn.call_async()` — inference runs cooperatively in the asyncio event loop without blocking other RPCs.

Includes both **unary** (`Predict`) and **bidirectional streaming** (`PredictStream`) RPCs.

---

## Prerequisites

1. Run the training flow first — see `examples/shared/README.md`
2. Have the **Avro** function reference (S3 path) from that run

```bash
pip install grpcio grpcio-tools httpx
```

---

## Step 1 — Generate protobuf stubs (one-time)

```bash
cd examples/grpc
bash generate_proto.sh
# Produces: predict_pb2.py  predict_pb2.pyi  predict_pb2_grpc.py
```

Commit these generated files. Re-run the script only when `predict.proto` changes.

---

## Step 2 — Get the Avro function reference

```bash
cd examples/shared

export METAFLOW_FUNCTION_REFERENCE=$(python3 -c "
from metaflow import Flow
run = Flow('PredictionTrainingFlow').latest_run
print(run['bind_functions'].task.data.avro_predict_fn.reference)
")
echo "Avro ref: $METAFLOW_FUNCTION_REFERENCE"
```

---

## Step 3 — Start the server

```bash
cd examples/grpc

export METAFLOW_DEFAULT_DATASTORE=s3
export AWS_ACCESS_KEY_ID=minioadmin       # or real creds
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:9000  # remove for real AWS

python server.py
```

Expected output:

```
INFO:__main__:Function loaded from: s3://metaflow-test/functions/...
INFO:__main__:gRPC server listening on [::]:50051
```

---

## Step 4 — Run the client

```bash
# Unary RPCs (one at a time)
python client.py

# Unary + streaming RPC
python client.py localhost:50051 --stream
```

Expected output:

```
--- Unary RPCs ---
  features=[1.0, 2.0, 3.0]   ->  prediction=4.4712
  features=[0.0, 0.0, 0.0]   ->  prediction=0.0401
  features=[-1.0, 0.5, 2.0]  ->  prediction=-0.9823
  features=[5.0, -3.0, 1.5]  ->  prediction=-0.3021
--- Streaming RPC ---
  response[0]: prediction=4.4712
  ...
```

---

## Service definition

```protobuf
service Predictor {
  rpc Predict       (PredictRequest)        returns (PredictResponse);
  rpc PredictStream (stream PredictRequest) returns (stream PredictResponse);
}

message PredictRequest  { repeated float features   = 1; }
message PredictResponse { float prediction = 1; string error = 2; }
```

---

## Configuration

| Env var | Default | Description |
|---|---|---|
| `METAFLOW_FUNCTION_REFERENCE` | required | S3 or local path to avro function JSON spec |
| `GRPC_PORT` | `50051` | Port the server listens on |
| `NUM_WORKERS` | `4` | (unused by aio server; kept for parity) |

---

## How it works

```
python server.py
  └─ PredictorServicer.__init__()
       └─ function_from_json(reference, use_proxy=True, backend="memory")
            └─ downloads spec JSON from S3
            └─ starts memory backend subprocess in the correct conda env

gRPC Predict RPC arrives
  └─ servicer.Predict(request, context)
       └─ payload = json.dumps({"features": [...]})
       └─ result_json = await fn.call_async(payload)
            └─ cooperative yield via asyncio.sleep(0) — other RPCs can run
            └─ IPC to subprocess → avro_predict(data, params)
       └─ PredictResponse(prediction=float)

SIGTERM / SIGINT
  └─ server.stop(grace=5)   # drain in-flight RPCs
  └─ close_function(fn)     # terminate subprocess + clean temp dirs
```

---

## Adding TLS (production)

Replace `add_insecure_port` with:

```python
with open("server.key", "rb") as f: private_key = f.read()
with open("server.crt", "rb") as f: cert_chain  = f.read()
creds = grpc.ssl_server_credentials([(private_key, cert_chain)])
server.add_secure_port("[::]:50051", creds)
```

---

## Swapping the model

Re-run the training flow, update `METAFLOW_FUNCTION_REFERENCE` to the new Avro reference, restart the server. No code changes.
