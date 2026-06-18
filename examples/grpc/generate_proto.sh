#!/usr/bin/env bash
# Run from examples/grpc/ to regenerate protobuf stubs.
# Requires: pip install grpcio-tools
set -euo pipefail

cd "$(dirname "$0")"

python -m grpc_tools.protoc \
  -I. \
  --python_out=. \
  --pyi_out=. \
  --grpc_python_out=. \
  predict.proto

echo "Generated: predict_pb2.py  predict_pb2.pyi  predict_pb2_grpc.py"
