"""
gRPC test client for the Metaflow Function server.

Usage:
    python client.py                     # default localhost:50051
    python client.py my-host:50051       # custom address
    python client.py localhost:50051 --stream   # test streaming RPC
"""

import asyncio
import sys

import grpc
import predict_pb2
import predict_pb2_grpc


ADDRESS = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
USE_STREAM = "--stream" in sys.argv

TEST_CASES = [
    [1.0, 2.0, 3.0],
    [0.0, 0.0, 0.0],
    [-1.0, 0.5, 2.0],
    [5.0, -3.0, 1.5],
]


async def run_unary(stub: predict_pb2_grpc.PredictorStub):
    """Single Predict RPC for each test case."""

    for features in TEST_CASES:
        resp = await stub.Predict(
            predict_pb2.PredictRequest(features=features)
        )
        if resp.error:
            print(f"  ERROR: {resp.error}")
        else:
            print(f"  features={features}  ->  prediction={resp.prediction:.4f}")


async def run_streaming(stub: predict_pb2_grpc.PredictorStub):
    """Bidirectional streaming: send all requests, collect all responses."""

    async def request_generator():
        for features in TEST_CASES:
            yield predict_pb2.PredictRequest(features=features)

    responses = []
    async for resp in stub.PredictStream(request_generator()):
        if resp.error:
            print(f"  ERROR: {resp.error}")
        else:
            responses.append(resp.prediction)

    for i, pred in enumerate(responses):
        print(f"  response[{i}]: prediction={pred:.4f}")


async def main():
    async with grpc.aio.insecure_channel(ADDRESS) as channel:
        stub = predict_pb2_grpc.PredictorStub(channel)

        await run_unary(stub)

        if USE_STREAM:
            await run_streaming(stub)


if __name__ == "__main__":
    asyncio.run(main())
