"""
gRPC server that serves a relocatable Metaflow Function.

The function is loaded ONCE when the servicer is instantiated.
Uses grpc.aio (asyncio) and fn.call_async() so inference doesn't block the event loop.

Required env vars:
    METAFLOW_FUNCTION_REFERENCE   — S3 (or local) path to the avro function .json spec
    METAFLOW_DEFAULT_DATASTORE    — set to "s3" for S3-backed functions
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION  — if S3

Run:
    METAFLOW_FUNCTION_REFERENCE="s3://..." python server.py

Generate protobuf stubs first (one-time):
    pip install grpcio-tools
    bash generate_proto.sh
"""

import asyncio
import json
import logging
import os
import signal

import grpc
import predict_pb2
import predict_pb2_grpc

from metaflow import function_from_json, close_function

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

_DEFAULT_PORT = int(os.getenv("GRPC_PORT", "50051"))
_MAX_WORKERS = int(os.getenv("NUM_WORKERS", "2"))


class PredictorServicer(predict_pb2_grpc.PredictorServicer):
    def __init__(self):
        reference = os.environ.get("METAFLOW_FUNCTION_REFERENCE")
        if not reference:
            raise RuntimeError("METAFLOW_FUNCTION_REFERENCE env var is required")

        # use_proxy=True  — this process never imports user code or their deps
        # backend="memory" — subprocess runs in the correct conda environment
        # start_runtime=True — subprocess started now; no cold-start on first RPC
        self.fn = function_from_json(
            reference,
            start_runtime=True,
            use_proxy=True,
            backend="memory",
        )
        log.info("Function loaded from: %s", reference)

    def close(self):
        close_function(self.fn)
        log.info("Function closed")

    async def Predict(
        self,
        request: predict_pb2.PredictRequest,
        context: grpc.aio.ServicerContext,
    ) -> predict_pb2.PredictResponse:
        """Single prediction RPC."""
        try:
            # avro_predict takes JSON-encoded string, returns JSON-encoded string
            payload = json.dumps({"features": list(request.features)})
            result_json = await self.fn.call_async(payload)
            result = json.loads(result_json)
            return predict_pb2.PredictResponse(prediction=float(result["prediction"]))
        except Exception as exc:
            log.exception("Predict failed")
            await context.abort(grpc.StatusCode.INTERNAL, str(exc))

    async def PredictStream(
        self,
        request_iterator,
        context: grpc.aio.ServicerContext,
    ):
        """Bidirectional streaming RPC — fan-out concurrent predictions."""
        sem = asyncio.Semaphore(_MAX_WORKERS)

        async def _call_one(req):
            async with sem:
                payload = json.dumps({"features": list(req.features)})
                result_json = await self.fn.call_async(payload)
                result = json.loads(result_json)
                return predict_pb2.PredictResponse(prediction=float(result["prediction"]))

        tasks = []
        async for req in request_iterator:
            tasks.append(asyncio.create_task(_call_one(req)))

        for task in asyncio.as_completed(tasks):
            try:
                yield await task
            except Exception as exc:
                yield predict_pb2.PredictResponse(error=str(exc))


async def serve():
    servicer = PredictorServicer()

    server = grpc.aio.server()
    predict_pb2_grpc.add_PredictorServicer_to_server(servicer, server)

    listen_addr = f"[::]:{_DEFAULT_PORT}"
    server.add_insecure_port(listen_addr)

    await server.start()
    log.info("gRPC server listening on %s", listen_addr)

    # Graceful shutdown on SIGTERM / SIGINT
    loop = asyncio.get_running_loop()

    async def _shutdown():
        log.info("Shutting down...")
        await server.stop(grace=5)
        servicer.close()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(_shutdown()))

    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
