"""
Modal serverless deployment for a relocatable Metaflow Function.

The function is loaded ONCE per container in @modal.enter() using backend="local".
Modal's container already provides env isolation, so no inner conda subprocess
is needed — the function's user code runs in-process inside the container.

Setup (one-time):
    pip install modal
    modal setup                          # authenticate with Modal
    modal secret create metaflow-aws \
        AWS_ACCESS_KEY_ID=... \
        AWS_SECRET_ACCESS_KEY=... \
        AWS_DEFAULT_REGION=us-east-1 \
        METAFLOW_FUNCTION_REFERENCE="s3://..." \
        METAFLOW_DEFAULT_DATASTORE=s3 \
        METAFLOW_FUNCTION_RUNTIME_PATH=/tmp/mf_functions

Deploy:
    modal serve examples/modal/serve.py   # ephemeral (dev mode, live reload)
    modal deploy examples/modal/serve.py  # production (stable HTTPS URL)

Call:
    curl -X POST <modal-url>/predict \
        -H "Content-Type: application/json" \
        -d '{"features": [1.0, 2.0, 3.0]}'
"""

import modal

app = modal.App("metaflow-functions-demo")

# ---------------------------------------------------------------------------
# Container image
# All packages the predict_fn needs must be declared here.
# modal handles building and caching this image.
# ---------------------------------------------------------------------------
image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("git") 
    .pip_install(
        "metaflow",
        "scikit-learn>=1.4.0",
        "numpy>=1.26.0",
        "fastavro",
        "psutil",
    )
    .pip_install(
        "git+https://github.com/Netflix/metaflow-nflx-extensions.git#subdirectory=metaflow_extensions"
    )
)


# ---------------------------------------------------------------------------
# Serving class
# ---------------------------------------------------------------------------
@app.cls(
    image=image,
    secrets=[modal.Secret.from_name("metaflow-aws")],
    # Uncomment to target GPU:
    # gpu="T4",
    min_containers=0,        # 0 = cold starts allowed; set 1 to keep warm
    scaledown_window=300,    # seconds idle before container terminates
)
class FunctionServer:

    @modal.enter()
    def load(self):
        """
        Runs ONCE when a container starts — not per request.

        Uses use_proxy=False so the function's code ZIP is extracted and loaded
        immediately. backend="local" runs inference in-process (Modal's container
        provides the isolation instead of a conda subprocess).
        """
        import os
        from metaflow import function_from_json

        reference = os.environ["METAFLOW_FUNCTION_REFERENCE"]

        # use_proxy=False  — load user code immediately (no lazy conversion on first call)
        # backend="local"  — in-process; Modal container is already the isolated env
        # start_runtime=False — no subprocess; execution is in-process
        self.fn = function_from_json(
            reference,
            use_proxy=False,
            backend="local",
            start_runtime=False,
        )
        print(f"[modal] Function loaded from: {reference}")

    @modal.exit()
    def cleanup(self):
        """Runs when container shuts down."""
        from metaflow import close_function
        close_function(self.fn)
        print("[modal] Function closed")

    @modal.fastapi_endpoint(method="POST")
    async def predict(self, body: dict) -> dict:
        """
        POST /predict
        Body:  {"features": [f1, f2, f3]}
        Returns: {"prediction": float, "n_features": int}
        """
        import asyncio
        from concurrent.futures import ThreadPoolExecutor

        features = body.get("features")
        if not features or not isinstance(features, list):
            return {"error": "body must have 'features' list"}

        # Offload CPU-bound inference to thread pool — keeps the event loop free
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = await loop.run_in_executor(
                pool, self.fn, {"features": features}
            )
        return result

    @modal.method()
    def predict_sync(self, features: list) -> dict:
        """
        Synchronous method — callable from other Modal functions or CLI:
            FunctionServer().predict_sync.remote([1.0, 2.0, 3.0])
        """
        return self.fn({"features": features})
