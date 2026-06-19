# metaflow-prebuilt

A [Metaflow](https://metaflow.org) extension that pre-bakes conda/PyPI
dependencies into Docker images at deploy time. Tasks start immediately —
no conda bootstrap overhead at runtime.

## How it works

When you deploy a flow with `--environment=prebuilt`, the extension:

1. Resolves the conda/PyPI environment spec for each step.
2. Builds a Docker image with the environment already installed.
3. Pushes the image to a registry.
4. Configures the remote runner (Batch, Kubernetes, etc.) to pull that image
   instead of bootstrapping conda at task start.

## Install

```bash
pip install metaflow-prebuilt
```

Optional extras for specific backends:

```bash
pip install 'metaflow-prebuilt[ecr]'       # ECR registry + CodeBuild service
pip install 'metaflow-prebuilt[gcr]'       # GCR registry
pip install 'metaflow-prebuilt[kaniko]'    # Kaniko build service (K8s)
```

## Quick start

```python
from metaflow import FlowSpec, step, conda

class MyFlow(FlowSpec):

    @conda(packages={"numpy": "1.26.4"})
    @step
    def start(self):
        import numpy
        print(numpy.__version__)
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == "__main__":
    MyFlow()
```

Deploy with the prebuilt environment (requires a configured registry and build
service — see below):

```bash
# Deploy: builds and pushes Docker images, then submits the flow
python my_flow.py --environment=prebuilt run

# Or on Batch/Kubernetes:
python my_flow.py --environment=prebuilt batch run
```

## Configuration

Select backends via environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `METAFLOW_PREBUILT_BUILD_SERVICE` | `docker` | Which build service to use |
| `METAFLOW_PREBUILT_IMAGE_REGISTRY` | `dockerhub` | Which image registry to use |

## Extension points

### `DockerBuildService`

Controls how Docker images are built and pushed. Select with
`METAFLOW_PREBUILT_BUILD_SERVICE=<name>`.

| Name | Class | Description |
|---|---|---|
| `docker` | `LocalDockerBuildService` | Local Docker daemon (`docker build` + `docker push`) |
| `buildx` | `BuildxBuildService` | Docker Buildx (`docker buildx build --push`), supports remote builders and cross-platform |
| `kaniko` | `KanikoBuildService` | Kaniko in a Kubernetes Job; uploads context to GCS/S3 |
| `codebuild` | `CodeBuildService` | AWS CodeBuild; uploads context to S3, polls for completion |

### `ImageRegistry`

Controls where images are stored. Select with
`METAFLOW_PREBUILT_IMAGE_REGISTRY=<name>`.

| Name | Class | Description |
|---|---|---|
| `dockerhub` | `DockerHubRegistry` | Docker Hub (`docker.io`) |
| `ecr` | `ECRRegistry` | Amazon ECR |
| `gcr` | `GCRRegistry` | Google Container Registry |
| `local` | `LocalRegistry` | Local `registry:2` container (dev/CI use) |

## Contributing a custom backend

### Custom build service

Subclass `DockerBuildService` and implement `build_and_push`:

```python
from metaflow_extensions.prebuilt.plugins.conda.build_service import DockerBuildService

class MyBuildService(DockerBuildService):
    def build_and_push(self, dockerfile, context_files, image_tag,
                       push_credentials, echo, target_platform=None) -> bool:
        # build and push; return True on success, False on failure.
        # target_platform (e.g. "linux/amd64") is the resolved step's arch for
        # cross-arch builds; None => builder default. Local builders should honor
        # it; remote builders with a fixed build arch may ignore it.
        ...
```

Register it in your `pyproject.toml`:

```toml
[project.entry-points."metaflow_prebuilt.build_services"]
mybuild = "mypackage.my_module:MyBuildService"
```

Then use it with `METAFLOW_PREBUILT_BUILD_SERVICE=mybuild`.

### Custom image registry

Subclass `ImageRegistry` and implement `image_tag`, `push_credentials`, and
`pull_config`:

```python
from metaflow_extensions.prebuilt.plugins.conda.image_registry import ImageRegistry

class MyRegistry(ImageRegistry):
    def image_tag(self, env_id) -> str: ...
    def push_credentials(self) -> dict: ...
    def pull_config(self, pull_tag) -> dict: ...
```

Register it:

```toml
[project.entry-points."metaflow_prebuilt.image_registries"]
myregistry = "mypackage.my_module:MyRegistry"
```

Then use it with `METAFLOW_PREBUILT_IMAGE_REGISTRY=myregistry`.

## License

Apache 2.0
