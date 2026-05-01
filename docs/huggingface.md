# Hugging Face Auth Management

The `@huggingface` step decorator provides auth-managed access to Hugging Face Hub
models from Metaflow steps. It centralizes token discovery and custom auth provider
selection, then exposes either downloaded snapshot paths or Hub metadata through
`current.huggingface`.

Use it when a step needs a model snapshot or model metadata without putting token
lookup, provider-specific auth logic, or Hub setup directly in flow code.

Install the optional dependency when you need the decorator:

```bash
pip install "metaflow-netflixext[huggingface]"
```

## Basic Usage

```python
from metaflow import FlowSpec, current, huggingface, step


class HuggingFaceFlow(FlowSpec):
    @huggingface(models={"gpt2": "openai-community/gpt2@main"})
    @step
    def start(self):
        self.model_path = current.huggingface.models["gpt2"]
        self.next(self.end)

    @step
    def end(self):
        print(self.model_path)
```

`models` can be a list of repo specs or a dict of alias to repo spec. A repo spec is
`repo_id` or `repo_id@revision`; omitted revisions default to `main`.

```python
@huggingface(models=["openai-community/gpt2"])
@huggingface(models={"gpt2": "openai-community/gpt2@main"})
```

Use dict aliases when a step needs stable, readable keys or multiple revisions of
the same repository.

## Authentication

Auth is resolved before the step body runs. The selected provider returns a token,
or `None` for public Hub access. That token is then used for metadata and download
requests.

The default auth provider is `env`. It reads the first non-empty value from:

1. `HF_TOKEN`
2. `HUGGING_FACE_TOKEN`
3. `HUGGING_FACE_HUB_TOKEN`

Set `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` to select another provider. Set
`METAFLOW_HUGGINGFACE_AUTH_PROVIDERS` to a JSON mapping from provider name to
import path:

```bash
export METAFLOW_HUGGINGFACE_AUTH_PROVIDER=my-provider
export METAFLOW_HUGGINGFACE_AUTH_PROVIDERS='{"my-provider": "my_pkg.provider.MyProvider"}'
```

Custom providers implement `HuggingFaceAuthProvider`:

```python
from typing import Optional
from metaflow_extensions.nflx.plugins.huggingface import HuggingFaceAuthProvider


class MyProvider(HuggingFaceAuthProvider):
    TYPE = "my-provider"

    def get_token(self) -> Optional[str]:
        return "..."
```

Custom providers are useful when tokens come from a local credential helper, a
private package, or another organization-specific auth system. If a configured
provider cannot be found or imported, `@huggingface` raises a `MetaflowException`
instead of silently falling back to another auth source.

Provider implementations that contain organization-specific logic or credential
access should live in a local/private package and be loaded with this config hook.

## Metadata-Only Mode

Use `metadata_only=True` to fetch `ModelInfo` objects instead of downloading files:

```python
@huggingface(models={"probe": "openai-community/gpt2"}, metadata_only=True)
@step
def start(self):
    info = current.huggingface.model_info["probe"]
```

Resolution is lazy by default. Auth is resolved before the step body runs, but each
model is fetched only when its key is accessed. Set `lazy=False` to prefetch every
listed model before the step body starts.

## Download Location And Endpoint

Snapshots are written under a parent directory selected in this order:

1. `@huggingface(local_dir=...)`
2. `METAFLOW_HUGGINGFACE_LOCAL_DIR`
3. `<task temp>/metaflow_huggingface`

Each repo gets a subdirectory named from the repo id and revision. Revision values
are encoded into a single path component, so slash-containing revisions such as
`refs/pr/1` cannot create nested directories.

Set `METAFLOW_HUGGINGFACE_ENDPOINT` only for a custom Hugging Face host.
