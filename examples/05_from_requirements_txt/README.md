# 05 — Resolve from `requirements.txt`

The extension extends pip's `requirements.txt` format with `--conda-pkg`, which
lets you specify non-Python Conda packages alongside ordinary PyPI dependencies.
This is useful when most of your stack is PyPI but you still need system
libraries that conda-forge provides.

## Resolve and alias

```bash
metaflow --environment=conda environment resolve \
    --python ">=3.10,<3.11" \
    --alias examples/req_demo \
    -r requirements.txt
```

## Use it in a flow

```python
from metaflow import FlowSpec, named_env, step

class ReqFlow(FlowSpec):
    @named_env(name="examples/req_demo")
    @step
    def start(self):
        import requests, pandas
        print(requests.__version__, pandas.__version__)
        self.next(self.end)

    @named_env(name="examples/req_demo")
    @step
    def end(self):
        pass

if __name__ == "__main__":
    ReqFlow()
```

## Supported `requirements.txt` options

In addition to ordinary requirement specifiers:

- `--extra-index-url <url>` — additional PyPI source
- `--pre` — allow pre-releases
- `--no-index` — disable the default PyPI index
- `-f` / `--find-links <url>` — additional wheel-finding source
- `--trusted-host <host>` — trust an HTTPS host
- `--conda-pkg <pkg>` — **extension**: include a non-Python Conda package

Not supported: constraints files (`-c`) and environment markers.

See [`docs/conda.md`](../../docs/conda.md) for restrictions on Git/URL/local
requirements when resolving across architectures.
