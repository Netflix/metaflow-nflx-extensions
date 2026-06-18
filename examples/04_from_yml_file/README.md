# 04 — Resolve from `environment.yml`

Use the `metaflow environment` CLI to resolve dependencies described in an
`environment.yml`. The resolved environment gets cached and can be referenced
by alias from any flow with `@named_env`—see [example 06](../06_named_environment/)
for a full walkthrough of that pattern (using its own alias).

The `environment.yml` here mixes Conda packages (numpy, pandas, python) with
PyPI packages (requests via the `pip:` section).

## Resolve and alias

```bash
metaflow --environment=conda environment resolve \
    --alias examples/yml_demo \
    -f environment.yml
```

## Inspect the resolved environment

```bash
metaflow --environment=conda environment show examples/yml_demo
```

## Use it in a flow

Once aliased, reference it with `@named_env`:

```python
from metaflow import FlowSpec, named_env, step

class YmlFlow(FlowSpec):
    @named_env(name="examples/yml_demo")
    @step
    def start(self):
        import pandas, requests
        print(pandas.__version__, requests.__version__)
        self.next(self.end)

    @named_env(name="examples/yml_demo")
    @step
    def end(self):
        pass

if __name__ == "__main__":
    YmlFlow()
```

## Supported `environment.yml` keys

- `channels:` — Conda channels
- `dependencies:` — top-level Conda packages and a nested `- pip:` list for
  PyPI packages
- `pypi-indices:` — additional PyPI sources
- `sys:` — system / virtual packages (e.g. `__cuda`, `__glibc`)

See [`docs/conda.md`](../../docs/conda.md) for the full set of options.
