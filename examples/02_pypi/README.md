# 02 — Basic `@pypi`

The `@pypi` decorator resolves PyPI packages instead of Conda packages. Use
this when you want plain `pip`-style dependencies (often faster to resolve, and
useful when a wheel exists on PyPI but not on conda-forge).

## Run

```bash
python flow.py --environment=conda run
```

## What to expect

- `start` runs in an environment with `pandas==1.4.0` installed from PyPI.
- `end` runs in an environment with `pandas==1.5.0` installed from PyPI.

## When to choose `@pypi` over `@conda`

- You only need pure-Python packages (or packages with manylinux wheels).
- The package you need only exists on PyPI.
- You want a smaller, faster-to-resolve environment.

Use `@conda` when you need non-Python libraries (e.g. `cuda`, `gdal`, system
libraries) that conda-forge ships alongside Python bindings.
