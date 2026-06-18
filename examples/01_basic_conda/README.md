# 01 — Basic `@conda`

The `@conda` decorator pins Conda packages on a per-step basis. Each step gets
its own environment, so different steps can use different versions of the same
library.

## Run

```bash
python flow.py --environment=conda run
```

## What to expect

- `start` runs in an environment with `pandas==2.1.4`.
- `end` runs in an environment with `pandas==2.2.2`.
- The first run resolves both environments (takes ~30s); subsequent runs reuse
  the cached environments.
