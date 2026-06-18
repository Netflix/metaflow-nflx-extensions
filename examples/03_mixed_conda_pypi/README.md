# 03 — Mixed `@conda` + `@pypi` with `@conda_base`

This example shows three things in one flow:

1. **`@conda_base`** at the flow level provides default packages
   (`numpy==1.26.4`, `python>=3.10,<3.11`) that apply to every step unless
   overridden.
2. **`@conda`** at the step level *adds* to the base (gets `numpy` + `scipy`).
3. **`@pypi`** at the step level *replaces* conda with PyPI for that step
   (gets `requests` from PyPI, no numpy).

## Run

```bash
python flow.py --environment=conda run
```

## What to expect

- `start` uses the `@conda_base` env (numpy only).
- `conda_step` extends with scipy.
- `pypi_step` uses a separate PyPI-only env (requests).
- `join` and `end` inherit the base.

Each unique environment is resolved once and cached.
