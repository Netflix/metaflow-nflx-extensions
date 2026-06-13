# Examples

Self-contained runnable examples demonstrating common metaflow-netflixext use
cases. Each directory contains a flow and any auxiliary files (requirements,
environment yaml) needed to run it.

Every example assumes you have `metaflow` and `metaflow-netflixext` installed,
and uses `--environment=conda` to activate the extension's environment
management.

```bash
pip install metaflow metaflow-netflixext
```

| # | Example | What it shows |
|---|---|---|
| 01 | [`01_basic_conda/`](01_basic_conda/) | Per-step `@conda` decorator with different package versions per step |
| 02 | [`02_pypi/`](02_pypi/) | Per-step `@pypi` decorator (PyPI packages instead of Conda) |
| 03 | [`03_mixed_conda_pypi/`](03_mixed_conda_pypi/) | Mixing `@conda` and `@pypi` in the same flow, and `@conda_base` defaults |
| 04 | [`04_from_yml_file/`](04_from_yml_file/) | Resolving an environment from `environment.yml` |
| 05 | [`05_from_requirements_txt/`](05_from_requirements_txt/) | Resolving an environment from `requirements.txt` (including `--conda-pkg`) |
| 06 | [`06_named_environment/`](06_named_environment/) | Resolving a named/aliased environment and reusing it with `@named_env` |

For the full reference, see [`docs/conda.md`](../docs/conda.md).
