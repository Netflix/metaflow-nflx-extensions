# Examples

Self-contained runnable examples demonstrating common metaflow-netflixext use
cases. Examples 01–03 and 06 each contain a runnable `flow.py`. Examples 04
and 05 demonstrate CLI-based environment resolution; their directories contain
the input file (`environment.yml` / `requirements.txt`) and a README with a
sample flow you can copy and run.

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
