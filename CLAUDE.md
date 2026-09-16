# CLAUDE.md

Guidance for Claude Code (claude.ai/code) and other AI coding agents working in this
repository.

## What this repository is

Netflix's extensions to [Metaflow](https://github.com/Netflix/metaflow), shipped as three
installable packages that plug into Metaflow's extension mechanism:

| Package | What it is |
|---|---|
| `metaflow-netflixext/` | The main extension. Its centre of gravity is the Conda/Pypi environment system under `metaflow_extensions/netflixext/plugins/conda/`. |
| `metaflow-prebuilt/` | Building and serving prebuilt container images for environments. |
| `metaflow-functions/` | The `metaflow-functions` OSS package. |

Extensions are wired up through `config/mfextinit_netflixext.py` and
`plugins/mfextinit_netflixext.py` — a new decorator or CLI command has to be registered
there or Metaflow will not see it.

## Setup

The test suite needs a real conda toolchain; there is no pure-pip path.

```bash
micromamba create -n nflxext-dev -f dev-env.yml -c conda-forge python=3.12
micromamba run -n nflxext-dev pip install -e metaflow-netflixext/
```

`dev-env.yml` pulls in `mamba`, `conda`, `conda-lock`, `uv` and the pytest plugins. CI
runs the matrix across `{ubuntu, macos} x python{3.10,3.12,3.13} x {mamba, conda,
micromamba}`, so a change that depends on one resolver's behaviour will surface there
even if it passes locally.

## Commands

```bash
# Unit tests -- what CI runs. No Atlas-style credentials needed.
METAFLOW_CONDA_TEST=1 METAFLOW_DATASTORE_SYSROOT_LOCAL=.metaflow \
  micromamba run -n nflxext-dev python -m pytest tests/environments tests/plugins/conda tests/namespace

# Formatting gate (pre-commit runs exactly this)
micromamba run -n nflxext-dev black -t py34 -t py35 -t py36 -t py37 -t py38 \
  -t py39 -t py310 -t py311 -t py312 .
```

`mkdir .metaflow` first: the local datastore root has to exist. `tests/plugins/conda/`
and `tests/namespace/` are fast and hermetic; `tests/environments/` actually resolves
environments and talks to the network, so it is slower and the likelier source of a
failure that has nothing to do with your change. **Check a suspicious failure against a
stashed tree before assuming you caused it.**

## The Conda plugin

Almost all the interesting code is in
`metaflow-netflixext/metaflow_extensions/netflixext/plugins/conda/`:

| File | Role |
|---|---|
| `env_descr.py` | The data model: `ResolvedEnvironment`, `PackageSpecification` (`CondaPackageSpecification` / `PypiPackageSpecification`), `EnvID`, `EnvType`, the cached-environment index. |
| `envsresolver.py` | Batches environments to resolve, mutualizes work, picks the resolver, handles co-resolution and caching. |
| `resolvers/` | One per strategy: `conda_resolver`, `pip_resolver`, `conda_lock_resolver`, `pylock_toml_resolver`, `builder_envs_resolver`. |
| `parsers.py` | Parsing of user-supplied requirement files (`requirements.txt`, `environment.yml`, `pyproject.toml`, `conda-lock.yml`). |
| `conda.py` | The `Conda` object: the package cache, the environment index, talking to the binaries. |
| `conda_step_decorator.py`, `conda_flow_mutator.py` | The `@conda`/`@pypi`/`@uv` decorators and the flow mutators that rewrite a flow's steps. |
| `cmd/environment/environment_cmd.py` | The `metaflow environment` CLI (`resolve`, `show`, `create`, ...). |

### Identity: req_id and full_id

Every environment has an `EnvID(req_id, full_id, arch)`.

- **`req_id`** hashes the *requirements* — user deps, sources, extra args. Environments
  asking for the same thing share it.
- **`full_id`** hashes the *resolved package set* (filenames + hashes), plus
  `full_id_unique_keys` for resolvers where the lock content matters beyond the packages.

Co-resolved environments (the same requirements solved for several architectures at
once) share a `req_id` and are given a single `full_id` by
`ResolvedEnvironment.set_coresolved_full_id`. **That function recomputes `full_id` from
the package set alone and ignores `full_id_unique_keys`** — so a unique key only takes
effect for a single-architecture environment. Know that before relying on one.

### Resolved vs. already-resolved inputs

Two different shapes of input, and mixing them up is the most common way to start down
the wrong path:

- **To resolve**: `requirements.txt`, `environment.yml`, `pyproject.toml`. Parsed into
  deps/sources, handed to `EnvsResolver`, which runs a solver.
- **Already resolved**: `uv.lock`, `pylock.toml` (PEP 751), `conda-lock.yml`. These pin
  exact URLs and hashes. There is nothing to solve: build `ResolvedEnvironment` and
  `PackageSpecification` objects directly and register them through the same
  `cache_environments` / `add_environments` calls the resolver path ends with.
  `pylock_toml_resolver.py` and `conda_lock_file.py` are the two worked examples.

A `PackageSpecification` needs a filename, a URL, a format and a hash — conda packages
are addressed by **md5**, pypi packages by **sha256**. `parse_explicit_url_conda` and
`parse_explicit_url_pypi` in `utils.py` derive the first three from a `url#hash` string;
reuse them rather than splitting URLs by hand.

Source distributions and `git+` URLs cannot be turned into a spec directly — they have to
be built, via the `PackageToBuild` / `build_pypi_packages` path, which needs a builder
environment and a storage backend. If you are adding a new already-resolved input format,
decide deliberately whether to support them or reject them with a clear message.

## Testing conventions

- Both `test_*.py` and `*_test.py` naming appear; match the closest sibling.
- Fixtures live in `tests/plugins/conda/data/` and `tests/environments/env_specs/`.
- **Prefer a real generated fixture to a hand-written one.** `dev-env.yml` ships
  `conda-lock` and `uv`, so you can generate genuine lockfiles instead of inventing a
  schema from documentation. Trim the real output rather than authoring it.
- **Prove a regression test fails without the fix.** Comment the change out, watch it go
  red, restore it. This is how the `--include-dev` no-op bug in `packages_for` was caught:
  conda-lock marks non-`main` packages *both* with a category and with `optional: true`,
  so filtering on `optional` alone silently did nothing.
- CLI behaviour is tested with `metaflow._vendor.click.testing.CliRunner` against the
  `environment` group — see `tests/plugins/conda/test_file_parsing.py`.

## Conventions

- `black` with the long `-t` target list from `.pre-commit-config.yaml` (it targets
  py34+, so no f-strings in library code — the existing style uses `%` formatting).
- Type comments (`# type: List[str]`) alongside annotations; `resolvers/__init__.py` is
  `pyright: strict`.
- Resolvers register themselves by subclassing `Resolver` and declaring `TYPES`, then
  being imported at the bottom of `resolvers/__init__.py`.
- Errors that a user should act on are `InvalidEnvironmentException`; internal failures
  are `CondaException`. Say what to do next in the message, not just what went wrong.
- `debug.conda_exec(...)` for tracing; enabled with `METAFLOW_DEBUG_CONDA=1`.

## Claude Retro Suggestions
<!-- claude-retro-auto -->
- Before implementing features requiring data queries, always ask 'what does the UI actually display with this data?' and verify the data source can provide it, rather than building the feature and discovering mid-implementation that the data structure doesn't support it.
- When debugging backend data bugs, check the data source (database query, API response, log output) directly with a quick query FIRST before investigating application code.
- When the user says 'do it', 'continue', 'just finish it', or 'DO EVERYTHING', work autonomously without asking for clarification or pausing for approval. Only return when blocked or asking would save significant wasted work.
- Before proposing large rewrites (UI redesigns, architectural changes, multi-file refactors), spend the first 1-2 turns validating the core problem with the user: 'What are the 2-3 specific things that need to change?' rather than proposing comprehensive redesigns.
- When setting up external tool integrations (MCP, Jira, credentials) in the middle of a session, immediately ask the user for configuration details rather than attempting multiple failed calls. A 2-minute credential clarification beats 60 turns of failed attempts.
<!-- claude-retro-auto -->
