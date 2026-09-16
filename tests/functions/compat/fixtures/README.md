# Reference-JSON compatibility fixtures

One directory per schema era, added when the change that ends it lands. Each holds a
reference JSON an older metaflow-functions wrote, plus the packages needed to run it.

## `post-runtime-components/`

The oldest schema we support: `f072249` (#98, runtime components), metaflow-functions
0.2.7. Older shapes are explicitly out of support — that release broke them.

`reference.json` is the real artifact, byte-for-byte as `Function._export` wrote it:
the `avro_simple_string` ux function bound by an actual flow run against a local S3
endpoint using the `test-functions` workflow's environment, so its paths are all
`s3://metaflow-test/...`. `system_metadata.runtime_components` is absent because no
components were configured at bind time — representative of the era, not older than it.

Alongside it: `task_package.tar` (1.9 MB) carries `.mf_code/metaflow` and
`.mf_code/metaflow_extensions`, i.e. the era's own runtime code, which
`update_packaging_env_vars` puts on the runtime subprocess's `PYTHONPATH`;
`function_package.zip` (10 KB) carries the function module and its schemas.
`setup_code_packages` extracts the tar then the zip over it, so both are needed.

## Replaying

`test_replay_old_function.py` rewrites three path fields into a temp copy rather than
doctoring the artifact: the two package fields (`download_s3_packages` passes non-S3
paths through untouched) and `reference`, which both backends re-read and download
themselves (`LocalBackend.apply`, `MemoryBackend.get_runtime_command`).

Two things a repo cannot carry, so replay does without them:

- the conda env behind `system_metadata.environment.alias` —
  `METAFLOW_FUNCTIONS_TEST_MODE=1` skips resolving it; the old *code* still comes from
  the package, so the compat surface is intact;
- the `artifacts` map, whose entries are metaflow datastore objects addressed by sha
  (`location: ":root:s3://..."`) rather than paths. Cleared, so the function falls back
  to its parameter defaults. Caller-side `params=` is not a substitute on the memory
  backend, whose runtime builds parameters itself inside the subprocess.

## Capturing another era

Check out the cutoff commit, start an S3 endpoint on `:9000` with a `metaflow-test`
bucket (MinIO, or `moto_server`), then export the env block from
`.github/workflows/test-functions.yml` plus `METAFLOW_USER` and
`PYTHONPATH=tests/functions/ux/flows`, and run:

```bash
python tests/functions/ux/flows/hellosimplefunction.py --environment=conda run
```

Copy down `Flow("HelloSimpleFunction").latest_run["bind_functions"].task.data`
`.avro_simple_function.reference` and the two objects it names, then add the era to
`ERA_TOP_LEVEL_KEYS` and `FIXTURE_ERAS`.
