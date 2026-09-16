# Reference-JSON compatibility fixtures

One directory per pinned old version, named for it. Each holds a reference JSON that
version wrote, plus the packages needed to run it. Add a newer one, or drop an older
one, whenever the versions worth testing against change.

## `v0.2.7/`

`reference.json` is the real artifact, byte-for-byte as `Function._export` wrote it:
the `avro_simple_string` ux function bound by an actual flow run against a local S3
endpoint using the `test-functions` workflow's environment, so its paths are all
`s3://metaflow-test/...`.

Alongside it: `task_package.tar` (1.9 MB) carries `.mf_code/metaflow` and
`.mf_code/metaflow_extensions`, i.e. that version's own runtime code, which
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

## Capturing another version

Check out that version's commit, start an S3 endpoint on `:9000` with a
`metaflow-test` bucket (MinIO, or `moto_server`), then export the env block from
`.github/workflows/test-functions.yml` plus `METAFLOW_USER` and
`PYTHONPATH=tests/functions/ux/flows`, and run:

```bash
python tests/functions/ux/flows/hellosimplefunction.py --environment=conda run
```

Copy down `Flow("HelloSimpleFunction").latest_run["bind_functions"].task.data`
`.avro_simple_function.reference` and the two objects it names, then add the version to
`FIXTURE_VERSIONS`.
