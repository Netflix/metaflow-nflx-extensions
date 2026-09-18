# Reference-JSON compatibility fixtures

This directory is used to store previous versions of the functions and code package 
for testing backward and forward compatibility. 

One directory per pinned old version, named for it. Each holds a reference JSON that
version wrote, plus the packages needed to run it. Add a newer one, or drop an older
one, whenever the versions worth testing against change.

## `v0.2.8/`

Captured at `f072249`

`reference.json` is the real artifact, byte-for-byte as `Function._export` wrote it. `task_package.tar` (1.9 MB) carries `.mf_code/metaflow` and
`.mf_code/metaflow_extensions`, i.e. that version's own runtime code.

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
