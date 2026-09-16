# Reference-JSON compatibility fixtures

One directory per schema era, added at the time of the change that ends it. A fixture
is a reference JSON that an *older* metaflow-functions wrote; the tests in
`tests/functions/compat/` assert that today's loader still reads it.

## `post-runtime-components/`

The oldest schema we support: captured from `f072249` (#98, which introduced runtime
components), `metaflow-functions` 0.2.7. Specs written before that commit are
explicitly out of support -- that release broke them and we are not carrying the cost
of the older shape.

`reference.json` is the real artifact, byte-for-byte as `Function._export` wrote it --
it is the `avro_simple_string` function from `tests/functions/ux/flows/`, bound by a
real flow run against a local S3 endpoint with the same environment the
`test-functions` workflow uses, so its paths are all `s3://metaflow-test/...`.

To capture another era, check out the cutoff commit and run:

```bash
# S3 endpoint on :9000 with a metaflow-test bucket (MinIO, or moto_server),
# then the env block from .github/workflows/test-functions.yml, plus:
#   METAFLOW_USER=<you>
#   PYTHONPATH=tests/functions/ux/flows
python tests/functions/ux/flows/hellosimplefunction.py --environment=conda run
```

Then read `Flow("HelloSimpleFunction").latest_run["bind_functions"].task.data`
`.avro_simple_function.reference` and copy that object down.

Note `system_metadata.runtime_components` is absent here. That section is written only
when a function is bound with components configured, so its absence is representative
of this era rather than older than it.

## What these fixtures cannot test

The loader only, which never dereferences the `s3://` paths -- not *running* an old
function. A replay test (load an old reference and call it, which is what would have
caught #98) needs the referenced artifacts to resolve, because the old runtime code
arrives in the code package (`.mf_code/` on the subprocess's `PYTHONPATH` via
`update_packaging_env_vars`). The local S3 endpoint above is ephemeral, so a replay
fixture has to carry its own copies: `download_s3_packages` passes non-S3 paths
through untouched, so `code_package` and `task_code_path` can point at committed
files. For this era that is 10 KB + 1.9 MB.
