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

## Replaying, and what a fixture cannot carry

`function_package.zip` (10 KB) and `task_package.tar` (1.9 MB) are committed beside
the reference. The tar is the one that matters for compat: it holds
`.mf_code/metaflow` and `.mf_code/metaflow_extensions`, the era's own runtime code,
which `update_packaging_env_vars` puts on the runtime subprocess's `PYTHONPATH`. The
zip holds the function's own module and schemas. `setup_code_packages` extracts the
tar and then the zip over it, so both are needed.

`test_replay_old_function.py` rewrites three path fields to point at them --
`code_package`, `task_code_path`, and `reference`, the last because both backends
re-read and download it themselves. Two things cannot be carried in the repo:

- the conda env behind `system_metadata.environment.alias`, skipped with
  `METAFLOW_FUNCTIONS_TEST_MODE=1`; the old code still comes from the package;
- the `artifacts` map, whose entries are metaflow datastore objects addressed by sha
  (`location: ":root:s3://..."`) rather than paths, so there is nothing to redirect.
  The replay clears it and the function falls back to its parameter defaults.

With those, replay needs no S3 at all and runs anywhere the unit tier runs.
