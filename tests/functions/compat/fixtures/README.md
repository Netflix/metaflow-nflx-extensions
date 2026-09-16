# Reference-JSON compatibility fixtures

One directory per schema era, added at the time of the change that ends it. A fixture
is a reference JSON that an *older* metaflow-functions wrote; the tests in
`tests/functions/compat/` assert that today's loader still reads it.

## `post-runtime-components/`

The oldest schema we support: as of `f072249` (#98, which introduced runtime
components). Specs written before that commit are explicitly out of support -- that
release broke them and we are not carrying the cost of the older shape.

Its distinguishing feature is `system_metadata.runtime_components`, the per-component
deploy-time config `function_from_json` reads to call `on_runtime_started`.

**Hand-written, not captured.** Its top-level key set is the field set of
`FunctionSpec` at `f072249` (verified with `git show`), and
`test_old_spec_loads.py::test_fixture_matches_its_era` keeps it that way. The values
are plausible but invented, and the `s3://` paths do not exist.

That is enough to test the loader, which never dereferences them, and not enough to
*run* the function. A replay test -- load an old reference and actually call it, which
is what would have caught #98 -- needs a genuinely captured reference whose
`code_package` and `task_code_path` still resolve in S3, since the old runtime code
arrives in that package (`.mf_code/` on the subprocess's `PYTHONPATH`). Capturing one
is a separate change; it makes this suite depend on S3, so the replay test belongs
beside the `ux/` tier rather than here.
