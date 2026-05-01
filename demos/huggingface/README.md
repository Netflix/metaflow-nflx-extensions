# `@huggingface` Demo

This demo runs a small Metaflow flow using the `metaflow-netflixext`
`@huggingface` decorator. It focuses on auth-managed Hub access: public access,
env-token access, metadata-only calls, full downloads, lazy access, eager prefetch,
and custom download locations.

Run commands from the repository root.

## Prerequisites

Use `uv` to run the demo against the local extension package:

```bash
uv run --with requests --with huggingface_hub --with-editable ./metaflow-netflixext \
  python demos/huggingface/run_huggingface_demo.py run --help
```

## Demo Modes

| What it tests | Command | Notes |
| --- | --- | --- |
| Public metadata-only default | `uv run --with requests --with huggingface_hub --with-editable ./metaflow-netflixext python demos/huggingface/run_huggingface_demo.py run` | Safe default. Uses the `env` auth provider with no token and calls Hub metadata API for `openai-community/gpt2`. |
| Lazy two-model access | `uv run --with requests --with huggingface_hub --with-editable ./metaflow-netflixext python demos/huggingface/run_huggingface_demo.py run --only-read-first-model` | Lists two public models but only reads the first key, showing that auth is resolved once while Hub calls stay lazy. |
| Public metadata prefetch | `uv run --with requests --with huggingface_hub --with-editable ./metaflow-netflixext python demos/huggingface/run_huggingface_demo.py run --fetch metadata --prefetch` | Sets `lazy=False`; auth and metadata resolve before the step body. |
| Public full download | `uv run --with requests --with huggingface_hub --with-editable ./metaflow-netflixext python demos/huggingface/run_huggingface_demo.py run --auth public --fetch download --use-demo-cache` | Uses the selected auth provider for `snapshot_download`; this can take time and disk space. |
| Custom download parent | `uv run --with requests --with huggingface_hub --with-editable ./metaflow-netflixext python demos/huggingface/run_huggingface_demo.py run --auth public --fetch download --local-dir /tmp/mf-hf-demo` | `--local-dir` overrides `--use-demo-cache`. |
| Private/env token | `HF_TOKEN=... uv run --with requests --with huggingface_hub --with-editable ./metaflow-netflixext python demos/huggingface/run_huggingface_demo.py run --auth env --fetch metadata` | Reads the token from the environment through the default auth provider. Edit `DEFAULT_PRIVATE_SPEC` in the demo to a repo your token can access. |

`--auth public` and `--auth env` both select the extension's `env` auth provider.
They differ only in which demo repo ids are used.

## Local Test Command

Run the no-network decorator and demo tests:

```bash
uv run --with requests --with-editable ./metaflow-netflixext \
  python demos/huggingface/run_huggingface_demo.py test
```

Run live public metadata smoke tests too:

```bash
uv run --with requests --with-editable ./metaflow-netflixext \
  python demos/huggingface/run_huggingface_demo.py test --live
```

Run the optional public download smoke test:

```bash
uv run --with requests --with-editable ./metaflow-netflixext \
  python demos/huggingface/run_huggingface_demo.py test --download
```

The `test` command runs the same editable-install pytest command used by CI-style
local checks. The live modes are opt-in and still run through this Python entrypoint.

Demo run metadata is written under `demos/huggingface/.metaflow/` by default.
Download-mode artifacts created with `--use-demo-cache` are written under
`demos/huggingface/.demo_hf_cache/`.

## Custom Auth Providers

Custom auth providers should live in a local/private package and be loaded with
config. This keeps provider-specific token lookup outside flow code and out of this
repository:

```bash
export METAFLOW_HUGGINGFACE_AUTH_PROVIDER=my-provider
export METAFLOW_HUGGINGFACE_AUTH_PROVIDERS='{"my-provider": "local_pkg.provider.MyProvider"}'
```

Do not commit provider implementations that contain organization-specific auth
logic or credential access.
