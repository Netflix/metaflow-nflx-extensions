# 06 — Named environments with `@named_env`

Named environments let you resolve once and reuse across many flows. Resolve a
set of dependencies, give it an alias, then reference the alias from any flow
with `@named_env`.

This pattern is useful for:

- Sharing a reproducible environment across a team.
- Pinning an environment for the life of a project, decoupled from any single
  flow file.
- Building an environment ahead of time so flow runs don't pay the resolve
  cost.

## 1. Resolve and alias

```bash
metaflow --environment=conda environment resolve \
    --python ">=3.10,<3.11" \
    --alias examples/named_demo \
    --conda-pkg "numpy==1.24.4"
```

Or, equivalently, from a yaml file:

```bash
cat > env.yml <<EOF
channels:
  - conda-forge
dependencies:
  - python=3.10
  - numpy=1.24.4
EOF

metaflow --environment=conda environment resolve \
    --alias examples/named_demo \
    -f env.yml
```

## 2. Inspect

```bash
metaflow --environment=conda environment show examples/named_demo
```

## 3. Use in a flow

```bash
python flow.py --environment=conda run
```

## Alias format

Aliases use slash-separated namespaces with an optional `:tag` for versions:

```
team/project/environment_name:v1
```

See [`docs/conda.md` § Named environments](../../docs/conda.md#named-environments)
for sharing, mutability, and pathspec equivalents.
