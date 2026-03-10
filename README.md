# metaflow-nflx-extensions

Metaflow extensions from Netflix. This is a monorepo containing the following packages:

| Package | Description | PyPI |
|---------|-------------|------|
| [metaflow-netflixext](metaflow-netflixext/) | Conda/Pip environment management for Metaflow | [![PyPI](https://img.shields.io/pypi/v/metaflow-netflixext)](https://pypi.org/project/metaflow-netflixext/) |
| [metaflow-functions](metaflow-functions/) | Relocatable computation: avro/json functions, memory/local/ray backends | [![PyPI](https://img.shields.io/pypi/v/metaflow-functions)](https://pypi.org/project/metaflow-functions/) |

## Installation

```bash
pip install metaflow-netflixext
pip install metaflow-functions
```

## Development

### Prerequisites

- Python 3.10+
- [Docker](https://docs.docker.com/get-docker/) (for the local devstack)

### Setup

```bash
git clone https://github.com/Netflix/metaflow-nflx-extensions.git
cd metaflow-nflx-extensions

python -m venv .venv
source .venv/bin/activate

pip install -e metaflow-netflixext
pip install -e metaflow-functions
pip install pytest fastavro cffi
```

### Running tests

Unit tests (no external dependencies):

```bash
pytest tests/functions/ --ignore=tests/functions/ux -v
```

UX integration tests require the [metaflow_rc](https://github.com/Netflix/metaflow_rc) devstack (MinIO for S3, local metadata):

```bash
# Start the devstack
cd /path/to/metaflow_rc
docker compose -f devtools/devtools/docker-compose.yml up -d

# Back in this repo, source the devstack env and run
cd /path/to/metaflow-nflx-extensions
source /path/to/metaflow_rc/run_test.sh  # sets env vars
pytest tests/functions/ux/ -v --timeout=600
```

## License

Apache-2.0
