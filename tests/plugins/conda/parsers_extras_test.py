import json
import os
import subprocess
import sys

# Import Metaflow first so its extension plugin registry is initialized before
# importing plugin modules directly.
import metaflow
from metaflow_extensions.netflixext.plugins.conda.parsers import (
    req_parser,
    toml_parser,
)

import pytest

REQ_CONTENT = "nflx-metaflow[stubs,fastdata,functions,serving]>=2.27.4\nnumpy\n"
TOML_CONTENT = (
    "[project]\n"
    'dependencies = ["nflx-metaflow[stubs,fastdata,functions,serving]>=2.27.4", '
    '"numpy"]\n'
)

# Extras are a set in packaging so their iteration order depends on the
# per-process hash seed. Run each parser in fresh interpreters with different
# seeds to make sure the dependency string (which feeds the req_id) is stable.
_SCRIPT = """
import json, sys
import metaflow
from metaflow_extensions.netflixext.plugins.conda.parsers import req_parser, toml_parser
parser = req_parser if sys.argv[1] == "req" else toml_parser
print(json.dumps(sorted(parser(sys.argv[2])["packages"])))
"""


@pytest.mark.parametrize(
    "parser, content",
    [(req_parser, REQ_CONTENT), (toml_parser, TOML_CONTENT)],
)
def test_extras_are_sorted(parser, content):
    packages = parser(content)["packages"]
    assert "nflx-metaflow[fastdata,functions,serving,stubs]" in packages


@pytest.mark.parametrize(
    "parser_name, content",
    [("req", REQ_CONTENT), ("toml", TOML_CONTENT)],
)
def test_extras_order_independent_of_hash_seed(parser_name, content):
    results = set()
    for seed in range(8):
        env = dict(os.environ, PYTHONHASHSEED=str(seed))
        out = subprocess.run(
            [sys.executable, "-c", _SCRIPT, parser_name, content],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        results.add(tuple(json.loads(out.strip().splitlines()[-1])))
    assert len(results) == 1, results
