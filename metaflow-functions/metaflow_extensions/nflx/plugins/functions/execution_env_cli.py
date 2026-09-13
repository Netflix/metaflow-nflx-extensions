"""Print the local prefix of the conda environment a published function uses.

For a host that has to hand a whole environment to something else rather than
run python itself. Triton's python backend is the case this exists for: its
``EXECUTION_ENV_PATH`` parameter takes a prefix, and the PPP JVM -- which owns
the Triton model repository and cannot import metaflow -- needs that path
before it asks Triton to load the model.

Usage::

    python -m metaflow_extensions.nflx.plugins.functions.execution_env_cli \\
        --alias checkmate_env:5f2a91c0 --arch linux-64

Prints one line, the prefix, on success. Anything else goes to stderr and the
exit status is non-zero, so a caller can treat stdout as the answer.
"""

import argparse
import sys


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Materialize a published function's conda environment "
        "and print its prefix."
    )
    parser.add_argument("--alias", required=True, help="Environment alias from the spec")
    parser.add_argument("--arch", default=None, help="Architecture, e.g. linux-64")
    args = parser.parse_args(argv)

    from metaflow_extensions.nflx.plugins.functions.environment import (
        materialize_conda_environment,
    )

    prefix = materialize_conda_environment(
        {"environment": {"alias": args.alias, "arch": args.arch}}
    )
    # stdout carries only the answer; the conda machinery logs to stderr.
    print(prefix)
    return 0


if __name__ == "__main__":
    sys.exit(main())
