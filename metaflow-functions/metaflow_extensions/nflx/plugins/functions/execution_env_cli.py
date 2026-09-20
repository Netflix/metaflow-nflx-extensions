"""Describe the conda environment a published function uses, for a non-python host.

For a host that has to hand a whole environment to something else rather than
run python itself. Triton's python backend is the case this exists for: its
``EXECUTION_ENV_PATH`` parameter takes a prefix, and the PPP JVM -- which owns
the Triton model repository and cannot import metaflow -- needs that path
before it asks Triton to load the model.

Usage::

    python -m metaflow_extensions.nflx.plugins.functions.execution_env_cli \\
        --alias checkmate_env:5f2a91c0 --arch linux-64

Prints one JSON object on stdout::

    {"prefix": "/tmp/metaflow-condav2-.../envs/...", "python": "3.10", "arch": "linux-64"}

``python`` is part of the answer because the host cannot safely work it out for
itself: a host that execs a prebuilt interpreter *into* this environment has to
match its python version, and inferring that from the prefix means a caller in
another language reimplementing conda's directory layout. It is null only when
the environment has no recognisable ``lib/pythonX.Y``.

Anything that is not the answer goes to stderr, and the exit status is non-zero,
so a caller can treat stdout as the answer and nothing else.

``--format prefix`` restores the original bare-prefix line, for a caller that
has not moved to the JSON form yet.
"""

import argparse
import json
import sys


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Materialize a published function's conda environment "
        "and describe it."
    )
    parser.add_argument("--alias", required=True, help="Environment alias from the spec")
    parser.add_argument("--arch", default=None, help="Architecture, e.g. linux-64")
    parser.add_argument(
        "--format",
        choices=("json", "prefix"),
        default="json",
        help="json (default): one object with prefix, python and arch. "
        "prefix: the bare prefix on one line, as this CLI first shipped.",
    )
    args = parser.parse_args(argv)

    from metaflow_extensions.nflx.plugins.functions.environment import (
        environment_python_version,
        materialize_conda_environment,
    )

    prefix = materialize_conda_environment(
        {"environment": {"alias": args.alias, "arch": args.arch}}
    )

    # stdout carries only the answer; the conda machinery logs to stderr.
    if args.format == "prefix":
        print(prefix)
    else:
        print(
            json.dumps(
                {
                    "prefix": prefix,
                    "python": environment_python_version(prefix),
                    "arch": args.arch,
                }
            )
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
