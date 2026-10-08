"""Materialize a published function's conda environment and describe it.

For hosts that cannot import metaflow, e.g. the PPP JVM building Triton's
``EXECUTION_ENV_PATH``::

    python -m metaflow_extensions.nflx.plugins.functions.execution_env_cli \\
        --alias checkmate_env:5f2a91c0 --arch linux-64

Prints one JSON object on stdout, ``{"prefix": ..., "python": "3.10", "arch": ...}``,
and everything else to stderr. ``python`` is null if it can't be determined.
``--format prefix`` prints only the prefix.
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

    env = materialize_conda_environment(
        {"environment": {"alias": args.alias, "arch": args.arch}}
    )
    if env.activate is None:
        print("Could not write an activate script in %s" % env.prefix, file=sys.stderr)
        return 1

    # stdout carries only the answer; the conda machinery logs to stderr.
    if args.format == "prefix":
        print(env.prefix)
    else:
        print(
            json.dumps(
                {
                    "prefix": env.prefix,
                    "python": environment_python_version(env.python),
                    "arch": args.arch,
                }
            )
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
