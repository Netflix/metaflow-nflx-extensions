#!/usr/bin/env python3

import sys

from metaflow._vendor import click


@click.command()
@click.option("--input-map", required=True, help="Name of the input shared memory map")
@click.option(
    "--output-map", required=True, help="Name of the output shared memory map"
)
@click.option(
    "--data-watcher", required=True, help="Name of the data watcher shared memory"
)
@click.option(
    "--reference", required=True, help="Path to the function specification file"
)
@click.option(
    "--prefetch-artifacts",
    is_flag=True,
    default=False,
    help="Pre-fetch all artifacts during initialization instead of lazy loading",
)
def main(input_map, output_map, data_watcher, reference, prefetch_artifacts):
    """
    Memory Backend Runtime for Metaflow Functions.

    This CLI is used internally by the memory backend to launch function
    runtime subprocesses with proper memory buffer connections.
    """
    from metaflow_extensions.nflx.plugins.functions.backends.memory.memory_backend import (
        MemoryBackend,
    )

    try:
        MemoryBackend.runtime_main(
            input_map_name=input_map,
            output_map_name=output_map,
            data_watcher_name=data_watcher,
            reference=reference,
            prefetch_artifacts=prefetch_artifacts,
        )
    except Exception as e:
        click.echo(f"Memory backend runtime error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
