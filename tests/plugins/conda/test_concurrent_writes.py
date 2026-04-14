import threading
import time
import json
import os
import tempfile
from unittest.mock import patch

from metaflow_extensions.nflx.plugins.conda.env_descr import (
    CachedEnvironmentInfo,
    ResolvedEnvironment,
    write_to_conda_manifest,
    read_conda_manifest,
)
from metaflow_extensions.nflx.plugins.conda.utils import get_conda_manifest_path


def test_write_to_conda_manifest_concurrent_access():
    """Test that concurrent writes don't corrupt the manifest file"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create initial environment info
        info1 = CachedEnvironmentInfo()
        info1.add_resolved_env(
            ResolvedEnvironment(
                user_dependencies={"conda": ["package1"]},
                user_sources=None,
                user_extra_args=None,
                arch="test_arch",
            )
        )

        info2 = CachedEnvironmentInfo()
        info2.add_resolved_env(
            ResolvedEnvironment(
                user_dependencies={"conda": ["package2"]},
                user_sources=None,
                user_extra_args=None,
                arch="test_arch",
            )
        )

        results = []
        exceptions = []

        def write_worker(info, worker_id):
            try:
                # Add small delay to increase chance of concurrent access
                time.sleep(0.01 * worker_id)
                write_to_conda_manifest(temp_dir, info)
                results.append(f"worker_{worker_id}_success")
            except Exception as e:
                exceptions.append((worker_id, e))

        # Start multiple threads writing concurrently
        threads = []
        for i in range(5):
            thread = threading.Thread(
                target=write_worker, args=(info1 if i % 2 == 0 else info2, i)
            )
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify no exceptions occurred
        assert len(exceptions) == 0, f"Concurrent writes failed: {exceptions}"

        # Verify file is not corrupted and can be read
        final_content = read_conda_manifest(temp_dir)
        assert isinstance(final_content, CachedEnvironmentInfo)

        # Verify the file contains valid JSON
        manifest_path = get_conda_manifest_path(temp_dir)
        with open(manifest_path, "r") as f:
            json.load(f)  # Should not raise an exception
