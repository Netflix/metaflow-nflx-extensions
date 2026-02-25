import json
import os
import tempfile
from unittest.mock import patch

from metaflow_extensions.netflix_ext.plugins.conda.env_descr import (
    CachedEnvironmentInfo,
    write_to_conda_manifest,
)


def test_write_to_conda_manifest_cleanup_on_rename_failure():
    """Test that temporary files are cleaned up when rename fails"""
    with tempfile.TemporaryDirectory() as temp_dir:
        info = CachedEnvironmentInfo()

        # Mock os.rename to raise an exception
        with patch("os.rename", side_effect=OSError("Rename failed")):
            with patch("tempfile.NamedTemporaryFile") as mock_temp:
                mock_temp_file = mock_temp.return_value.__enter__.return_value
                mock_temp_file.name = os.path.join(temp_dir, "temp_file")
                mock_temp_file.flush.return_value = None
                mock_temp_file.fileno.return_value = 1

                # Create actual temp file to test cleanup
                with open(mock_temp_file.name, "w") as f:
                    f.write('{"test": "data"}')

                with patch("os.path.exists", return_value=True):
                    with patch("os.unlink") as mock_unlink:
                        try:
                            write_to_conda_manifest(temp_dir, info)
                        except OSError:
                            pass  # Expected due to mocked rename failure

                        # Verify cleanup was attempted
                        mock_unlink.assert_called_once_with(mock_temp_file.name)
