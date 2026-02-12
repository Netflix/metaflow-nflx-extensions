import os

os.environ["FUNCTION_CLEAN_DIR_ON_DEL"] = "1"

import glob
import gzip
import shutil

import pytest

pytestmark = pytest.mark.no_backend_parametrization
from metaflow_extensions.nflx.plugins.functions.logger.logger import (
    RotatingLogWriter,
)


def test_basic_writing(tmp_path):
    log_file = tmp_path / "stdout.log"
    with RotatingLogWriter(str(log_file), max_size=100, total_max_size=500) as writer:
        writer.write("Hello, world!\n")
    with open(log_file, "r", encoding="utf-8") as f:
        assert f.read() == "Hello, world!\n"


def test_rotation_without_compression(tmp_path):
    log_file = tmp_path / "stdout.log"
    with RotatingLogWriter(str(log_file), max_size=10, total_max_size=100) as writer:
        writer.write("A" * 11 + "\n")  # 12 bytes > max_size
    assert os.path.exists(log_file)
    assert os.path.getsize(log_file) == 0  # New file after rotation
    rotated_files = glob.glob(str(tmp_path / "stdout.log.*"))
    assert len(rotated_files) == 1
    assert rotated_files[0].endswith("stdout.log.1")
    with open(rotated_files[0], "r", encoding="utf-8") as f:
        assert f.read() == "A" * 11 + "\n"


def test_total_size_enforcement_no_compression(tmp_path):
    log_file = tmp_path / "stdout.log"
    with RotatingLogWriter(str(log_file), max_size=10, total_max_size=20) as writer:
        writer.write("A" * 11 + "\n")  # Rotates to stdout.log.1 (12 bytes)
        writer.write(
            "B" * 11 + "\n"
        )  # Rotates to stdout.log.2, total > 20, deletes stdout.log.1
    rotated_files = sorted(glob.glob(str(tmp_path / "stdout.log.*")))
    assert len(rotated_files) == 1
    assert rotated_files[0].endswith("stdout.log.2")
    with open(rotated_files[0], "r", encoding="utf-8") as f:
        assert f.read() == "B" * 11 + "\n"


def test_rotation_with_compression(tmp_path):
    log_file = tmp_path / "stdout.log"
    with RotatingLogWriter(
        str(log_file), max_size=10, total_max_size=100, compress=True
    ) as writer:
        writer.write("A" * 11 + "\n")  # Rotates and compresses to stdout.log.1.gz
    assert os.path.exists(log_file)
    assert os.path.getsize(log_file) == 0
    compressed_files = glob.glob(str(tmp_path / "stdout.log.*.gz"))
    assert len(compressed_files) == 1
    assert compressed_files[0].endswith("stdout.log.1.gz")
    with gzip.open(compressed_files[0], "rt", encoding="utf-8") as f:
        assert f.read() == "A" * 11 + "\n"


def test_total_size_enforcement_with_compression(tmp_path):
    log_file = tmp_path / "stdout.log"
    max_size = 10
    total_max_size = 100

    with RotatingLogWriter(
        str(log_file), max_size=max_size, total_max_size=total_max_size, compress=True
    ) as writer:
        # Write enough data to cause rotation and compression
        writer.write("A" * 11 + "\n")  # First rotation
        writer.flush()

        # Write more data to cause another rotation and potentially deletion
        writer.write("B" * 11 + "\n")
        writer.flush()

    # Calculate total size of all files
    all_files = os.listdir(tmp_path)
    total_size = sum(os.path.getsize(os.path.join(tmp_path, f)) for f in all_files)

    # Verify the total size is under the limit
    assert (
        total_size <= total_max_size
    ), f"Total size {total_size} exceeds limit of {total_max_size}"

    # Check if we have at least one file
    assert len(all_files) >= 1, "No files found after test"

    # Check if we have at least one compressed file
    compressed_files = [f for f in all_files if f.endswith(".gz")]
    assert len(compressed_files) >= 1, "No compressed files found after test"


def test_multiple_writes_without_rotation(tmp_path):
    log_file = tmp_path / "stdout.log"
    with RotatingLogWriter(str(log_file), max_size=20, total_max_size=100) as writer:
        writer.write("Hello\n")
        writer.write("World\n")
    with open(log_file, "r", encoding="utf-8") as f:
        assert f.read() == "Hello\nWorld\n"


def test_existing_files_no_compression(tmp_path):
    log_file = tmp_path / "stdout.log"
    with open(log_file, "w", encoding="utf-8") as f:
        f.write("Existing content\n")
    rotated_file = tmp_path / "stdout.log.1"
    with open(rotated_file, "w", encoding="utf-8") as f:
        f.write("Rotated content\n")

    # Get the actual physical sizes
    current_size = os.path.getsize(log_file)
    rotated_size = os.path.getsize(rotated_file)
    expected_size = current_size + rotated_size

    with RotatingLogWriter(str(log_file), max_size=100, total_max_size=500) as writer:
        assert writer._total_size == expected_size


def test_existing_files_with_compression(tmp_path):
    log_file = tmp_path / "stdout.log"
    with open(log_file, "w", encoding="utf-8") as f:
        f.write("Current content\n")

    # Create a temporary file with content to compress
    temp_file = tmp_path / "temp.log"
    with open(temp_file, "w", encoding="utf-8") as f:
        f.write("Rotated content\n")

    # Compress the temporary file
    compressed_file = tmp_path / "stdout.log.1.gz"
    with open(temp_file, "rb") as f_in:
        with gzip.open(compressed_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Get the actual physical size of the compressed file
    compressed_size = os.path.getsize(compressed_file)
    current_size = os.path.getsize(log_file)

    os.remove(temp_file)

    with RotatingLogWriter(
        str(log_file), max_size=100, total_max_size=500, compress=True
    ) as writer:
        # Expected size is the sum of physical sizes
        expected_size = current_size + compressed_size
        assert writer._total_size == expected_size


def test_context_manager_cleanup(tmp_path):
    log_file = tmp_path / "stdout.log"
    with RotatingLogWriter(str(log_file), max_size=100, total_max_size=500) as writer:
        writer.write("Test\n")
    assert writer._file.closed
