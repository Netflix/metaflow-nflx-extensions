import glob
import gzip
import os
import shutil
from typing import Any, Dict, Optional, TextIO, Type


class RotatingLogWriter:
    """
    A log writer that automatically rotates files based on size constraints.

    This class provides a file-like object that writes to a log file and automatically
    rotates the file when it reaches a specified maximum size. It also maintains a
    maximum total size for all log files by removing the oldest files when necessary.

    Features:
    - Automatic file rotation when a single file exceeds max_size
    - Total size management across all rotated files
    - Optional compression of rotated files
    - Context manager support for clean resource handling

    Usage:

    ```python
    with RotatingLogWriter('app.log', max_size=1024*1024, total_max_size=10*1024*1024) as writer:
        writer.write("Log message\n")
    ```
    """

    def __init__(
        self,
        base_path: str,
        max_size: int = 10 * 1024 * 1024,
        total_max_size: int = 50 * 1024 * 1024,
        compress: bool = False,
    ) -> None:
        """
        Initialize the RotatingLogWriter.

        Parameters
        -----------
        base_path : str
            Path to the log file. The log file will be created if it does not exist.
        max_size : int
            Maximum size in bytes for a single log file before rotation.
        total_max_size : int
            Maximum total size in bytes for all log files combined.
        compress : bool
            Whether to compress rotated files using gzip.
        """
        self.base_path: str = base_path
        self.max_size: int = max_size
        self.total_max_size: int = total_max_size
        self.compress: bool = compress
        self._file: Optional[TextIO] = None
        self._size: int = (
            os.path.getsize(self.base_path) if os.path.exists(self.base_path) else 0
        )  # Current file size
        self._rotated_sizes: Dict[str, int] = {}  # Physical sizes of rotated files
        self._total_size: int = self._calculate_total_size()

    def _calculate_total_size(self) -> int:
        """
        Calculate the total size of all log files.

        Returns
        -------
        int
            Total size in bytes of all log files
        """
        total = 0
        # Add current file size if it exists
        if os.path.exists(self.base_path):
            total += os.path.getsize(self.base_path)

        # Add sizes of rotated files
        patterns = [f"{self.base_path}.*"]
        for pattern in patterns:
            for file in glob.glob(pattern):
                if os.path.isfile(file):
                    size = os.path.getsize(file)
                    self._rotated_sizes[file] = size
                    total += size
        return total

    def __enter__(self) -> "RotatingLogWriter":
        """
        Enter the context manager, opening the log file for appending.

        Returns
        -------
        RotatingLogWriter
            Self for use in context manager
        """
        self._file = open(self.base_path, "a", encoding="utf-8")
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """
        Exit the context manager, closing the log file.

        Parameters
        ----------
        exc_type : Optional[Type[BaseException]]
            Exception type if an exception was raised
        exc_val : Optional[BaseException]
            Exception value if an exception was raised
        exc_tb : Optional[Any]
            Exception traceback if an exception was raised
        """
        if self._file:
            self._file.close()

    def write(self, data: str) -> None:
        """
        Write data to the log file, rotating if necessary.

        This method writes the provided data to the log file. If the file size
        exceeds max_size after writing, the file is rotated.

        Parameters
        -----------
        data : str
            The data to write to the log file.
        """
        if not self._file:
            raise RuntimeError(
                "File not opened. Use as context manager or open file first."
            )
        self._file.write(data)
        data_bytes = len(data.encode("utf-8"))
        self._size += data_bytes
        self._total_size += data_bytes
        if self._size > self.max_size:
            self._rotate()

    def _rotate(self) -> None:
        """
        Rotate the current log file.

        This method:
        1. Closes the current log file
        2. Renames it with a numbered suffix
        3. Compresses it if compression is enabled
        4. Opens a new log file
        5. Enforces the total size limit
        """
        if not self._file:
            raise RuntimeError(
                "File not opened. Use as context manager or open file first."
            )
        self._file.close()
        i = 1
        while os.path.exists(f"{self.base_path}.{i}") or os.path.exists(
            f"{self.base_path}.{i}.gz"
        ):
            i += 1
        rotated_file = f"{self.base_path}.{i}"
        os.rename(self.base_path, rotated_file)

        # Store the physical size of the rotated file
        physical_size = os.path.getsize(rotated_file)
        self._rotated_sizes[rotated_file] = physical_size

        if self.compress:
            compressed_file = f"{rotated_file}.gz"
            try:
                with open(rotated_file, "rb") as f_in:
                    with gzip.open(compressed_file, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(rotated_file)
                # Update with the compressed file's physical size
                compressed_size = os.path.getsize(compressed_file)
                self._total_size = self._total_size - physical_size + compressed_size
                self._rotated_sizes[compressed_file] = compressed_size
                del self._rotated_sizes[rotated_file]
            except Exception:
                # If compression fails, keep the original file
                pass

        self._file = open(self.base_path, "w", encoding="utf-8")
        self._size = 0
        self._enforce_total_size()

    def _enforce_total_size(self) -> None:
        """
        Enforce the total size limit by removing the oldest files if necessary.

        This method removes the oldest rotated files until the total size
        of all log files is below the total_max_size limit.
        """
        while self._total_size > self.total_max_size and self._rotated_sizes:
            oldest_file = self._get_oldest_file()
            if oldest_file:
                size = self._rotated_sizes.pop(oldest_file)
                os.remove(oldest_file)
                self._total_size -= size

    def _get_oldest_file(self) -> Optional[str]:
        """
        Get the oldest rotated log file.

        Returns
        -------
        Optional[str]
            The path to the oldest rotated log file, or None if no files exist.
        """
        rotated_files = list(self._rotated_sizes.keys())
        if not rotated_files:
            return None
        return min(
            rotated_files,
            key=lambda f: int(
                f.split(".")[-2] if f.endswith(".gz") else f.split(".")[-1]
            ),
        )

    def flush(self) -> None:
        """
        Flush the log file, ensuring all data is written to disk.
        """
        if not self._file:
            raise RuntimeError(
                "File not opened. Use as context manager or open file first."
            )
        self._file.flush()
