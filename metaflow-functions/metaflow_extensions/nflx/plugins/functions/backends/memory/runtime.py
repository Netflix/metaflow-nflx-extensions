import os
import select
import shutil
import subprocess
import threading
import time
import uuid
from collections import defaultdict
from typing import IO, Dict, List, Optional, cast, Any

from metaflow_extensions.nflx.config.mfextinit_functions import FUNCTION_RUNTIME_PATH
from metaflow_extensions.nflx.plugins.functions.debug import debug
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionRuntimeException,
)
from metaflow_extensions.nflx.plugins.functions.backends.memory.runtime_config import (
    RuntimeConfig,
)
from metaflow_extensions.nflx.plugins.functions.config import Config
from metaflow_extensions.nflx.plugins.functions.logger.logger import (
    RotatingLogWriter,
)
from metaflow_extensions.nflx.plugins.functions.common.runtime_utils import (
    set_shell_env,
)
from metaflow_extensions.nflx.plugins.functions.core.function import (
    MetaflowFunction,
)


# Global reference counter for shared directories (thread-safe)
_dir_ref_count: Dict[str, int] = defaultdict(int)
_dir_ref_lock = threading.Lock()


def _increment_dir_ref(dir_path: str) -> None:
    """Increment reference count for a directory path."""
    with _dir_ref_lock:
        _dir_ref_count[dir_path] += 1


def _decrement_dir_ref(dir_path: str) -> bool:
    """
    Decrement reference count for a directory path.

    Returns
    -------
    bool
        True if this was the last reference and directory should be cleaned up
    """
    with _dir_ref_lock:
        _dir_ref_count[dir_path] -= 1
        should_cleanup = _dir_ref_count[dir_path] <= 0
        if should_cleanup:
            del _dir_ref_count[dir_path]
        return should_cleanup


class FunctionRuntime:
    """
    Manages a group of FunctionRuntimeInstances for multi-process management.
    """

    def __init__(
        self,
        function: MetaflowFunction,
        base_path: Optional[str] = None,
        process: int = 1,
    ) -> None:
        """
        Create a group of FunctionRuntimeInstances sized to the number
        of 'process' specified.

        Parameters
        ----------
        function : MetaflowFunction
            The Metaflow Function to load and monitor
        base_path : str, optional, default None
            Base path for all file operations
        process : int, optional, default 1
            number of processes
        """
        base_path = base_path if base_path else FUNCTION_RUNTIME_PATH
        self._function_dir = None
        self._constituent_dirs = None
        self.connection_params: Optional[Dict[str, Any]] = None
        self.io: Optional[Dict[str, Any]] = None
        self._runtime: Dict[str, Any] = {}

        # Track function identity
        self.function = function
        self.function_spec = self.function.spec
        if not self.function_spec.uuid:
            raise MetaflowFunctionRuntimeException("Metaflow Function uuid is missing")
        if not self.function_spec.reference:
            raise MetaflowFunctionRuntimeException(
                "Metaflow Function reference is missing"
            )
        self.instance_id = str(uuid.uuid4())[0:8]
        self.function_id = self.function_spec.uuid
        self.uuid: str = self.function_id + "-" + self.instance_id

        # Generate path information
        constituent_ids = []
        if self.function_spec.system_metadata is not None:
            functions = self.function_spec.system_metadata.get("functions", [])
            for func in functions:
                constituent_ids.append(func["uuid"])

        self._constituent_dirs = [
            os.path.join(base_path, f"{Config.RUNTIME_FUNCTION_DIR_PREFIX}{fid}")
            for fid in constituent_ids
        ]
        self._function_dir = os.path.join(
            base_path, f"{Config.RUNTIME_FUNCTION_DIR_PREFIX}{self.function_id}"
        )

        # Increment reference count for shared directory
        _increment_dir_ref(self._function_dir)

        try:
            # Create the environment if necessary
            # Lazy import to avoid circular imports
            from metaflow_extensions.nflx.plugins.functions.environment import (
                extract_code_packages,
                resolve_conda_environment,
                generate_trampolines_for_directory,
            )

            from metaflow_extensions.nflx.plugins.functions.backends.memory.memory_backend import (
                MemoryBackend,
            )

            # Extract code
            code_dir = extract_code_packages(
                cast(str, self.function_spec.code_package),
                cast(str, self.function_spec.task_code_path),
                self._function_dir,
            )

            # Resolve environment
            self.interpreter = resolve_conda_environment(
                cast(Dict[str, Any], self.function_spec.system_metadata)
            )

            # Generate trampolines - verify directory exists first
            if not os.path.exists(code_dir):
                raise MetaflowFunctionRuntimeException(
                    f"Code directory {code_dir} does not exist after extraction. "
                    "This may indicate a race condition during parallel function initialization."
                )
            generate_trampolines_for_directory(code_dir)

            # Generate IO connection parameters
            # First check if function has prefetch_artifacts flag set
            # (e.g., from start_runtime=True)
            prefetch_artifacts = getattr(function, "_prefetch_artifacts", False)
            self.connection_params = MemoryBackend.generate_connection_params(
                self.uuid,
                prefetch_artifacts=prefetch_artifacts,
                process=process,
            )

            # Open IO buffers
            self.io = MemoryBackend.open_connection(self.connection_params)

            # Start the runtimes
            for i in range(process):
                fri = FunctionRuntimeInstance(
                    self._function_dir,
                    self.interpreter,
                    self.connection_params,
                    self.function.reference,
                    self.uuid,
                )
                self._runtime[fri.uuid] = fri

        except Exception as e:
            self.close(clean_dir=True)
            raise e

    def start(self):
        """
        Start all runtimes that are not already running.
        """
        if self._runtime:
            for runtime_instance in self._runtime.values():
                if not runtime_instance.alive:
                    runtime_instance.start()

    def process_io(self, timeout: float = 0.1) -> None:
        """
        Process available I/O from subprocess pipes using select.
        Writes data to the appropriate stream.

        Parameters
        ----------
        timeout : float, default 0.1
            The timeout for the select operation, defaults to 0.1
        """
        if self._runtime:
            for runtime_instance in self._runtime.values():
                runtime_instance.process_io(timeout)

    @property
    def failed(self) -> bool:
        """
        Return True if any subprocess failed.

        Returns False if still running or exited with code 0 (normal exit).
        Returns True if process wasn't started or failed.

        Returns
        -------
        bool
            True if any process failed, False otherwise
        """
        if not self._runtime:
            return True

        return any(
            runtime_instance.failed for runtime_instance in self._runtime.values()
        )

    def close(self, clean_dir: bool = True) -> None:
        """
        Shutdown all runtimes

        Parameters
        ----------
        clean_dir : bool
            Whether to clean up the temporary directory, defaults to True
        """
        # CRITICAL: Terminate subprocess BEFORE closing IO connections
        # to prevent race condition where subprocess tries to access
        # semaphores that have been unlinked
        if self._runtime:
            for runtime_instance in self._runtime.values():
                runtime_instance.close()

        # Now that subprocess has terminated, safe to close IO connections
        # which will unlink semaphores
        debug.functions_exec("Close IO connections")
        if hasattr(self, "io") and self.io is not None:
            # Close MemoryBackend connection (subprocess-based execution)
            # Lazy import to avoid circular imports
            from metaflow_extensions.nflx.plugins.functions.backends.memory.memory_backend import (
                MemoryBackend,
            )

            MemoryBackend.close_connection(self.io)
            self.io = None

        def clean_one_dir(fdir):
            if _decrement_dir_ref(fdir):
                if not RuntimeConfig.IS_DEBUG:
                    debug.functions_exec(f"Remove directory {fdir}")
                    try:
                        shutil.rmtree(fdir)
                    except FileNotFoundError:
                        pass
            else:
                debug.functions_exec(
                    f"Directory {fdir} still has active references, not removing"
                )

        # Cleanup the home directory using reference counting
        # Only cleanup if this is the last reference to the directory
        if clean_dir and self._function_dir:
            clean_one_dir(self._function_dir)
            self._function_dir = None

        if clean_dir and self._constituent_dirs:
            for fdir in self._constituent_dirs:
                clean_one_dir(fdir)
            self._constituent_dirs = None

    def raise_on_process_exit(self, context: str = "", clean_dir: bool = True) -> None:
        """
        Raise if the process exited with an error code. Close the
        runtime, it must be restarted after a process error.
        """
        if self._runtime:
            for runtime_instance in self._runtime.values():
                try:
                    runtime_instance.raise_on_process_exit(context)

                except Exception as e:
                    self.close(clean_dir)
                    raise e


class FunctionRuntimeInstance:
    """
    A FunctionRuntime has a few main functions:
    1) Launch a system's runtime as a subprocess
    2) Monitor the subprocess
    3) Manage subprocess lifecycle and logging
    4) Wait for subprocess completion
    5) Multiple runtime objects can use the same directory
    """

    def __init__(
        self,
        function_dir: str,
        interpreter: str,
        connection_params: Dict[str, Any],
        reference: str,
        parent_uuid: str,
    ) -> None:
        """
        Start the Metaflow Function runtime and initialize the control
        object.  When the object is created, the function is ready to
        accept data. After close is called the object cannot be
        reused.

        Parameters
        ----------
        function_dir : str
            Base directory for the function
        interpreter : str
            Path to the Python interpreter
        connection_params : Dict[str, Any]
            Backend-specific connection parameters
        reference : str
            Path to function reference file
        parent_uuid : str
            UUID of the parent FunctionRuntime
        """
        self._process = None
        self._function_dir = function_dir
        self._interpreter = interpreter
        self._connection_params = connection_params
        self._reference = reference
        self._parent_uuid = parent_uuid

        # Generate unique instance ID
        self.instance_id = str(uuid.uuid4())[0:8]
        self.uuid: str = parent_uuid + "-" + self.instance_id

        # Set up log file paths
        fname = f"{self.instance_id}-stdout.log"
        errfname = f"{self.instance_id}-stderr.log"
        self.log_fname = os.path.join(self._function_dir, fname)
        self.errlog_fname = os.path.join(self._function_dir, errfname)
        debug.functions_exec(f"Function log file: {self.log_fname}")
        debug.functions_exec(f"Function error log file: {self.errlog_fname}")

        self._logs = ""
        self._errlogs = ""
        try:
            # Set up RotatingLogWriter instances
            self.stdout_writer: Optional[RotatingLogWriter] = RotatingLogWriter(
                self.log_fname,
                max_size=RuntimeConfig.MAX_SINGLE_LOG_FILE_SIZE_BYTES,
                total_max_size=RuntimeConfig.MAX_TOTAL_LOG_FILE_SIZE_BYTES,
            )
            self.stderr_writer: Optional[RotatingLogWriter] = RotatingLogWriter(
                self.errlog_fname,
                max_size=RuntimeConfig.MAX_SINGLE_LOG_FILE_SIZE_BYTES,
                total_max_size=RuntimeConfig.MAX_TOTAL_LOG_FILE_SIZE_BYTES,
            )
            self.stdout_writer.__enter__()
            self.stderr_writer.__enter__()

            # Get the command to run
            self._generated_command = self._command(
                self._interpreter,
                self._connection_params,
                self._reference,
            )

            # Start subprocess
            if not self.start():
                errlogs = self.errlogs
                raise MetaflowFunctionRuntimeException(
                    f"Failed to start subprocess: {errlogs}"
                )

        except Exception as e:
            self.close()
            raise e

    def start(self):
        # Launch the command
        debug.functions_exec(
            f"Starting process with command: {self._generated_command} in cwd {self._function_dir}"
        )
        function_dir = f"{self._function_dir}"

        self._process = subprocess.Popen(
            self._generated_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=set_shell_env(function_dir),
            cwd=self._function_dir,
        )

        # Check if we started
        if not self.alive:
            debug.functions_exec(
                f"Subprocess for function '{self._reference}' failed to start"
            )
            return False
        debug.functions_exec(f"Subprocess for function '{self._reference}' started")
        return True

    def _command(
        self,
        interpreter: str,
        connection_params: Dict[str, Any],
        reference: str,
    ) -> List[str]:
        """
        Generate the backend-specific command to run the function runtime subprocess.

        Parameters
        ----------
        interpreter : str
            The path to the interpreter to use for the subprocess
        connection_params : Dict[str, Any]
            Backend-specific connection parameters
        reference : str
            The path to the system reference file
        """
        # Get the runtime command from MemoryBackend (subprocess-based execution)
        # Lazy import to avoid circular imports
        from metaflow_extensions.nflx.plugins.functions.backends.memory.memory_backend import (
            MemoryBackend,
        )

        return MemoryBackend.get_runtime_command(
            connection_params, reference, interpreter
        )

    def process_io(self, timeout: float = 0.1) -> None:
        """
        Process available I/O from subprocess pipes using select.
        Writes data to the appropriate stream.

        Parameters
        ----------
        timeout : float, default 0.1
            The timeout for the select operation, defaults to 0.1
        """
        if not self._process:
            return

        readable, _, _ = select.select(
            [self._process.stdout, self._process.stderr], [], [], timeout
        )

        for stream in readable:
            self._read_stream_data(stream)

    def _read_stream_data(self, stream: Optional[IO[bytes]] = None) -> None:
        """
        Read data from a stream and write it to the appropriate writer.

        Parameters
        ----------
        stream : IO[bytes], optional, default None
            The stream to read data from
        """
        if not stream:
            return

        writer = (
            self.stdout_writer
            if self._process is not None and stream is self._process.stdout
            else self.stderr_writer
        )
        data = os.read(stream.fileno(), RuntimeConfig.STREAM_READ_BUFFER_SIZE)

        if data and writer is not None:
            writer.write(data.decode("utf-8"))
            writer.flush()

    def _read_all_stream_data(self, stream: Optional[IO[bytes]] = None) -> None:
        """
        Read all remaining data from a stream until EOF.

        Parameters
        ----------
        stream : IO[bytes], optional, default None
            The stream to read data from
        """
        if not stream:
            return

        writer = (
            self.stdout_writer
            if self._process is not None and stream is self._process.stdout
            else self.stderr_writer
        )

        while True:
            data = os.read(stream.fileno(), RuntimeConfig.STREAM_READ_BUFFER_SIZE)
            if not data:  # Empty byte string indicates EOF
                break
            if writer is not None:
                writer.write(data.decode("utf-8"))
                writer.flush()

    @property
    def alive(self) -> bool:
        """
        Return true if this subprocess is still alive.

        Returns
        -------
        bool
            True if the process is alive, False if never started or terminated
        """
        if not self._process:
            return False
        return self._process.poll() is None

    @property
    def return_code(self) -> Optional[int]:
        """
        Get the process return code, None if the process is still
        running or -100 if it never started

        Returns
        -------
        Optional[int]
            The return code of the process, None if still running, -100 if never started
        """
        if not self._process:
            return -100
        return self._process.returncode

    @property
    def failed(self) -> bool:
        """
        Return True if the subprocess failed.

        Returns False if still running or exited with code 0 (normal exit).
        Returns True if process wasn't started or failed.

        Returns
        -------
        bool
            True if the process failed, False otherwise
        """
        if not self._process:
            return True

        returncode = self._process.poll()
        if returncode is None:
            return False

        if self._process.returncode != 0:
            return True

        return False

    @property
    def pid(self) -> Optional[int]:
        """
        Process ID of the function system, None if the process never
        started

        Returns
        -------
        Optional[int]
            The process ID if started, None otherwise
        """
        if not self._process:
            return None
        return self._process.pid

    def wait(self, timeout: float = RuntimeConfig.WAIT_TIMEOUT) -> bool:
        """
        Wait until the subprocess exits, processing I/O in the meantime.
        Returns True if exited or never started, False if timeout expires.

        Parameters
        ----------
        timeout : float, default WAIT_TIMEOUT
            The timeout for waiting, defaults to WAIT_TIMEOUT

        Returns
        -------
        bool
            True if process exited or never started, False if timeout expires
        """
        if not self._process:
            return True

        start_time = time.time()
        while True:
            if self._process.poll() is not None:
                return True

            self.process_io()
            if time.time() - start_time > timeout:
                return False

    def terminate(self) -> None:
        """
        Shutdown process gracefully
        """
        if self._process is not None:
            self._process.terminate()

    def kill(self) -> None:
        """
        Kill the function system
        """
        if self._process is not None:
            self._process.kill()

    def close(self) -> None:
        """
        Terminate the subprocess, read remaining output, and close log writers.

        The process is terminated gracefully, then killed if it doesn't exit within
        the timeout period. All remaining data from stdout/stderr is read before
        closing the streams.
        """
        # Try to terminate gracefully, and wait for the process to
        # exit, if it timeouts then kill it.
        if self.alive:
            debug.functions_exec("Terminate process")
            self.terminate()

        # Wait for the process to exit or kill it
        debug.functions_exec("Waiting for process to terminate")
        if not self.wait():
            debug.functions_exec("Waiting timeout, killing process")
            self.kill()

        # Read all remaining data and close the pipes.
        debug.functions_exec("Read and close remaining buffers")
        if hasattr(self, "_process") and self._process:
            if self._process.stdout:
                self._read_all_stream_data(self._process.stdout)
                self._process.stdout.close()

            if self._process.stderr:
                self._read_all_stream_data(self._process.stderr)
                self._process.stderr.close()

            self._process = None

        # Buffer the logs so they are available after close
        self._logs = self.logs
        self._errlogs = self.errlogs

        # Close logging
        if hasattr(self, "stdout_writer") and self.stdout_writer is not None:
            self.stdout_writer.__exit__(None, None, None)
        if hasattr(self, "stderr_writer") and self.stderr_writer is not None:
            self.stderr_writer.__exit__(None, None, None)
        self.stdout_writer = None
        self.stderr_writer = None

    def raise_on_process_exit(self, context: str = "") -> None:
        """
        Raise if the process exited with an error code. Close the
        runtime, it must be restarted after a process error.
        """
        # If we are already alive exit
        if self.alive:
            return

        if self.failed:
            # Print logs and throw exception
            return_code = self.return_code

            # Get any remaining log data
            self.process_io()

            # Grab the logs
            logs = self.logs
            errlogs = self.errlogs

            # Close IO maps
            self.close()

            # Throw an exception
            debug.functions_exec(
                f"Subprocess failed: {context}, return_code={return_code}"
            )
            if return_code == RuntimeConfig.USER_EXIT_CODE:
                raise MetaflowFunctionRuntimeException(
                    f"Subprocess terminated with user error ({context}): {errlogs}"
                )

            if return_code == RuntimeConfig.SYSTEM_EXIT_CODE:
                raise MetaflowFunctionRuntimeException(
                    f"Subprocess terminated with system error ({context}): {errlogs}"
                )

            if return_code == -9:
                raise MetaflowFunctionRuntimeException(
                    f"Subprocess was force terminated ({context}): {errlogs}"
                )

            raise MetaflowFunctionRuntimeException(
                f"Subprocess terminated with error code '{return_code}' ({context}): {errlogs}"
            )

    @property
    def logs(self) -> str:
        """
        Get the latest logs

        Returns
        -------
        str
            The contents of the log file
        """
        if self._logs:
            return self._logs

        if self._process is not None and self._process.poll() is not None:
            self._read_all_stream_data(self._process.stdout)
        try:
            with open(self.log_fname, "r") as fh:
                return fh.read()
        except FileNotFoundError:
            return ""

    @property
    def errlogs(self) -> str:
        """
        Get the latest error logs

        Returns
        -------
        str
            The contents of the error log file
        """
        if self._errlogs:
            return self._errlogs

        if self._process is not None and self._process.poll() is not None:
            self._read_all_stream_data(self._process.stderr)
        try:
            with open(self.errlog_fname, "r") as fh:
                return fh.read()
        except FileNotFoundError:
            return ""
