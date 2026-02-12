import os
import stat
import errno

from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionMemoryException,
    MetaflowFunctionMemorySignalException,
)


LIBRT = None
FFI = None


def lazy_import_librt():
    global LIBRT
    global FFI
    if not LIBRT:
        from .librt import LIBRT, FFI


# errno -> string mapping
_sem_error_str = {
    errno.EACCES: "Permission to the semaphore object was denied.",
    errno.EEXIST: "Both O_CREAT and O_EXCL were specified with open but the semaphore name already exists.",
    errno.EINVAL: "The semaphore name is invalid or specified value is too large.",
    errno.ENAMETOOLONG: "The semaphore name is too long, see PATH_MAX on this system.",
    errno.ENFILE: "The system-wide limit on the total number of open files has been reached.",
    errno.EMFILE: "The per-process limit on the number of open file descriptors has been reached.",
    errno.ENOENT: "Attempt to open or unlink a semaphore name that does not exist and O_CREAT was not specified.",
    errno.ENOMEM: "Insufficient memory for semaphore.",
}


def errno_to_str(error_no):
    if error_no in _sem_error_str:
        return _sem_error_str[error_no]
    return f"Unknown error code ({error_no})."


class Semaphore:
    def __init__(self, name: str, initial_value: int = 1):
        """
        Wrapper for a librt semaphore
        """
        lazy_import_librt()  # for semaphore

        self._open = False
        self.name = name
        self.initial_value = initial_value

        # Open a named semaphore
        c_mode = FFI.cast("int", stat.S_IRWXU)  # type: ignore
        c_initial_val = FFI.cast("int", self.initial_value)  # type: ignore
        self._sem_name = b"/" + self.name.encode()
        self._sem = LIBRT.sem_open(self._sem_name, os.O_CREAT, c_mode, c_initial_val)  # type: ignore
        if self._sem == FFI.NULL:  # type: ignore
            err_str = errno_to_str(FFI.errno)  # type: ignore
            raise MetaflowFunctionMemoryException(
                f"Failed to open an operating system semaphore '{self._sem_name.decode()}'. {err_str}"
            )
        self._open = True

    def _handle_error(self):
        error_code = FFI.errno
        if error_code == errno.EINTR:  # call interupted by signal handler
            raise MetaflowFunctionMemorySignalException(
                f"Semaphore '{self.name}' shutdown from signal handler"
            )
        elif error_code == errno.EINVAL:  # not a valid signal handler
            raise MetaflowFunctionMemoryException(f"Semaphore '{self.name}' Invalid")

    def post(self):
        if not self._open:
            raise MetaflowFunctionMemoryException(
                f"Semaphore '{self._sem_name.decode()}' is not open."
            )
        if LIBRT.sem_post(self._sem) < 0:
            self._handle_error()

    def wait(self):
        if not self._open:
            raise MetaflowFunctionMemoryException(
                f"Semaphore '{self._sem_name.decode()}' is not open."
            )
        if LIBRT.sem_wait(self._sem) < 0:
            self._handle_error()

    def close(self):
        if self._open:
            LIBRT.sem_close(self._sem)
            LIBRT.sem_unlink(self._sem_name)
            self._open = False


class Lock:
    """
    Build a lock with a librt semaphore
    """

    def __init__(self, semaphore: Semaphore):
        # import lib rt for semaphore
        self._sem = semaphore

    def __enter__(self):
        self._sem.wait()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._sem.post()
        return False  # False will re-raise any exceptions
