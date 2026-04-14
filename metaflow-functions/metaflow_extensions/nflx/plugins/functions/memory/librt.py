import cffi
import platform

FFI = cffi.FFI()

# Define semaphore header interface
FFI.cdef(
    """
typedef struct { } sem_t;
sem_t *sem_open(const char *name, int oflag, ...);
int sem_close(sem_t *sem);
int sem_unlink(const char *name);
int sem_wait(sem_t *sem);
int sem_post(sem_t *sem);
"""
)

# On Linux, semaphores are in librt. On macOS, they're in libc (the C standard library).
try:
    if platform.system() == "Darwin":
        # macOS: semaphores are in libc
        LIBRT = FFI.dlopen(None)  # None loads the C standard library
    else:
        # Linux: semaphores are in librt
        LIBRT = FFI.dlopen("rt")
except Exception:
    pass
