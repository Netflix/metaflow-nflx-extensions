from multiprocessing import shared_memory, Process
import struct
import time
import os
from random import random

from metaflow_extensions.nflx.plugins.functions.exceptions import MetaflowFunctionMemoryException
from metaflow_extensions.nflx.plugins.functions.memory.concurrency import Semaphore, Lock

import pytest

pytestmark = pytest.mark.memory_only

MAX_VAL = 100000


def rand_sleep():
    if random() < 0.5:
        time.sleep(0.00001)


# Module-level worker functions (required for macOS spawn method)
def worker_basic(name, shm_name):
    shm = shared_memory.SharedMemory(name=shm_name)
    sem = Semaphore("test_basic_semaphore")
    while True:
        rand_sleep()
        value = 0
        with Lock(sem):
            value = struct.unpack("<i", shm.buf[0:4])[0]
            if value >= MAX_VAL:
                break
            value += 1
            shm.buf[0:4] = struct.pack("<i", value)
    sem.close()


def worker_fail_sem(name, shm_name):
    shm = shared_memory.SharedMemory(name=shm_name)
    sem = Semaphore("test_fail_sem_semaphore")
    if name == "w3":
        os.kill(os.getpid(), 9)

    while True:
        rand_sleep()
        value = 0
        with Lock(sem):
            value = struct.unpack("<i", shm.buf[0:4])[0]
            if value >= MAX_VAL:
                break
            value += 1
            shm.buf[0:4] = struct.pack("<i", value)
    sem.close()


def worker_fail_critical(name, shm_name):
    shm = shared_memory.SharedMemory(name=shm_name)
    sem = Semaphore("test_fail_critical_semaphore")
    while True:
        rand_sleep()
        value = 0
        with Lock(sem):
            if name == "w3":
                os.kill(os.getpid(), 9)

            value = struct.unpack("<i", shm.buf[0:4])[0]
            if value >= MAX_VAL:
                break
            value += 1
            shm.buf[0:4] = struct.pack("<i", value)
    sem.close()


def test_basic():
    """
    Open shared memory and test atomic incrementing the shared memory
    """
    shm = shared_memory.SharedMemory(create=True, size=4)

    workers = [Process(target=worker_basic, args=(f"w{i}", shm.name)) for i in range(5)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    val = struct.unpack("<i", shm.buf[0:4])[0]
    shm.close()
    shm.unlink()
    assert val == MAX_VAL


def test_fail_sem():
    """
    Check if process termination after sem acquisition works
    """
    shm = shared_memory.SharedMemory(create=True, size=4)

    workers = [
        Process(target=worker_fail_sem, args=(f"w{i}", shm.name)) for i in range(5)
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    val = struct.unpack("<i", shm.buf[0:4])[0]
    shm.close()
    shm.unlink()
    assert val == MAX_VAL


def test_fail_critical():
    """
    Failing in a critical section may leave the sem in a wait state
    for other process. to mock a process monitor, check that if a
    process died with kill -9 then make an extra post.
    """
    shm = shared_memory.SharedMemory(create=True, size=4)

    workers = [
        Process(target=worker_fail_critical, args=(f"w{i}", shm.name)) for i in range(5)
    ]
    for w in workers:
        w.start()

    pids = [w.pid for w in workers]
    while pids:
        pids_to_remove = []
        for pid in pids:
            try:
                pid_returned, status = os.waitpid(pid, os.WNOHANG)
                if pid_returned == pid:
                    if os.WIFEXITED(status):
                        pids_to_remove.append(pid)
                    elif os.WIFSIGNALED(status):
                        pids_to_remove.append(pid)
                        sem = Semaphore("test_fail_critical_semaphore")
                        sem.post()
                        sem.close()
            except ChildProcessError:
                # Process already reaped or no longer exists - remove from tracking
                pids_to_remove.append(pid)

        for pid in pids_to_remove:
            pids.remove(pid)

    for w in workers:
        w.join()

    val = struct.unpack("<i", shm.buf[0:4])[0]
    shm.close()
    shm.unlink()
    assert val == MAX_VAL
