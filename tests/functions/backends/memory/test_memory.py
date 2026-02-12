import time
from multiprocessing import Process
import uuid

import pytest

pytestmark = pytest.mark.memory_only
from metaflow_extensions.nflx.plugins.functions.exceptions import MetaflowFunctionMemoryException
from metaflow_extensions.nflx.plugins.functions.memory.memory import ReadBuffer, WriteBuffer


def _write_loop(wb, ITERS, WORDS):
    count = 0
    wcount = 0
    while count < ITERS:
        with wb.acquire() as mem:
            if mem.valid:
                if not len(WORDS[wcount]):
                    msg = b""
                else:
                    msg = (f"{WORDS[wcount]} {count}").encode()
                mem.buf[0 : len(msg)] = msg
                mem.truncate(len(msg))
                wb.commit(mem)
                count += 1
                wcount += 1
                wcount = wcount % len(WORDS)
    wb.close()


def _read_loop(rb, ITERS, WORDS):
    count = 0
    wcount = 0
    while count < ITERS:
        with rb.acquire() as mem:
            if mem.valid:
                msg = mem.buf.tobytes().decode()
                if len(msg) != 0:
                    assert msg == f"{WORDS[wcount]} {count}"
                rb.free(mem)
                count += 1
                wcount += 1
                wcount = wcount % len(WORDS)
    rb.close()


# Module-level functions for multiprocessing (required for macOS spawn method)
def _reader_process(ITERS, WORDS, NAME):
    rb = ReadBuffer(NAME)
    _read_loop(rb, ITERS, WORDS)


def _writer_process(ITERS, WORDS, NAME):
    wb = WriteBuffer(NAME)
    _write_loop(wb, ITERS, WORDS)


@pytest.mark.parametrize("owner", ["reader", "writer"])
@pytest.mark.parametrize("rings", [1, 2, 10, 50])
def test_basic_memory(owner, rings):
    ITERS = 100
    WORDS = ["", "hello", "hola", "h"]
    NAME = "metaflow_function_memory-" + str(uuid.uuid4())[0:8]

    if owner == "writer":
        wb = WriteBuffer(NAME, owns=True, buffer_size=100, num_rings=rings)
        try:
            read_p = Process(target=_reader_process, args=(ITERS, WORDS, NAME))
            read_p.start()
            _write_loop(wb, ITERS, WORDS)
            read_p.join()

            assert read_p.exitcode == 0
        finally:
            # Ensure cleanup happens even if test fails or wb wasn't closed in _write_loop
            if hasattr(wb, "_shm") and wb._shm is not None:
                wb.close()

    else:
        rb = ReadBuffer(NAME, owns=True, buffer_size=100, num_rings=rings)
        try:
            write_p = Process(target=_writer_process, args=(ITERS, WORDS, NAME))
            write_p.start()
            _read_loop(rb, ITERS, WORDS)
            write_p.join()

            assert write_p.exitcode == 0
        finally:
            # Ensure cleanup happens even if test fails or rb wasn't closed in _read_loop
            if hasattr(rb, "_shm") and rb._shm is not None:
                rb.close()


def test_multiple_owners():
    UUID = str(uuid.uuid4())[0:8]
    name = "test_multiple_owners-" + UUID
    wb1 = WriteBuffer(name, owns=True)
    try:
        with pytest.raises(MetaflowFunctionMemoryException):
            wb2 = WriteBuffer(name, owns=True)
    finally:
        wb1.close()


def test_nonexistent_buffer(monkeypatch):
    monkeypatch.setattr(
        "metaflow_extensions.nflx.plugins.functions.memory.memory.INIT_TIMEOUT", 0.1
    )  # Reduce timeout to .1s
    UUID = str(uuid.uuid4())[0:8]
    name = "nonexistent_buffer-" + UUID
    with pytest.raises(MetaflowFunctionMemoryException):
        wb = WriteBuffer(name, owns=False)


def test_insufficient_memory(monkeypatch):
    monkeypatch.setattr(
        "metaflow_extensions.nflx.plugins.functions.memory.memory.dev_shm_size",
        lambda free: 1000 if free else 1000,
    )
    UUID = str(uuid.uuid4())[0:8]
    name = "test_insufficient-" + UUID
    with pytest.raises(MetaflowFunctionMemoryException):
        _ = WriteBuffer(name, owns=True, buffer_size=1000)
