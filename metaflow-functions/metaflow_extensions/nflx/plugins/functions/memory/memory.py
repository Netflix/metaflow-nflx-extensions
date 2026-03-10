import ctypes
import os
import platform
import struct
import time
from dataclasses import dataclass, astuple
from multiprocessing.shared_memory import SharedMemory
from abc import ABC
from typing import Optional

from metaflow_extensions.nflx.plugins.functions.debug import debug
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionMemoryException,
)
from metaflow_extensions.nflx.plugins.functions.config import (
    get_default_buffer_size,
    get_min_buffer_size,
)

from .concurrency import Semaphore, Lock

IS_DEBUG = 0  # os.environ.get("METAFLOW_DEBUG_FUNCTIONS", "") == "1"


def dev_shm_size(free: bool = False) -> int:
    """
    Returns the size of shared memory in bytes.
    On Linux, this checks /dev/shm. On macOS, this returns available system memory
    since macOS manages shared memory differently.
    """
    system = platform.system()

    if system == "Darwin":
        # macOS: Use psutil for accurate memory info
        # macOS doesn't have a /dev/shm limit, it uses swap/system memory
        import psutil

        mem = psutil.virtual_memory()
        return mem.available if free else mem.total
    else:
        # Linux: Use /dev/shm
        try:
            statvfs = os.statvfs("/dev/shm")
        except FileNotFoundError:
            raise MetaflowFunctionMemoryException(
                "Shared memory must be enabled to use 'metaflow.functions'. '/dev/shm' not found."
            )
        if free:
            m = statvfs.f_bfree
        else:
            m = statvfs.f_blocks

        return statvfs.f_frsize * m


def _aligned_buffer_offset(buf: memoryview, alignment: int) -> int:
    base_address = ctypes.addressof(ctypes.c_char.from_buffer(buf))
    offset = (alignment - (base_address % alignment)) % alignment
    return offset


def _reserve_memory(shm: SharedMemory) -> None:
    free_shm = dev_shm_size(free=True)
    if free_shm < shm.size:
        raise MetaflowFunctionMemoryException(
            "Cannot reserve %d shared memory when only %d is available."
            % (shm.size, free_shm)
        )
    base_address = ctypes.addressof(ctypes.c_char.from_buffer(shm.buf))
    ctypes.memset(base_address, 0, shm.size)


def _compute_header_size(num_rings):
    """
    Header (config + control + data_desc)
    """
    control_bytes = num_rings * CONTROL_BYTES_BASE
    data_desc_bytes = num_rings * DATA_DESC_BYTES_BASE
    return CONTROL_OFFSET + control_bytes + data_desc_bytes


def _compute_total_shm_size(buffer_size, num_rings):
    """
    Total SHM required for buffer_size and num_rings
    """
    constant = _compute_header_size(num_rings)
    total_size = (buffer_size + ALIGN_BYTES) * num_rings + constant
    return int(total_size)


def _compute_ring_size(size, num_rings):
    """
    Full ring size including alignment bytes from an buffer size
    """
    constant = _compute_header_size(num_rings)
    ring_size = (size - constant) // num_rings
    return int(ring_size)


def _compute_data_desc_offset(num_rings):
    """
    Get the data_desc offset
    """
    control_bytes = num_rings * CONTROL_BYTES_BASE
    return CONTROL_OFFSET + control_bytes


@dataclass
class Config:
    nrings: int  # 4 byte uint, number of rings/slots in the memory


@dataclass
class Count:
    write: int  # 4 byte uint, number of rings/slots written
    read: int  # 4 byte uint, number of rings/slots read
    w_ring: int  # 4 byte uint, last write ring written
    r_ring: int  # 4 byte uint, last read ring written


@dataclass
class Control:
    assigned: (
        bool  # 4 byte bool as int, whether this slot has been assigned to a writer
    )
    ready: bool  # 4 byte bool as int, whether this slot if ready for reading
    uuid: bytes  # 64 byte char, the uuid of the request
    pid: int  # 8 byte uint, the pid of the last processor to work on this buffer
    written: int  # 8 byte uint, the number of bytes written to this memory region


@dataclass
class Data:
    offset: int  # 8 byte uint
    size: int  # 8 byte uint


# Default settings
SHM_RETRY_INTERVAL = 0.001
INIT_TIMEOUT = 1  # seconds
DEFAULT_NUM_RINGS = 2
DEFAULT_BUFFER_SHM = get_default_buffer_size()
MIN_BUFFER_SHM = get_min_buffer_size()
ALIGN_BYTES = 64  # Number of bytes for alignment
UUID_BYTES = 64

# SHM offsets
CONFIG_OFFSET = 0
CONFIG_BYTES = 4
COUNT_OFFSET = CONFIG_OFFSET + CONFIG_BYTES
COUNT_BYTES = 16
CONTROL_OFFSET = CONFIG_OFFSET + CONFIG_BYTES + COUNT_BYTES
CONTROL_BYTES_BASE = 24 + UUID_BYTES
DATA_DESC_BYTES_BASE = 16

# binary formats
CONFIG_FORMAT = "@I"
COUNT_FORMAT = "@IIII"
CONTROL_FORMAT = "@II64sQQ"
DATA_DESC_FORMAT = "@QQ"

# Zeros
CONFIG_ZEROS = bytearray(CONFIG_BYTES)
UUID_ZEROS = bytearray(UUID_BYTES)


class BufferBase(ABC):
    """
    Base class for a read/write shared memory buffer for use in
    multiprocessing environments.
    Key features:
    - async friendly
    - multi-process friendly
    - posix compliant, implementable in many languages
    """

    class MemoryReference:
        """
        A class to represent a memory reference. It holds the underlying
        buffer and metadata necessary to reference it in the larger memory
        space.
        """

        def __init__(self, id=None, buf=None, uuid=None, pid=None):
            self.id = id
            self._buf = buf
            self.uuid = uuid
            self.pid = pid
            if self._buf is None:
                self.size = 0
            else:
                self.size = len(self._buf)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.close()

        @property
        def valid(self):
            if self._buf is not None:
                return True
            return False

        @property
        def buf(self):
            return self._buf[0 : self.size]

        def truncate(self, size: int):
            if size < 0:
                raise MetaflowFunctionMemoryException(
                    f"Invalid size for truncate: {size}. Size must be non-negative."
                )
            if self._buf is not None and size > len(self._buf):
                raise MetaflowFunctionMemoryException(
                    f"Invalid size for truncate: {size}. Size exceeds buffer length {len(self._buf)}."
                )
            self.size = size

        def close(self):
            self.id = None
            self._buf = None
            self.uuid = None
            self.pid = None
            self.size = 0

    def __init__(
        self,
        name: str,
        owns: bool = False,
        buffer_size: Optional[int] = None,
        num_rings: Optional[int] = None,
        reserve: bool = True,
    ) -> None:
        """
        Create a buffer object. Do not call this class directly but
        rather through one of the derived classes.

        num_rings and buffer_size, and reserve are only applied if the
        buffer is the owner.
        """
        self._name = None
        self._shm = None
        self._size = 0
        self._owns = owns  # Store ownership for proper cleanup

        # macOS POSIX shm_open has a 31-char limit (shm_open prepends '/' automatically)
        # Shorten names to fit: keep user-visible part under 30 chars
        original_name = name
        if platform.system() == "Darwin":
            # Shorten common prefix: metaflow_function_* -> mff/*
            if name.startswith("metaflow_function_"):
                name = "mff/" + name[len("metaflow_function_") :]
            # Fallback: truncate long names with hash suffix
            if len(name) > 30:
                import hashlib

                prefix = name[:21]
                name_hash = hashlib.md5(name.encode()).hexdigest()[:8]
                name = f"{prefix}_{name_hash}"
                debug.functions_exec(
                    f"Buffer name shortened: '{original_name}' -> '{name}' (owns={owns})"
                )

        # Determine size of shm
        if owns:
            if num_rings is None:
                num_rings = DEFAULT_NUM_RINGS

            # Dynamically adjust buffer_size based on available shared memory
            if buffer_size is None:
                free_shm = dev_shm_size(free=True)
                # Use a smaller default if the system has limited memory
                # Reserve 50% of available memory, with a cap at DEFAULT_BUFFER_SHM
                # and a minimum of MIN_BUFFER_SHM to ensure functionality
                # Account for alignment and header overhead in the calculation
                target_total = int(free_shm * 0.5)
                constant = _compute_header_size(num_rings)
                # Solve: total_size = (buffer_size + ALIGN_BYTES) * num_rings + constant
                # for buffer_size to ensure total_size <= target_total
                max_buffer_size = max(
                    0, (target_total - constant) // num_rings - ALIGN_BYTES
                )
                buffer_size = min(
                    DEFAULT_BUFFER_SHM, max(MIN_BUFFER_SHM, max_buffer_size)
                )
                debug.functions_exec(
                    "Auto-selected buffer_size: %d based on free shm: %d"
                    % (buffer_size, free_shm)
                )

            free_shm = dev_shm_size(free=True)
            total_size = _compute_total_shm_size(buffer_size, num_rings)
            debug.functions_exec(
                "free shm size: %d, requested size: %d" % (free_shm, total_size)
            )
            if total_size > free_shm:
                raise MetaflowFunctionMemoryException(
                    "Error: Cannot request %d bytes of shared memory because only %d is available."
                    % (total_size, free_shm)
                )
        else:
            total_size = 0

        # Connect to shared memory
        debug.functions_exec(f"Connecting to shm file '{name}'.")
        start = time.time()
        now = start
        while (now - start) < INIT_TIMEOUT:
            try:
                # Buffer size is ignored if owns=False
                self._shm = SharedMemory(name=name, create=owns, size=total_size)
                break

            # If we don't own the buffer, we need to wait for the
            # owner to create it. We will timeout after a while if it
            # doesn't become available.
            except FileNotFoundError as e:
                time.sleep(SHM_RETRY_INTERVAL)

            # Handle a race condition where the shared memory is accessed before fully initialized
            except ValueError as e:
                if "cannot mmap an empty file" in str(e):
                    time.sleep(SHM_RETRY_INTERVAL)
                    continue
                # Wrap other ValueErrors with MetaflowFunctionMemoryException
                raise MetaflowFunctionMemoryException(
                    f"Failed to create shared memory buffer '{name}': {str(e)}"
                ) from e

            # We can only create buffers if we own them.
            except FileExistsError as e:
                raise MetaflowFunctionMemoryException(
                    "Cannot create buffer '%s' because it "
                    "already exists. Only one buffer can be the owner." % name
                )

            # update time
            now = time.time()

        # Timeout if we can't connect to the buffer
        else:
            self.close()
            raise MetaflowFunctionMemoryException(
                f"Cannot open buffer '{name}' (owns={owns}) because it "
                "has not been created. One and only one buffer must "
                "be the owner."
            )

        # Get actual name, size, and buffer
        self._name = self._shm.name
        self._size = self._shm.size
        self._buf = self._shm.buf

        # Open a semaphore of the same name to protect control section
        try:
            self._sem = Semaphore(name)
        except Exception as e:
            self.close()
            raise e

        # Setup buffer
        if owns:
            with Lock(self._sem):
                # reserve the space in the shm by setting to zeros
                if reserve:
                    debug.functions_exec(f"Reserving size of shm file '{self._name}'.")
                    try:
                        _reserve_memory(self._shm)
                    except Exception as e:
                        self.close()
                        raise e

                # setup memory allocator regions
                self._configure_allocator(num_rings)
                self._debug_state()

    def close(self) -> None:
        """
        Shutdown concurrency resources
        """
        # close semaphore (only close/unlink if we own it)
        if hasattr(self, "_sem") and self._sem is not None:
            debug.functions_exec(f"Closing sem file '{self._name}'")
            if hasattr(self, "_owns") and self._owns:
                # Owners close and unlink the semaphore
                self._sem.close()
            # Non-owners don't close the semaphore - it's shared and still in use
            self._sem = None  # type: ignore

        # close shared memory (only unlink if we own it)
        if hasattr(self, "_shm") and self._shm is not None:
            debug.functions_exec(f"Closing shm file '{self._name}'")
            self._shm.close()
            if hasattr(self, "_owns") and self._owns:
                try:
                    # unlink() automatically unregisters from resource_tracker
                    self._shm.unlink()
                    # Prevent Python's __del__ from trying to unregister again at exit
                    # This avoids KeyError from double-unregister
                    self._shm._name = None  # type: ignore[attr-defined]
                except (FileNotFoundError, OSError):
                    pass
            self._shm = None

    def _get_config_buffer(self):
        return self._buf[CONFIG_OFFSET : CONFIG_OFFSET + CONFIG_BYTES]

    def _get_count_buffer(self):
        return self._buf[COUNT_OFFSET : COUNT_OFFSET + COUNT_BYTES]

    def _get_control_buffer(self):
        config = self._read_config()
        r0 = CONTROL_OFFSET
        r1 = r0 + CONTROL_BYTES_BASE * config.nrings
        return self._buf[r0:r1]

    def _get_data_desc_buffer(self):
        config = self._read_config()
        r0 = _compute_data_desc_offset(config.nrings)
        r1 = r0 + DATA_DESC_BYTES_BASE * config.nrings
        return self._buf[r0:r1]

    def _read_config(self):
        """
        Decode and return config section as an object
        """
        b = self._get_config_buffer()
        return Config(*struct.unpack(CONFIG_FORMAT, b))

    def _set_config(self, config):
        """
        Set config section from an object
        """
        b = self._get_config_buffer()
        struct.pack_into(CONFIG_FORMAT, b, 0, *astuple(config))

    def _read_count(self):
        """
        Decode and return control section as an object
        """
        b = self._get_count_buffer()
        return Count(*struct.unpack(COUNT_FORMAT, b))

    def _set_count(self, count):
        """
        Set control section from an object
        """
        b = self._get_count_buffer()
        struct.pack_into(COUNT_FORMAT, b, 0, *astuple(count))

    def _read_control(self, ring):
        """
        Decode and return control section as an object for a ring
        """
        r0 = ring * CONTROL_BYTES_BASE
        r1 = r0 + CONTROL_BYTES_BASE
        b = self._get_control_buffer()[r0:r1]
        return Control(*struct.unpack(CONTROL_FORMAT, b))

    def _set_control(self, ring, control):
        """
        Set control section from an object
        """
        r0 = ring * CONTROL_BYTES_BASE
        r1 = r0 + CONTROL_BYTES_BASE
        b = self._get_control_buffer()[r0:r1]
        struct.pack_into(CONTROL_FORMAT, b, 0, *astuple(control))

    def _read_data_desc(self, ring):
        """
        Read the data_desc section given a ring
        """
        r0 = ring * DATA_DESC_BYTES_BASE
        r1 = r0 + DATA_DESC_BYTES_BASE
        b = self._get_data_desc_buffer()[r0:r1]
        return Data(*struct.unpack(DATA_DESC_FORMAT, b))

    def _set_data_desc(self, ring, data_desc):
        """
        Set the data_desc section for a ring with the data_desc data
        """
        r0 = ring * DATA_DESC_BYTES_BASE
        r1 = r0 + DATA_DESC_BYTES_BASE
        b = self._get_data_desc_buffer()[r0:r1]
        struct.pack_into(DATA_DESC_FORMAT, b, 0, *astuple(data_desc))

    def _is_config_set(self):
        """
        Check if the config section has been set
        """
        b = self._get_config_buffer()
        return b != CONFIG_ZEROS

    def _configure_allocator(self, nrings):
        # Compute actual buffer size
        buffer_size = _compute_ring_size(
            self._size, nrings
        )  # This includes the alignment size

        # Compute data offsets for writing
        data_ranges = []
        for i in range(nrings):
            head = _compute_header_size(nrings) + i * buffer_size
            buf = self._buf[head : head + buffer_size]
            offset = _aligned_buffer_offset(buf, ALIGN_BYTES)
            head += offset
            data_ranges.append((head, buffer_size))
            debug.functions_exec(f"'{self._name}' map data region at {data_ranges[-1]}")

        # Set the config section
        self._set_config(Config(nrings=nrings))

        # Set the data_desc
        for i, data_range in enumerate(data_ranges):
            self._set_data_desc(i, Data(*data_range))

    def _debug_state(self):
        if IS_DEBUG:
            s = list()
            config = self._read_config()
            s.append(str(config))
            count = self._read_count()
            s.append(str(count))
            for i in range(config.nrings):
                control = self._read_control(i)
                data = self._read_data_desc(i)
                s.append(str(control))
                s.append(str(data))
            debug.functions_exec("\n".join(s))

    def clear(self):
        with Lock(self._sem):
            count = self._read_count()
            config = self._read_config()
            self._set_count(Count(0, 0, 0, 0))
            for i in range(config.nrings):
                self._set_control(i, Control(0, 0, UUID_ZEROS, 0, 0))


class WriteBuffer(BufferBase):
    """
    Subclass for a writable buffer

    """

    def __init__(
        self,
        name: str,
        owns: bool = False,
        buffer_size: Optional[int] = None,
        num_rings: Optional[int] = None,
        reserve: bool = True,
    ) -> None:
        """
        Configure a write buffer. Note that buffer_size, num_rings and
        reserve are all ignored if owns is False.

        Parameters
        ----------
        name : str
            Map name
        owns : bool
            If this class is the owner
        buffer_size : int, optional, default None
            Buffer size in bytes
        num_rings : int, optional, default None
            Number of rings to use
        reserve : bool, default True
            True to reserve space in the shm. This costs extra setup time.
        """
        debug.functions_exec(f"Creating writer for '{name}'")
        super().__init__(
            name,
            owns=owns,
            buffer_size=buffer_size,
            num_rings=num_rings,
            reserve=reserve,
        )

    def acquire(self, uuid: bytes = UUID_ZEROS) -> BufferBase.MemoryReference:
        """
        Acquire a reference to a writable memory region. If no writable
        region is available then the returned MemoryReference object
        will be set to invalid.

        Returns
        -------
        MemoryReference
            A reference to the acquired memory

        """
        with Lock(self._sem):
            # Check if we have any writers available
            count = self._read_count()
            config = self._read_config()
            if count.write == config.nrings:
                self._debug_state()
                return BufferBase.MemoryReference()

            # Find the first writer and set its attributes
            for i in range(count.w_ring, count.w_ring + config.nrings):
                i = i % config.nrings
                control = self._read_control(i)
                if control.assigned == 0:
                    # Set control attributes FIRST before incrementing count
                    # to avoid readers seeing an inconsistent state
                    control.assigned = 1
                    control.ready = 0
                    control.uuid = uuid
                    control.pid = os.getpid()
                    self._set_control(i, control)

                    # Update count and ring position AFTER control is written
                    count.write += 1
                    count.w_ring = i + 1
                    self._set_count(count)
                    rng = self._read_data_desc(i)
                    b = self._buf[rng.offset : rng.offset + rng.size]
                    self._debug_state()
                    return BufferBase.MemoryReference(
                        id=i, buf=b, uuid=control.uuid, pid=control.pid
                    )

            else:
                raise MetaflowFunctionMemoryException(
                    f"The current buffer write count ({count.write}) is "
                    f"less than the number of available buffers "
                    f"({config.nrings}) yet no free buffer was found."
                )

    def commit(self, reference: BufferBase.MemoryReference) -> None:
        """
        Commit the memory reference to the shared memory making it
        ready for reading from another process.

        Parameters
        ----------
        reference: MemoryReference
             The MemoryReference
        """
        if not reference.valid:
            return

        with Lock(self._sem):
            # update control section FIRST, this section is ready to read
            # IMPORTANT: We must set control.ready = 1 BEFORE incrementing count.read
            # to avoid ARM memory ordering issues where a reader might see count.read > 0
            # but control.ready still at 0
            control = self._read_control(reference.id)
            control.ready = 1
            control.written = reference.size
            self._set_control(reference.id, control)

            # update counter for ready available AFTER control is marked ready
            count = self._read_count()
            count.read += 1
            self._set_count(count)
            self._debug_state()


class ReadBuffer(BufferBase):
    """
    Configure a read buffer. Note that buffer_size, num_rings and
    reserve are all ignored if owns is False.

    """

    def __init__(
        self,
        name: str,
        owns: bool = False,
        buffer_size: Optional[int] = None,
        num_rings: Optional[int] = None,
        reserve: bool = True,
    ) -> None:
        """
        Instantiate a writable buffer.
        Parameters
        ----------
        name : str
            Map name
        owns : bool
            If this class is the owner
        buffer_size : int, optional, default None
            Buffer size in bytes
        num_rings : int, optional, default None
            Number of rings to use
        reserve : bool, default True
            True to reserve space in the shm. This costs extra setup time.
        """
        debug.functions_exec(f"Creating reader for '{name}'")
        super().__init__(
            name,
            owns=owns,
            buffer_size=buffer_size,
            num_rings=num_rings,
            reserve=reserve,
        )

    def acquire(self, uuid: Optional[bytes] = None) -> BufferBase.MemoryReference:
        """
        Acquire a reference to a readable memory region. If no readable
        region is available then the returned MemoryReference object
        will be set to invalid.

        Returns
        -------
        MemoryReference
            A reference to the acquired memory

        """
        with Lock(self._sem):
            count = self._read_count()
            if count.read == 0:
                self._debug_state()
                return BufferBase.MemoryReference()

            config = self._read_config()
            for i in range(count.r_ring, count.r_ring + config.nrings):
                i = i % config.nrings
                control = self._read_control(i)
                if control.ready == 1:
                    if uuid and control.uuid != uuid:
                        continue

                    control.ready = 0
                    count.read -= 1
                    control.pid = os.getpid()
                    uuid = control.uuid
                    count.r_ring = i + 1
                    self._set_count(count)
                    self._set_control(i, control)
                    rng = self._read_data_desc(i)
                    b = self._buf[rng.offset : rng.offset + control.written]
                    self._debug_state()
                    return BufferBase.MemoryReference(
                        id=i, buf=b, uuid=uuid, pid=control.pid
                    )

            return BufferBase.MemoryReference()

    def free(self, reference: BufferBase.MemoryReference) -> None:
        """
        Free the buffer at the memory reference, making it available
        to writers.

        Parameters
        ----------
        reference: MemoryReference
             The MemoryReference
        """
        if not reference.valid:
            return

        with Lock(self._sem):
            count = self._read_count()
            control = self._read_control(reference.id)
            control.assigned = 0
            control.ready = 0
            control.uuid = UUID_ZEROS
            control.pid = 0
            control.written = 0
            count.write -= 1
            self._set_count(count)
            self._set_control(reference.id, control)
            self._debug_state()
