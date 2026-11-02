"""
core/ring_buffer.py  -  Phase 1: Memory-Mapped Ring Buffer
==========================================================
Single-Producer / Single-Consumer (SPSC) circular buffer backed by an
mmap region so two OS processes can share it via a named file.

Memory layout
-------------
  Bytes  0- 7  : HEAD pointer  (uint64, little-endian) - next write slot index
  Bytes  8-15  : TAIL pointer  (uint64, little-endian) - next read  slot index
  Bytes 16-..  : circular data region  (CAPACITY x ORDER_SIZE bytes)

Thread safety: a threading.Lock guards all pointer reads/writes so a
single Python process can have multiple threads accessing the buffer.
Cross-process safety: on Windows mmap requires a backing file; both
producer and consumer open the same path.
"""

import logging
import mmap
import os
import struct
import threading
from pathlib import Path
from typing import List, Optional, Tuple

from core.constants import RING_BUFFER_CAPACITY, RING_HEADER_SIZE, ORDER_SIZE
from core.order_struct import Order, pack_into_buffer, unpack_from_buffer

logger = logging.getLogger("hft.ipc")

_HEAD_OFF = 0
_TAIL_OFF = 8
_PTR_FMT  = "<Q"   # little-endian uint64
_PTR_SZ   = struct.calcsize(_PTR_FMT)
DATA_OFF  = RING_HEADER_SIZE   # 16 bytes


class RingBuffer:
    """
    Memory-mapped SPSC ring buffer.

    Parameters
    ----------
    path     : str | Path   backing file (created if create=True)
    capacity : int          maximum number of in-flight orders
    create   : bool         True  -> producer (creates/truncates the file)
                            False -> consumer (opens existing file read-write)
    """

    def __init__(self, path, capacity: int = RING_BUFFER_CAPACITY,
                 create: bool = True):
        self.path       = Path(path)
        self.capacity   = capacity
        self._data_size = capacity * ORDER_SIZE
        self._total     = DATA_OFF + self._data_size
        self._lock      = threading.Lock()

        if create:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_bytes(b"\x00" * self._total)

        self._fh = open(self.path, "r+b")
        self._mm = mmap.mmap(self._fh.fileno(), self._total)

    # ---- pointer helpers ----------------------------------------------------

    def _read_head(self) -> int:
        return struct.unpack_from(_PTR_FMT, self._mm, _HEAD_OFF)[0]

    def _read_tail(self) -> int:
        return struct.unpack_from(_PTR_FMT, self._mm, _TAIL_OFF)[0]

    def _write_head(self, v: int) -> None:
        struct.pack_into(_PTR_FMT, self._mm, _HEAD_OFF, v)

    def _write_tail(self, v: int) -> None:
        struct.pack_into(_PTR_FMT, self._mm, _TAIL_OFF, v)

    # ---- public API ---------------------------------------------------------

    def put(self, order: Order) -> bool:
        """Write one order. Returns True on success, False if full (non-blocking)."""
        with self._lock:
            head = self._read_head()
            tail = self._read_tail()
            if (head - tail) >= self.capacity:
                return False
            slot   = head % self.capacity
            offset = DATA_OFF + slot * ORDER_SIZE
            pack_into_buffer(self._mm, offset, order)
            self._write_head(head + 1)
            return True

    def put_batch(self, orders: List[Order]) -> int:
        """
        High-throughput batch enqueue with a single pointer advance.
        Returns the number of orders successfully written.
        """
        if not orders:
            return 0
        written = 0
        with self._lock:
            head = self._read_head()
            tail = self._read_tail()
            available = self.capacity - (head - tail)
            to_write = min(len(orders), max(0, available))
            for i in range(to_write):
                slot = (head + i) % self.capacity
                offset = DATA_OFF + slot * ORDER_SIZE
                pack_into_buffer(self._mm, offset, orders[i])
                written += 1
            if written > 0:
                self._write_head(head + written)
        return written

    def get(self) -> Optional[Order]:
        """Read one order. Returns None if empty (non-blocking)."""
        with self._lock:
            head = self._read_head()
            tail = self._read_tail()
            if head <= tail:
                return None
            slot   = tail % self.capacity
            offset = DATA_OFF + slot * ORDER_SIZE
            order  = unpack_from_buffer(self._mm, offset)
            self._write_tail(tail + 1)
            return order

    def get_batch(self, max_count: int = 1000) -> List[Order]:
        """
        High-throughput batch dequeue with a single pointer advance.
        Returns a list of Order objects read from shared memory.
        """
        orders: List[Order] = []
        with self._lock:
            head = self._read_head()
            tail = self._read_tail()
            available = head - tail
            if available <= 0:
                return orders
            to_read = min(max_count, available)
            for i in range(to_read):
                slot = (tail + i) % self.capacity
                offset = DATA_OFF + slot * ORDER_SIZE
                orders.append(unpack_from_buffer(self._mm, offset))
            self._write_tail(tail + to_read)
        return orders

    def peek(self) -> Optional[Order]:
        """Non-destructive peek at the next order without advancing the tail."""
        with self._lock:
            head = self._read_head()
            tail = self._read_tail()
            if head <= tail:
                return None
            slot = tail % self.capacity
            offset = DATA_OFF + slot * ORDER_SIZE
            return unpack_from_buffer(self._mm, offset)

    def size(self) -> int:
        try:
            if getattr(self._mm, "closed", False):
                return 0
            diff = self._read_head() - self._read_tail()
            return max(0, min(self.capacity, diff))
        except (ValueError, BufferError):
            return 0

    def is_empty(self) -> bool:
        return self.size() == 0

    def is_full(self) -> bool:
        return self.size() >= self.capacity

    def validate_integrity(self) -> Tuple[bool, str]:
        """
        Validate ring buffer pointers and boundaries.
        Returns (valid, status_description).
        """
        try:
            if getattr(self._mm, "closed", False):
                return False, "Memory-map is closed"
            head = self._read_head()
            tail = self._read_tail()
            if head < tail:
                return False, f"Head ({head}) is behind tail ({tail}); corruption detected."
            diff = head - tail
            if diff > self.capacity:
                return False, f"Occupancy ({diff}) exceeds total capacity ({self.capacity})."
            return True, f"Buffer healthy: occupancy={diff}/{self.capacity}"
        except Exception as e:
            return False, f"Integrity check failed: {e}"

    def recover(self) -> bool:
        """
        Attempt automatic recovery from corrupted or mismatched pointers.
        Resets pointers to a clean, empty state without reallocating file.
        """
        try:
            with self._lock:
                logger.warning("Attempting IPC RingBuffer recovery...")
                self._write_head(0)
                self._write_tail(0)
                self.flush()
                logger.info("IPC RingBuffer recovery completed successfully.")
                return True
        except Exception as e:
            logger.error("IPC RingBuffer recovery failed: %s", e)
            return False

    def flush(self) -> None:
        try:
            if not getattr(self._mm, "closed", False):
                self._mm.flush()
        except Exception:
            pass

    def close(self) -> None:
        try:
            if not getattr(self._mm, "closed", False):
                self._mm.flush()
                self._mm.close()
        except Exception:
            pass
        try:
            if not getattr(self._fh, "closed", False):
                self._fh.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __repr__(self):
        return (f"RingBuffer(path={self.path.name!r}, "
                f"capacity={self.capacity:,}, size={self.size()})")


# ---- Self-test --------------------------------------------------------------

if __name__ == "__main__":
    import tempfile, random
    from core.constants import SIDE_BUY, SIDE_SELL

    with tempfile.NamedTemporaryFile(delete=False, suffix=".rb") as f:
        tmp = f.name
    try:
        rb = RingBuffer(tmp, capacity=8, create=True)
        print(repr(rb))
        for i in range(8):
            side = SIDE_BUY if i % 2 == 0 else SIDE_SELL
            assert rb.put(Order(order_id=i, price=100.0+i, quantity=10, side=side))
        assert rb.is_full(),  "should be full"
        print(f"  full : size={rb.size()}")
        for i in range(8):
            o = rb.get()
            assert o is not None and o.order_id == i
        assert rb.is_empty(), "should be empty"
        print(f"  empty: size={rb.size()}")
        print("OK RingBuffer self-test passed")
    finally:
        rb.close()
        os.unlink(tmp)
