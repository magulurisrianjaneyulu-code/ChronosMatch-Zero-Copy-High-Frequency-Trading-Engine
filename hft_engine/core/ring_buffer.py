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

import mmap
import os
import struct
import threading
from pathlib import Path

from core.constants import RING_BUFFER_CAPACITY, RING_HEADER_SIZE, ORDER_SIZE
from core.order_struct import Order, pack_into_buffer, unpack_from_buffer

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
        """Write one order.  Returns True on success, False if full (non-blocking)."""
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

    def get(self):
        """Read one order.  Returns None if empty (non-blocking)."""
        with self._lock:
            head = self._read_head()
            tail = self._read_tail()
            if head == tail:
                return None
            slot   = tail % self.capacity
            offset = DATA_OFF + slot * ORDER_SIZE
            order  = unpack_from_buffer(self._mm, offset)
            self._write_tail(tail + 1)
            return order

    def size(self) -> int:
        try:
            if getattr(self._mm, "closed", False):
                return 0
            return self._read_head() - self._read_tail()
        except (ValueError, BufferError):
            return 0

    def is_empty(self) -> bool:
        return self.size() == 0

    def is_full(self) -> bool:
        return self.size() >= self.capacity

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
