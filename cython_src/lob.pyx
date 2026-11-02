# cython_src/lob.pyx  -  Phase 2: Cython Limit Order Book
# =========================================================
# Compile:
#   python setup.py build_ext --inplace
#
# Requires: Cython + MSVC (Windows) or GCC/Clang (Linux/macOS)
# Achieves sub-microsecond match latency once compiled.
#
# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False

from libc.stdlib  cimport malloc, free, qsort
from libc.string  cimport memset, memmove
from libc.stdint  cimport uint64_t, uint32_t, int8_t, int32_t, int64_t
import time

DEF SIDE_BUY  =  1
DEF SIDE_SELL = -1
DEF MAX_DEPTH = 65536   # max resting orders per side

# ---- C structs --------------------------------------------------------------

cdef struct COrder:
    uint64_t order_id
    double   price
    uint32_t quantity
    int8_t   side
    uint64_t seq

cdef struct CTrade:
    uint64_t aggressor_id
    uint64_t resting_id
    double   price
    uint32_t quantity
    uint64_t latency_ns
    int8_t   is_whale
    int32_t  cleared_levels

# ---- qsort comparators ------------------------------------------------------

cdef int bid_cmp(const void* a, const void* b) noexcept nogil:
    """Bids: descending price, then ascending seq."""
    cdef const COrder* oa = <const COrder*>a
    cdef const COrder* ob = <const COrder*>b
    if ob.price > oa.price: return  1
    if ob.price < oa.price: return -1
    if oa.seq   < ob.seq:   return -1
    if oa.seq   > ob.seq:   return  1
    return 0

cdef int ask_cmp(const void* a, const void* b) noexcept nogil:
    """Asks: ascending price, then ascending seq."""
    cdef const COrder* oa = <const COrder*>a
    cdef const COrder* ob = <const COrder*>b
    if oa.price < ob.price: return -1
    if oa.price > ob.price: return  1
    if oa.seq   < ob.seq:   return -1
    if oa.seq   > ob.seq:   return  1
    return 0

# ---- helper: binary search insertion for sorted orders ----------------------

cdef inline int _find_bid_insert_idx(const COrder* arr, int n, double price, uint64_t seq) noexcept nogil:
    """Bids descending: price descending, seq ascending."""
    cdef int low = 0
    cdef int high = n
    cdef int mid
    while low < high:
        mid = (low + high) >> 1
        if arr[mid].price > price:
            low = mid + 1
        elif arr[mid].price < price:
            high = mid
        elif arr[mid].seq < seq:
            low = mid + 1
        else:
            high = mid
    return low

cdef inline int _find_ask_insert_idx(const COrder* arr, int n, double price, uint64_t seq) noexcept nogil:
    """Asks ascending: price ascending, seq ascending."""
    cdef int low = 0
    cdef int high = n
    cdef int mid
    while low < high:
        mid = (low + high) >> 1
        if arr[mid].price < price:
            low = mid + 1
        elif arr[mid].price > price:
            high = mid
        elif arr[mid].seq < seq:
            low = mid + 1
        else:
            high = mid
    return low

cdef inline void _insert_bid(COrder* arr, int* count, const COrder* order) noexcept nogil:
    if count[0] >= MAX_DEPTH:
        return
    cdef int idx = _find_bid_insert_idx(arr, count[0], order.price, order.seq)
    if idx < count[0]:
        memmove(&arr[idx + 1], &arr[idx], (count[0] - idx) * sizeof(COrder))
    arr[idx] = order[0]
    count[0] += 1

cdef inline void _insert_ask(COrder* arr, int* count, const COrder* order) noexcept nogil:
    if count[0] >= MAX_DEPTH:
        return
    cdef int idx = _find_ask_insert_idx(arr, count[0], order.price, order.seq)
    if idx < count[0]:
        memmove(&arr[idx + 1], &arr[idx], (count[0] - idx) * sizeof(COrder))
    arr[idx] = order[0]
    count[0] += 1

# ---- helper: remove element at index from C array ---------------------------

cdef inline void _remove_at(COrder* arr, int* count, int idx) noexcept nogil:
    cdef int tail = count[0] - 1
    if idx < tail:
        memmove(&arr[idx], &arr[idx+1], (tail - idx) * sizeof(COrder))
    count[0] -= 1

# ---- CythonLOB --------------------------------------------------------------

cdef class CythonLOB:
    """
    Cython Limit Order Book.
    All hot-path logic runs at C speed with no Python heap allocation.

    Public methods (callable from Python):
        submit(order_id, price, quantity, side) -> list of (agg_id, rest_id, px, qty)
        best_bid  : float | None
        best_ask  : float | None
    """
    cdef COrder*  _bids
    cdef COrder*  _asks
    cdef int      _bid_n
    cdef int      _ask_n
    cdef uint64_t _seq

    cdef public long total_orders
    cdef public long total_trades
    cdef public long total_volume
    cdef public double last_match_us

    def __cinit__(self):
        self._bids  = <COrder*>malloc(MAX_DEPTH * sizeof(COrder))
        self._asks  = <COrder*>malloc(MAX_DEPTH * sizeof(COrder))
        self._bid_n = 0
        self._ask_n = 0
        self._seq   = 0
        self.total_orders  = 0
        self.total_trades  = 0
        self.total_volume  = 0
        self.last_match_us = 0.0
        if not self._bids or not self._asks:
            raise MemoryError("CythonLOB: malloc failed")

    def __dealloc__(self):
        if self._bids: free(self._bids)
        if self._asks: free(self._asks)

    cpdef list submit(self, uint64_t order_id, double price,
                      uint32_t quantity, int8_t side, int64_t entry_ns = 0):
        """Match and/or rest an order. Returns list of trade tuples."""
        self._seq += 1
        self.total_orders += 1

        cdef COrder entry
        entry.order_id = order_id
        entry.price    = price
        entry.quantity = quantity
        entry.side     = side
        entry.seq      = self._seq

        cdef list trades = []
        cdef CTrade t
        cdef COrder* best
        cdef uint32_t fill
        cdef int distinct_levels = 0
        cdef double last_px = -1.0
        cdef uint64_t trade_vol = 0

        cdef int64_t t0_ns = time.perf_counter_ns()
        if entry_ns == 0:
            entry_ns = t0_ns

        if side == SIDE_BUY:
            while entry.quantity > 0 and self._ask_n > 0:
                best = &self._asks[0]
                if best.price > price:
                    break
                if best.price != last_px:
                    distinct_levels += 1
                    last_px = best.price
                fill = entry.quantity if entry.quantity < best.quantity else best.quantity
                t.aggressor_id = order_id
                t.resting_id   = best.order_id
                t.price        = best.price
                t.quantity     = fill
                t.latency_ns   = <uint64_t>(time.perf_counter_ns() - entry_ns)
                t.is_whale     = 1 if (distinct_levels >= 2 or fill >= 5000) else 0
                t.cleared_levels = distinct_levels
                trades.append((t.aggressor_id, t.resting_id, t.price, t.quantity, t.latency_ns, t.is_whale, t.cleared_levels))
                entry.quantity -= fill
                best.quantity  -= fill
                trade_vol      += fill
                if best.quantity == 0:
                    _remove_at(self._asks, &self._ask_n, 0)
            if entry.quantity > 0:
                _insert_bid(self._bids, &self._bid_n, &entry)
        else:  # SIDE_SELL
            while entry.quantity > 0 and self._bid_n > 0:
                best = &self._bids[0]
                if best.price < price:
                    break
                if best.price != last_px:
                    distinct_levels += 1
                    last_px = best.price
                fill = entry.quantity if entry.quantity < best.quantity else best.quantity
                t.aggressor_id = order_id
                t.resting_id   = best.order_id
                t.price        = best.price
                t.quantity     = fill
                t.latency_ns   = <uint64_t>(time.perf_counter_ns() - entry_ns)
                t.is_whale     = 1 if (distinct_levels >= 2 or fill >= 5000) else 0
                t.cleared_levels = distinct_levels
                trades.append((t.aggressor_id, t.resting_id, t.price, t.quantity, t.latency_ns, t.is_whale, t.cleared_levels))
                entry.quantity -= fill
                best.quantity  -= fill
                trade_vol      += fill
                if best.quantity == 0:
                    _remove_at(self._bids, &self._bid_n, 0)
            if entry.quantity > 0:
                _insert_ask(self._asks, &self._ask_n, &entry)

        cdef int64_t elapsed_ns = time.perf_counter_ns() - t0_ns
        self.last_match_us  = elapsed_ns / 1000.0
        self.total_trades  += len(trades)
        self.total_volume  += trade_vol
        return trades

    @property
    def best_bid(self):
        if self._bid_n == 0: return None
        return self._bids[0].price

    @property
    def best_ask(self):
        if self._ask_n == 0: return None
        return self._asks[0].price

    def stats(self):
        return {
            "total_orders"  : self.total_orders,
            "total_trades"  : self.total_trades,
            "total_volume"  : self.total_volume,
            "bid_depth"     : self._bid_n,
            "ask_depth"     : self._ask_n,
            "best_bid"      : self.best_bid,
            "best_ask"      : self.best_ask,
            "last_match_us" : self.last_match_us,
        }
