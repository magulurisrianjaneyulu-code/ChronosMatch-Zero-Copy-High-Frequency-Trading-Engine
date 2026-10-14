# HFT Engine - Low-Latency Matching System

A complete high-frequency trading matching engine in Python demonstrating
binary IPC, memory-mapped ring buffers, async order generation, Price-Time
Priority matching, and live terminal dashboards.

## Phases

| Phase | Module | Description |
|-------|--------|-------------|
| 1 | `core/order_struct.py` | 21-byte binary wire format (struct module) |
| 1 | `core/ring_buffer.py`  | mmap SPSC ring buffer with lock-free pointers |
| 1 | `core/firehose.py`     | asyncio firehose up to 100k orders/sec |
| 2 | `engine/limit_order_book.py` | Price-Time Priority LOB (pure Python) |
| 2 | `cython_src/lob.pyx`   | Cython LOB with C structs (< 1 us latency) |
| 2 | `ui/dashboard.py`      | Live ANSI terminal dashboard @ 20 FPS |
| 3 | `bench/ipc_audit.py`   | Zero-copy vs pickle/JSON benchmark (1M orders) |
| 3 | `bench/engine_verify.py` | Deterministic matching + latency tests |

## Quick Start

```powershell
cd D:\hft_engine
python -m pip install -r requirements.txt

# 1. Struct smoke test (Phase 1)
python main.py struct-test

# 2. Ring buffer smoke test (Phase 1)
python main.py ring-test

# 3. Engine verification (Phase 3)
python main.py verify

# 4. IPC zero-copy benchmark - 1M orders (Phase 3)
python main.py audit --orders 1000000

# 5. Live dashboard (Phase 2) - 30 sec at 100k orders/sec
python main.py dashboard --duration 30 --target-ops 100000

# 6. Quick 5-second integrated demo
python scripts/run_demo.py --duration 5 --target-ops 50000
```

## Project Layout

```
hft_engine/
+-- main.py                     Unified CLI
+-- setup.py                    Cython build script
+-- requirements.txt
+-- core/
|   +-- constants.py            Central config (single source of truth)
|   +-- order_struct.py         Phase 1: binary layout (21 bytes)
|   +-- ring_buffer.py          Phase 1: mmap ring buffer (SPSC)
|   +-- firehose.py             Phase 1: async order generator
|   +-- metrics.py              Latency histogram + throughput counters
+-- engine/
|   +-- limit_order_book.py     Phase 2: Price-Time Priority LOB
+-- ui/
|   +-- dashboard.py            Phase 2: Live ANSI terminal dashboard
+-- bench/
|   +-- ipc_audit.py            Phase 3: zero-copy vs pickle/JSON
|   +-- engine_verify.py        Phase 3: deterministic matching tests
+-- cython_src/
|   +-- lob.pyx                 Phase 2: Cython LOB (compile for <1 us)
+-- scripts/
    +-- run_demo.py             Integrated single-process demo
    +-- run_producer.py         Multi-process producer
    +-- run_consumer.py         Multi-process consumer
```

## Binary Wire Format

```
ORDER_SIZE = 21 bytes  (struct format =QdIb, native byte order)

  Offset  Size  Type    Field
  ------  ----  ------  --------
     0     8    uint64  order_id
     8     8    float64 price
    16     4    uint32  quantity
    20     1    int8    side  (+1=BUY, -1=SELL)
```

## Ring Buffer Layout

```
mmap region:
  [0:8]   HEAD pointer  (uint64 LE) - next write slot index
  [8:16]  TAIL pointer  (uint64 LE) - next read  slot index
  [16..]  circular data (capacity x 21 bytes)
```
- SPSC design with threading.Lock for intra-process safety
- Backed by a named temp file for cross-process sharing on Windows

## Benchmark Results (Python 3.14, pure Python, no Cython)

| Metric                      | Result           |
|-----------------------------|------------------|
| Firehose throughput         | ~100,000 ord/sec |
| mmap ring buffer            | ~258,000 ops/sec |
| Match latency avg           | ~4 us            |
| Match latency p99           | < 15 us          |
| GC pauses (hot path)        | 0                |
| pickle (relative to ring)   | 1.5x slower      |
| JSON   (relative to ring)   | 1.8x slower      |

## Cython Compilation (optional)

Requires MSVC (Windows) or GCC (Linux/macOS):

```powershell
python -m pip install cython
python setup.py build_ext --inplace
```

After compilation the `CythonLOB` class provides < 1 us match latency
by using C-level `cdef struct` arrays and `qsort` with no Python heap
allocation on the hot path.

## Multi-Process IPC Demo

```powershell
# Terminal 1
python scripts/run_producer.py --path D:\hft.rb --duration 10

# Terminal 2 (simultaneously)
python scripts/run_consumer.py --path D:\hft.rb
```
