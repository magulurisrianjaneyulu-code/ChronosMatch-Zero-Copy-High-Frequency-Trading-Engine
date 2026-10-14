"""
bench/ipc_audit.py  -  Phase 3: IPC Zero-Copy Audit
====================================================
Benchmark 1,000,000 orders through four transport methods:
  1. mmap Ring Buffer (zero-copy, our architecture)
  2. struct pack/unpack only (codec baseline, no IPC overhead)
  3. pickle serialisation + deserialisation
  4. JSON  serialisation + deserialisation

Prints a comparison table demonstrating zero-copy throughput advantage.

Usage:
    python -m bench.ipc_audit --orders 1000000
"""

import gc
import json
import os
import pickle
import sys
import tempfile
import time
import argparse

sys.stdout.reconfigure(encoding="utf-8")

from core.constants import ORDER_SIZE, SIDE_BUY, SIDE_SELL
from core.order_struct import Order, pack_order, unpack_order
from core.ring_buffer import RingBuffer


# ---- helpers ----------------------------------------------------------------

def _make_orders(n: int):
    orders = []
    for i in range(n):
        price = 100.0 + (i % 101) * 0.01
        qty   = (i % 999) + 1
        side  = SIDE_BUY if i % 2 == 0 else SIDE_SELL
        orders.append(Order(order_id=i, price=price, quantity=qty, side=side))
    return orders


def _run(fn, orders):
    """Run fn(orders) with GC disabled; return (ops/sec, bytes_moved, elapsed)."""
    gc.disable()
    t0    = time.perf_counter()
    moved = fn(orders)
    el    = time.perf_counter() - t0
    gc.enable()
    return len(orders) / el, moved, el


# ---- transport implementations ----------------------------------------------

def bench_ring_buffer(orders):
    n   = len(orders)
    cap = min(n, 131_072)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".rb") as f:
        tmp = f.name
    try:
        rb   = RingBuffer(tmp, capacity=cap, create=True)
        sent = recv = idx = 0
        while recv < n:
            while not rb.is_full() and idx < n:
                rb.put(orders[idx]); idx += 1; sent += 1
            while not rb.is_empty():
                rb.get(); recv += 1
        rb.close()
        return n * ORDER_SIZE * 2
    finally:
        try: os.unlink(tmp)
        except: pass


def bench_struct_only(orders):
    total = 0
    for o in orders:
        raw = pack_order(o); total += len(raw); unpack_order(raw)
    return total


def bench_pickle(orders):
    total = 0
    for o in orders:
        raw = pickle.dumps(o); total += len(raw); pickle.loads(raw)
    return total


def bench_json(orders):
    total = 0
    for o in orders:
        raw = json.dumps({"id": o.order_id, "px": o.price,
                           "qty": o.quantity, "side": o.side}).encode()
        total += len(raw); json.loads(raw)
    return total


# ---- main -------------------------------------------------------------------

def run_audit(n: int = 1_000_000):
    print(f"\n{'='*66}")
    print(f"  HFT ENGINE  -  IPC Zero-Copy Audit")
    print(f"  Orders : {n:,}   |   Wire size: {ORDER_SIZE} bytes/order")
    print(f"{'='*66}\n")

    cases = [
        ("mmap Ring Buffer (zero-copy)", bench_ring_buffer),
        ("struct pack/unpack only",      bench_struct_only),
        ("pickle serialisation",          bench_pickle),
        ("JSON  serialisation",           bench_json),
    ]

    # Warmup
    warm = _make_orders(1_000)
    for _, fn in cases:
        fn(warm)

    results = []
    orders  = _make_orders(n)
    for name, fn in cases:
        print(f"  Running: {name} ...", end="\r", flush=True)
        ops, moved, el = _run(fn, orders)
        mb = moved / 1e6
        print(f"  {name:<38}  {ops:>12,.0f} ops/s  {mb:>7.1f} MB  {el:.3f}s")
        results.append((name, ops, mb, el))

    baseline = results[0][1]
    fastest  = max(r[1] for r in results)

    print(f"\n  {'Method':<38}  {'ops/sec':>12}  {'speedup':>9}")
    print(f"  {'-'*64}")
    for name, ops, mb, el in results:
        ratio = ops / baseline
        mark  = "  <-- FASTEST" if ops == fastest else ""
        print(f"  {name:<38}  {ops:>12,.0f}  {ratio:>8.2f}x{mark}")

    ring_ops = results[0][1]
    pkl_ops  = results[2][1]
    json_ops = results[3][1]
    print(f"\n  Zero-copy is {ring_ops/pkl_ops:.1f}x faster than pickle")
    print(f"  Zero-copy is {ring_ops/json_ops:.1f}x faster than JSON")
    print(f"{'='*66}\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=1_000_000)
    run_audit(n=ap.parse_args().orders)
