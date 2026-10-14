"""
scripts/run_demo.py  -  Integrated Demo
========================================
Single-process demo that runs producer + consumer + prints a quick
summary.  Great for CI or a rapid smoke-test.

Usage:
    python scripts/run_demo.py --duration 5 --target-ops 50000
"""

import asyncio
import gc
import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
if hasattr(sys.stdout, "reconfigure"):
    try: sys.stdout.reconfigure(encoding="utf-8")
    except: pass

from core.ring_buffer import RingBuffer
from core.metrics import EngineMetrics
from core.firehose import Firehose
from engine.limit_order_book import LimitOrderBook
import argparse


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration",   type=float, default=5.0)
    ap.add_argument("--target-ops", type=int,   default=50_000)
    args = ap.parse_args()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".rb") as f:
        tmp = f.name

    try:
        rb      = RingBuffer(tmp, create=True)
        metrics = EngineMetrics()
        fh      = Firehose(rb, metrics, target_ops=args.target_ops)
        lob     = LimitOrderBook()
        stop    = threading.Event()
        lats    = []

        def consumer():
            while not stop.is_set():
                order = rb.get()
                if order is None:
                    time.sleep(0.0001); continue
                t0     = time.perf_counter_ns()
                trades = lob.submit(order)
                lat_ns = time.perf_counter_ns() - t0
                metrics.matched(len(trades),
                                sum(t.quantity for t in trades), lat_ns)
                lats.append(lat_ns / 1_000)

        gc.disable()
        cons = threading.Thread(target=consumer, daemon=True)
        cons.start()

        print(f"Demo: {args.target_ops:,} ord/sec  x  {args.duration}s")
        asyncio.run(fh.run(duration=args.duration))
        time.sleep(0.5)   # drain remainder
        stop.set()
        gc.enable()

        s  = metrics.snapshot()
        st = lob.stats()
        lats.sort()
        p50  = lats[int(len(lats)*0.50)] if lats else 0
        p99  = lats[int(len(lats)*0.99)] if lats else 0
        avg  = sum(lats)/len(lats) if lats else 0

        print(f"\n{'='*55}")
        print(f"  Orders sent   : {s['orders_sent']:>12,}")
        print(f"  Orders matched: {s['orders_matched']:>12,}")
        print(f"  Trades fired  : {s['trades_fired']:>12,}")
        print(f"  Volume traded : {s['volume_traded']:>12,}")
        print(f"  Dropped       : {s['orders_dropped']:>12,}")
        print(f"  Lat avg       : {avg:>12.2f} us")
        print(f"  Lat p50       : {p50:>12.2f} us")
        print(f"  Lat p99       : {p99:>12.2f} us")
        print(f"  Best Bid      : {st['best_bid']}")
        print(f"  Best Ask      : {st['best_ask']}")
        print(f"{'='*55}")
    finally:
        rb.close()
        try: os.unlink(tmp)
        except: pass


if __name__ == "__main__":
    main()
