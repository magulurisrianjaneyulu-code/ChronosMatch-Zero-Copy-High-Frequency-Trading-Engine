"""
core/firehose.py  -  Phase 1: Async Market Firehose
====================================================
Asynchronous order generator targeting up to 100,000 orders/sec.
Packs mock trade orders into binary bytes and writes them into the
shared mmap ring buffer.

The firehose exposes a shared EngineMetrics object so the dashboard
can read live stats without any extra synchronisation overhead.

Usage (standalone):
    python -m core.firehose --duration 5 --target-ops 100000

Usage (embedded):
    from core.firehose import Firehose
    fh = Firehose(ring_buffer, metrics, target_ops=100_000)
    asyncio.run(fh.run(duration=10.0))
"""

import asyncio
import random
import time
import argparse

from core.constants import SIDE_BUY, SIDE_SELL, DEFAULT_TARGET_OPS, DEFAULT_DURATION
from core.order_struct import Order
from core.metrics import EngineMetrics

# Simulated price levels
_PRICES = [round(99.0 + i * 0.25, 2) for i in range(9)]


class Firehose:
    """Async producer that blasts orders into a RingBuffer."""

    BATCH = 64   # orders per asyncio iteration

    def __init__(self, ring_buffer, metrics: EngineMetrics,
                 target_ops: int = DEFAULT_TARGET_OPS):
        self._rb      = ring_buffer
        self._metrics = metrics
        self._target  = target_ops
        self._rng     = random.Random()
        self._seq     = 0

    async def run(self, duration: float = DEFAULT_DURATION) -> None:
        """Run the firehose for `duration` seconds."""
        m         = self._metrics
        rb        = self._rb
        rng       = self._rng
        prices    = _PRICES
        batch     = self.BATCH
        interval  = batch / self._target      # seconds per batch

        m.start_time = time.monotonic()
        deadline     = m.start_time + duration

        while time.monotonic() < deadline:
            t0 = time.perf_counter()

            for _ in range(batch):
                price = rng.choice(prices) + rng.uniform(-0.005, 0.005)
                qty   = rng.randint(1, 1000)
                side  = SIDE_BUY if rng.random() < 0.5 else SIDE_SELL
                order = Order(order_id=self._seq, price=price,
                              quantity=qty, side=side)
                self._seq += 1

                if rb.put(order):
                    m.sent()
                else:
                    m.dropped()

            elapsed = time.perf_counter() - t0
            sleep   = interval - elapsed
            if sleep > 0.0005:
                await asyncio.sleep(sleep)
            else:
                await asyncio.sleep(0)   # yield to event loop


if __name__ == "__main__":
    import sys, os, tempfile
    sys.stdout.reconfigure(encoding="utf-8")
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

    from core.ring_buffer import RingBuffer

    ap = argparse.ArgumentParser()
    ap.add_argument("--duration",   type=float, default=5.0)
    ap.add_argument("--target-ops", type=int,   default=DEFAULT_TARGET_OPS)
    args = ap.parse_args()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".rb") as f:
        tmp = f.name
    try:
        rb      = RingBuffer(tmp, create=True)
        metrics = EngineMetrics()
        fh      = Firehose(rb, metrics, target_ops=args.target_ops)
        print(f"Firehose: target={args.target_ops:,}/sec  duration={args.duration}s")
        asyncio.run(fh.run(duration=args.duration))
        s = metrics.snapshot()
        print(f"  Sent    : {s['orders_sent']:>10,}")
        print(f"  Dropped : {s['orders_dropped']:>10,}")
        print(f"  Rate    : {s['enqueue_rate']:>10,.0f} orders/sec")
    finally:
        rb.close()
        try: os.unlink(tmp)
        except: pass
