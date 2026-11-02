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
# Simulated price levels centered around $99.50
_PRICES = [round(99.4950 + i * 0.0010, 4) for i in range(12)]


class Firehose:
    """Async & Threaded producer that blasts orders into a RingBuffer."""

    BATCH = 256   # orders per batch for smooth high-frequency flow

    def __init__(self, ring_buffer, metrics: EngineMetrics,
                 target_ops: int = DEFAULT_TARGET_OPS):
        self._rb        = ring_buffer
        self._metrics   = metrics
        self._target    = target_ops
        self._rng       = random.Random()
        self._seq       = 0
        self.is_paused  = False
        self.is_running = True
        self._mid_px    = 99.5004

    def update_mid(self, mid: float) -> None:
        if mid and mid > 0:
            self._mid_px = mid

    def pause(self) -> None:
        self.is_paused = True

    def resume(self) -> None:
        self.is_paused = False

    def toggle(self) -> bool:
        self.is_paused = not self.is_paused
        return not self.is_paused

    def set_target_ops(self, rate: int) -> None:
        self._target = max(1_000, int(rate))

    def stop(self) -> None:
        self.is_running = False

    def inject_shock(self, whale_volume: int = 15_000) -> None:
        """Inject an aggressive market shock with whale orders."""
        rng = self._rng
        # Inject multiple large sell and buy sweeps
        for _ in range(4):
            px = self._mid_px + rng.uniform(-0.004, 0.004)
            self._seq += 1
            o = Order(order_id=self._seq, price=round(px, 4), quantity=whale_volume, side=SIDE_SELL)
            self._rb.put(o)
            self._metrics.sent()
        for _ in range(4):
            px = self._mid_px + rng.uniform(-0.004, 0.004)
            self._seq += 1
            o = Order(order_id=self._seq, price=round(px, 4), quantity=whale_volume, side=SIDE_BUY)
            self._rb.put(o)
            self._metrics.sent()

    async def run(self, duration: float = DEFAULT_DURATION) -> None:
        """Run the firehose for `duration` seconds."""
        m         = self._metrics
        rb        = self._rb
        rng       = self._rng
        batch     = self.BATCH

        m.start_time = time.monotonic()
        deadline     = m.start_time + duration if duration > 0 else float("inf")
        self.is_running = True

        while self.is_running and time.monotonic() < deadline:
            if self.is_paused:
                await asyncio.sleep(0.05)
                continue

            interval = batch / max(1000, self._target)
            t0 = time.perf_counter()

            for _ in range(batch):
                mid = self._mid_px
                # 50% chance aggressive cross-spread matching, 50% passive resting
                if rng.random() < 0.50:
                    side = SIDE_BUY if rng.random() < 0.5 else SIDE_SELL
                    price = mid + (0.0008 if side == SIDE_BUY else -0.0008) + rng.uniform(-0.0003, 0.0003)
                    qty = rng.randint(50, 1200)
                else:
                    side = SIDE_BUY if rng.random() < 0.5 else SIDE_SELL
                    price = mid + (-0.0010 if side == SIDE_BUY else 0.0010) + rng.uniform(-0.0015, 0.0015)
                    qty = rng.randint(20, 800)

                self._seq += 1
                order = Order(order_id=self._seq, price=round(price, 4),
                              quantity=qty, side=side)

                if rb.put(order):
                    m.sent()
                else:
                    m.dropped()

            elapsed = time.perf_counter() - t0
            sleep = interval - elapsed
            if sleep > 0.001:
                await asyncio.sleep(sleep)
            else:
                await asyncio.sleep(0)

    def run_sync(self, duration: float = 0) -> None:
        """Run the firehose synchronously in a dedicated OS worker thread."""
        m        = self._metrics
        rb       = self._rb
        rng      = self._rng
        batch    = self.BATCH

        m.start_time = time.monotonic()
        deadline     = m.start_time + duration if duration > 0 else float("inf")
        self.is_running = True

        while self.is_running and time.monotonic() < deadline:
            try:
                if self.is_paused:
                    time.sleep(0.05)
                    continue

                interval = batch / max(1000, self._target)
                t0 = time.perf_counter()

                for _ in range(batch):
                    mid = self._mid_px
                    # 55% aggressive cross-spread (triggers real-time trades), 45% passive resting
                    if rng.random() < 0.55:
                        side = SIDE_BUY if rng.random() < 0.5 else SIDE_SELL
                        price = mid + (0.0008 if side == SIDE_BUY else -0.0008) + rng.uniform(-0.0004, 0.0004)
                        qty = rng.randint(50, 1500)
                    else:
                        side = SIDE_BUY if rng.random() < 0.5 else SIDE_SELL
                        price = mid + (-0.0010 if side == SIDE_BUY else 0.0010) + rng.uniform(-0.0020, 0.0020)
                        qty = rng.randint(20, 900)

                    # Rare simulated whale order (0.3% chance)
                    if rng.random() < 0.003:
                        qty = rng.randint(8000, 20000)
                        price = mid + (0.0030 if side == SIDE_BUY else -0.0030)

                    self._seq += 1
                    order = Order(order_id=self._seq, price=round(price, 4),
                                  quantity=qty, side=side)

                    if rb.put(order):
                        m.sent()
                    else:
                        m.dropped()

                elapsed = time.perf_counter() - t0
                sleep   = interval - elapsed
                if sleep > 0.003:
                    time.sleep(sleep)
                elif sleep > 0.0005:
                    time.sleep(0.001)
            except Exception:
                time.sleep(0.05)


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
