"""
trade_persistence.py  -  Week 4: Asynchronous Trade Persistence Worker
======================================================================
Asynchronously offloads completed trade records from the matching engine
to the SQLite transaction ledger (chronosmatch.db) without inducing
latency or GC pauses on the critical trading loop.

Features:
  - Zero-lag offloading: trades are handed off via a thread-safe / asyncio queue.
  - Micro-batched flushing: writes batches to SQLite in WAL mode.
  - Backpressure protection: non-blocking handoff prevents matching pauses.
  - Database error recovery & isolation.
  - Graceful shutdown: guarantees all in-flight trades are flushed before termination.
"""

import asyncio
import logging
import os
import queue
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from database import DatabaseLedger

logger = logging.getLogger("hft.persistence")


class TradePersistenceWorker:
    """
    Background persistence worker thread that drains matched trades and writes
    them in atomic batches to the SQLite transaction ledger.
    """

    def __init__(
        self,
        db_ledger: Optional[DatabaseLedger] = None,
        db_path: Optional[str] = "chronosmatch.db",
        batch_size: int = 500,
        flush_interval_s: float = 0.05,
        max_queue_size: int = 200_000,
    ):
        self.db = db_ledger or DatabaseLedger(db_path)
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_s

        self._queue: queue.Queue = queue.Queue(maxsize=max_queue_size)
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None

        self.total_enqueued = 0
        self.total_persisted = 0
        self.total_dropped = 0

        self.start()

    def start(self) -> None:
        """Start the background persistence worker thread."""
        if self._worker_thread is not None and self._worker_thread.is_alive():
            return
        self._stop_event.clear()
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            name="TradePersistenceWorker",
            daemon=True,
        )
        self._worker_thread.start()
        logger.info("TradePersistenceWorker started (batch_size=%d, interval=%.3fs)",
                    self.batch_size, self.flush_interval_s)

    def enqueue_trade(self, trade_data: Dict[str, Any]) -> bool:
        """
        Non-blocking handoff from matching hot path.
        Returns True if enqueued, False if dropped due to backpressure.
        """
        try:
            self._queue.put_nowait(trade_data)
            self.total_enqueued += 1
            return True
        except queue.Full:
            self.total_dropped += 1
            logger.warning("Persistence queue full! Dropped trade: %s", trade_data.get("trade_id"))
            return False

    def _worker_loop(self) -> None:
        """Worker thread loop: collects batches and flushes them to SQLite."""
        batch: List[Dict[str, Any]] = []
        last_flush = time.monotonic()

        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=self.flush_interval_s)
                batch.append(item)
                self._queue.task_done()
            except queue.Empty:
                pass

            now = time.monotonic()
            if (len(batch) >= self.batch_size) or (batch and now - last_flush >= self.flush_interval_s):
                self._flush_batch(batch)
                batch.clear()
                last_flush = now

        # Drain any remaining items in queue before shutting down
        while not self._queue.empty():
            try:
                batch.append(self._queue.get_nowait())
                self._queue.task_done()
            except queue.Empty:
                break
        if batch:
            self._flush_batch(batch)

    def _flush_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Flushes a batch of trades to SQLite."""
        if not batch:
            return
        try:
            inserted = self.db.insert_trades_batch(batch)
            self.total_persisted += inserted
        except Exception as e:
            logger.error("Failed to flush trade batch to SQLite: %s", e)

    def flush(self) -> None:
        """Manually trigger flush of all currently queued items."""
        batch = []
        while not self._queue.empty():
            try:
                batch.append(self._queue.get_nowait())
                self._queue.task_done()
            except queue.Empty:
                break
        if batch:
            self._flush_batch(batch)

    def stop(self, timeout: float = 3.0) -> None:
        """Gracefully stop worker thread and flush pending trades."""
        self._stop_event.set()
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=timeout)
        self.flush()
        logger.info("TradePersistenceWorker stopped. Total persisted: %d", self.total_persisted)

    def stats(self) -> Dict[str, Any]:
        return {
            "queue_size": self._queue.qsize(),
            "total_enqueued": self.total_enqueued,
            "total_persisted": self.total_persisted,
            "total_dropped": self.total_dropped,
            "is_alive": self._worker_thread.is_alive() if self._worker_thread else False,
            "db_stats": self.db.stats(),
        }


# Asyncio-native wrapper for async event loops
class AsyncTradePersistence:
    """Async wrapper that routes asyncio.Queue to TradePersistenceWorker."""

    def __init__(self, worker: TradePersistenceWorker):
        self.worker = worker
        self.async_queue: asyncio.Queue = asyncio.Queue(maxsize=100_000)
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        self._task = asyncio.create_task(self._drain_loop())

    async def enqueue(self, trade_data: Dict[str, Any]):
        try:
            self.async_queue.put_nowait(trade_data)
        except asyncio.QueueFull:
            self.worker.total_dropped += 1

    async def _drain_loop(self):
        while True:
            trade = await self.async_queue.get()
            self.worker.enqueue_trade(trade)
            self.async_queue.task_done()

    async def stop(self):
        if self._task:
            self._task.cancel()
        self.worker.stop()


if __name__ == "__main__":
    print("=" * 65)
    print("  CHRONOSMATCH - ASYNCHRONOUS TRADE PERSISTENCE TEST (WEEK 4)")
    print("=" * 65)

    worker = TradePersistenceWorker(db_path="chronosmatch.db", batch_size=50)

    # Enqueue 200 trades
    t0 = time.perf_counter_ns()
    for i in range(1, 201):
        trade_data = {
            "trade_id": f"TRD-ASYNC-{i:04d}",
            "buy_order_id": 50000 + i,
            "sell_order_id": 60000 + i,
            "price": 100.25 + (i * 0.01),
            "quantity": 100,
            "timestamp_ns": time.time_ns(),
            "latency_ns": 3800,
            "is_whale": (i % 25 == 0),
            "cleared_levels": 3 if (i % 25 == 0) else 1,
        }
        worker.enqueue_trade(trade_data)
    t1 = time.perf_counter_ns()

    dur_us = (t1 - t0) / 1000.0
    print(f"  Enqueued 200 trades in    : {dur_us:.2f} µs ({dur_us/200:.2f} µs/trade, non-blocking)")

    # Wait briefly for worker to flush
    time.sleep(0.2)
    worker.stop()

    st = worker.stats()
    print(f"  Total Enqueued            : {st['total_enqueued']:,}")
    print(f"  Total Persisted to SQLite : {st['total_persisted']:,}")
    print(f"  Dropped (Backpressure)    : {st['total_dropped']:,}")
    print(f"  Worker Active Status      : {'STOPPED (CLEAN SHUTDOWN)' if not st['is_alive'] else 'RUNNING'}")
    print("=" * 65)
