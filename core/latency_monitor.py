"""
latency_monitor.py  -  Week 3: Nanosecond Latency Monitoring & Spike Detection
==============================================================================
Accurately records order journey timestamps:
  1. Ingestion Timestamp  (t_ingest): Captured when network packet/order enters ring buffer.
  2. Engine Entry Timestamp (t_entry) : Captured when dequeued from mmap into matching engine.
  3. Execution Timestamp   (t_exec)  : Captured when trade execution completes.

Calculates:
  - Processing Latency: t_exec - t_entry (pure matching engine loop time)
  - End-to-End Latency: t_exec - t_ingest (total latency including zero-copy IPC transit)
  - Percentiles: p50, p90, p95, p99, p99.9
  - Spike Detection: Flagged when latency exceeds SLA threshold (e.g. 50 µs)
  - Export: JSON and CSV reporting
"""

import csv
import json
import logging
import math
import os
import sys
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger("hft.latency")


@dataclass(slots=True)
class LatencyRecord:
    order_id: int
    ingestion_ns: int
    entry_ns: int
    execution_ns: int
    processing_latency_ns: int
    end_to_end_latency_ns: int
    is_spike: bool = False

    @property
    def processing_latency_us(self) -> float:
        return self.processing_latency_ns / 1_000.0

    @property
    def end_to_end_latency_us(self) -> float:
        return self.end_to_end_latency_ns / 1_000.0


class LatencyMonitor:
    """
    High-frequency nanosecond latency telemetry engine.
    Maintains a rolling sample window and tracks latency statistics without GC interference.
    """

    def __init__(self, window_size: int = 50_000, spike_threshold_us: float = 50.0):
        self.window_size = window_size
        self.spike_threshold_us = spike_threshold_us
        self._lock = threading.Lock()

        # Rolling sample deques of latency in microseconds
        self._processing_samples: Deque[float] = deque(maxlen=window_size)
        self._e2e_samples: Deque[float] = deque(maxlen=window_size)
        self._records: Deque[LatencyRecord] = deque(maxlen=window_size)

        self._in_flight_ingestion: Dict[int, int] = {}

        self.total_recorded = 0
        self.total_spikes = 0
        self.last_spike: Optional[Dict[str, Any]] = None

    def start_order(self, order_id: int) -> int:
        """Capture ingestion timestamp when order enters system/buffer."""
        t_ingest = time.perf_counter_ns()
        with self._lock:
            self._in_flight_ingestion[order_id] = t_ingest
        return t_ingest

    def enter_engine(self, order_id: int) -> int:
        """Capture engine entry timestamp when order reaches matching loop."""
        return time.perf_counter_ns()

    def record_trade(
        self,
        order_id: int,
        entry_ns: Optional[int] = None,
        ingestion_ns: Optional[int] = None,
        execution_ns: Optional[int] = None,
    ) -> LatencyRecord:
        """
        Record a trade execution and calculate processing & end-to-end latencies.
        """
        now_ns = execution_ns or time.perf_counter_ns()

        with self._lock:
            t_ingest = ingestion_ns or self._in_flight_ingestion.pop(order_id, now_ns)

        t_entry = entry_ns or t_ingest
        proc_ns = max(0, now_ns - t_entry)
        e2e_ns = max(0, now_ns - t_ingest)

        proc_us = proc_ns / 1_000.0
        e2e_us = e2e_ns / 1_000.0
        is_spike = e2e_us >= self.spike_threshold_us

        rec = LatencyRecord(
            order_id=order_id,
            ingestion_ns=t_ingest,
            entry_ns=t_entry,
            execution_ns=now_ns,
            processing_latency_ns=proc_ns,
            end_to_end_latency_ns=e2e_ns,
            is_spike=is_spike,
        )

        with self._lock:
            self._processing_samples.append(proc_us)
            self._e2e_samples.append(e2e_us)
            self._records.append(rec)
            self.total_recorded += 1
            if is_spike:
                self.total_spikes += 1
                self.last_spike = {
                    "order_id": order_id,
                    "latency_us": e2e_us,
                    "threshold_us": self.spike_threshold_us,
                    "timestamp_ns": now_ns,
                }

        return rec

    def _calc_stats(self, samples: List[float]) -> Dict[str, float]:
        if not samples:
            return {
                "avg": 0.0,
                "min": 0.0,
                "max": 0.0,
                "p50": 0.0,
                "p90": 0.0,
                "p95": 0.0,
                "p99": 0.0,
                "p999": 0.0,
                "count": 0,
            }
        s = sorted(samples)
        n = len(s)
        avg = sum(s) / n
        p50 = s[int(n * 0.50)]
        p90 = s[int(n * 0.90)]
        p95 = s[int(n * 0.95)]
        p99 = s[min(n - 1, int(n * 0.99))]
        p999 = s[min(n - 1, int(n * 0.999))]

        return {
            "avg": round(avg, 3),
            "min": round(s[0], 3),
            "max": round(s[-1], 3),
            "p50": round(p50, 3),
            "p90": round(p90, 3),
            "p95": round(p95, 3),
            "p99": round(p99, 3),
            "p999": round(p999, 3),
            "count": n,
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Generate comprehensive metrics snapshot."""
        with self._lock:
            proc_list = list(self._processing_samples)
            e2e_list = list(self._e2e_samples)
            total_spikes = self.total_spikes
            last_spike = self.last_spike
            total_rec = self.total_recorded

        proc_stats = self._calc_stats(proc_list)
        e2e_stats = self._calc_stats(e2e_list)

        return {
            "processing_latency_us": proc_stats,
            "end_to_end_latency_us": e2e_stats,
            "total_recorded": total_rec,
            "spike_threshold_us": self.spike_threshold_us,
            "total_spikes": total_spikes,
            "last_spike": last_spike,
            "sla_compliant": e2e_stats["p99"] < self.spike_threshold_us,
        }

    def export_json(self, filepath: str) -> None:
        """Export current latency summary and recent samples to JSON."""
        metrics = self.get_metrics()
        with self._lock:
            recent_recs = [
                {
                    "order_id": r.order_id,
                    "proc_us": r.processing_latency_us,
                    "e2e_us": r.end_to_end_latency_us,
                    "is_spike": r.is_spike,
                }
                for r in list(self._records)[-100:]
            ]
        data = {
            "timestamp": time.time(),
            "metrics": metrics,
            "recent_samples": recent_recs,
        }
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logger.info("Exported latency metrics JSON to %s", filepath)

    def export_csv(self, filepath: str) -> None:
        """Export recent records to CSV."""
        with self._lock:
            recs = list(self._records)
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "order_id",
                "ingestion_ns",
                "entry_ns",
                "execution_ns",
                "processing_us",
                "end_to_end_us",
                "is_spike",
            ])
            for r in recs:
                writer.writerow([
                    r.order_id,
                    r.ingestion_ns,
                    r.entry_ns,
                    r.execution_ns,
                    r.processing_latency_us,
                    r.end_to_end_latency_us,
                    1 if r.is_spike else 0,
                ])
        logger.info("Exported %d latency records to %s", len(recs), filepath)


# Standalone demonstration
if __name__ == "__main__":
    import random
    print("=" * 65)
    print("  CHRONOSMATCH - NANOSECOND LATENCY MONITOR (WEEK 3)")
    print("=" * 65)

    monitor = LatencyMonitor(spike_threshold_us=50.0)

    # Simulate 5,000 orders
    for i in range(1, 5001):
        t_ingest = monitor.start_order(i)
        # simulated IPC transit delay (0.5 to 2.5 µs)
        time.sleep(0.000001)
        t_entry = monitor.enter_engine(i)
        # simulated matching engine execution (1 to 10 µs)
        t_exec = time.perf_counter_ns()
        monitor.record_trade(i, entry_ns=t_entry, ingestion_ns=t_ingest, execution_ns=t_exec)

    m = monitor.get_metrics()
    p = m["processing_latency_us"]
    e = m["end_to_end_latency_us"]

    print(f"  Total Trades Audited : {m['total_recorded']:,}")
    print(f"  Processing Latency   : Avg: {p['avg']} µs | p50: {p['p50']} µs | p95: {p['p95']} µs | p99: {p['p99']} µs")
    print(f"  End-to-End Latency   : Avg: {e['avg']} µs | p50: {e['p50']} µs | p95: {e['p95']} µs | p99: {e['p99']} µs")
    print(f"  Spikes (> 50 µs)     : {m['total_spikes']} spikes")
    print(f"  Sub-50 µs SLA Status : {'COMPLIANT' if m['sla_compliant'] else 'BREACH'}")

    out_json = "logs/latency_metrics.json"
    out_csv = "logs/latency_samples.csv"
    monitor.export_json(out_json)
    monitor.export_csv(out_csv)
    print(f"  Exported reports to {out_json} & {out_csv}")
    print("=" * 65)
