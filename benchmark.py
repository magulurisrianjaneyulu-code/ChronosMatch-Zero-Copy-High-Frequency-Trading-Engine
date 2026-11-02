"""
benchmark.py  -  Week 3: High-Frequency Performance Benchmarking Suite
======================================================================
Tests ChronosMatch under varying loads:
  - 1,000 orders
  - 10,000 orders
  - 100,000 orders
  - 1,000,000 orders

Measures:
  - Total elapsed wall time
  - Orders per second (Throughput)
  - Trades per second
  - Execution Latency (Avg, p50, p90, p95, p99, p99.9) in microseconds
  - Python GC collection events
  - Outputs structured CSV and JSON benchmark reports.
"""

import argparse
import csv
import gc
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from core.constants import SIDE_BUY, SIDE_SELL
from core.order_struct import Order
from engine.limit_order_book import LimitOrderBook


def generate_mock_orders(n: int, seed: int = 42) -> List[Order]:
    """
    Generates n realistic alternating Buy and Sell orders centered around $100.00.
    Ensures liquidity resting and realistic crossing trades.
    """
    rng = random.Random(seed)
    orders: List[Order] = []
    base_price = 100.00

    for i in range(1, n + 1):
        # 50/50 buy vs sell
        side = SIDE_BUY if rng.random() > 0.50 else SIDE_SELL
        # Price tick variance around mid
        offset = round(rng.gauss(0, 0.05), 2)
        price = max(0.01, round(base_price + offset, 4))
        qty = rng.choice([10, 50, 100, 200, 500, 1000])

        orders.append(Order(
            order_id=i,
            price=price,
            quantity=qty,
            side=side
        ))
    return orders


def run_single_benchmark(n_orders: int, disable_gc: bool = True) -> Dict[str, Any]:
    """Executes a benchmark run for a specific number of orders."""
    print(f"  --> Benchmarking {n_orders:,} orders (GC disabled={disable_gc})...")
    orders = generate_mock_orders(n_orders)

    lob = LimitOrderBook()

    if disable_gc:
        gc.collect()
        gc.disable()
    gc_collections_before = sum(s['collections'] for s in gc.get_stats()) if hasattr(gc, 'get_stats') else 0

    latencies_us: List[float] = []
    total_trades = 0
    total_volume = 0

    t_start = time.perf_counter_ns()

    for order in orders:
        t0 = time.perf_counter_ns()
        trades = lob.submit(order, entry_ns=t0)
        t1 = time.perf_counter_ns()

        lat_us = (t1 - t0) / 1000.0
        latencies_us.append(lat_us)

        if trades:
            total_trades += len(trades)
            total_volume += sum(t.quantity for t in trades)

    t_end = time.perf_counter_ns()
    gc_collections_after = sum(s['collections'] for s in gc.get_stats()) if hasattr(gc, 'get_stats') else 0
    if disable_gc:
        gc.enable()
    gc_spikes = gc_collections_after - gc_collections_before

    elapsed_s = (t_end - t_start) / 1_000_000_000.0
    orders_per_sec = n_orders / elapsed_s if elapsed_s > 0 else 0.0
    trades_per_sec = total_trades / elapsed_s if elapsed_s > 0 else 0.0

    sorted_lats = sorted(latencies_us)
    avg_lat = sum(sorted_lats) / len(sorted_lats)
    p50 = sorted_lats[int(len(sorted_lats) * 0.50)]
    p90 = sorted_lats[int(len(sorted_lats) * 0.90)]
    p95 = sorted_lats[int(len(sorted_lats) * 0.95)]
    p99 = sorted_lats[int(len(sorted_lats) * 0.99)]
    p999 = sorted_lats[min(len(sorted_lats) - 1, int(len(sorted_lats) * 0.999))]

    result = {
        "workload_orders": n_orders,
        "elapsed_seconds": round(elapsed_s, 4),
        "orders_per_sec": round(orders_per_sec, 2),
        "total_trades": total_trades,
        "trades_per_sec": round(trades_per_sec, 2),
        "total_volume": total_volume,
        "avg_latency_us": round(avg_lat, 3),
        "p50_latency_us": round(p50, 3),
        "p90_latency_us": round(p90, 3),
        "p95_latency_us": round(p95, 3),
        "p99_latency_us": round(p99, 3),
        "p999_latency_us": round(p999, 3),
        "min_latency_us": round(sorted_lats[0], 3),
        "max_latency_us": round(sorted_lats[-1], 3),
        "gc_collection_events": gc_spikes,
        "sub_50us_compliant": p99 < 50.0,
    }
    return result


def export_reports(results: List[Dict[str, Any]], out_dir: str = "logs") -> Tuple[str, str]:
    """Exports benchmark results to both JSON and CSV files."""
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(out_dir, f"benchmark_report_{ts}.json")
    csv_path = os.path.join(out_dir, f"benchmark_report_{ts}.csv")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "benchmarks": results
        }, f, indent=2)

    fieldnames = [
        "workload_orders", "elapsed_seconds", "orders_per_sec", "total_trades",
        "trades_per_sec", "total_volume", "avg_latency_us", "p50_latency_us",
        "p90_latency_us", "p95_latency_us", "p99_latency_us", "p999_latency_us",
        "min_latency_us", "max_latency_us", "gc_collection_events", "sub_50us_compliant"
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    return json_path, csv_path


def print_comparison_table(results: List[Dict[str, Any]]) -> None:
    """Prints a terminal comparison table across tested workloads."""
    print("\n" + "=" * 90)
    print("  CHRONOSMATCH - HFT BENCHMARK PERFORMANCE COMPARISON TABLE")
    print("=" * 90)
    header = (
        f"  {'Orders':>10} | {'Wall Time':>10} | {'Throughput':>14} | "
        f"{'Avg Lat':>9} | {'p50':>8} | {'p95':>8} | {'p99':>8} | {'GC Events':>9}"
    )
    print(header)
    print("  " + "-" * 88)
    for r in results:
        ord_str = f"{r['workload_orders']:,}"
        time_str = f"{r['elapsed_seconds']:.2f}s"
        tps_str = f"{r['orders_per_sec']:,.0f} ops"
        avg_str = f"{r['avg_latency_us']:.2f} µs"
        p50_str = f"{r['p50_latency_us']:.2f} µs"
        p95_str = f"{r['p95_latency_us']:.2f} µs"
        p99_str = f"{r['p99_latency_us']:.2f} µs"
        gc_str = str(r["gc_collection_events"])
        print(f"  {ord_str:>10} | {time_str:>10} | {tps_str:>14} | {avg_str:>9} | {p50_str:>8} | {p95_str:>8} | {p99_str:>8} | {gc_str:>9}")
    print("=" * 90)


def main():
    parser = argparse.ArgumentParser(description="ChronosMatch Performance Benchmarking Suite")
    parser.add_argument("--orders", type=int, default=None,
                        help="Specific order count to benchmark (e.g. 1000, 10000, 100000, 1000000)")
    parser.add_argument("--all", action="store_true",
                        help="Run full comparison suite across 1K, 10K, 100K, and 1M orders")
    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("  CHRONOSMATCH - LOW-LATENCY MATCHING ENGINE BENCHMARK SUITE")
    print("=" * 70)

    workloads = []
    if args.orders:
        workloads = [args.orders]
    elif args.all:
        workloads = [1_000, 10_000, 100_000, 1_000_000]
    else:
        # Default run: quick multi-tier demonstration
        workloads = [1_000, 10_000, 100_000]

    results = []
    for w in workloads:
        res = run_single_benchmark(w)
        results.append(res)

    print_comparison_table(results)

    json_f, csv_f = export_reports(results)
    print(f"\n  [REPORTS GENERATED]")
    print(f"  • JSON Report : {json_f}")
    print(f"  • CSV Report  : {csv_f}\n")


if __name__ == "__main__":
    main()
