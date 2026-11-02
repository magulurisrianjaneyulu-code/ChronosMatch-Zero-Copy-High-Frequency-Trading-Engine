"""
bench/engine_verify.py  -  Phase 3: Matching Engine Verification
================================================================
Deterministic correctness and latency tests for the LOB engine.

Tests:
  1. Basic exact-price match
  2. No match (spread)
  3. Partial fill
  4. Price-Time priority (FIFO at same price)
  5. Price priority (best price fills first)
  6. Multi-level aggressive sweep
  7. Latency benchmark (100k orders, GC off, p99 < 50 us, 0 GC pauses)

Usage:
    python -m bench.engine_verify
"""

import gc
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")

from core.constants import SIDE_BUY, SIDE_SELL
from core.order_struct import Order
from engine.limit_order_book import LimitOrderBook, Trade

# ---- helpers ----------------------------------------------------------------
_failures = []

def check(label: str, cond: bool, detail: str = ""):
    tag = "  PASS" if cond else "  FAIL"
    col = "\033[92m" if cond else "\033[91m"
    msg = f"{col}{tag}\033[0m  {label}"
    if detail: msg += f"  [{detail}]"
    print(msg)
    if not cond:
        _failures.append(label)


# ---- test cases -------------------------------------------------------------

def test_basic_match():
    lob = LimitOrderBook()
    lob.submit(Order(1, 100.00, 100, SIDE_SELL))
    t = lob.submit(Order(2, 100.00, 100, SIDE_BUY))
    check("basic_match: one trade",        len(t) == 1)
    check("basic_match: price == 100.00",  t[0].price == 100.00)
    check("basic_match: qty == 100",       t[0].quantity == 100)
    check("basic_match: book empty",       lob.bid_depth == 0 and lob.ask_depth == 0)


def test_no_match():
    lob = LimitOrderBook()
    lob.submit(Order(1, 100.50, 100, SIDE_SELL))
    t = lob.submit(Order(2,  99.00, 100, SIDE_BUY))
    check("no_match: zero trades",         len(t) == 0)
    check("no_match: both sides resting",  lob.bid_depth == 1 and lob.ask_depth == 1)


def test_partial_fill():
    lob = LimitOrderBook()
    lob.submit(Order(1, 100.00, 100, SIDE_SELL))
    t = lob.submit(Order(2, 100.00, 150, SIDE_BUY))
    check("partial: one trade",       len(t) == 1)
    check("partial: fill qty=100",    t[0].quantity == 100)
    check("partial: ask empty",       lob.ask_depth == 0)
    check("partial: 50 resting bid",  lob.bid_depth == 1)


def test_price_time_priority():
    lob = LimitOrderBook()
    for i in range(3):
        lob.submit(Order(i, 100.00, 50, SIDE_SELL))
    t = lob.submit(Order(99, 100.00, 150, SIDE_BUY))
    check("ptp: 3 trades",      len(t) == 3)
    check("ptp: FIFO order",    [tr.resting_id for tr in t] == [0, 1, 2])


def test_price_priority():
    lob = LimitOrderBook()
    lob.submit(Order(1, 100.50, 100, SIDE_SELL))   # higher
    lob.submit(Order(2, 100.00, 100, SIDE_SELL))   # lower -> fills first
    t = lob.submit(Order(99, 101.00, 100, SIDE_BUY))
    check("price_prio: fills cheapest",   t[0].resting_id == 2)
    check("price_prio: trade @ 100.00",   t[0].price == 100.00)


def test_multi_sweep():
    lob = LimitOrderBook()
    for i, px in enumerate([100.00, 100.25, 100.50, 100.75, 101.00]):
        lob.submit(Order(i, px, 20, SIDE_SELL))
    t = lob.submit(Order(99, 101.00, 100, SIDE_BUY))
    check("sweep: 5 trades",      len(t) == 5)
    check("sweep: total qty=100", sum(tr.quantity for tr in t) == 100)


def test_whale_sweep_detection():
    lob = LimitOrderBook()
    # Populate multiple resting ask price levels
    for i, px in enumerate([100.00, 100.10, 100.20, 100.30]):
        lob.submit(Order(i, px, 50, SIDE_SELL))
    # Aggressive whale sweeps across 3 price levels
    whale_order = Order(999, 100.25, 120, SIDE_BUY)
    trades = lob.submit(whale_order)
    check("whale_sweep: matched multiple levels", len(trades) >= 3)
    check("whale_sweep: flagged is_whale", trades[0].is_whale == True)
    check("whale_sweep: cleared_levels >= 3", trades[0].cleared_levels >= 3)
    check("whale_sweep: event captured in lob", lob.last_whale_event is not None)


def test_sqlite_audit_ledger():
    import tempfile, os
    from core.ledger import AuditLedger
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
        tmp_db = f.name
    try:
        ledger = AuditLedger(db_path=tmp_db, batch_size=5, flush_interval_s=0.01)
        lob = LimitOrderBook(ledger=ledger)

        # Place matching orders
        lob.submit(Order(1, 100.00, 50, SIDE_SELL))
        lob.submit(Order(2, 100.00, 50, SIDE_BUY))

        # Allow worker thread to flush
        time.sleep(0.08)
        ledger.close()

        records = ledger.get_recent_trades(limit=10)
        check("ledger: trade persisted to sqlite", len(records) >= 1)
        if records:
            check("ledger: valid price recorded", records[0]["price"] == 100.00)
            check("ledger: valid quantity recorded", records[0]["quantity"] == 50)
            check("ledger: cryptographic hash exists", len(records[0]["hash_checksum"]) == 64)

        integrity = ledger.verify_integrity()
        check("ledger: sha256 cryptographic chain valid", integrity["valid"] == True)
    finally:
        try: os.unlink(tmp_db)
        except Exception: pass


def test_nanosecond_metrics():
    lob = LimitOrderBook()
    lob.submit(Order(1, 100.00, 10, SIDE_SELL))
    t0_ns = time.perf_counter_ns()
    trades = lob.submit(Order(2, 100.00, 10, SIDE_BUY), entry_ns=t0_ns)
    check("nanosecond: trade generated", len(trades) == 1)
    check("nanosecond: timestamp_ns > 0", trades[0].timestamp_ns > 0)
    check("nanosecond: latency_ns > 0", trades[0].latency_ns > 0)



def test_latency_hot_path(n: int = 100_000):
    lob = LimitOrderBook()
    # Seed book so every order matches
    for i in range(500):
        lob.submit(Order(i,     100.00, 10, SIDE_BUY))
        lob.submit(Order(i+500, 100.00, 10, SIDE_SELL))

    latencies = [0.0] * n

    # Use gc.callbacks to detect actual collection events (Python 3.12+)
    gc_events = []
    gc_cb = lambda phase, info: gc_events.append(phase)
    gc.callbacks.append(gc_cb)
    gc.disable()

    for i in range(n):
        side  = SIDE_BUY if i % 2 == 0 else SIDE_SELL
        o     = Order(order_id=10_000 + i, price=100.00, quantity=1, side=side)
        t0    = time.perf_counter_ns()
        lob.submit(o)
        latencies[i] = (time.perf_counter_ns() - t0) / 1_000.0  # us

    gc.enable()
    gc.callbacks.remove(gc_cb)
    gc_pauses = sum(1 for p in gc_events if p == "start")

    s   = sorted(latencies)
    avg = sum(latencies) / len(latencies)
    p50 = s[int(n * 0.50)]
    p99 = s[int(n * 0.99)]
    p999= s[int(n * 0.999)]

    print(f"\n  Latency benchmark ({n:,} orders, GC disabled):")
    print(f"    avg  : {avg:.3f} us")
    print(f"    p50  : {p50:.3f} us")
    print(f"    p99  : {p99:.3f} us")
    print(f"    p99.9: {p999:.3f} us")
    print(f"    GC collection events: {gc_pauses}")

    check(f"latency_p99 < 50 us  (got {p99:.2f} us)", p99 < 50.0)
    check("zero_gc_pauses", gc_pauses == 0)


# ---- runner -----------------------------------------------------------------


def test_trade_persistence_worker():
    from trade_persistence import TradePersistenceWorker
    import tempfile, os
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
        tmp_db = f.name
    try:
        worker = TradePersistenceWorker(db_path=tmp_db, batch_size=5, flush_interval_s=0.01)
        for i in range(10):
            worker.enqueue_trade({
                "trade_id": f"TRD-TEST-{i}",
                "buy_order_id": 100+i,
                "sell_order_id": 200+i,
                "price": 100.0,
                "quantity": 10,
                "latency_ns": 4000,
                "is_whale": False,
                "cleared_levels": 1
            })
        time.sleep(0.08)
        worker.stop()
        st = worker.stats()
        check("persistence: all trades persisted", st["total_persisted"] == 10)
        check("persistence: zero dropped trades", st["total_dropped"] == 0)
    finally:
        try: os.unlink(tmp_db)
        except Exception: pass


def test_whale_detector_module():
    from whale_detector import WhaleDetector
    detector = WhaleDetector(quantity_threshold=5000, level_threshold=2)
    trades = [{"price": 100.1, "quantity": 1000}, {"price": 100.2, "quantity": 2000}]
    ev = detector.evaluate_execution(99, side=1, submitted_qty=3000, trades=trades, latency_us=5.5)
    check("whale_detector: multi-level sweep flagged", ev is not None and ev.is_whale)
    check("whale_detector: cleared levels == 2", ev is not None and ev.cleared_levels == 2)


def test_sqlite_transaction_ledger():
    from database import DatabaseLedger
    import tempfile, os
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
        tmp_db = f.name
    try:
        db = DatabaseLedger(db_path=tmp_db)
        t = {
            "trade_id": "TRD-DB-TEST-001",
            "buy_order_id": 101,
            "sell_order_id": 202,
            "price": 100.25,
            "quantity": 500,
            "timestamp_ns": time.time_ns(),
            "latency_ns": 3200,
            "is_whale": False,
            "cleared_levels": 1,
        }
        ok1 = db.insert_trade(t)
        check("database_ledger: single trade inserted", ok1)
        ok2 = db.insert_trade(t)
        check("database_ledger: duplicate trade prevented", not ok2)
        integ = db.verify_integrity()
        check("database_ledger: pragma integrity check ok", integ["status"] == "PASS")
    finally:
        try: os.unlink(tmp_db)
        except Exception: pass


def test_nanosecond_latency_monitor():
    from latency_monitor import LatencyMonitor
    lm = LatencyMonitor(spike_threshold_us=50.0)
    t_in = lm.start_order(1)
    t_en = lm.enter_engine(1)
    rec = lm.record_trade(1, entry_ns=t_en, ingestion_ns=t_in)
    check("latency_monitor: record generated", rec.order_id == 1)
    check("latency_monitor: processing latency computed", rec.processing_latency_ns >= 0)
    check("latency_monitor: end-to-end latency computed", rec.end_to_end_latency_ns >= 0)


def main():
    print(f"\n{'='*60}")
    print("  HFT ENGINE  -  Matching Engine Verification (Weeks 1 - 4)")
    print(f"{'='*60}\n")

    test_basic_match()
    test_no_match()
    test_partial_fill()
    test_price_time_priority()
    test_price_priority()
    test_multi_sweep()
    test_whale_sweep_detection()
    test_whale_detector_module()
    test_sqlite_audit_ledger()
    test_sqlite_transaction_ledger()
    test_trade_persistence_worker()
    test_nanosecond_metrics()
    test_nanosecond_latency_monitor()
    test_latency_hot_path()

    print(f"\n{'='*60}")
    if _failures:
        print(f"\033[91m  {len(_failures)} FAILED: {_failures}\033[0m")
    else:
        print("\033[92m  All tests PASSED\033[0m")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
