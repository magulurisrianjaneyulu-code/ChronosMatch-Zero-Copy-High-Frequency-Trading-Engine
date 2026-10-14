"""
main.py  -  HFT Engine Unified CLI Launcher
============================================
All three phases accessible from one entry point.

Commands
--------
  python main.py struct-test               # Phase 1: binary layout smoke test
  python main.py ring-test                 # Phase 1: ring buffer smoke test
  python main.py firehose --duration 5     # Phase 1: async firehose standalone
  python main.py verify                    # Phase 3: matching engine tests
  python main.py audit --orders 1000000    # Phase 3: IPC zero-copy benchmark
  python main.py dashboard --duration 30   # Phase 2: live terminal dashboard
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def cmd_struct_test(args):
    from core.order_struct import Order, pack_order, unpack_order
    from core.constants import SIDE_BUY, SIDE_SELL, ORDER_SIZE
    print(f"ORDER_SIZE = {ORDER_SIZE} bytes  (format =QdIb)")
    for side, label in [(SIDE_BUY, "BUY"), (SIDE_SELL, "SELL")]:
        o   = Order(order_id=1, price=100.0, quantity=500, side=side)
        raw = pack_order(o)
        o2  = unpack_order(raw)
        assert o == o2, f"Round-trip failed for {label}"
        print(f"  {label:<5}  hex={raw.hex()}  round-trip OK")
    print("PASS struct self-test")


def cmd_ring_test(args):
    import tempfile, os
    from core.order_struct import Order
    from core.ring_buffer import RingBuffer
    from core.constants import SIDE_BUY, SIDE_SELL
    with tempfile.NamedTemporaryFile(delete=False, suffix=".rb") as f:
        tmp = f.name
    try:
        rb = RingBuffer(tmp, capacity=16, create=True)
        print(repr(rb))
        for i in range(16):
            rb.put(Order(order_id=i, price=100.0+i, quantity=10,
                         side=SIDE_BUY if i%2==0 else SIDE_SELL))
        assert rb.is_full()
        print(f"  full : size={rb.size()}")
        for i in range(16):
            o = rb.get()
            assert o.order_id == i
        assert rb.is_empty()
        print(f"  empty: size={rb.size()}")
        print("PASS ring-buffer self-test")
    finally:
        rb.close()
        os.unlink(tmp)


def cmd_firehose(args):
    import asyncio, tempfile, os
    from core.ring_buffer import RingBuffer
    from core.metrics import EngineMetrics
    from core.firehose import Firehose
    with tempfile.NamedTemporaryFile(delete=False, suffix=".rb") as f:
        tmp = f.name
    try:
        rb      = RingBuffer(tmp, create=True)
        metrics = EngineMetrics()
        fh      = Firehose(rb, metrics, target_ops=args.target_ops)
        print(f"Firehose: target={args.target_ops:,} ord/sec  duration={args.duration}s")
        asyncio.run(fh.run(duration=args.duration))
        s = metrics.snapshot()
        print(f"  Sent    : {s['orders_sent']:>10,}")
        print(f"  Dropped : {s['orders_dropped']:>10,}")
        print(f"  Rate    : {s['enqueue_rate']:>10,.0f} ord/sec")
    finally:
        rb.close()
        try: os.unlink(tmp)
        except: pass


def cmd_verify(args):
    from bench.engine_verify import main as verify_main
    verify_main()


def cmd_audit(args):
    from bench.ipc_audit import run_audit
    run_audit(n=args.orders)


def cmd_dashboard(args):
    from ui.dashboard import Dashboard
    Dashboard(duration=args.duration, target_ops=args.target_ops).run()


def build_parser():
    p   = argparse.ArgumentParser(prog="hft_engine",
              description="HFT Matching Engine - Low Latency Trading System")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("struct-test", help="Binary struct layout smoke test")
    sub.add_parser("ring-test",   help="Ring buffer smoke test")
    sub.add_parser("verify",      help="Matching engine verification suite")

    fh = sub.add_parser("firehose", help="Async firehose standalone")
    fh.add_argument("--duration",   type=float, default=5.0)
    fh.add_argument("--target-ops", type=int,   default=100_000)

    au = sub.add_parser("audit", help="IPC zero-copy benchmark")
    au.add_argument("--orders", type=int, default=1_000_000)

    ds = sub.add_parser("dashboard", help="Live ANSI terminal dashboard")
    ds.add_argument("--duration",   type=float, default=30.0)
    ds.add_argument("--target-ops", type=int,   default=100_000)

    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    {
        "struct-test": cmd_struct_test,
        "ring-test"  : cmd_ring_test,
        "firehose"   : cmd_firehose,
        "verify"     : cmd_verify,
        "audit"      : cmd_audit,
        "dashboard"  : cmd_dashboard,
    }[args.cmd](args)
