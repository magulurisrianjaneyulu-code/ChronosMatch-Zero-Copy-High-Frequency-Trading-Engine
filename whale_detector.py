"""
whale_detector.py  -  Week 4: Whale Order & Multi-Level Sweep Detection
=======================================================================
Identifies large institutional "Whale" orders that aggressively consume
liquidity across multiple resting price levels on the Limit Order Book.

Features:
  - Configurable thresholds: Quantity threshold (e.g. >= 5,000 shares)
    and Multi-level sweep threshold (e.g. >= 2 distinct resting price levels).
  - Detects large Buy and Sell aggressor orders.
  - Tracks execution details: volume, average execution price, levels hit, execution latency.
  - Generates alert payloads and integrates with dashboard banners & WebSockets.
  - Strictly preserves Price-Time Priority FIFO matching rules.
"""

import logging
import time
from collections import deque
from dataclasses import asdict, dataclass
from typing import Any, Callable, Deque, Dict, List, Optional

logger = logging.getLogger("hft.whale")


@dataclass(slots=True)
class WhaleOrderEvent:
    order_id: int
    side: str
    quantity: int
    avg_price: float
    cleared_levels: int
    latency_us: float
    timestamp_ns: int
    is_whale: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "order_id": self.order_id,
            "side": self.side,
            "quantity": self.quantity,
            "avg_price": round(self.avg_price, 4),
            "cleared_levels": self.cleared_levels,
            "latency_us": round(self.latency_us, 2),
            "timestamp_ns": self.timestamp_ns,
            "is_whale": self.is_whale,
            "summary": (
                f"WHALE {self.side}: {self.quantity:,} shares swept "
                f"{self.cleared_levels} levels @ avg ${self.avg_price:.4f} in {self.latency_us:.2f} µs"
            ),
        }


class WhaleDetector:
    """
    Real-time multi-level liquidity sweep and whale order detector.
    """

    def __init__(
        self,
        quantity_threshold: int = 5_000,
        level_threshold: int = 2,
        notional_threshold: float = 100_000.0,
        max_history: int = 200,
    ):
        self.quantity_threshold = quantity_threshold
        self.level_threshold = level_threshold
        self.notional_threshold = notional_threshold

        self._history: Deque[WhaleOrderEvent] = deque(maxlen=max_history)
        self._listeners: List[Callable[[WhaleOrderEvent], None]] = []
        self.total_whales_detected = 0
        self.last_whale_event: Optional[WhaleOrderEvent] = None

    def add_listener(self, fn: Callable[[WhaleOrderEvent], None]) -> None:
        """Register alert callbacks for dashboard or websocket broadcasting."""
        self._listeners.append(fn)

    def is_whale_order(self, quantity: int) -> bool:
        """Pre-check if order size alone qualifies as a whale."""
        return quantity >= self.quantity_threshold

    def evaluate_execution(
        self,
        order_id: int,
        side: int,
        submitted_qty: int,
        trades: List[Any],
        latency_us: float = 0.0,
        entry_ns: Optional[int] = None,
    ) -> Optional[WhaleOrderEvent]:
        """
        Evaluates executed trade fills for multi-level sweep or volume whale threshold.
        """
        if not trades:
            return None

        # Determine distinct price levels cleared
        prices = set()
        total_fill_qty = 0
        total_notional = 0.0

        for t in trades:
            px = getattr(t, "price", None) or (t.get("price") if isinstance(t, dict) else t[2])
            qty = getattr(t, "quantity", None) or (t.get("quantity") if isinstance(t, dict) else t[3])
            prices.add(px)
            total_fill_qty += qty
            total_notional += (px * qty)

        num_levels = len(prices)
        avg_price = (total_notional / total_fill_qty) if total_fill_qty > 0 else 0.0

        is_whale = (
            num_levels >= self.level_threshold or
            submitted_qty >= self.quantity_threshold or
            total_fill_qty >= self.quantity_threshold or
            total_notional >= self.notional_threshold
        )

        if not is_whale:
            return None

        side_str = "BUY" if side == 1 else "SELL"
        now_ns = time.perf_counter_ns()
        if entry_ns and latency_us <= 0.0:
            latency_us = (now_ns - entry_ns) / 1000.0

        event = WhaleOrderEvent(
            order_id=order_id,
            side=side_str,
            quantity=total_fill_qty,
            avg_price=avg_price,
            cleared_levels=num_levels,
            latency_us=latency_us,
            timestamp_ns=now_ns,
            is_whale=True,
        )

        self.total_whales_detected += 1
        self.last_whale_event = event
        self._history.appendleft(event)

        logger.info("🚨 %s", event.to_dict()["summary"])

        # Dispatch alerts
        for listener in self._listeners:
            try:
                listener(event)
            except Exception as e:
                logger.warning("Whale listener exception: %s", e)

        return event

    def get_recent_whales(self, limit: int = 50) -> List[Dict[str, Any]]:
        return [w.to_dict() for w in list(self._history)[:limit]]

    def stats(self) -> Dict[str, Any]:
        return {
            "total_whales_detected": self.total_whales_detected,
            "quantity_threshold": self.quantity_threshold,
            "level_threshold": self.level_threshold,
            "notional_threshold": self.notional_threshold,
            "last_whale": self.last_whale_event.to_dict() if self.last_whale_event else None,
        }


if __name__ == "__main__":
    print("=" * 65)
    print("  CHRONOSMATCH - WHALE ORDER DETECTOR TEST (WEEK 4)")
    print("=" * 65)

    detector = WhaleDetector(quantity_threshold=5000, level_threshold=2)

    # 1. Test normal small fill (should NOT trigger whale)
    small_trades = [{"price": 100.10, "quantity": 100}]
    ev_small = detector.evaluate_execution(101, side=1, submitted_qty=100, trades=small_trades, latency_us=2.5)
    print(f"  Small Order (100 shs)     : {'NOT WHALE (PASS)' if ev_small is None else 'TRIGGERED'}")

    # 2. Test large quantity whale (>= 5,000)
    qty_trades = [{"price": 100.10, "quantity": 8000}]
    ev_qty = detector.evaluate_execution(102, side=1, submitted_qty=8000, trades=qty_trades, latency_us=4.8)
    print(f"  Large Qty Order (8000 shs): {'TRIGGERED WHALE (PASS)' if ev_qty else 'MISSED'}")
    if ev_qty:
        print(f"    Alert Summary: {ev_qty.to_dict()['summary']}")

    # 3. Test multi-level sweep (swept 3 distinct price levels)
    sweep_trades = [
        {"price": 100.10, "quantity": 1000},
        {"price": 100.20, "quantity": 1500},
        {"price": 100.30, "quantity": 2000},
    ]
    ev_sweep = detector.evaluate_execution(103, side=1, submitted_qty=4500, trades=sweep_trades, latency_us=7.2)
    print(f"  Multi-Level Sweep (3 levels): {'TRIGGERED WHALE (PASS)' if ev_sweep else 'MISSED'}")
    if ev_sweep:
        print(f"    Alert Summary: {ev_sweep.to_dict()['summary']}")

    print(f"  Total Whales Detected     : {detector.total_whales_detected}")
    print("=" * 65)

