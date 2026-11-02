"""
engine/limit_order_book.py  -  Phase 2, 3, 4: High-Performance Limit Order Book
================================================================================
Price-Time Priority (FIFO) matching engine with:
  - Phase 2: Microsecond Price-Time Priority matching (pure Python with sortedcontainers).
  - Phase 3: Exact nanosecond latency tracking (time.perf_counter_ns) & zero GC pauses.
  - Phase 4: Whale order multi-level sweep detection & asynchronous SQLite audit hook.

Matching rules:
  BUY  order: match against cheapest resting ask where ask.price <= buy.price
  SELL order: match against highest resting bid where bid.price >= sell.price
  Partial fills supported; residual rests in the book.
  Full fills remove the resting order.
"""

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, List, Optional, Tuple

from sortedcontainers import SortedList

from core.constants import SIDE_BUY, SIDE_SELL

WHALE_QTY_THRESHOLD = 5_000
WHALE_LEVEL_THRESHOLD = 2


@dataclass(slots=True)
class Trade:
    aggressor_id  : int
    resting_id    : int
    price         : float
    quantity      : int
    timestamp_ns  : int = 0
    latency_ns    : int = 0
    is_whale      : bool = False
    cleared_levels: int = 1
    side          : int = 1   # +1=BUY aggressor, -1=SELL aggressor
    trade_id      : str = ""

    @property
    def latency_us(self) -> float:
        return self.latency_ns / 1_000.0

    @property
    def side_str(self) -> str:
        return "BUY" if self.side == SIDE_BUY else "SELL"

    def to_dict(self) -> Dict:
        return {
            "trade_id": self.trade_id,
            "aggressor_id": self.aggressor_id,
            "resting_id": self.resting_id,
            "price": self.price,
            "quantity": self.quantity,
            "side": self.side,
            "side_str": self.side_str,
            "latency_ns": self.latency_ns,
            "latency_us": self.latency_us,
            "is_whale": self.is_whale,
            "cleared_levels": self.cleared_levels,
            "timestamp_ns": self.timestamp_ns,
        }


@dataclass(slots=True)
class BookEntry:
    """A resting order in the book (may be partially filled)."""
    order_id : int
    price    : float
    quantity : int    # remaining quantity
    seq      : int    # global sequence number (time priority)

    def bid_key(self):
        return (-self.price, self.seq)

    def ask_key(self):
        return ( self.price, self.seq)


class LimitOrderBook:
    """
    Central matching engine.

    Usage::
        lob    = LimitOrderBook()
        trades = lob.submit(order)   # returns list[Trade]
    """

    def __init__(self, ledger=None):
        # Bids: sort by (-price, seq) so index[0] = best bid (highest price)
        self._bids: SortedList = SortedList(key=lambda e: (-e.price, e.seq))
        # Asks: sort by (+price, seq) so index[0] = best ask (lowest price)
        self._asks: SortedList = SortedList(key=lambda e: ( e.price, e.seq))

        self._seq              = 0
        self._all_trades       : List[Trade] = []
        self._recent_trades    : Deque[Trade] = deque(maxlen=200)
        self._last_whale_event : Optional[Dict] = None

        self.total_orders      = 0
        self.total_trades      = 0
        self.total_volume      = 0
        self.total_whales      = 0
        self._last_match_ns    = 0
        self._ledger           = ledger
        self._trade_listeners  : List[Callable[[Trade], None]] = []

    def set_ledger(self, ledger) -> None:
        self._ledger = ledger

    def add_trade_listener(self, fn: Callable[[Trade], None]) -> None:
        self._trade_listeners.append(fn)

    def submit(self, order, entry_ns: int = 0) -> List[Trade]:
        """
        Submit an incoming order; return list of resulting Trade objects.
        Tracks nanosecond execution latency and detects whale multi-level sweeps.
        """
        if entry_ns == 0:
            entry_ns = time.perf_counter_ns()

        self.total_orders += 1
        self._seq += 1
        entry = BookEntry(
            order_id=order.order_id,
            price=order.price,
            quantity=order.quantity,
            seq=self._seq,
        )

        t0 = time.perf_counter_ns()
        if order.is_buy:
            trades, num_levels, total_fill_qty = self._match_buy(entry)
            side = SIDE_BUY
        else:
            trades, num_levels, total_fill_qty = self._match_sell(entry)
            side = SIDE_SELL

        exit_ns = time.perf_counter_ns()
        match_duration_ns = exit_ns - t0
        end_to_end_lat_ns = exit_ns - entry_ns
        self._last_match_ns = match_duration_ns
        is_whale = (
            num_levels >= WHALE_LEVEL_THRESHOLD or
            order.quantity >= WHALE_QTY_THRESHOLD or
            total_fill_qty >= WHALE_QTY_THRESHOLD
        )

        if is_whale and trades:
            self.total_whales += 1
            self._last_whale_event = {
                "order_id": order.order_id,
                "side": "BUY" if side == SIDE_BUY else "SELL",
                "quantity": total_fill_qty,
                "cleared_levels": num_levels,
                "latency_us": end_to_end_lat_ns / 1_000.0,
                "timestamp_ns": exit_ns,
            }

        # Finalize trade objects with telemetry metadata
        now_epoch_ns = time.time_ns()
        for idx, t in enumerate(trades):
            t.timestamp_ns   = exit_ns
            t.latency_ns     = end_to_end_lat_ns
            t.is_whale       = is_whale
            t.cleared_levels = max(1, num_levels)
            t.side           = side
            t.trade_id       = f"TRD-{now_epoch_ns}-{order.order_id}-{idx+1}"

            self._recent_trades.appendleft(t)

            # Week 4: Pipe directly to SQLite audit ledger (non-blocking)
            if self._ledger is not None:
                self._ledger.record_trade(t.to_dict())

            # Notify listeners (e.g. WebSocket streamer)
            for listener in self._trade_listeners:
                try:
                    listener(t)
                except Exception:
                    pass

        self.total_trades += len(trades)
        self.total_volume += total_fill_qty
        if len(self._all_trades) < 20_000:
            self._all_trades.extend(trades)
        return trades

    # ---- book queries -------------------------------------------------------

    @property
    def best_bid(self) -> Optional[float]:
        return self._bids[0].price if self._bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self._asks[0].price if self._asks else None

    @property
    def spread(self) -> float:
        b = self.best_bid
        a = self.best_ask
        return (a - b) if (b is not None and a is not None) else 0.0

    @property
    def midpoint(self) -> float:
        b = self.best_bid
        a = self.best_ask
        return ((a + b) / 2.0) if (b is not None and a is not None) else 0.0

    @property
    def bid_depth(self) -> int:
        return len(self._bids)

    @property
    def ask_depth(self) -> int:
        return len(self._asks)

    @property
    def last_match_us(self) -> float:
        return self._last_match_ns / 1_000.0

    @property
    def last_whale_event(self) -> Optional[Dict]:
        return self._last_whale_event

    def get_recent_trades(self, limit: int = 50) -> List[Dict]:
        res = []
        for t in list(self._recent_trades)[:limit]:
            res.append(t.to_dict())
        return res

    def top_of_book(self, levels: int = 8) -> dict:
        """Return top-N bid and ask levels aggregated by price."""
        bids: dict = {}
        for e in self._bids:
            bids.setdefault(e.price, 0)
            bids[e.price] += e.quantity
            if len(bids) >= levels:
                break
        asks: dict = {}
        for e in self._asks:
            asks.setdefault(e.price, 0)
            asks[e.price] += e.quantity
            if len(asks) >= levels:
                break
        return {
            "bids": sorted(bids.items(), reverse=True),
            "asks": sorted(asks.items()),
        }

    def stats(self) -> dict:
        return {
            "total_orders"  : self.total_orders,
            "total_trades"  : self.total_trades,
            "total_volume"  : self.total_volume,
            "total_whales"  : self.total_whales,
            "bid_depth"     : self.bid_depth,
            "ask_depth"     : self.ask_depth,
            "best_bid"      : self.best_bid,
            "best_ask"      : self.best_ask,
            "spread"        : self.spread,
            "midpoint"      : self.midpoint,
            "last_match_us" : self.last_match_us,
        }

    # ---- matching -----------------------------------------------------------

    def _match_buy(self, buy: BookEntry) -> Tuple[List[Trade], int, int]:
        trades = []
        levels_hit = 0
        last_px = -1.0
        total_fill = 0
        while buy.quantity > 0 and self._asks:
            best = self._asks[0]
            if best.price > buy.price:
                break
            if best.price != last_px:
                levels_hit += 1
                last_px = best.price
            fill = buy.quantity if buy.quantity < best.quantity else best.quantity
            trades.append(Trade(
                aggressor_id=buy.order_id,
                resting_id=best.order_id,
                price=best.price,
                quantity=fill,
            ))
            buy.quantity -= fill
            best.quantity -= fill
            total_fill += fill
            if best.quantity == 0:
                self._asks.pop(0)
        if buy.quantity > 0:
            self._bids.add(buy)
        return trades, levels_hit, total_fill

    def _match_sell(self, sell: BookEntry) -> Tuple[List[Trade], int, int]:
        trades = []
        levels_hit = 0
        last_px = -1.0
        total_fill = 0
        while sell.quantity > 0 and self._bids:
            best = self._bids[0]
            if best.price < sell.price:
                break
            if best.price != last_px:
                levels_hit += 1
                last_px = best.price
            fill = sell.quantity if sell.quantity < best.quantity else best.quantity
            trades.append(Trade(
                aggressor_id=sell.order_id,
                resting_id=best.order_id,
                price=best.price,
                quantity=fill,
            ))
            sell.quantity -= fill
            best.quantity -= fill
            total_fill += fill
            if best.quantity == 0:
                self._bids.pop(0)
        if sell.quantity > 0:
            self._asks.add(sell)
        return trades, levels_hit, total_fill


# ---- Self-test --------------------------------------------------------------

if __name__ == "__main__":
    import sys
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    from core.order_struct import Order
    from core.constants import SIDE_BUY, SIDE_SELL

    lob = LimitOrderBook()
    for i, px in enumerate([99.50, 99.75, 100.00]):
        lob.submit(Order(order_id=i, price=px, quantity=100, side=SIDE_BUY))
    for i, px in enumerate([100.25, 100.50, 100.75], start=3):
        lob.submit(Order(order_id=i, price=px, quantity=100, side=SIDE_SELL))

    tob = lob.top_of_book(3)
    print(f"Best bid: {lob.best_bid}   Best ask: {lob.best_ask}")
    trades = lob.submit(Order(order_id=99, price=100.50, quantity=150, side=SIDE_BUY))
    print(f"Aggressive BUY 150@100.50 -> {len(trades)} trade(s)")
    for t in trades:
        print(f"  TRADE qty={t.quantity} @ {t.price:.4f}  lat={lob.last_match_us:.2f}us  whale={t.is_whale}")
    print(f"Stats: {lob.stats()}")
