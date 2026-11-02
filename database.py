"""
database.py  -  Week 4: SQLite Transaction Ledger (chronosmatch.db)
===================================================================
Robust, ACID-compliant SQLite ledger for high-frequency matched trades.
Features:
  - High performance WAL (Write-Ahead Logging) mode.
  - Primary key idempotency on trade_id (prevents duplicate trades).
  - Explicit commit and rollback transaction semantics.
  - Parameterized queries to eliminate injection vulnerabilities.
  - Comprehensive trade history querying and filtering.
  - Built-in SQLite integrity check (PRAGMA integrity_check).
"""

import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("hft.database")

DEFAULT_DB_FILE = Path(os.environ.get("CHRONOS_DB_PATH", "chronosmatch.db"))


class DatabaseLedger:
    """
    Manages chronosmatch.db and executes atomic trade logging with rollback capabilities.
    """

    def __init__(self, db_path: Optional[str or Path] = None):
        self.db_path = Path(db_path or DEFAULT_DB_FILE)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _init_db(self) -> None:
        """Create trades table and high-performance indexes."""
        with self._lock, self._get_connection() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                buy_order_id INTEGER NOT NULL,
                sell_order_id INTEGER NOT NULL,
                execution_price REAL NOT NULL,
                quantity INTEGER NOT NULL,
                execution_timestamp INTEGER NOT NULL,
                latency_ns INTEGER NOT NULL DEFAULT 0,
                latency_us REAL NOT NULL DEFAULT 0.0,
                is_whale INTEGER NOT NULL DEFAULT 0,
                cleared_levels INTEGER NOT NULL DEFAULT 1,
                side INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );
            """)

            # High-performance query indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_exec_ts ON trades(execution_timestamp);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_price ON trades(execution_price);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_whale ON trades(is_whale);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_buy ON trades(buy_order_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_sell ON trades(sell_order_id);")
            conn.commit()

    def insert_trade(self, t: Dict[str, Any]) -> bool:
        """
        Inserts a single trade using parameterized SQL and transaction rollback on error.
        Returns True if inserted, False if duplicate or failed.
        """
        trade_id = t.get("trade_id") or f"TRD-{time.time_ns()}-{t.get('buy_order_id', 0)}"
        buy_id = t.get("buy_order_id") or t.get("aggressor_id", 0)
        sell_id = t.get("sell_order_id") or t.get("resting_id", 0)
        price = float(t.get("price") or t.get("execution_price", 0.0))
        qty = int(t.get("quantity", 0))
        ts_ns = int(t.get("execution_timestamp") or t.get("timestamp_ns", time.time_ns()))
        lat_ns = int(t.get("latency_ns", 0))
        lat_us = float(t.get("latency_us", lat_ns / 1000.0))
        is_whale = 1 if t.get("is_whale") else 0
        cleared_levels = int(t.get("cleared_levels", 1))
        side = int(t.get("side", 1))
        created_at = t.get("created_at") or datetime.now(timezone.utc).isoformat()

        conn = self._get_connection()
        try:
            with self._lock:
                cur = conn.cursor()
                cur.execute("""
                INSERT OR IGNORE INTO trades (
                    trade_id, buy_order_id, sell_order_id, execution_price,
                    quantity, execution_timestamp, latency_ns, latency_us,
                    is_whale, cleared_levels, side, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    trade_id, buy_id, sell_id, price, qty, ts_ns,
                    lat_ns, lat_us, is_whale, cleared_levels, side, created_at
                ))
                conn.commit()
                return cur.rowcount > 0
        except Exception as e:
            conn.rollback()
            logger.error("Database insert error: %s (Transaction rolled back)", e)
            return False
        finally:
            conn.close()

    def insert_trades_batch(self, trades: List[Dict[str, Any]]) -> int:
        """
        Atomic bulk insertion inside a single transaction.
        Rolls back all changes if an unexpected database fault occurs.
        """
        if not trades:
            return 0

        rows = []
        now_iso = datetime.now(timezone.utc).isoformat()
        now_ns = time.time_ns()

        for t in trades:
            trade_id = t.get("trade_id") or f"TRD-{now_ns}-{t.get('buy_order_id', 0)}"
            buy_id = t.get("buy_order_id") or t.get("aggressor_id", 0)
            sell_id = t.get("sell_order_id") or t.get("resting_id", 0)
            price = float(t.get("price") or t.get("execution_price", 0.0))
            qty = int(t.get("quantity", 0))
            ts = int(t.get("execution_timestamp") or t.get("timestamp_ns", now_ns))
            lat_ns = int(t.get("latency_ns", 0))
            lat_us = float(t.get("latency_us", lat_ns / 1000.0))
            is_whale = 1 if t.get("is_whale") else 0
            cleared = int(t.get("cleared_levels", 1))
            side = int(t.get("side", 1))
            created_at = t.get("created_at") or now_iso

            rows.append((
                trade_id, buy_id, sell_id, price, qty, ts,
                lat_ns, lat_us, is_whale, cleared, side, created_at
            ))

        conn = self._get_connection()
        try:
            with self._lock:
                cur = conn.cursor()
                cur.executemany("""
                INSERT OR IGNORE INTO trades (
                    trade_id, buy_order_id, sell_order_id, execution_price,
                    quantity, execution_timestamp, latency_ns, latency_us,
                    is_whale, cleared_levels, side, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, rows)
                conn.commit()
                return cur.rowcount
        except Exception as e:
            conn.rollback()
            logger.error("Batch database insert error: %s (Transaction rolled back)", e)
            return 0
        finally:
            conn.close()

    def get_trade_history(
        self,
        limit: int = 100,
        offset: int = 0,
        is_whale: Optional[bool] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves trade records using parameterized SQL filters.
        """
        query = "SELECT * FROM trades WHERE 1=1"
        params: List[Any] = []

        if is_whale is not None:
            query += " AND is_whale = ?"
            params.append(1 if is_whale else 0)

        if min_price is not None:
            query += " AND execution_price >= ?"
            params.append(min_price)

        if max_price is not None:
            query += " AND execution_price <= ?"
            params.append(max_price)

        query += " ORDER BY execution_timestamp DESC LIMIT ? OFFSET ?;"
        params.extend([limit, offset])

        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]

    def get_trade_by_id(self, trade_id: str) -> Optional[Dict[str, Any]]:
        """Finds a single trade by Trade ID."""
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM trades WHERE trade_id = ?;", (trade_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def verify_integrity(self) -> Dict[str, Any]:
        """Runs SQLite PRAGMA integrity_check and computes ledger statistics."""
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA integrity_check;")
            integrity_rows = [r[0] for r in cur.fetchall()]
            is_ok = len(integrity_rows) == 1 and integrity_rows[0].lower() == "ok"

            cur.execute("SELECT COUNT(*), COALESCE(SUM(quantity), 0), COALESCE(SUM(is_whale), 0) FROM trades;")
            count, volume, whales = cur.fetchone()

        return {
            "status": "PASS" if is_ok else "FAIL",
            "integrity_check": integrity_rows,
            "database_file": str(self.db_path),
            "total_trades": count,
            "total_volume": volume,
            "whale_trades": whales,
        }

    def stats(self) -> Dict[str, Any]:
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
            SELECT 
                COUNT(*) as total_trades,
                COALESCE(SUM(quantity), 0) as total_volume,
                COALESCE(AVG(execution_price), 0.0) as avg_price,
                COALESCE(AVG(latency_us), 0.0) as avg_latency_us,
                COALESCE(SUM(is_whale), 0) as total_whales
            FROM trades;
            """)
            row = cur.fetchone()
            return {
                "database_file": str(self.db_path),
                "total_trades": row["total_trades"],
                "total_volume": row["total_volume"],
                "avg_price": round(row["avg_price"], 4),
                "avg_latency_us": round(row["avg_latency_us"], 2),
                "total_whales": row["total_whales"],
            }


if __name__ == "__main__":
    print("=" * 65)
    print("  CHRONOSMATCH - SQLITE TRANSACTION LEDGER TEST (WEEK 4)")
    print("=" * 65)

    db = DatabaseLedger("chronosmatch.db")

    # 1. Insert sample trade
    sample_t = {
        "trade_id": "TRD-VERIFY-001",
        "buy_order_id": 1001,
        "sell_order_id": 2002,
        "price": 100.50,
        "quantity": 250,
        "timestamp_ns": time.time_ns(),
        "latency_ns": 4500,
        "is_whale": False,
        "cleared_levels": 1,
    }
    inserted = db.insert_trade(sample_t)
    print(f"  Single Trade Insertion : {'SUCCESS' if inserted else 'FAILED'}")

    # 2. Duplicate prevention test
    dup_inserted = db.insert_trade(sample_t)
    print(f"  Duplicate Trade Blocked: {'PASS (Duplicate Ignored)' if not dup_inserted else 'FAIL'}")

    # 3. Batch insertion test
    batch = [
        {
            "trade_id": f"TRD-BATCH-{i}",
            "buy_order_id": 1000 + i,
            "sell_order_id": 2000 + i,
            "price": 100.0 + (i * 0.1),
            "quantity": 100 * i,
            "timestamp_ns": time.time_ns(),
            "latency_ns": 3200,
            "is_whale": (i > 8),
            "cleared_levels": 2 if i > 8 else 1,
        }
        for i in range(1, 11)
    ]
    batch_count = db.insert_trades_batch(batch)
    print(f"  Batch Insert (10 Trades) : {batch_count}/10 Persisted")

    # 4. History retrieval
    history = db.get_trade_history(limit=5)
    print(f"  History Query Count      : {len(history)} records retrieved")

    # 5. Integrity check
    integ = db.verify_integrity()
    print(f"  SQLite PRAGMA Integrity  : {integ['status']} ({integ['integrity_check'][0]})")
    print(f"  Total Trades in Database : {integ['total_trades']:,}")
    print("=" * 65)

