"""
core/ledger.py  -  Phase 4: SQLite Asynchronous Audit Ledger
============================================================
Background persistence worker that asynchronously flushes matched trades
from the matching engine to an ACID-compliant SQLite ledger.

Features:
  - Zero-lag offloading: trades are queued in-memory and flushed in batches.
  - High throughput WAL (Write-Ahead Logging) mode.
  - Cryptographic hash chaining (SHA-256) for immutable audit compliance.
  - Whale order tracking and multi-level sweep flags.
  - Full querying capabilities (by ID, whale, side, time range, latency).
"""

import hashlib
import json
import logging
import os
import queue
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("hft.ledger")

DEFAULT_DB_PATH = Path("logs") / "trades_audit.db"


class AuditLedger:
    """
    Asynchronous trade ledger backed by SQLite with WAL mode and SHA-256 chaining.
    """

    def __init__(self, db_path: Optional[str or Path] = None, batch_size: int = 500, flush_interval_s: float = 0.05):
        if db_path is None:
            db_path = DEFAULT_DB_PATH
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_s

        self._queue: queue.Queue = queue.Queue(maxsize=200_000)
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        self._last_hash = "GENESIS_BLOCK_CHRONOS_MATCH_00000000"
        self._total_written = 0
        self._total_whales = 0

        self._init_db()
        self._start_worker()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA cache_size=-64000;")  # 64MB cache
        conn.execute("PRAGMA temp_store=MEMORY;")
        return conn

    def _init_db(self) -> None:
        """Create schema and indexes."""
        conn = self._get_connection()
        try:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS trades_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id TEXT UNIQUE NOT NULL,
                aggressor_id INTEGER NOT NULL,
                resting_id INTEGER NOT NULL,
                side INTEGER NOT NULL,
                side_label TEXT NOT NULL,
                price REAL NOT NULL,
                quantity INTEGER NOT NULL,
                volume REAL NOT NULL,
                latency_ns INTEGER NOT NULL,
                latency_us REAL NOT NULL,
                is_whale INTEGER NOT NULL DEFAULT 0,
                cleared_levels INTEGER NOT NULL DEFAULT 1,
                timestamp_ns INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                prev_hash TEXT NOT NULL,
                hash_checksum TEXT NOT NULL
            );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_created_at ON trades_ledger(created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_whale ON trades_ledger(is_whale);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_price ON trades_ledger(price);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_side ON trades_ledger(side);")

            # Retrieve the latest hash to continue the cryptographic chain
            cur = conn.cursor()
            cur.execute("SELECT hash_checksum, id FROM trades_ledger ORDER BY id DESC LIMIT 1;")
            row = cur.fetchone()
            if row:
                self._last_hash = row["hash_checksum"]
                cur.execute("SELECT COUNT(*), SUM(is_whale) FROM trades_ledger;")
                count_row = cur.fetchone()
                self._total_written = count_row[0] or 0
                self._total_whales = count_row[1] or 0
            conn.commit()
        finally:
            conn.close()

    def _start_worker(self) -> None:
        self._worker_thread = threading.Thread(
            target=self._flush_loop,
            name="AuditLedgerWorker",
            daemon=True
        )
        self._worker_thread.start()

    def record_trade(self, trade_dict: Dict[str, Any]) -> bool:
        """
        Hot path enqueue. Non-blocking. Returns True if queued, False if dropped.
        """
        try:
            self._queue.put_nowait(trade_dict)
            return True
        except queue.Full:
            return False

    def _flush_loop(self) -> None:
        batch: List[Dict[str, Any]] = []
        last_flush = time.monotonic()
        conn = self._get_connection()

        try:
            while not self._stop_event.is_set() or not self._queue.empty():
                try:
                    item = self._queue.get(timeout=0.01)
                    batch.append(item)
                    if len(batch) >= self.batch_size:
                        self._persist_batch(batch, conn)
                        batch = []
                        last_flush = time.monotonic()
                except queue.Empty:
                    pass

                if batch and (time.monotonic() - last_flush) >= self.flush_interval_s:
                    self._persist_batch(batch, conn)
                    batch = []
                    last_flush = time.monotonic()

            if batch:
                self._persist_batch(batch, conn)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _persist_batch(self, batch: List[Dict[str, Any]], conn: Optional[sqlite3.Connection] = None) -> None:
        if not batch:
            return

        rows = []
        with self._lock:
            for item in batch:
                trade_id = item.get("trade_id") or f"TRD-{int(time.time()*1e6)}-{item.get('aggressor_id', 0)}"
                agg_id = item.get("aggressor_id", 0)
                rest_id = item.get("resting_id", 0)
                side = item.get("side", 1)
                side_label = "BUY" if side == 1 else "SELL"
                price = float(item.get("price", 0.0))
                qty = int(item.get("quantity", 0))
                volume = price * qty
                lat_ns = int(item.get("latency_ns", 0))
                lat_us = lat_ns / 1000.0
                is_whale = 1 if item.get("is_whale", False) else 0
                cleared_levels = int(item.get("cleared_levels", 1))
                ts_ns = int(item.get("timestamp_ns", time.perf_counter_ns()))
                created_at = item.get("created_at") or datetime.now(timezone.utc).isoformat()

                # Cryptographic hash: SHA-256(prev_hash + trade_id + price + qty + ts_ns)
                payload = f"{self._last_hash}:{trade_id}:{agg_id}:{rest_id}:{price:.4f}:{qty}:{ts_ns}"
                curr_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
                prev_hash = self._last_hash
                self._last_hash = curr_hash

                rows.append((
                    trade_id, agg_id, rest_id, side, side_label,
                    price, qty, volume, lat_ns, lat_us,
                    is_whale, cleared_levels, ts_ns, created_at,
                    prev_hash, curr_hash
                ))

                self._total_written += 1
                if is_whale:
                    self._total_whales += 1

        try:
            target_conn = conn if conn is not None else self._get_connection()
            target_conn.executemany("""
            INSERT OR IGNORE INTO trades_ledger (
                trade_id, aggressor_id, resting_id, side, side_label,
                price, quantity, volume, latency_ns, latency_us,
                is_whale, cleared_levels, timestamp_ns, created_at,
                prev_hash, hash_checksum
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, rows)
            target_conn.commit()
            if conn is None:
                target_conn.close()
        except Exception as e:
            logger.error("Failed to commit audit ledger batch: %s", e)

    def close(self, timeout: float = 2.0) -> None:
        self._stop_event.set()
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=timeout)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            pending = self._queue.qsize()
            total = self._total_written
            whales = self._total_whales
            last_hash = self._last_hash

        return {
            "total_persisted": total,
            "whales_recorded": whales,
            "pending_in_queue": pending,
            "last_block_hash": last_hash,
            "database_file": str(self.db_path),
        }

    def get_recent_trades(self, limit: int = 50, only_whales: bool = False) -> List[Dict[str, Any]]:
        query = "SELECT * FROM trades_ledger"
        params = []
        if only_whales:
            query += " WHERE is_whale = 1"
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute(query, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def verify_integrity(self, limit: int = 5000) -> Dict[str, Any]:
        """Verify the complete SHA-256 cryptographic chain of records."""
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            if limit and limit > 0:
                cur.execute("""
                SELECT id, trade_id, aggressor_id, resting_id, price, quantity, timestamp_ns, prev_hash, hash_checksum
                FROM (
                    SELECT * FROM trades_ledger ORDER BY id DESC LIMIT ?
                ) ORDER BY id ASC;
                """, (limit,))
            else:
                cur.execute("""
                SELECT id, trade_id, aggressor_id, resting_id, price, quantity, timestamp_ns, prev_hash, hash_checksum
                FROM trades_ledger ORDER BY id ASC;
                """)
            rows = cur.fetchall()

            if not rows:
                return {"valid": True, "count": 0, "message": "Ledger is empty"}

            expected_prev = rows[0]["prev_hash"]
            for r in rows:
                if r["prev_hash"] != expected_prev:
                    return {
                        "valid": False,
                        "broken_at_id": r["id"],
                        "error": f"Chain broken at record #{r['id']}: expected prev {expected_prev}, got {r['prev_hash']}"
                    }
                payload = f"{expected_prev}:{r['trade_id']}:{r['aggressor_id']}:{r['resting_id']}:{r['price']:.4f}:{r['quantity']}:{r['timestamp_ns']}"
                expected_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
                if r["hash_checksum"] != expected_hash:
                    return {
                        "valid": False,
                        "broken_at_id": r["id"],
                        "error": f"Hash mismatch at record #{r['id']}"
                    }
                expected_prev = r["hash_checksum"]

            return {
                "valid": True,
                "count": len(rows),
                "latest_hash": expected_prev,
                "message": f"All {len(rows):,} ledger records verified with zero tampering."
            }
        finally:
            conn.close()
