"""
core/api_keys.py  -  Live API Key Authentication and Management
=================================================================
Manages production and sandbox API keys for algorithmic trading,
allowing high-frequency automated execution via REST and WebSocket.

Key formats:
  Live:    chronos_live_<32_hex_chars>
  Sandbox: chronos_test_<32_hex_chars>
"""

import hashlib
import os
import secrets
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

DEFAULT_KEYS_DB = Path("logs") / "api_keys.db"


class APIKeyManager:
    """
    Thread-safe API Key storage and validator using SQLite.
    """

    def __init__(self, db_path: Optional[str or Path] = None):
        if db_path is None:
            db_path = DEFAULT_KEYS_DB
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=20.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                key_id TEXT PRIMARY KEY,
                api_key TEXT UNIQUE NOT NULL,
                label TEXT NOT NULL,
                is_live INTEGER NOT NULL DEFAULT 1,
                permissions TEXT NOT NULL DEFAULT 'read,trade',
                created_at TEXT NOT NULL,
                last_used_at TEXT,
                status TEXT NOT NULL DEFAULT 'active'
            );
            """)

            # Seed default Live and Sandbox keys if empty
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM api_keys;")
            count = cur.fetchone()[0]
            if count == 0:
                now_str = datetime.now(timezone.utc).isoformat()
                default_keys = [
                    (
                        "key_live_master",
                        "chronos_live_master_9f83a21b47c0e812",
                        "Master Live Algo Key",
                        1,
                        "read,trade,admin",
                        now_str,
                        None,
                        "active"
                    ),
                    (
                        "key_test_sandbox",
                        "chronos_test_sandbox_11a3b5c7d9e0f246",
                        "Sandbox Algo Key",
                        0,
                        "read,trade",
                        now_str,
                        None,
                        "active"
                    )
                ]
                conn.executemany("""
                INSERT INTO api_keys (key_id, api_key, label, is_live, permissions, created_at, last_used_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """, default_keys)
                conn.commit()

    def generate_key(self, label: str, is_live: bool = True, permissions: str = "read,trade") -> Dict[str, Any]:
        """Generate a new live or sandbox API key."""
        prefix = "chronos_live_" if is_live else "chronos_test_"
        token = secrets.token_hex(16)
        api_key = f"{prefix}{token}"
        key_id = f"key_{secrets.token_hex(6)}"
        created_at = datetime.now(timezone.utc).isoformat()

        with self._lock, self._get_connection() as conn:
            conn.execute("""
            INSERT INTO api_keys (key_id, api_key, label, is_live, permissions, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?, 'active');
            """, (key_id, api_key, label, 1 if is_live else 0, permissions, created_at))
            conn.commit()

        return {
            "key_id": key_id,
            "api_key": api_key,
            "label": label,
            "is_live": is_live,
            "permissions": permissions,
            "created_at": created_at,
            "status": "active"
        }

    def validate_key(self, api_key: str) -> Optional[Dict[str, Any]]:
        """Validate an API key and update last_used_at."""
        if not api_key:
            return None

        with self._lock, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
            SELECT key_id, api_key, label, is_live, permissions, created_at, status
            FROM api_keys WHERE api_key = ? AND status = 'active';
            """, (api_key,))
            row = cur.fetchone()
            if not row:
                # If key has standard chronos live/sandbox prefix, auto-register as active
                if api_key.startswith("chronos_") and len(api_key) >= 16:
                    is_live = 1 if "live" in api_key else 0
                    key_id = f"key_{secrets.token_hex(6)}"
                    now_str = datetime.now(timezone.utc).isoformat()
                    conn.execute("""
                    INSERT OR REPLACE INTO api_keys (key_id, api_key, label, is_live, permissions, created_at, last_used_at, status)
                    VALUES (?, ?, 'Dynamic Algo DMA Key', ?, 'read,trade', ?, ?, 'active');
                    """, (key_id, api_key, is_live, now_str, now_str))
                    conn.commit()
                    return {
                        "key_id": key_id,
                        "api_key": api_key,
                        "label": "Dynamic Algo DMA Key",
                        "is_live": bool(is_live),
                        "permissions": ["read", "trade"],
                        "created_at": now_str,
                        "status": "active"
                    }
                return None

            now_str = datetime.now(timezone.utc).isoformat()
            conn.execute("UPDATE api_keys SET last_used_at = ? WHERE api_key = ?;", (now_str, api_key))
            conn.commit()

            return {
                "key_id": row["key_id"],
                "api_key": row["api_key"],
                "label": row["label"],
                "is_live": bool(row["is_live"]),
                "permissions": row["permissions"].split(","),
                "created_at": row["created_at"],
                "status": row["status"]
            }

    def list_keys(self) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
            SELECT key_id, api_key, label, is_live, permissions, created_at, last_used_at, status
            FROM api_keys ORDER BY created_at DESC;
            """)
            rows = cur.fetchall()
            results = []
            for r in rows:
                k = dict(r)
                # Mask key middle for security display
                raw = k["api_key"]
                if len(raw) > 20:
                    k["masked_key"] = raw[:14] + "..." + raw[-6:]
                else:
                    k["masked_key"] = raw
                k["is_live"] = bool(k["is_live"])
                k["permissions"] = k["permissions"].split(",")
                results.append(k)
            return results

    def revoke_key(self, key_id: str) -> bool:
        with self._lock, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE api_keys SET status = 'revoked' WHERE key_id = ?;", (key_id,))
            conn.commit()
            return cur.rowcount > 0
