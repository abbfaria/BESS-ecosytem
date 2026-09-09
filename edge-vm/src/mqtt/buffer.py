"""
SQLite-backed MQTT message buffer.

Guarantees at-least-once delivery to the cloud broker:
  - Outbound messages stored locally before sending
  - On successful PUBACK (QoS 1), message marked delivered
  - On reconnect, undelivered messages replayed in FIFO order
  - Configurable max size; oldest entries purged when full
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path
from typing import AsyncIterator, NamedTuple, Optional

from ..utils.logging_config import get_logger

log = get_logger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS mqtt_buffer (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    topic       TEXT    NOT NULL,
    payload     BLOB    NOT NULL,
    qos         INTEGER NOT NULL DEFAULT 1,
    enqueued_at REAL    NOT NULL,
    sent_at     REAL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    delivered   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_undelivered ON mqtt_buffer (delivered, id);
"""


class BufferedMessage(NamedTuple):
    id:          int
    topic:       str
    payload:     bytes
    qos:         int
    enqueued_at: float
    attempts:    int


class MQTTBuffer:
    """Thread-safe (via asyncio lock) SQLite message buffer."""

    def __init__(
        self,
        db_path: str | Path = "/data/mqtt_buffer.db",
        max_messages: int   = 10_000,
        max_age_hours: float = 24.0,
    ) -> None:
        self._path         = Path(db_path)
        self._max_messages = max_messages
        self._max_age_s    = max_age_hours * 3600
        self._lock         = asyncio.Lock()
        self._conn: Optional[sqlite3.Connection] = None
        self._stats        = {"enqueued": 0, "delivered": 0, "purged": 0}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self._path),
            check_same_thread=False,
            isolation_level=None,   # autocommit
        )
        self._conn.executescript(_SCHEMA)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        log.info("MQTT buffer opened", path=str(self._path))

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # ── Write API ─────────────────────────────────────────────────────────────

    async def enqueue(self, topic: str, payload: bytes | str, qos: int = 1) -> int:
        """Store a message for future delivery. Returns row id."""
        if isinstance(payload, str):
            payload = payload.encode()
        async with self._lock:
            cur = self._conn.execute(
                "INSERT INTO mqtt_buffer (topic, payload, qos, enqueued_at)"
                " VALUES (?, ?, ?, ?)",
                (topic, payload, qos, time.time()),
            )
            row_id = cur.lastrowid
            self._stats["enqueued"] += 1
            await self._maybe_purge()
            return row_id

    async def mark_delivered(self, row_id: int) -> None:
        async with self._lock:
            self._conn.execute(
                "UPDATE mqtt_buffer SET delivered=1, sent_at=? WHERE id=?",
                (time.time(), row_id),
            )
            self._stats["delivered"] += 1

    async def increment_attempts(self, row_id: int) -> None:
        async with self._lock:
            self._conn.execute(
                "UPDATE mqtt_buffer SET attempts=attempts+1 WHERE id=?",
                (row_id,),
            )

    # ── Read API ──────────────────────────────────────────────────────────────

    async def pending_count(self) -> int:
        async with self._lock:
            cur = self._conn.execute(
                "SELECT COUNT(*) FROM mqtt_buffer WHERE delivered=0"
            )
            return cur.fetchone()[0]

    async def iter_pending(
        self, batch_size: int = 50
    ) -> AsyncIterator[BufferedMessage]:
        """Yield buffered, undelivered messages in FIFO order."""
        async with self._lock:
            cur = self._conn.execute(
                "SELECT id, topic, payload, qos, enqueued_at, attempts"
                " FROM mqtt_buffer WHERE delivered=0"
                " ORDER BY id ASC LIMIT ?",
                (batch_size,),
            )
            rows = cur.fetchall()
        for row in rows:
            yield BufferedMessage(*row)

    # ── Maintenance ───────────────────────────────────────────────────────────

    async def _maybe_purge(self) -> None:
        """Delete old delivered records and overflow entries. Called under lock."""
        now = time.time()
        # 1. Remove delivered messages older than max_age
        r = self._conn.execute(
            "DELETE FROM mqtt_buffer WHERE delivered=1 AND enqueued_at < ?",
            (now - self._max_age_s,),
        )
        self._stats["purged"] += r.rowcount

        # 2. If still too many undelivered, drop oldest (lossy but bounded)
        cur = self._conn.execute(
            "SELECT COUNT(*) FROM mqtt_buffer WHERE delivered=0"
        )
        count = cur.fetchone()[0]
        if count > self._max_messages:
            overflow = count - self._max_messages
            r = self._conn.execute(
                "DELETE FROM mqtt_buffer WHERE id IN ("
                "  SELECT id FROM mqtt_buffer WHERE delivered=0"
                "  ORDER BY id ASC LIMIT ?"
                ")",
                (overflow,),
            )
            self._stats["purged"] += r.rowcount
            log.warning("Buffer overflow: purged oldest messages", count=overflow)

    async def purge_delivered(self) -> int:
        async with self._lock:
            r = self._conn.execute(
                "DELETE FROM mqtt_buffer WHERE delivered=1"
            )
            n = r.rowcount
        self._stats["purged"] += n
        return n

    # ── Stats ────────────────────────────────────────────────────────────────

    async def stats(self) -> dict:
        pending = await self.pending_count()
        return {
            **self._stats,
            "pending": pending,
            "db_path": str(self._path),
        }
