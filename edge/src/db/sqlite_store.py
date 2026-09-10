"""
SQLite storage for edge device telemetry and diagnostic data.

Stores the last N telemetry readings locally for:
  - Local API queries (without cloud connectivity)
  - Pre-send inspection and sanity checks
  - Offline data export
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

from ..utils.logging_config import get_logger

log = get_logger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS telemetry (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT    NOT NULL,
    device_id    TEXT    NOT NULL,
    sequence_num INTEGER NOT NULL,
    payload_json TEXT    NOT NULL,
    stored_at    REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ts ON telemetry (ts DESC);

CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        TEXT    NOT NULL,
    event_type TEXT   NOT NULL,
    severity  TEXT    NOT NULL DEFAULT 'INFO',
    message   TEXT    NOT NULL,
    data_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events (ts DESC);
"""

_RETENTION_HOURS = 48
_MAX_ROWS        = 50_000


class SQLiteStore:
    """Local edge telemetry and event store."""

    def __init__(self, db_path: str | Path = "/data/edge.db") -> None:
        self._path = Path(db_path)
        self._conn: Optional[sqlite3.Connection] = None

    def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self._path),
            check_same_thread=False,
            isolation_level=None,
        )
        self._conn.executescript(_SCHEMA)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        log.info("SQLite store opened", path=str(self._path))

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # ── Telemetry ─────────────────────────────────────────────────────────────

    def insert_telemetry(self, snapshot_dict: dict) -> None:
        self._conn.execute(
            "INSERT INTO telemetry (ts, device_id, sequence_num, payload_json, stored_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (
                snapshot_dict["ts"],
                snapshot_dict["device_id"],
                snapshot_dict.get("sequence_num", 0),
                json.dumps(snapshot_dict),
                time.time(),
            ),
        )
        self._maybe_trim_telemetry()

    def get_latest_telemetry(self, limit: int = 1) -> list[dict]:
        cur = self._conn.execute(
            "SELECT payload_json FROM telemetry ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return [json.loads(row[0]) for row in cur.fetchall()]

    def get_telemetry_range(self, from_ts: str, to_ts: str) -> list[dict]:
        cur = self._conn.execute(
            "SELECT payload_json FROM telemetry"
            " WHERE ts >= ? AND ts <= ? ORDER BY ts ASC",
            (from_ts, to_ts),
        )
        return [json.loads(row[0]) for row in cur.fetchall()]

    # ── Events ────────────────────────────────────────────────────────────────

    def log_event(self, event_type: str, message: str,
                  severity: str = "INFO", data: Optional[dict] = None) -> None:
        from datetime import datetime, timezone
        self._conn.execute(
            "INSERT INTO events (ts, event_type, severity, message, data_json)"
            " VALUES (?, ?, ?, ?, ?)",
            (
                datetime.now(timezone.utc).isoformat(),
                event_type,
                severity,
                message,
                json.dumps(data) if data else None,
            ),
        )

    def get_recent_events(self, limit: int = 100, severity: Optional[str] = None) -> list[dict]:
        if severity:
            cur = self._conn.execute(
                "SELECT ts, event_type, severity, message, data_json"
                " FROM events WHERE severity=? ORDER BY id DESC LIMIT ?",
                (severity, limit),
            )
        else:
            cur = self._conn.execute(
                "SELECT ts, event_type, severity, message, data_json"
                " FROM events ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        rows = cur.fetchall()
        return [
            {
                "ts": r[0], "event_type": r[1], "severity": r[2],
                "message": r[3],
                "data": json.loads(r[4]) if r[4] else None,
            }
            for r in rows
        ]

    # ── Maintenance ───────────────────────────────────────────────────────────

    def _maybe_trim_telemetry(self) -> None:
        cutoff = time.time() - _RETENTION_HOURS * 3600
        self._conn.execute(
            "DELETE FROM telemetry WHERE stored_at < ?", (cutoff,)
        )
        # Hard row cap
        cur = self._conn.execute("SELECT COUNT(*) FROM telemetry")
        count = cur.fetchone()[0]
        if count > _MAX_ROWS:
            self._conn.execute(
                "DELETE FROM telemetry WHERE id IN ("
                "  SELECT id FROM telemetry ORDER BY id ASC LIMIT ?"
                ")",
                (count - _MAX_ROWS,),
            )

    def get_stats(self) -> dict:
        cur = self._conn.execute("SELECT COUNT(*) FROM telemetry")
        t_count = cur.fetchone()[0]
        cur = self._conn.execute("SELECT COUNT(*) FROM events")
        e_count = cur.fetchone()[0]
        size_kb = self._path.stat().st_size // 1024 if self._path.exists() else 0
        return {
            "telemetry_rows": t_count,
            "event_rows": e_count,
            "db_size_kb": size_kb,
        }
