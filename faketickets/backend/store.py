"""Backend persistence: run records + events (no secrets are ever stored here)."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    workload TEXT NOT NULL,
    params TEXT NOT NULL,
    connection_ref TEXT,
    connection_name TEXT,
    target TEXT,
    owner TEXT,
    label TEXT,
    state TEXT NOT NULL,
    reason TEXT,
    error TEXT,
    result TEXT,
    created_at REAL NOT NULL,
    started_at REAL,
    ended_at REAL,
    next_id INTEGER,
    total_ok INTEGER DEFAULT 0,
    total_err INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS runs_target ON runs (connection_ref, target);
CREATE TABLE IF NOT EXISTS events (
    run_id TEXT NOT NULL,
    t REAL NOT NULL,
    level TEXT NOT NULL,
    msg TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_run ON events (run_id, t);
"""

ACTIVE_STATES = ("starting", "running", "stopping")
COLUMNS = (
    "workload", "params", "connection_ref", "connection_name", "target", "owner", "label", "state", "reason",
    "error", "result", "created_at", "started_at", "ended_at", "next_id", "total_ok", "total_err",
)


class RunStore:
    def __init__(self, path: str) -> None:
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.executescript(SCHEMA)

    def mark_interrupted(self) -> int:
        """At startup: runs left active by a previous process are now interrupted."""
        with self._lock:
            cur = self._db.execute(
                f"UPDATE runs SET state='interrupted', reason='backend_restart', ended_at=? "
                f"WHERE state IN ({','.join('?' * len(ACTIVE_STATES))})",
                (time.time(), *ACTIVE_STATES),
            )
            return cur.rowcount

    def upsert(self, run_id: str, **fields: Any) -> None:
        for k in ("params", "result"):
            if k in fields and not isinstance(fields[k], (str, type(None))):
                fields[k] = json.dumps(fields[k], default=str)
        bad = set(fields) - set(COLUMNS)
        if bad:
            raise ValueError(f"unknown columns {bad}")
        with self._lock:
            exists = self._db.execute("SELECT 1 FROM runs WHERE id=?", (run_id,)).fetchone()
            if exists:
                if fields:
                    sets = ", ".join(f"{k}=?" for k in fields)
                    self._db.execute(f"UPDATE runs SET {sets} WHERE id=?", (*fields.values(), run_id))
            else:
                cols = ["id", *fields]
                self._db.execute(
                    f"INSERT INTO runs ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                    (run_id, *fields.values()),
                )

    def add_event(self, run_id: str, t: float, level: str, msg: str) -> None:
        with self._lock:
            self._db.execute("INSERT INTO events VALUES (?, ?, ?, ?)", (run_id, t, level, msg[:2000]))

    def events(self, run_id: str, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._db.execute(
                "SELECT t, level, msg FROM events WHERE run_id=? ORDER BY t DESC LIMIT ?", (run_id, limit)
            ).fetchall()
        return [dict(r) for r in reversed(rows)]

    @staticmethod
    def _row(r: sqlite3.Row) -> dict[str, Any]:
        d = dict(r)
        for k in ("params", "result"):
            if d.get(k):
                d[k] = json.loads(d[k])
        return d

    def get(self, run_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            r = self._db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        return self._row(r) if r else None

    def list(self, limit: int = 200, owner: Optional[str] = None) -> list[dict[str, Any]]:
        q, args = "SELECT * FROM runs", []
        if owner:
            q += " WHERE owner=?"
            args.append(owner)
        q += " ORDER BY created_at DESC LIMIT ?"
        args.append(limit)
        with self._lock:
            return [self._row(r) for r in self._db.execute(q, args).fetchall()]

    def delete(self, run_id: str) -> None:
        with self._lock:
            self._db.execute("DELETE FROM events WHERE run_id=?", (run_id,))
            self._db.execute("DELETE FROM runs WHERE id=?", (run_id,))

    def cursor(self, connection_ref: str, target: str) -> Optional[int]:
        """Highest next_id written to ``target`` (keyspace.table) through this connection."""
        with self._lock:
            r = self._db.execute(
                "SELECT MAX(next_id) FROM runs WHERE connection_ref=? AND target=? AND next_id IS NOT NULL",
                (connection_ref, target),
            ).fetchone()
        return r[0] if r and r[0] is not None else None
