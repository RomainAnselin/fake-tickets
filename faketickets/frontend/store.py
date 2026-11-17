"""Frontend SQLite: connection profiles (secrets encrypted), local users, sessions."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import threading
import time
import uuid
from typing import Any, Optional

from ..models import SECRET_FIELDS, ConnectionSpec
from .crypto import SecretBox, hash_password, verify_password

SCHEMA = """
CREATE TABLE IF NOT EXISTS connections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT UNIQUE NOT NULL,
    name TEXT UNIQUE NOT NULL,
    kind TEXT NOT NULL,
    public TEXT NOT NULL,
    secret BLOB,
    created_by TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
    token_hash TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    expires_at REAL NOT NULL
);
"""

AAD_PREFIX = "fake-tickets/connection/v1/"


class FrontStore:
    def __init__(self, path: str, box: SecretBox) -> None:
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self.box = box
        with self._lock:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.executescript(SCHEMA)

    # ------------------------------------------------------------ connections
    @staticmethod
    def _split(spec: ConnectionSpec) -> tuple[dict[str, Any], dict[str, Any]]:
        d = spec.model_dump()
        secret = {k: d.pop(k) for k in SECRET_FIELDS if d.get(k)}
        for k in SECRET_FIELDS:
            d.pop(k, None)
        return d, secret

    def _public_view(self, row: sqlite3.Row) -> dict[str, Any]:
        pub = json.loads(row["public"])
        has = pub.pop("_has", {})
        return {
            "id": row["id"],
            "uid": row["uid"],
            "name": row["name"],
            "kind": row["kind"],
            **pub,
            "has_secret": has,
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list_connections(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM connections ORDER BY name").fetchall()
        return [self._public_view(r) for r in rows]

    def get_connection(self, cid: int) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._db.execute("SELECT * FROM connections WHERE id=?", (cid,)).fetchone()
        return self._public_view(row) if row else None

    def save_connection(self, name: str, data: dict[str, Any], user: Optional[str], cid: Optional[int] = None,
                        scb_filename: Optional[str] = None) -> dict[str, Any]:
        """Create or update. On update, secret fields that are absent/empty keep their stored value."""
        name = name.strip()
        if not name or len(name) > 100:
            raise ValueError("name is required (max 100 chars)")
        uid = str(uuid.uuid4())
        if cid is not None:
            with self._lock:
                row = self._db.execute("SELECT * FROM connections WHERE id=?", (cid,)).fetchone()
            if row is None:
                raise KeyError(cid)
            uid = row["uid"]
            old_secret = self.box.open(row["secret"], AAD_PREFIX + uid) if row["secret"] else {}
            data = {**{k: v for k, v in old_secret.items()}, **{k: v for k, v in data.items() if not (k in SECRET_FIELDS and not v)}}
        spec = ConnectionSpec(**data)  # validates
        public, secret = self._split(spec)
        public["_has"] = {k: True for k in secret}
        if spec.kind == "astra":
            prev = json.loads(row["public"]).get("scb_filename") if cid is not None else None
            public["scb_filename"] = scb_filename or prev
        blob = self.box.seal(secret, AAD_PREFIX + uid) if secret else None
        now = time.time()
        with self._lock:
            try:
                if cid is None:
                    cur = self._db.execute(
                        "INSERT INTO connections (uid, name, kind, public, secret, created_by, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (uid, name, spec.kind, json.dumps(public), blob, user, now, now),
                    )
                    cid = cur.lastrowid
                else:
                    self._db.execute(
                        "UPDATE connections SET name=?, kind=?, public=?, secret=?, updated_at=? WHERE id=?",
                        (name, spec.kind, json.dumps(public), blob, now, cid),
                    )
            except sqlite3.IntegrityError:
                raise ValueError(f"a connection named {name!r} already exists") from None
        return self.get_connection(cid)  # type: ignore[arg-type]

    def delete_connection(self, cid: int) -> None:
        with self._lock:
            self._db.execute("DELETE FROM connections WHERE id=?", (cid,))

    def connection_spec(self, cid: int) -> tuple[ConnectionSpec, dict[str, Any]]:
        """Decrypt and return the full spec (with secrets) for a run."""
        with self._lock:
            row = self._db.execute("SELECT * FROM connections WHERE id=?", (cid,)).fetchone()
        if row is None:
            raise KeyError(cid)
        public = json.loads(row["public"])
        public.pop("_has", None)
        public.pop("scb_filename", None)
        secret = self.box.open(row["secret"], AAD_PREFIX + row["uid"]) if row["secret"] else {}
        return ConnectionSpec(**{**public, **secret}), self._public_view(row)

    # ------------------------------------------------------------ users
    def upsert_user(self, username: str, password: str) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO users VALUES (?, ?, ?) ON CONFLICT(username) DO UPDATE SET password_hash=excluded.password_hash",
                (username, hash_password(password), time.time()),
            )

    def check_user(self, username: str, password: str) -> bool:
        with self._lock:
            row = self._db.execute("SELECT password_hash FROM users WHERE username=?", (username,)).fetchone()
        if row is None:
            verify_password(password, hash_password("timing-equaliser"))
            return False
        return verify_password(password, row["password_hash"])

    # ------------------------------------------------------------ sessions
    @staticmethod
    def _h(token: str) -> str:
        return hashlib.sha256(token.encode()).hexdigest()

    def create_session(self, username: str, ttl_s: float) -> str:
        token = secrets.token_urlsafe(32)
        with self._lock:
            self._db.execute("DELETE FROM sessions WHERE expires_at < ?", (time.time(),))
            self._db.execute("INSERT INTO sessions VALUES (?, ?, ?)", (self._h(token), username, time.time() + ttl_s))
        return token

    def session_user(self, token: Optional[str]) -> Optional[str]:
        if not token:
            return None
        with self._lock:
            row = self._db.execute(
                "SELECT username FROM sessions WHERE token_hash=? AND expires_at > ?", (self._h(token), time.time())
            ).fetchone()
        return row["username"] if row else None

    def delete_session(self, token: Optional[str]) -> None:
        if token:
            with self._lock:
                self._db.execute("DELETE FROM sessions WHERE token_hash=?", (self._h(token),))
