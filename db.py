"""
DutyWatch SQLite helpers

Tables
------
devices, policy, acks, notifications      # original app tables
events_cache(scope TEXT PK, uid_hash, json, updated_at)
kv(key TEXT PK, value TEXT)               # tiny key/value for metadata

Key helpers
-----------
read_events_cache(scope) -> list[dict]
overwrite_events_cache(scope, events, *, uid_hash=None) -> None
read_uid_hash(scope) -> str | None
write_uid_hash(scope, uid_hash) -> None
read_last_pull_utc(scope) -> str | None
set_last_pull_utc(scope, iso_ts) -> None
clear_events_cache(scope) -> None
"""

from __future__ import annotations

import os
import json
import sqlite3
import datetime as dt
from typing import Any

# Store DB under ./data/
DB_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DB_DIR, exist_ok=True)
DB_FILE = os.path.join(DB_DIR, "dutywatch.db")


def get_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_FILE, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def init_db() -> None:
    """Create all required tables if they don't exist."""
    with get_db() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS devices(
                id INTEGER PRIMARY KEY,
                device_token TEXT UNIQUE,
                created_at TEXT
            );

            CREATE TABLE IF NOT EXISTS policy(
                id INTEGER PRIMARY KEY CHECK (id=1),
                json TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS acks(
                ack_id TEXT PRIMARY KEY,
                event_uid TEXT,
                deadline_utc TEXT,
                state TEXT,
                last_update_utc TEXT
            );

            CREATE TABLE IF NOT EXISTS notifications(
                id INTEGER PRIMARY KEY,
                ack_id TEXT,
                event_uid TEXT,
                event_start_utc TEXT,
                fire_at_utc TEXT,
                kind TEXT,
                attempt INTEGER DEFAULT 0,
                sent INTEGER DEFAULT 0
            );

            -- Snapshots of calendar pulls (rolling or month scopes)
            CREATE TABLE IF NOT EXISTS events_cache(
                scope TEXT PRIMARY KEY,
                uid_hash TEXT,
                json TEXT,          -- JSON array of event dicts
                updated_at TEXT
            );

            -- Tiny KV for misc metadata (e.g., last pull time per scope)
            CREATE TABLE IF NOT EXISTS kv(
                key TEXT PRIMARY KEY,
                value TEXT
            );

            /* ADDED */ 
            CREATE TABLE IF NOT EXISTS hidden_items(
                key TEXT PRIMARY KEY,      -- Apple UID preferred; fallback to ack_id
                fpr TEXT NOT NULL,         -- fingerprint when hidden
                created_utc TEXT NOT NULL,
                last_update_utc TEXT NOT NULL
            );
            /* ADDED END */
            """
        )


# -------------------- events_cache helpers --------------------

def read_events_cache(scope: str) -> list[dict[str, Any]]:
    with get_db() as c:
        row = c.execute("SELECT json FROM events_cache WHERE scope=?", (scope,)).fetchone()
        if not row or not row["json"]:
            return []
        try:
            return json.loads(row["json"])
        except Exception:
            return []


def overwrite_events_cache(scope: str, events: list[dict[str, Any]], *, uid_hash: str | None = None) -> None:
    """Replace the entire snapshot for a scope."""
    payload = json.dumps(events)
    now = dt.datetime.utcnow().isoformat()
    with get_db() as c:
        c.execute(
            "INSERT INTO events_cache(scope, uid_hash, json, updated_at) "
            "VALUES(?,?,?,?) "
            "ON CONFLICT(scope) DO UPDATE SET uid_hash=excluded.uid_hash, json=excluded.json, updated_at=excluded.updated_at",
            (scope, uid_hash, payload, now),
        )


def clear_events_cache(scope: str) -> None:
    with get_db() as c:
        c.execute("DELETE FROM events_cache WHERE scope=?", (scope,))


def read_uid_hash(scope: str) -> str | None:
    with get_db() as c:
        row = c.execute("SELECT uid_hash FROM events_cache WHERE scope=?", (scope,)).fetchone()
        return row["uid_hash"] if row else None


def write_uid_hash(scope: str, uid_hash: str | None) -> None:
    """Upsert just the uid_hash for a scope (creates row if missing)."""
    now = dt.datetime.utcnow().isoformat()
    with get_db() as c:
        cur = c.execute("SELECT 1 FROM events_cache WHERE scope=?", (scope,)).fetchone()
        if cur:
            c.execute(
                "UPDATE events_cache SET uid_hash=?, updated_at=? WHERE scope=?",
                (uid_hash, now, scope),
            )
        else:
            c.execute(
                "INSERT INTO events_cache(scope, uid_hash, json, updated_at) VALUES(?,?,?,?)",
                (scope, uid_hash, "[]", now),
            )


# -------------------- kv helpers (last pull time) --------------------

def read_last_pull_utc(scope: str) -> str | None:
    key = f"{scope}:last_pull_utc"
    with get_db() as c:
        row = c.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None


def set_last_pull_utc(scope: str, iso_ts: str | None = None) -> None:
    key = f"{scope}:last_pull_utc"
    if iso_ts is None:
        iso_ts = dt.datetime.utcnow().isoformat()
    with get_db() as c:
        c.execute(
            "INSERT INTO kv(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, iso_ts),
        )


# -------------------- misc --------------------

def list_scopes() -> list[str]:
    with get_db() as c:
        rows = c.execute("SELECT scope FROM events_cache ORDER BY scope").fetchall()
        return [r["scope"] for r in rows]
