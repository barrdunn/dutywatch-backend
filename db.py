"""
DutyWatch SQLite helpers

Tables
------
devices, policy, acks, notifications
events_cache(scope TEXT PK, uid_hash, json, updated_at)
kv(key TEXT PK, value TEXT)
hidden_items(uid TEXT PRIMARY KEY, created_at TEXT)
live_pairings(pairing_id TEXT PRIMARY KEY, row_json TEXT, release_local_iso TEXT, updated_at TEXT)

Key helpers
-----------
read_events_cache(scope) -> list[dict]
overwrite_events_cache(scope, events, *, uid_hash=None) -> None
read_uid_hash(scope) -> str | None
write_uid_hash(scope, uid_hash) -> None
read_last_pull_utc(scope) -> str | None
set_last_pull_utc(scope, iso_ts) -> None
clear_events_cache(scope) -> None

hide_uid(uid: str) -> None
list_hidden_uids() -> set[str]
hidden_count() -> int
unhide_all() -> None

upsert_live_row(row: dict) -> None
list_live_rows() -> list[dict]
purge_expired_live(now_iso: str) -> None
delete_live_pairing(pairing_id: str) -> None
"""

from __future__ import annotations

import os
import json
import sqlite3
import datetime as dt
from typing import Any, List, Dict

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

            CREATE TABLE IF NOT EXISTS events_cache(
                scope TEXT PRIMARY KEY,
                uid_hash TEXT,
                json TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS kv(
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS hidden_items(
                uid TEXT PRIMARY KEY,
                created_at TEXT
            );

            -- Sticky cache of in-progress pairings, so they remain visible
            -- until their release_local_iso passes.
            CREATE TABLE IF NOT EXISTS live_pairings(
                pairing_id TEXT PRIMARY KEY,
                row_json TEXT NOT NULL,
                release_local_iso TEXT,
                updated_at TEXT
            );
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
    now = dt.datetime.utcnow().isoformat()
    with get_db() as c:
        cur = c.execute("SELECT 1 FROM events_cache WHERE scope=?", (scope,)).fetchone()
        if cur:
            c.execute("UPDATE events_cache SET uid_hash=?, updated_at=? WHERE scope=?", (uid_hash, now, scope))
        else:
            c.execute("INSERT INTO events_cache(scope, uid_hash, json, updated_at) VALUES(?,?,?,?)", (scope, uid_hash, "[]", now))


# -------------------- kv helpers --------------------

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


# -------------------- hidden items helpers --------------------

def hide_uid(uid: str) -> None:
    if not uid:
        return
    now = dt.datetime.utcnow().isoformat()
    with get_db() as c:
        c.execute(
            "INSERT INTO hidden_items(uid, created_at) VALUES(?, ?) "
            "ON CONFLICT(uid) DO NOTHING",
            (uid, now),
        )


def list_hidden_uids() -> set[str]:
    with get_db() as c:
        rows = c.execute("SELECT uid FROM hidden_items").fetchall()
        return {r["uid"] for r in rows}


def hidden_count() -> int:
    with get_db() as c:
        row = c.execute("SELECT COUNT(*) AS n FROM hidden_items").fetchone()
        return int(row["n"] if row and row["n"] is not None else 0)


def unhide_all() -> None:
    with get_db() as c:
        c.execute("DELETE FROM hidden_items")


# -------------------- live pairings (sticky until release) --------------------

def upsert_live_row(row: Dict[str, Any]) -> None:
    """Persist the current enriched row for an in-progress pairing."""
    pairing_id = str(row.get("pairing_id") or "")
    if not pairing_id:
        return
    rel_iso = str(row.get("release_local_iso") or "")
    now = dt.datetime.utcnow().isoformat()
    payload = json.dumps(row)
    with get_db() as c:
        c.execute(
            "INSERT INTO live_pairings(pairing_id, row_json, release_local_iso, updated_at) "
            "VALUES(?,?,?,?) "
            "ON CONFLICT(pairing_id) DO UPDATE SET row_json=excluded.row_json, release_local_iso=excluded.release_local_iso, updated_at=excluded.updated_at",
            (pairing_id, payload, rel_iso, now),
        )


def list_live_rows() -> List[Dict[str, Any]]:
    with get_db() as c:
        rows = c.execute("SELECT row_json FROM live_pairings").fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            try:
                out.append(json.loads(r["row_json"]))
            except Exception:
                pass
        return out


def purge_expired_live(now_iso: str) -> None:
    """Drop any sticky rows with release_local_iso < now."""
    with get_db() as c:
        c.execute("DELETE FROM live_pairings WHERE release_local_iso <> '' AND release_local_iso < ?", (now_iso,))


def delete_live_pairing(pairing_id: str) -> None:
    with get_db() as c:
        c.execute("DELETE FROM live_pairings WHERE pairing_id=?", (pairing_id,))
