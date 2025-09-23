"""
DutyWatch SQLite Database Helper (with events & schedule cache)

- Core tables: devices, policy, acks, notifications
- Cache tables:
    • schedule_cache  : last computed pairings/time-off table + content hash
    • events_cache    : last fetched events for a given "scope" (e.g. a month)
- Helpers:
    • save_schedule_cache() / load_schedule_cache()
    • overwrite_events_cache(scope, events) / read_events_cache(scope)
"""

import os
import sqlite3
import json
import datetime as dt
from typing import List, Dict, Optional

# Store DB under ./data/ so it doesn't clutter the repo root
DB_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DB_DIR, exist_ok=True)
DB_FILE = os.path.join(DB_DIR, "dutywatch.db")


def get_db():
    """
    Returns a sqlite3 connection. Works with `with get_db() as c:`
    and sets row_factory so you can access columns by name.
    """
    con = sqlite3.connect(DB_FILE, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    """
    Creates all required tables if they don't exist.
    Safe to call multiple times.
    """
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

            -- Cache of the computed schedule table (pairings + time off)
            CREATE TABLE IF NOT EXISTS schedule_cache (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                hash TEXT,       -- sha256 over significant event fields
                json TEXT,       -- full cached payload (rows, hash, etc.)
                updated_at TEXT  -- UTC timestamp when cache was written
            );

            -- Cache of events fetched for a specific scope (e.g. "month:2025-10")
            CREATE TABLE IF NOT EXISTS events_cache (
                id INTEGER PRIMARY KEY,
                scope TEXT NOT NULL,     -- e.g. "month:2025-10" or "range:2025-10-01..2025-11-01"
                uid TEXT,
                calendar TEXT,
                summary TEXT,
                location TEXT,
                description TEXT,
                start_utc TEXT,
                end_utc TEXT,
                last_modified TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_events_cache_scope ON events_cache(scope);
            """
        )


# ---------- schedule cache helpers ----------

def save_schedule_cache(cache: dict) -> None:
    """
    Persist the latest schedule table + its hash.
    Expects a dict like: {"hash": "...", "rows": [...] }
    """
    with get_db() as c:
        c.execute(
            "INSERT OR REPLACE INTO schedule_cache(id, hash, json, updated_at) VALUES(1,?,?,?)",
            (cache.get("hash"), json.dumps(cache), dt.datetime.utcnow().isoformat()),
        )


def load_schedule_cache() -> Optional[dict]:
    """
    Return the last cached schedule table, or None if empty.
    """
    with get_db() as c:
        row = c.execute("SELECT json FROM schedule_cache WHERE id=1").fetchone()
        if not row:
            return None
        return json.loads(row["json"])


# ---------- events cache helpers (overwrite-on-pull) ----------

def overwrite_events_cache(scope: str, events: List[Dict]) -> None:
    """
    Blow away old rows for 'scope' and insert the new snapshot.
    Each event dict should contain: uid, calendar, summary, location,
    description, start_utc, end_utc, last_modified.
    """
    with get_db() as c:
        c.execute("DELETE FROM events_cache WHERE scope=?", (scope,))
        if not events:
            return
        c.executemany(
            """
            INSERT INTO events_cache
            (scope, uid, calendar, summary, location, description, start_utc, end_utc, last_modified)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    scope,
                    e.get("uid"),
                    e.get("calendar"),
                    e.get("summary"),
                    e.get("location"),
                    e.get("description"),
                    e.get("start_utc"),
                    e.get("end_utc"),
                    e.get("last_modified"),
                )
                for e in events
            ],
        )


def read_events_cache(scope: str) -> List[Dict]:
    """
    Return cached events for 'scope' (ordered by start_utc).
    """
    with get_db() as c:
        rows = c.execute(
            """SELECT uid, calendar, summary, location, description, start_utc, end_utc, last_modified
               FROM events_cache WHERE scope=? ORDER BY start_utc""",
            (scope,),
        ).fetchall()
        return [dict(r) for r in rows]
