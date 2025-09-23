"""
DutyWatch SQLite Database Helper (minimal + schedule cache)

- Keeps your existing tables (devices, policy, acks, notifications)
- Adds a small 'schedule_cache' table so we can store the latest
  pairings/time-off table and a content hash for change detection.
- Exposes save_schedule_cache() / load_schedule_cache() helpers.
"""

import os
import sqlite3
import json
import datetime as dt

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

            -- NEW: cache of the computed schedule table (pairings + time off)
            CREATE TABLE IF NOT EXISTS schedule_cache (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                hash TEXT,       -- sha256 over significant event fields
                json TEXT,       -- full cached payload (rows, hash)
                updated_at TEXT  -- UTC timestamp when cache was written
            );
            """
        )


# ---------- Helpers for schedule cache ----------

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


def load_schedule_cache() -> dict | None:
    """
    Return the last cached schedule table, or None if empty.
    """
    with get_db() as c:
        row = c.execute("SELECT json FROM schedule_cache WHERE id=1").fetchone()
        if not row:
            return None
        return json.loads(row["json"])
