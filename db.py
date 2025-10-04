# db.py  /* CHANGED */
from __future__ import annotations  # /* ADDED */

import os  # /* ADDED */
import json  # /* ADDED */
import sqlite3  # /* ADDED */
import datetime as dt  # /* ADDED */
from typing import Any, Dict, List, Optional  # /* ADDED */

# ---- Paths -----------------------------------------------------------------  /* ADDED */

BASE_DIR = os.path.dirname(__file__)  # /* ADDED */
DATA_DIR = os.path.join(BASE_DIR, "data")  # /* ADDED */
os.makedirs(DATA_DIR, exist_ok=True)  # /* ADDED */
DB_FILE = os.path.join(DATA_DIR, "dutywatch.db")  # /* ADDED */


# ---- Connection -------------------------------------------------------------  /* ADDED */

def get_db() -> sqlite3.Connection:  # /* ADDED */
    con = sqlite3.connect(DB_FILE, check_same_thread=False)
    con.row_factory = sqlite3.Row
    # Pragmas tuned for a small local app
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con


# ---- Schema (idempotent, safe on every start) ------------------------------  /* ADDED */

def init_db() -> None:  # /* ADDED */
    with get_db() as c:
        c.executescript(
            """
            -- Original/expected app tables
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

            -- Pairings/events the user asked to hide (by pairing_id)
            CREATE TABLE IF NOT EXISTS hidden_items(
                pairing_id TEXT PRIMARY KEY,
                report_local_iso TEXT,
                created_at TEXT
            );

            -- VEVENT UIDs hidden (server-side hide by UID)
            CREATE TABLE IF NOT EXISTS hidden_uids(
                uid TEXT PRIMARY KEY,
                created_at TEXT
            );

            -- Sticky live rows so in-progress pairings persist in UI
            CREATE TABLE IF NOT EXISTS live_rows(
                pairing_id TEXT PRIMARY KEY,
                json TEXT,
                release_local_iso TEXT,
                can_hide INTEGER DEFAULT 0,
                updated_at TEXT
            );
            """
        )
        # Ensure unique indexes (idempotent)
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_hidden_pairing_id ON hidden_items(pairing_id)")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_hidden_uid ON hidden_uids(uid)")


# -------------------- events_cache helpers ----------------------------------  /* ADDED */

def read_events_cache(scope: str) -> List[Dict[str, Any]]:  # /* ADDED */
    with get_db() as c:
        row = c.execute("SELECT json FROM events_cache WHERE scope=?", (scope,)).fetchone()
        if not row or not row["json"]:
            return []
        try:
            return json.loads(row["json"])
        except Exception:
            return []

def overwrite_events_cache(scope: str, events: List[Dict[str, Any]], *, uid_hash: Optional[str] = None) -> None:  # /* ADDED */
    payload = json.dumps(events, ensure_ascii=False)
    now = dt.datetime.utcnow().isoformat()
    with get_db() as c:
        c.execute(
            "INSERT INTO events_cache(scope, uid_hash, json, updated_at) "
            "VALUES(?,?,?,?) "
            "ON CONFLICT(scope) DO UPDATE SET uid_hash=excluded.uid_hash, json=excluded.json, updated_at=excluded.updated_at",
            (scope, uid_hash, payload, now),
        )

def clear_events_cache(scope: str) -> None:  # /* ADDED */
    with get_db() as c:
        c.execute("DELETE FROM events_cache WHERE scope=?", (scope,))

def read_uid_hash(scope: str) -> Optional[str]:  # /* ADDED */
    with get_db() as c:
        row = c.execute("SELECT uid_hash FROM events_cache WHERE scope=?", (scope,)).fetchone()
        return row["uid_hash"] if row else None

def write_uid_hash(scope: str, uid_hash: Optional[str]) -> None:  # /* ADDED */
    now = dt.datetime.utcnow().isoformat()
    with get_db() as c:
        cur = c.execute("SELECT 1 FROM events_cache WHERE scope=?", (scope,)).fetchone()
        if cur:
            c.execute("UPDATE events_cache SET uid_hash=?, updated_at=? WHERE scope=?", (uid_hash, now, scope))
        else:
            c.execute("INSERT INTO events_cache(scope, uid_hash, json, updated_at) VALUES(?,?,?,?)", (scope, uid_hash, "[]", now))


# -------------------- kv helpers (last pull time, misc) ---------------------  /* ADDED */

def read_last_pull_utc(scope: str) -> Optional[str]:  # /* ADDED */
    key = f"{scope}:last_pull_utc"
    with get_db() as c:
        row = c.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

def set_last_pull_utc(scope: str, iso_ts: Optional[str] = None) -> None:  # /* ADDED */
    key = f"{scope}:last_pull_utc"
    if iso_ts is None:
        iso_ts = dt.datetime.utcnow().isoformat()
    with get_db() as c:
        c.execute(
            "INSERT INTO kv(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, iso_ts),
        )


# -------------------- hidden helpers (pairing_id) ---------------------------  /* ADDED */

def hidden_add(pairing_id: str, report_local_iso: Optional[str] = None) -> None:  # /* ADDED */
    if not pairing_id:
        return
    now = dt.datetime.utcnow().isoformat()
    with get_db() as c:
        c.execute(
            "INSERT INTO hidden_items(pairing_id, report_local_iso, created_at) VALUES(?,?,?) "
            "ON CONFLICT(pairing_id) DO UPDATE SET report_local_iso=excluded.report_local_iso",
            (pairing_id, report_local_iso or "", now),
        )

def hidden_clear_all() -> None:  # /* ADDED */
    with get_db() as c:
        c.execute("DELETE FROM hidden_items")

def hidden_all() -> List[str]:  # /* ADDED */
    with get_db() as c:
        rows = c.execute("SELECT pairing_id FROM hidden_items").fetchall()
        return [r["pairing_id"] for r in rows]

def hidden_count() -> int:  # /* ADDED */
    """Return total hidden count across both mechanisms for the UI chip."""
    with get_db() as c:
        r1 = c.execute("SELECT COUNT(*) AS n FROM hidden_items").fetchone()
        r2 = c.execute("SELECT COUNT(*) AS n FROM hidden_uids").fetchone()
        n1 = int(r1["n"] if r1 else 0)
        n2 = int(r2["n"] if r2 else 0)
        return n1 + n2


# -------------------- hidden helpers (UID-based) ----------------------------  /* ADDED */

def hide_uid(uid: str) -> None:  # /* ADDED */
    if not uid:
        return
    now = dt.datetime.utcnow().isoformat()
    with get_db() as c:
        c.execute(
            "INSERT INTO hidden_uids(uid, created_at) VALUES(?,?) "
            "ON CONFLICT(uid) DO NOTHING",
            (uid, now),
        )

def list_hidden_uids() -> List[str]:  # /* ADDED */
    with get_db() as c:
        rows = c.execute("SELECT uid FROM hidden_uids").fetchall()
        return [r["uid"] for r in rows]

def unhide_all() -> None:  # /* ADDED */
    with get_db() as c:
        c.execute("DELETE FROM hidden_uids")


# -------------------- live row helpers --------------------------------------  /* ADDED */

def upsert_live_row(row: Dict[str, Any]) -> None:  # /* ADDED */
    """
    Persist a rendered row so an in-progress pairing isn't dropped mid-fly.
    Expects row['pairing_id'] and (optionally) row['release_local_iso'], row['can_hide'].
    """
    pid = str(row.get("pairing_id") or "").strip()
    if not pid:
        return
    release_local_iso = str(row.get("release_local_iso") or "")
    can_hide_int = 1 if row.get("can_hide") else 0
    now = dt.datetime.utcnow().isoformat()
    blob = json.dumps(row, ensure_ascii=False)

    with get_db() as c:
        c.execute(
            "INSERT INTO live_rows(pairing_id, json, release_local_iso, can_hide, updated_at) "
            "VALUES(?,?,?,?,?) "
            "ON CONFLICT(pairing_id) DO UPDATE SET "
            "  json=excluded.json, release_local_iso=excluded.release_local_iso, "
            "  can_hide=excluded.can_hide, updated_at=excluded.updated_at",
            (pid, blob, release_local_iso, can_hide_int, now),
        )

def list_live_rows() -> List[Dict[str, Any]]:  # /* ADDED */
    with get_db() as c:
        rows = c.execute("SELECT json FROM live_rows").fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            try:
                out.append(json.loads(r["json"]))
            except Exception:
                pass
        return out

def purge_expired_live(now_iso: str) -> None:  # /* ADDED */
    """Remove sticky rows after their release time (if present)."""
    try:
        now = dt.datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
    except Exception:
        now = dt.datetime.utcnow()

    with get_db() as c:
        rows = c.execute("SELECT pairing_id, release_local_iso FROM live_rows").fetchall()
        to_delete: List[str] = []
        for r in rows:
            rel = r["release_local_iso"]
            if not rel:
                continue
            try:
                rel_dt = dt.datetime.fromisoformat(str(rel).replace("Z", "+00:00"))
            except Exception:
                continue
            if rel_dt < now:
                to_delete.append(r["pairing_id"])
        if to_delete:
            c.executemany("DELETE FROM live_rows WHERE pairing_id=?", [(pid,) for pid in to_delete])


# -------------------- misc ---------------------------------------------------  /* ADDED */

def list_scopes() -> List[str]:  # /* ADDED */
    with get_db() as c:
        rows = c.execute("SELECT scope FROM events_cache ORDER BY scope").fetchall()
        return [r["scope"] for r in rows]
