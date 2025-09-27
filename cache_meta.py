"""
cache_meta.py
-------------
In plain English: this file is the "inbox" where we keep the latest calendar
snapshot the server pulled from iCloud.

- We save ONE thing under a fixed name ("rolling"): a JSON blob that contains:
    {
      "events": [...],            # the raw calendar events we pulled
      "last_pull_utc": "...",     # when we last pulled from iCloud
      "next_run_utc": "...",      # when the poller plans to pull again
      "refresh_minutes": 30       # how often we intend to refresh
    }

- When the API needs to show the UI, we READ this blob, pull out "events",
  and build the table rows in memory (we DO NOT store rows in the DB).

- If we just pulled fresh data, we WRITE the blob back with updated fields.

Why keep this separate?
- It isolates the storage details (SQLite table shape, JSON quirks) from the
  rest of the app. If we ever swap SQLite for Redis/Postgres, only this file
  changes.
- It "normalizes" whatever is in the DB into the simple shapes the app expects,
  so the rest of the code can assume it has a clean list of events every time.
"""

from __future__ import annotations
import json
from typing import Any, Dict, List
from db import read_events_cache, overwrite_events_cache

# We keep one snapshot under this scope name.
SCOPE = "rolling"


def normalize_cached_events(raw) -> List[Dict[str, Any]]:
    """
    Take whatever we got from the DB and return a clean list of event dicts.

    The DB row might contain:
      - a plain list:             [ {...}, {...} ]
      - a dict with "events":     { "events": [ ... ], ... }
      - a JSON string of either of the above

    No matter what, this function returns a LIST (possibly empty).
    """
    if raw is None:
        return []

    # Already a list?
    if isinstance(raw, list):
        return raw

    # A dict that already has events?
    if isinstance(raw, dict):
        if isinstance(raw.get("events"), list):
            return raw["events"]
        return []

    # A JSON string?
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except Exception:
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and isinstance(data.get("events"), list):
            return data["events"]
        return []

    # Anything else -> empty
    return []


def read_cache_meta() -> Dict[str, Any]:
    """
    Read the snapshot from SQLite and return a dict with at least "events".

    The DB stores just one JSON blob per scope. We accept multiple shapes and
    coerce to a dict:
      { "events": [...], "last_pull_utc": "...", ... }

    If nothing is stored yet, return { "events": [] } so callers can proceed.
    """
    raw = read_events_cache(SCOPE)
    # If it's already a dict, pass it through (it should carry meta keys).
    if isinstance(raw, dict):
        raw.setdefault("events", [])
        return raw

    # If it's a list or JSON string, wrap it into a dict with "events".
    events = normalize_cached_events(raw)
    return {"events": events}


def write_cache_meta(meta: Dict[str, Any]) -> None:
    """
    Persist the snapshot back to SQLite.

    We guarantee the JSON blob at least has:
      - "events": a list (may be empty)

    Callers typically set/update:
      - "last_pull_utc"
      - "next_run_utc"
      - "refresh_minutes"
    """
    if "events" not in meta or not isinstance(meta["events"], list):
        meta["events"] = []
    overwrite_events_cache(SCOPE, meta)
