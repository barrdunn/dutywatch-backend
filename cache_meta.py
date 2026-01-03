"""
Cache Meta - Simple JSON file storage for calendar events.

On refresh: Replace ALL events, rebuild everything from scratch.
No merging, no partial updates.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger("cache_meta")

CACHE_FILE = Path(__file__).parent / "cache_meta.json"

DEFAULT_META = {
    "events": [],
    "events_digest": "",
    "last_pull_utc": "",
    "next_run_utc": "",
    "refresh_minutes": 30,
}


def read_cache_meta() -> Dict[str, Any]:
    """Read the cache meta file."""
    if not CACHE_FILE.exists():
        return dict(DEFAULT_META)
    
    try:
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)
        # Ensure all keys exist
        for key, default in DEFAULT_META.items():
            if key not in data:
                data[key] = default
        return data
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Failed to read cache: {e}")
        return dict(DEFAULT_META)


def write_cache_meta(meta: Dict[str, Any]) -> None:
    """Write the cache meta file. REPLACES entire file."""
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(meta, f, indent=2, default=str)
    except IOError as e:
        logger.error(f"Failed to write cache: {e}")


def clear_cache() -> None:
    """Clear all cached data."""
    write_cache_meta(dict(DEFAULT_META))
    logger.info("Cache cleared")


def normalize_cached_events(meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Get events from cache meta, ensuring they're in the right format.
    Returns a NEW list (not a reference to cached data).
    """
    events = meta.get("events", [])
    
    # Return a copy to avoid mutations affecting cache
    return [dict(e) for e in events]


def update_events(new_events: List[Dict[str, Any]], meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Replace ALL events in the cache.
    Returns updated meta dict.
    """
    # Complete replacement - no merging
    meta["events"] = new_events
    return meta