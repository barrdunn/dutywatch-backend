"""
DutyWatch utility helpers - consolidated

Shared datetime/formatting functions used across app.py, rows.py, and cal_client.py
"""

from __future__ import annotations
import datetime as dt
import os
from typing import Optional, Union
from zoneinfo import ZoneInfo

# Also support pytz for cal_client.py compatibility
try:
    import pytz
    HAS_PYTZ = True
except ImportError:
    HAS_PYTZ = False

from config import TIMEZONE

LOCAL_TZ = ZoneInfo(TIMEZONE)


# ---- datetime parsing/conversion ----

def iso_to_dt(s: Optional[str]) -> Optional[dt.datetime]:
    """Parse ISO string to datetime, handling Z suffix."""
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def to_local(d: Optional[dt.datetime]) -> Optional[dt.datetime]:
    """Convert datetime to local timezone."""
    if not d:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(LOCAL_TZ)


def to_utc(d: Union[dt.datetime, dt.date, None]) -> Optional[dt.datetime]:
    """
    Convert datetime/date to UTC. 
    Handles both naive and aware datetimes, and plain dates.
    Compatible with both zoneinfo and pytz.
    """
    if d is None:
        return None
    
    # Convert date to datetime if needed
    if isinstance(d, dt.date) and not isinstance(d, dt.datetime):
        d = dt.datetime.combine(d, dt.time.min)
    
    if d.tzinfo is None:
        # Assume local timezone for naive datetimes
        d = d.replace(tzinfo=LOCAL_TZ)
    
    return d.astimezone(dt.timezone.utc)


# ---- time formatting ----

def ensure_hhmm(s: str) -> str:
    """Normalize time string to 4-digit HHMM format (e.g., '800' -> '0800')."""
    if not s:
        return ""
    return s.zfill(4) if len(s) < 4 else s


def to_12h(hhmm: str) -> str:
    """Convert HHMM to 12-hour format (e.g., '1430' -> '2:30 PM')."""
    hhmm = ensure_hhmm(hhmm)
    if not hhmm or len(hhmm) < 4:
        return ""
    h = int(hhmm[:2])
    m = hhmm[2:]
    ampm = "AM" if h < 12 else "PM"
    h12 = h % 12 or 12
    return f"{h12}:{m} {ampm}"


def fmt_time(hhmm: str, use_24h: bool) -> str:
    """Format HHMM string based on clock preference."""
    if not hhmm:
        return ""
    hhmm = ensure_hhmm(hhmm)
    if use_24h:
        return f"{hhmm[:2]}:{hhmm[2:]}"
    return to_12h(hhmm)


def time_display(hhmm: Optional[str], is_24h: bool) -> str:
    """Format time for display, handling None gracefully."""
    if not hhmm:
        return ""
    return fmt_time(hhmm, is_24h)


# ---- human-readable formatting ----

def human_ago(from_dt: Optional[dt.datetime]) -> str:
    """Return human-readable 'X ago' string (simple version)."""
    if not from_dt:
        return "never"
    now = dt.datetime.now(LOCAL_TZ)
    delta = now - from_dt
    secs = int(delta.total_seconds())
    if secs < 60:
        return "just now"
    mins = secs // 60
    if mins < 60:
        return f"{mins} min{'s' if mins != 1 else ''} ago"
    hours = mins // 60
    if hours < 24:
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = hours // 24
    return f"{days} day{'s' if days != 1 else ''} ago"


def human_ago_precise(from_dt: Optional[dt.datetime]) -> str:
    """Return precise 'Xm Ys ago' string."""
    if not from_dt:
        return "never"
    now = dt.datetime.now(LOCAL_TZ)
    delta = now - from_dt
    if delta.total_seconds() < 0:
        delta = dt.timedelta(0)
    s = int(delta.total_seconds())
    m = s // 60
    s = s % 60
    if m and s:
        return f"{m}m {s}s ago"
    if m:
        return f"{m}m ago"
    return f"{s}s ago"


def human_duration(td: dt.timedelta) -> str:
    """Format timedelta as human-readable duration (e.g., '2d 5h' or '37h')."""
    total_h = max(0, int(td.total_seconds() // 3600))
    if total_h >= 48:
        d = total_h // 24
        h = total_h % 24
        return f"{d}d {h}h"
    return f"{total_h}h"