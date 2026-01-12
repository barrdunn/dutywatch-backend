"""
Utility functions for DutyWatch.
"""

from __future__ import annotations
import os
import datetime as dt
from typing import Optional
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "America/Chicago"))


def iso_to_dt(s: Optional[str]) -> Optional[dt.datetime]:
    """Parse ISO datetime string to datetime object."""
    if not s:
        return None
    try:
        # Handle various ISO formats
        s = s.replace("Z", "+00:00")
        if "+" not in s and "-" not in s[10:]:
            # No timezone, assume UTC
            s = s + "+00:00"
        return dt.datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def to_local(d: Optional[dt.datetime]) -> Optional[dt.datetime]:
    """Convert datetime to local timezone."""
    if not d:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(LOCAL_TZ)


def to_utc(dt_obj):
    """Convert datetime or date to UTC datetime"""
    if dt_obj is None:
        return None
    
    # Handle all-day events (date objects)
    if isinstance(dt_obj, dt.date) and not isinstance(dt_obj, dt.datetime):
        # Convert date to datetime at midnight UTC
        return dt.datetime(dt_obj.year, dt_obj.month, dt_obj.day, tzinfo=dt.timezone.utc)
    
    # Handle datetime objects
    if dt_obj.tzinfo is None:
        # Naive datetime - assume local timezone
        dt_obj = dt_obj.replace(tzinfo=LOCAL_TZ)
    
    return dt_obj.astimezone(dt.timezone.utc)

def ensure_hhmm(s: Optional[str]) -> str:
    """Normalize time string to HHMM format (e.g., '800' -> '0800')."""
    if not s:
        return ""
    s = str(s).strip()
    if len(s) == 3:
        return "0" + s
    return s


def to_12h(hhmm: str) -> str:
    """Convert HHMM to 12-hour format (e.g., '1430' -> '2:30 PM')."""
    if not hhmm or len(hhmm) < 4:
        return hhmm
    try:
        h = int(hhmm[:2])
        m = int(hhmm[2:4])
        suffix = "AM" if h < 12 else "PM"
        h12 = h % 12
        if h12 == 0:
            h12 = 12
        if m == 0:
            return f"{h12} {suffix}"
        return f"{h12}:{m:02d} {suffix}"
    except (ValueError, TypeError):
        return hhmm


def time_display(hhmm: str, is_24h: bool) -> str:
    """Format time for display based on 12h/24h preference."""
    if not hhmm:
        return ""
    hhmm = ensure_hhmm(hhmm)
    if is_24h:
        return f"{hhmm[:2]}:{hhmm[2:]}"
    return to_12h(hhmm)


def fmt_time(hhmm: str, is_24h: bool) -> str:
    """Alias for time_display."""
    return time_display(hhmm, is_24h)


def human_duration(td: dt.timedelta) -> str:
    """Format a timedelta as human-readable string."""
    total_seconds = int(td.total_seconds())
    if total_seconds < 0:
        return "0m"
    
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes or not parts:
        parts.append(f"{minutes}m")
    
    return " ".join(parts)


def human_ago(d: Optional[dt.datetime]) -> str:
    """Return human-readable 'time ago' string."""
    if not d:
        return "never"
    
    now = dt.datetime.now(LOCAL_TZ)
    d_local = to_local(d)
    if not d_local:
        return "never"
    
    delta = now - d_local
    seconds = int(delta.total_seconds())
    
    if seconds < 0:
        return "just now"
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        mins = seconds // 60
        return f"{mins}m ago"
    if seconds < 86400:
        hours = seconds // 3600
        return f"{hours}h ago"
    
    days = seconds // 86400
    return f"{days}d ago"


def human_ago_precise(d: Optional[dt.datetime]) -> str:
    """Return precise 'time ago' string with minutes."""
    if not d:
        return "never"
    
    now = dt.datetime.now(LOCAL_TZ)
    d_local = to_local(d)
    if not d_local:
        return "never"
    
    delta = now - d_local
    seconds = int(delta.total_seconds())
    
    if seconds < 0:
        return "just now"
    if seconds < 60:
        return f"{seconds}s ago"
    if seconds < 3600:
        mins = seconds // 60
        secs = seconds % 60
        if secs:
            return f"{mins}m {secs}s ago"
        return f"{mins}m ago"
    
    hours = seconds // 3600
    mins = (seconds % 3600) // 60
    if mins:
        return f"{hours}h {mins}m ago"
    return f"{hours}h ago"