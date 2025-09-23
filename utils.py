"""
DutyWatch utility helpers
"""

import datetime as dt
import hashlib
from typing import Optional, Union
import pytz

from config import TIMEZONE

LOCAL_TZ = pytz.timezone(TIMEZONE)

# ---- time helpers ----

def _ensure_datetime(x) -> Optional[dt.datetime]:
    if x is None:
        return None
    if isinstance(x, dt.datetime):
        return x
    if isinstance(x, dt.date):
        return dt.datetime.combine(x, dt.time.min)
    return None

def _resolve_tz(tz_opt):
    """
    Accepts None, a tz name string (e.g., 'America/Chicago'), or a tzinfo object.
    Returns a tzinfo object.
    """
    default_tz = pytz.timezone(TIMEZONE)
    if tz_opt is None:
        return default_tz
    if isinstance(tz_opt, str):
        return pytz.timezone(tz_opt)
    return tz_opt  # assume tzinfo

def to_utc(d: Union[dt.datetime, dt.date, None]) -> Optional[dt.datetime]:
    """Return timezone-aware UTC datetime."""
    d = _ensure_datetime(d)
    if d is None:
        return None
    if d.tzinfo is None:
        # assume local TZ if naive
        d = LOCAL_TZ.localize(d)
    return d.astimezone(pytz.utc)

def to_local(d: Union[dt.datetime, dt.date, None], tz_opt=None) -> Optional[dt.datetime]:
    """
    Convert datetime/date to target timezone (defaults to config TIMEZONE).
    Accepts either a tz name string or tzinfo in tz_opt.
    """
    d = _ensure_datetime(d)
    if d is None:
        return None
    tz_target = _resolve_tz(tz_opt)
    if d.tzinfo is None:
        # treat naive as UTC
        d = d.replace(tzinfo=pytz.utc)
    return d.astimezone(tz_target)

def to_local_iso(d: Union[dt.datetime, dt.date, None], tz_opt=None) -> Optional[str]:
    """
    Return ISO-like string with a SPACE between date and time so code using
    .split(" ")[0] (date) and [1] (time...) works.
    Example: '2025-09-22 07:15:00-0500'
    """
    dl = to_local(d, tz_opt)
    if not dl:
        return None
    # ISO string -> replace 'T' with ' ' so schedule_builder split works
    s = dl.isoformat()
    s = s.replace("T", " ")
    return s

# ---- humanization ----

def humanize_gap_hours(hours: float) -> str:
    """
    Render hours like '37h (1d 13h)'. Rounds to the nearest hour.
    """
    total = int(round(hours))
    days, rem = divmod(total, 24)
    parts = []
    if days:
        parts.append(f"{days}d")
    if rem or not parts:
        parts.append(f"{rem}h")
    return f"{total}h ({' '.join(parts)})"

# ---- deterministic IDs ----

def deterministic_ack_id(event_uid: str, start_utc: dt.datetime) -> str:
    """
    Stable, short ack id based on event UID + start time (UTC).
    """
    if start_utc.tzinfo is None:
        start_utc = start_utc.replace(tzinfo=pytz.utc)
    base = f"{event_uid}|{start_utc.astimezone(pytz.utc).isoformat()}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]
