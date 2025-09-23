"""
DutyWatch iCloud CalDAV client
- diagnose(): basic connection test + calendar names
- fetch_upcoming_events(hours_ahead): next N hours
- fetch_events_between(start_iso, end_iso): arbitrary window
- fetch_month(year, month): whole calendar month convenience

Each event includes:
uid, calendar, summary, location, description, start_utc, end_utc, last_modified
"""

from __future__ import annotations

import datetime as dt
from typing import List, Dict, Any, Optional

import pytz
from caldav import DAVClient
from icalendar import Calendar

from config import (
    ICLOUD_USER,
    ICLOUD_APP_PW,
    CALDAV_URL,
    LOOKAHEAD_HOURS,
    CALENDAR_NAME_FILTER,
)

UTC = pytz.utc


# ---------------------------
# Internal helpers
# ---------------------------

def _require_env():
    if not (ICLOUD_USER and ICLOUD_APP_PW and CALDAV_URL):
        raise RuntimeError("ICLOUD_USER / ICLOUD_APP_PW / CALDAV_URL are not set in .env")


def _client_principal():
    _require_env()
    cli = DAVClient(url=CALDAV_URL, username=ICLOUD_USER, password=ICLOUD_APP_PW)
    return cli.principal()


def _calendar_display_name(calendar) -> str:
    # Try WebDAV displayname prop
    try:
        props = calendar.get_properties([('DAV:', 'displayname')]) or {}
        name = str(props.get('{DAV:}displayname', '')).strip()
        if name:
            return name
    except Exception:
        pass
    # Fallbacks
    try:
        if getattr(calendar, "name", None):
            return str(calendar.name)
    except Exception:
        pass
    try:
        href = str(getattr(calendar, "url", "") or "")
        leaf = href.strip("/").split("/")[-1]
        return leaf or "<unnamed>"
    except Exception:
        return "<unnamed>"


def _to_dt(x) -> Optional[dt.datetime]:
    if x is None:
        return None
    if isinstance(x, dt.datetime):
        return x
    if isinstance(x, dt.date):
        return dt.datetime.combine(x, dt.time.min)
    # icalendar property
    try:
        val = getattr(x, "dt", None)
        if isinstance(val, dt.datetime):
            return val
        if isinstance(val, dt.date):
            return dt.datetime.combine(val, dt.time.min)
    except Exception:
        pass
    return None


def _to_utc_iso(d: Optional[dt.datetime]) -> Optional[str]:
    if not d:
        return None
    if d.tzinfo is None:
        # iCloud VEVENTs are usually tz-aware; if not, assume UTC
        d = d.replace(tzinfo=UTC)
    return d.astimezone(UTC).isoformat()


def _extract_events(cal_component: Calendar, calname: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for comp in cal_component.walk("VEVENT"):
        try:
            uid = str(comp.get("uid") or "").strip()
            summary = str(comp.get("summary") or "").strip()
            location = (str(comp.get("location") or "").strip()) or None
            description = (str(comp.get("description") or "").strip()) or None

            ds = comp.get("dtstart")
            de = comp.get("dtend")
            start = _to_dt(getattr(ds, "dt", ds)) if ds else None
            end = _to_dt(getattr(de, "dt", de)) if de else None

            last_mod = _to_dt(getattr(comp.get("last-modified"), "dt", None))
            out.append(
                {
                    "uid": uid,
                    "calendar": calname,
                    "summary": summary,
                    "location": location,
                    "description": description,
                    "start_utc": _to_utc_iso(start),
                    "end_utc": _to_utc_iso(end),
                    "last_modified": _to_utc_iso(last_mod),
                }
            )
        except Exception:
            # Skip malformed VEVENTs but keep going
            continue
    return out


def _iter_selected_calendars():
    principal = _client_principal()
    wanted = (CALENDAR_NAME_FILTER or "").strip().lower()
    for cal in principal.calendars():
        name = _calendar_display_name(cal)
        if wanted and wanted not in name.lower():
            continue
        yield cal, name


# ---------------------------
# Public API
# ---------------------------

def diagnose() -> dict:
    try:
        principal = _client_principal()
        names = []
        for cal in principal.calendars():
            names.append(_calendar_display_name(cal))
        return {
            "ok": True,
            "user": ICLOUD_USER,
            "url": CALDAV_URL,
            "filter": (CALENDAR_NAME_FILTER or "").lower(),
            "calendars": names,
        }
    except Exception as e:
        return {
            "ok": False,
            "error": f"{type(e).__name__}: {e}",
            "user_set": bool(ICLOUD_USER),
            "pw_set": bool(ICLOUD_APP_PW),
            "url": CALDAV_URL,
        }


def fetch_upcoming_events(hours_ahead: int | None = None) -> List[Dict[str, Any]]:
    hours = int(hours_ahead if hours_ahead is not None else LOOKAHEAD_HOURS)
    now = dt.datetime.utcnow().replace(tzinfo=UTC)
    end = now + dt.timedelta(hours=hours)
    return fetch_events_between(now.isoformat(), end.isoformat())


def fetch_events_between(start_iso: str, end_iso: str) -> List[Dict[str, Any]]:
    """
    Fetch events between two ISO datetimes (interpreted as UTC).
    """
    start = dt.datetime.fromisoformat(start_iso)
    end = dt.datetime.fromisoformat(end_iso)
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)

    results: List[Dict[str, Any]] = []

    for cal, calname in _iter_selected_calendars():
        try:
            # iCloud CalDAV expects tz-aware datetimes
            events = cal.date_search(start=start, end=end)
        except Exception:
            continue

        for ev in events:
            try:
                calobj = Calendar.from_ical(ev.data)
            except Exception:
                continue
            results.extend(_extract_events(calobj, calname))

    # de-dup on (uid, start_utc) and sort
    dedup = {}
    for e in results:
        key = (e.get("uid"), e.get("start_utc"))
        dedup[key] = e
    results = list(dedup.values())
    results.sort(key=lambda x: (x.get("start_utc") or "9999-12-31T23:59:59Z"))
    return results


def fetch_month(year: int, month: int) -> List[Dict[str, Any]]:
    """
    Convenience: fetch events for the entire [year-month).
    """
    start = dt.datetime(year, month, 1, tzinfo=UTC)
    if month == 12:
        end = dt.datetime(year + 1, 1, 1, tzinfo=UTC)
    else:
        end = dt.datetime(year, month + 1, 1, tzinfo=UTC)
    return fetch_events_between(start.isoformat(), end.isoformat())
