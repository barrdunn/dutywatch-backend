"""
DutyWatch iCloud CalDAV Client – month + rolling window helpers

Exports:
- diagnose()
- fetch_upcoming_events(hours_ahead: int|None)
- fetch_events_between(start_iso: str, end_iso: str)
- fetch_month(year: int, month: int)
- list_uids_between(start_iso: str, end_iso: str)
"""

import datetime as dt
from typing import Any, Dict, List, Set, Optional
import pytz
from caldav import DAVClient
from icalendar import Calendar
from config import (
    ICLOUD_USER, ICLOUD_APP_PW, CALDAV_URL,
    LOOKAHEAD_HOURS, CALENDAR_NAME_FILTER, TIMEZONE,
)
from utils import to_utc

TZ = pytz.timezone(TIMEZONE)


def _principal():
    if not (ICLOUD_USER and ICLOUD_APP_PW and CALDAV_URL):
        raise RuntimeError("ICLOUD_USER / ICLOUD_APP_PW / CALDAV_URL are not set in .env")
    client = DAVClient(url=CALDAV_URL, username=ICLOUD_USER, password=ICLOUD_APP_PW)
    return client.principal()


def _calendar_display_name(calendar) -> str:
    try:
        props = calendar.get_properties([('DAV:', 'displayname')]) or {}
        name = str(props.get('{DAV:}displayname', '')).strip()
        if name:
            return name
    except Exception:
        pass
    try:
        name = getattr(calendar, "name", None)
        if name:
            return str(name)
    except Exception:
        pass
    try:
        href = str(getattr(calendar, "url", "") or "")
        leaf = href.strip("/").split("/")[-1]
        return leaf or "<unnamed>"
    except Exception:
        return "<unnamed>"


def diagnose() -> dict:
    try:
        principal = _principal()
        cals = principal.calendars()
        names = []
        for cal in cals:
            names.append(_calendar_display_name(cal) or "<unnamed>")
        return {
            "ok": True,
            "user": ICLOUD_USER,
            "url": CALDAV_URL,
            "filter": (CALENDAR_NAME_FILTER or ""),
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


def _want_calendar(name: str) -> bool:
    if not CALENDAR_NAME_FILTER:
        return True
    return CALENDAR_NAME_FILTER.lower() in (name or "").lower()


def _event_records_from_ical(calname: str, ics: bytes) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    calobj = Calendar.from_ical(ics)
    for comp in calobj.walk("VEVENT"):
        uid = str(comp.get("uid") or "").strip()
        summary = str(comp.get("summary") or "").strip()
        location = str(comp.get("location") or "").strip() or None
        description = str(comp.get("description") or "").strip() or None

        ds = comp.get("dtstart")
        de = comp.get("dtend")
        sdt = ds.dt if ds else None
        edt = de.dt if de else None

        sdt_utc = to_utc(sdt).isoformat() if sdt else None
        edt_utc = to_utc(edt).isoformat() if edt else None

        last_mod = comp.get("last-modified")
        last_iso = last_mod.dt.isoformat() if last_mod is not None else None

        out.append({
            "uid": uid,
            "calendar": calname,
            "summary": summary,
            "location": location,
            "description": description,
            "start_utc": sdt_utc,
            "end_utc": edt_utc,
            "last_modified": last_iso,
        })
    return out


def _search_between(start: dt.datetime, end: dt.datetime) -> List[bytes]:
    principal = _principal()
    events_raw: List[bytes] = []
    for cal in principal.calendars():
        name = _calendar_display_name(cal)
        if not _want_calendar(name):
            continue
        try:
            items = cal.date_search(start=start, end=end)
        except Exception:
            continue
        for ev in items:
            try:
                events_raw.append(ev.data)
            except Exception:
                pass
    return events_raw


def fetch_events_between(start_iso: str, end_iso: str) -> List[Dict[str, Any]]:
    start = dt.datetime.fromisoformat(start_iso)
    end = dt.datetime.fromisoformat(end_iso)
    if start.tzinfo is None: start = start.replace(tzinfo=pytz.utc)
    if end.tzinfo is None: end = end.replace(tzinfo=pytz.utc)

    events: List[Dict[str, Any]] = []
    for ics in _search_between(start, end):
        try:
            calname = ""  # optional
            events.extend(_event_records_from_ical(calname, ics))
        except Exception:
            pass
    events.sort(key=lambda e: (e.get("start_utc") or "9999"))
    return events


def list_uids_between(start_iso: str, end_iso: str) -> Set[str]:
    start = dt.datetime.fromisoformat(start_iso)
    end = dt.datetime.fromisoformat(end_iso)
    if start.tzinfo is None: start = start.replace(tzinfo=pytz.utc)
    if end.tzinfo is None: end = end.replace(tzinfo=pytz.utc)

    uids: Set[str] = set()
    for ics in _search_between(start, end):
        try:
            calobj = Calendar.from_ical(ics)
            for comp in calobj.walk("VEVENT"):
                uid = str(comp.get("uid") or "").strip()
                if uid:
                    uids.add(uid)
        except Exception:
            pass
    return uids


def fetch_upcoming_events(hours_ahead: Optional[int] = None) -> List[Dict[str, Any]]:
    hrs = hours_ahead if hours_ahead is not None else LOOKAHEAD_HOURS
    now = dt.datetime.utcnow().replace(tzinfo=pytz.utc)
    end = now + dt.timedelta(hours=hrs)
    return fetch_events_between(now.isoformat(), end.isoformat())


def fetch_month(year: int, month: int) -> List[Dict[str, Any]]:
    start = dt.datetime(year, month, 1, tzinfo=pytz.utc)
    if month == 12:
        end = dt.datetime(year + 1, 1, 1, tzinfo=pytz.utc)
    else:
        end = dt.datetime(year, month + 1, 1, tzinfo=pytz.utc)
    return fetch_events_between(start.isoformat(), end.isoformat())
