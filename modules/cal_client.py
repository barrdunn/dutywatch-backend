# cal_client_multiuser.py
"""
Multi-user iCloud CalDAV Client for DutyWatch
This replaces/extends the existing cal_client.py to support per-user credentials
"""

import datetime as dt
import logging
from typing import Any, Dict, List, Set, Optional
import pytz
from caldav import DAVClient
from icalendar import Calendar

log = logging.getLogger("cal_client_multiuser")


def _to_utc(val) -> Optional[dt.datetime]:
    """Convert a datetime to UTC."""
    if val is None:
        return None
    if isinstance(val, dt.date) and not isinstance(val, dt.datetime):
        val = dt.datetime.combine(val, dt.time.min)
    if val.tzinfo is None:
        val = val.replace(tzinfo=pytz.utc)
    return val.astimezone(pytz.utc)


def _principal_for_user(icloud_user: str, icloud_app_pw: str, caldav_url: str = "https://caldav.icloud.com/"):
    """Get CalDAV principal for a specific user's credentials."""
    if not icloud_user or not icloud_app_pw:
        raise RuntimeError("iCloud credentials not configured for this user")
    
    client = DAVClient(url=caldav_url, username=icloud_user, password=icloud_app_pw)
    return client.principal()


def _calendar_display_name(calendar) -> str:
    """Extract display name from a calendar object."""
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


def list_calendars_for_user(icloud_user: str, icloud_app_pw: str, caldav_url: str = "https://caldav.icloud.com/") -> List[Dict[str, str]]:
    """
    List all calendars available for a user.
    Returns list of {"name": "Calendar Name", "url": "calendar_url"}
    
    This is called during user setup to let them pick which calendar to use.
    """
    try:
        principal = _principal_for_user(icloud_user, icloud_app_pw, caldav_url)
        calendars = principal.calendars()
        
        result = []
        for cal in calendars:
            name = _calendar_display_name(cal)
            url = str(getattr(cal, "url", "") or "")
            result.append({
                "name": name,
                "url": url
            })
        
        return result
    except Exception as e:
        log.error(f"Failed to list calendars: {e}")
        raise


def diagnose_user_connection(icloud_user: str, icloud_app_pw: str, caldav_url: str = "https://caldav.icloud.com/") -> Dict[str, Any]:
    """
    Test a user's iCloud connection and return diagnostic info.
    """
    try:
        principal = _principal_for_user(icloud_user, icloud_app_pw, caldav_url)
        cals = principal.calendars()
        names = [_calendar_display_name(cal) for cal in cals]
        
        return {
            "ok": True,
            "user": icloud_user,
            "url": caldav_url,
            "calendars": names,
            "calendar_count": len(names)
        }
    except Exception as e:
        return {
            "ok": False,
            "error": f"{type(e).__name__}: {e}",
            "user": icloud_user,
            "url": caldav_url
        }


def _event_records_from_ical(calname: str, ics: bytes) -> List[Dict[str, Any]]:
    """Parse iCal data into event records."""
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

        sdt_utc = _to_utc(sdt).isoformat() if sdt else None
        edt_utc = _to_utc(edt).isoformat() if edt else None

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


def _search_between_for_user(
    icloud_user: str, 
    icloud_app_pw: str, 
    caldav_url: str,
    calendar_filter: Optional[str],
    start: dt.datetime, 
    end: dt.datetime
) -> List[tuple]:
    """
    Search for events between dates for a specific user.
    Returns list of (calendar_name, ics_bytes) tuples.
    """
    principal = _principal_for_user(icloud_user, icloud_app_pw, caldav_url)
    events_raw: List[tuple] = []
    
    for cal in principal.calendars():
        name = _calendar_display_name(cal)
        
        # Filter by calendar name if specified
        if calendar_filter and calendar_filter.lower() not in (name or "").lower():
            continue
        
        try:
            items = cal.date_search(start=start, end=end)
            log.info(f"Calendar '{name}': found {len(items)} items")
        except Exception as e:
            log.warning(f"Calendar '{name}': search failed - {e}")
            continue
        
        for ev in items:
            try:
                events_raw.append((name, ev.data))
            except Exception as e:
                log.warning(f"Calendar '{name}': failed to get event data - {e}")
    
    return events_raw


def fetch_events_for_user(
    icloud_user: str,
    icloud_app_pw: str,
    caldav_url: str,
    calendar_filter: Optional[str],
    start_iso: str,
    end_iso: str
) -> List[Dict[str, Any]]:
    """
    Fetch events for a specific user between two dates.
    
    Args:
        icloud_user: User's iCloud email
        icloud_app_pw: App-specific password (decrypted)
        caldav_url: CalDAV URL (usually https://caldav.icloud.com/)
        calendar_filter: Calendar name to filter on (or None for all)
        start_iso: Start date ISO string
        end_iso: End date ISO string
    
    Returns:
        List of event dictionaries
    """
    start = dt.datetime.fromisoformat(start_iso)
    end = dt.datetime.fromisoformat(end_iso)
    
    if start.tzinfo is None:
        start = start.replace(tzinfo=pytz.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=pytz.utc)

    events: List[Dict[str, Any]] = []
    
    for calname, ics in _search_between_for_user(
        icloud_user, icloud_app_pw, caldav_url, calendar_filter, start, end
    ):
        try:
            events.extend(_event_records_from_ical(calname, ics))
        except Exception as e:
            log.warning(f"Failed to parse ical data: {e}")
    
    log.info(f"Total events parsed for user: {len(events)}")
    events.sort(key=lambda e: (e.get("start_utc") or "9999"))
    return events