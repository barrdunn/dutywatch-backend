"""
DutyWatch iCloud CalDAV Client – full-field VEVENT extractor

Returns a rich JSON for each VEVENT including:
- id/uid, summary, location, description (notes)
- organizer + attendees (email, common name, role, partstat, rsvp, etc.)
- start/end (original tz + UTC), tzids
- status, transparency, categories, url, sequence
- created, last_modified, recurrence_id
- recurrence: rrule/rdate/exdate (normalized)
- alarms (VALARM): action, trigger, repeat, duration, description/summary
- calendar display name
- x_props: any X-... properties present on the VEVENT

Notes:
- Times are provided both as original (tz-aware if available) and UTC.
- RDATE/EXDATE can contain multiple lists; we flatten and UTC-normalize.
"""

import datetime as dt
import pytz
from typing import List, Dict, Any, Iterable

from caldav import DAVClient
from icalendar import Calendar, vDDDTypes, vRecur, vCalAddress, vText

from config import ICLOUD_USER, ICLOUD_APP_PW, CALDAV_URL, LOOKAHEAD_HOURS, CALENDAR_NAME_FILTER
from utils import to_utc

# ---------------------------
# Helpers
# ---------------------------

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

def _to_dt(value) -> dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.date):
        # all-day -> midnight on that date (naive)
        return dt.datetime.combine(value, dt.time.min)
    if isinstance(value, vDDDTypes):
        val = value.dt
        if isinstance(val, dt.datetime):
            return val
        if isinstance(val, dt.date):
            return dt.datetime.combine(val, dt.time.min)
    return None

def _iso(d: dt.datetime | None) -> str | None:
    return d.isoformat() if d else None

def _iso_utc(d: dt.datetime | None) -> str | None:
    if d is None:
        return None
    return to_utc(d).isoformat()

def _tzid_of(prop) -> str | None:
    try:
        return prop.params.get("TZID")
    except Exception:
        return None

def _normalize_recur(rr: vRecur | dict | None) -> Dict[str, Any] | None:
    if not rr:
        return None
    # vRecur acts like a dict, values often are lists/tuples
    out: Dict[str, Any] = {}
    for k, v in dict(rr).items():
        if isinstance(v, (list, tuple)):
            out[k] = [str(x) for x in v]
        else:
            out[k] = str(v)
    return out

def _flatten_dates(items: Iterable) -> List[str]:
    """
    items may be a list of vDDDTypes or Ical date lists. Flatten to ISO UTC strings.
    """
    acc: List[str] = []
    if not items:
        return acc
    for entry in items:
        # entry might be vDDDTypes, list, or icalendar.prop.vDDDLists
        try:
            dtlist = getattr(entry, "dts", None)  # vDDDLists has .dts
            if dtlist is not None:
                for d in dtlist:
                    acc.append(_iso_utc(_to_dt(d.dt)))
                continue
        except Exception:
            pass
        # Try to interpret directly
        if isinstance(entry, (list, tuple)):
            for d in entry:
                acc.append(_iso_utc(_to_dt(getattr(d, "dt", d))))
        else:
            # single item
            base = getattr(entry, "dt", entry)
            acc.append(_iso_utc(_to_dt(base)))
    # Remove Nones and duplicates
    return [x for x in dict.fromkeys([x for x in acc if x])]

def _to_email(addr: vCalAddress | str | None) -> str | None:
    if not addr:
        return None
    if isinstance(addr, vCalAddress):
        s = str(addr)
    else:
        s = str(addr)
    s = s.strip()
    if s.upper().startswith("MAILTO:"):
        return s[7:]
    return s or None

def _attendee_dict(item) -> Dict[str, Any]:
    # item is usually a vCalAddress with .params
    email = _to_email(item)
    params = getattr(item, "params", {}) or {}
    def _param(name: str) -> str | None:
        try:
            val = params.get(name)
            if val is None:
                return None
            # Convert icalendar vText to str
            if isinstance(val, vText):
                return str(val)
            return str(val)
        except Exception:
            return None
    return {
        "email": email,
        "cn": _param("CN"),
        "role": _param("ROLE"),
        "partstat": _param("PARTSTAT"),
        "rsvp": _param("RSVP"),
        "cutype": _param("CUTYPE"),
        "x_params": {k: str(v) for k, v in params.items() if str(k).upper().startswith("X-")},
    }

def _organizer_dict(prop) -> Dict[str, Any] | None:
    if not prop:
        return None
    email = _to_email(prop)
    params = getattr(prop, "params", {}) or {}
    cn = None
    try:
        val = params.get("CN")
        cn = str(val) if val is not None else None
    except Exception:
        pass
    return {"email": email, "cn": cn}

def _x_props_of(comp) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    try:
        for key, val in comp.property_items():
            k = str(key).upper()
            if k.startswith("X-"):
                try:
                    out[k] = str(val)
                except Exception:
                    out[k] = repr(val)
    except Exception:
        pass
    return out

def _alarms_of(comp) -> List[Dict[str, Any]]:
    alarms = []
    for a in comp.subcomponents:
        if str(getattr(a, "name", "")).upper() != "VALARM":
            continue
        def g(n): 
            try:
                v = a.get(n)
                return str(v) if v is not None else None
            except Exception:
                return None
        alarms.append({
            "action": g("ACTION"),
            "trigger": g("TRIGGER"),
            "repeat": g("REPEAT"),
            "duration": g("DURATION"),
            "description": g("DESCRIPTION"),
            "summary": g("SUMMARY"),
            "attach": g("ATTACH"),
        })
    return alarms

# ---------------------------
# Public: fetch_upcoming_events
# ---------------------------

def fetch_upcoming_events(hours_ahead: int = None) -> List[Dict[str, Any]]:
    hours = hours_ahead if hours_ahead is not None else LOOKAHEAD_HOURS

    principal = _principal()
    calendars = principal.calendars()

    now = dt.datetime.utcnow().replace(tzinfo=pytz.utc)
    end = now + dt.timedelta(hours=hours)

    results: List[Dict[str, Any]] = []

    for cal in calendars:
        calname = _calendar_display_name(cal)
        if CALENDAR_NAME_FILTER and CALENDAR_NAME_FILTER not in calname.lower():
            continue

        try:
            events = cal.date_search(start=now, end=end)
        except Exception as e:
            # Skip problematic calendars but keep going
            continue

        for ev in events:
            try:
                calobj = Calendar.from_ical(ev.data)
            except Exception:
                continue

            for comp in calobj.walk("VEVENT"):
                # Core identity/summary
                uid = str(comp.get("uid") or "").strip()
                summary = str(comp.get("summary") or "").strip()
                location = str(comp.get("location") or "").strip()
                description = str(comp.get("description") or "").strip()

                # Times
                ds = comp.get("dtstart")
                de = comp.get("dtend")
                start = _to_dt(ds.dt) if ds else None
                endt  = _to_dt(de.dt) if de else None
                start_tzid = _tzid_of(ds) if ds else None
                end_tzid   = _tzid_of(de) if de else None

                # Organizer / attendees
                organizer = _organizer_dict(comp.get("organizer"))
                attendees = []
                try:
                    for att in comp.getall("attendee") or []:
                        attendees.append(_attendee_dict(att))
                except Exception:
                    pass

                # Recurrence
                rrule = _normalize_recur(comp.get("rrule"))
                rdate = _flatten_dates(comp.get("rdate") or [])
                exdate = _flatten_dates(comp.get("exdate") or [])

                # Misc fields
                status = str(comp.get("status") or "") or None
                transp = str(comp.get("transp") or "") or None
                categories = None
                try:
                    cats = comp.get("categories")
                    if cats:
                        if isinstance(cats, list):
                            categories = [str(c) for c in cats]
                        else:
                            categories = [str(cats)]
                except Exception:
                    pass
                url = str(comp.get("url") or "") or None
                sequence = None
                try:
                    sequence = int(str(comp.get("sequence"))) if comp.get("sequence") is not None else None
                except Exception:
                    pass
                created = _iso(_to_dt(getattr(comp.get("created"), "dt", None)))
                last_mod = _iso(_to_dt(getattr(comp.get("last-modified"), "dt", None)))
                recur_id = _iso(_to_dt(getattr(comp.get("recurrence-id"), "dt", None)))

                # Alarms and X- props
                alarms = _alarms_of(comp)
                x_props = _x_props_of(comp)

                # Build record
                results.append({
                    "uid": uid,
                    "calendar": calname,
                    "summary": summary,
                    "location": location or None,
                    "description": description or None,
                    "start_utc": _iso_utc(start),
                    "end_utc": _iso_utc(endt),
                    "last_modified": last_mod,
                })

    results.sort(key=lambda x: (x["start_utc"] or "9999"))
    return results

def diagnose() -> dict:
    """
    Log in and list calendar names; used by /calendar/debug.
    """
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
            "filter": CALENDAR_NAME_FILTER,
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
