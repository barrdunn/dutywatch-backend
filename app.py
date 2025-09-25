from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from db import (
    init_db,
    read_events_cache,
    overwrite_events_cache,
)
import cal_client as cal

# Optional LLM parser if you have llm_parser.py in the repo
try:
    from llm_parser import parse_pairing_days as parse_pairing_days_llm
except Exception:
    parse_pairing_days_llm = None


app = FastAPI(title="DutyWatch Backend")
templates = Jinja2Templates(directory="templates")


# -------------------------- Time helpers --------------------------

def month_bounds(year: int, month: int) -> Tuple[dt.datetime, dt.datetime]:
    start = dt.datetime(year, month, 1, tzinfo=dt.timezone.utc)
    if month == 12:
        end = dt.datetime(year + 1, 1, 1, tzinfo=dt.timezone.utc)
    else:
        end = dt.datetime(year, month + 1, 1, tzinfo=dt.timezone.utc)
    return start, end


def iso_to_dt(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def pick_default_month(events: List[Dict[str, Any]]) -> Tuple[int, int]:
    today = dt.datetime.utcnow().date()

    def start_date(e) -> Optional[dt.date]:
        try:
            return iso_to_dt(e.get("start_utc")).date()  # type: ignore[union-attr]
        except Exception:
            return None

    events_sorted = sorted(
        (e for e in events if start_date(e)),
        key=lambda e: start_date(e)  # type: ignore[arg-type]
    )
    for e in events_sorted:
        d = start_date(e)
        if d and d >= today:
            return d.year, d.month

    now = dt.datetime.utcnow()
    return now.year, now.month


def filter_events_to_month(events: List[Dict[str, Any]], year: int, month: int) -> List[Dict[str, Any]]:
    start, end = month_bounds(year, month)
    out: List[Dict[str, Any]] = []
    for e in events:
        s = iso_to_dt(e.get("start_utc"))
        if s and (start <= s < end):
            out.append(e)
    return out


def fmt_last_pull(when: Optional[str]) -> str:
    if not when:
        return "never"
    try:
        dtv = dt.datetime.fromisoformat(when)
        return dtv.strftime("%a %b %d, %Y %H:%M UTC")
    except Exception:
        return when


# -------------------------- Parsing helpers --------------------------

REPORT_RE = re.compile(r"\bReport:\s*(\d{3,4})L\b", re.IGNORECASE)
LEG_RE = re.compile(r"\b(\d{3,4})\s+([A-Z]{3})-([A-Z]{3})\s+(\d{3,4})-(\d{3,4})\b")
HOTEL_RE = re.compile(r"(Westin|Element|Embassy|Marriott|Hilton|Hyatt|Holiday|Sheraton|Aloft|Courtyard)[^\n]*", re.IGNORECASE)

def ensure_hhmm(s: str) -> str:
    return s if len(s) == 4 else s.zfill(4)

def minutes_from_hhmm(hhmm: str) -> int:
    hhmm = ensure_hhmm(hhmm)
    return int(hhmm[:2]) * 60 + int(hhmm[2:])

def hhmm_from_minutes(total: int) -> str:
    total = total % (24 * 60)
    h = total // 60
    m = total % 60
    return f"{h:02d}{m:02d}"

def to_12h(hhmm: str) -> str:
    hhmm = ensure_hhmm(hhmm)
    h = int(hhmm[:2])
    m = hhmm[2:]
    ampm = "AM" if h < 12 else "PM"
    h12 = h % 12
    if h12 == 0:
        h12 = 12
    return f"{h12}:{m} {ampm}"

def time_display(hhmm: str, is_24h: bool) -> str:
    """
    12-hour: '7:00 AM'  (no 'L')
    24-hour: '07:00L'
    """
    if is_24h:
        return ensure_hhmm(hhmm) + "L"
    return to_12h(hhmm)


def parse_with_regex(text: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"days": []}

    m = REPORT_RE.search(text or "")
    report = ensure_hhmm(m.group(1)) if m else None

    legs = []
    for m in LEG_RE.finditer(text or ""):
        num, dep, arr, t_dep, t_arr = m.groups()
        legs.append({
            "flight": f"FFT{num}",
            "dep": dep,
            "arr": arr,
            "dep_time": ensure_hhmm(t_dep),
            "arr_time": ensure_hhmm(t_arr),
        })

    hotel = None
    hm = HOTEL_RE.search(text or "")
    if hm:
        hotel = hm.group(0).strip()

    if report or legs or hotel:
        release = None
        if legs:
            last_arr = legs[-1]["arr_time"]
            release = hhmm_from_minutes(minutes_from_hhmm(last_arr) + 15)
        out["days"].append({
            "report": report,
            "legs": legs,
            "release": release,
            "hotel": hotel,
        })
    return out


def parse_pairing_days(description: Optional[str]) -> Dict[str, Any]:
    text = description or ""
    if parse_pairing_days_llm:
        try:
            return parse_pairing_days_llm(text)
        except Exception:
            pass
    return parse_with_regex(text)


# -------------------------- Pairing rows --------------------------

def grouping_key(e: Dict[str, Any]) -> str:
    pid = (e.get("summary") or "").strip()
    if pid:
        return pid
    uid = (e.get("uid") or "")[:8]
    return f"PAIR-{uid}"

def build_pairing_rows(
    events: List[Dict[str, Any]],
    is_24h: bool,
    only_reports: bool,
) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for e in events:
        groups.setdefault(grouping_key(e), []).append(e)

    rows: List[Dict[str, Any]] = []
    for pairing_id, evs in groups.items():
        evs_sorted = sorted(evs, key=lambda x: iso_to_dt(x.get("start_utc")) or dt.datetime.min)

        parsed_days: List[Dict[str, Any]] = []
        for e in evs_sorted:
            parsed = parse_pairing_days(e.get("description"))
            days = parsed.get("days") or []
            if only_reports:
                days = [d for d in days if d.get("report")]
            parsed_days.extend(days)

        # Prepare time strings on legs
        for d in parsed_days:
            for leg in d.get("legs", []):
                # normalize FFT prefix
                if not str(leg.get("flight", "")).startswith("FFT"):
                    nums = re.findall(r"\d{3,4}", str(leg.get("flight", ""))) or []
                    if nums:
                        leg["flight"] = f"FFT{nums[0]}"
                # display strings
                leg["dep_time_str"] = time_display(ensure_hhmm(leg.get("dep_time", "")), is_24h) if leg.get("dep_time") else ""
                leg["arr_time_str"] = time_display(ensure_hhmm(leg.get("arr_time", "")), is_24h) if leg.get("arr_time") else ""

        # Pairing start/end strings
        first_event_start = iso_to_dt(evs_sorted[0].get("start_utc"))
        last_event_end = iso_to_dt(evs_sorted[-1].get("end_utc"))

        def date_word(d: Optional[dt.datetime]) -> str:
            return d.strftime("%a %b %d") if d else ""

        report_hhmm = parsed_days[0].get("report") if parsed_days else None
        release_hhmm = parsed_days[-1].get("release") if parsed_days else None

        pairing_start_str = date_word(first_event_start)
        if report_hhmm:
            pairing_start_str = f"{pairing_start_str} {time_display(report_hhmm, is_24h)}".strip()

        pairing_end_str = date_word(last_event_end)
        if release_hhmm:
            pairing_end_str = f"{pairing_end_str} {time_display(release_hhmm, is_24h)}".strip()

        rows.append({
            "pairing_id": pairing_id,
            "pairing_start": first_event_start.isoformat() if first_event_start else None,
            "pairing_end": last_event_end.isoformat() if last_event_end else None,
            "display": {
                "pairing_start_str": pairing_start_str,
                "pairing_end_str": pairing_end_str,
            },
            "days": parsed_days,  # shown in expandable section
        })

    rows.sort(key=lambda r: r["pairing_start"] or "")
    return rows


# -------------------------- Rolling window fetch --------------------------

ROLLING_SCOPE = "rolling"

def fetch_current_to_next_eom() -> List[Dict[str, Any]]:
    now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    # first day of month after next
    y, m = now.year, now.month
    if m == 12:
        end = dt.datetime(y + 1, 2, 1, tzinfo=dt.timezone.utc)
    else:
        end = dt.datetime(y, m + 2, 1, tzinfo=dt.timezone.utc)

    if hasattr(cal, "fetch_events_between"):
        return cal.fetch_events_between(now.isoformat(), end.isoformat())
    else:
        hours = int((end - now).total_seconds() // 3600) + 1
        return cal.fetch_upcoming_events(hours_ahead=hours)


# -------------------------- Routes --------------------------

@app.on_event("startup")
async def on_startup():
    init_db()


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/calendar/refresh")
def calendar_refresh(request: Request):
    try:
        events = fetch_current_to_next_eom()
        overwrite_events_cache(ROLLING_SCOPE, events)
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {e}"})

    # redirect back to pairings with same query params
    q = request.query_params
    target = "/calendar/pairings"
    if q:
        target += f"?{q}"
    return RedirectResponse(target, status_code=303)


@app.get("/calendar/pairings")
def pairings_page(
    request: Request,
    year: Optional[int] = Query(default=None, ge=1970, le=2100),
    month: Optional[int] = Query(default=None, ge=1, le=12),
    only_reports: int = Query(default=1),
    is_24h: int = Query(default=0),  # default 12-hour
):
    cached = read_events_cache(ROLLING_SCOPE)
    events: List[Dict[str, Any]] = cached or []

    if not events:
        try:
            events = fetch_current_to_next_eom()
            overwrite_events_cache(ROLLING_SCOPE, events)
        except Exception as e:
            return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {e}"})

    y, m = (year, month) if (year and month) else pick_default_month(events)
    month_events = filter_events_to_month(events, y, m)

    rows = build_pairing_rows(
        month_events,
        is_24h=bool(is_24h),
        only_reports=bool(only_reports),
    )

    return templates.TemplateResponse(
        "pairings.html",
        {
            "request": request,
            "rows": rows,
            "year": y,
            "month": m,
            "only_reports": int(bool(only_reports)),
            "is_24h": int(bool(is_24h)),
            "last_pull_str": "never",  # wire a real timestamp later if you like
            "pairing_count": len(rows),
            "total_days": sum(len(r["days"]) for r in rows),
        },
    )
