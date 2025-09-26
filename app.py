from __future__ import annotations

import os
import asyncio
import datetime as dt
import json
import re
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.concurrency import run_in_threadpool
from zoneinfo import ZoneInfo

from db import init_db, read_events_cache, overwrite_events_cache
import cal_client as cal

# ---------------- Config / Time ----------------
LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "America/Chicago"))
ROLLING_SCOPE = "rolling"

app = FastAPI(title="DutyWatch Backend (Viewer-Only)")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


class State:
    refresh_seconds: int = int(os.getenv("REFRESH_SECONDS", "300"))  # default 5 min
    poll_task: Optional[asyncio.Task] = None
    version: int = 0
    sse_queue: "asyncio.Queue[dict]" = asyncio.Queue()
    shutdown_event: asyncio.Event = asyncio.Event()
    wake: asyncio.Event = asyncio.Event()


state = State()


def _now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)


# ---------------- Time helpers ----------------
def month_bounds(year: int, month: int) -> Tuple[dt.datetime, dt.datetime]:
    start = dt.datetime(year, month, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(year + (month // 12), (month % 12) + 1, 1, tzinfo=dt.timezone.utc)
    return start, end


def end_of_next_month_local() -> dt.datetime:
    now_local = dt.datetime.now(LOCAL_TZ)
    y, m = now_local.year, now_local.month
    first_next = dt.datetime(y + (1 if m == 12 else 0), (m % 12) + 1, 1, tzinfo=LOCAL_TZ)
    y2, m2 = (
        first_next.year + (1 if first_next.month == 12 else 0),
        (first_next.month % 12) + 1,
    )
    first_after_next = dt.datetime(y2, m2, 1, tzinfo=LOCAL_TZ)
    return (first_after_next - dt.timedelta(seconds=1)).replace(microsecond=0)


def next_refresh_at_local(freq_minutes: int, now: Optional[dt.datetime] = None) -> dt.datetime:
    if now is None:
        now = dt.datetime.now(LOCAL_TZ)
    base = now.replace(second=0, microsecond=0)
    m = base.minute
    k = (m // freq_minutes) * freq_minutes
    slot = base.replace(minute=k)
    if slot <= now:
        slot += dt.timedelta(minutes=freq_minutes)
    return slot


def iso_to_dt(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def to_local(d: Optional[dt.datetime]) -> Optional[dt.datetime]:
    if not d:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(LOCAL_TZ)


def pick_default_month(events: List[Dict[str, Any]]) -> Tuple[int, int]:
    today = dt.datetime.now(LOCAL_TZ).date()

    def start_local_date(e) -> Optional[dt.date]:
        s = to_local(iso_to_dt(e.get("start_utc")))
        return s.date() if s else None

    events_sorted = sorted(
        (e for e in events if start_local_date(e)),
        key=lambda e: start_local_date(e),  # type: ignore[arg-type]
    )
    for e in events_sorted:
        d = start_local_date(e)
        if d and d >= today:
            return d.year, d.month

    now = dt.datetime.now(LOCAL_TZ)
    return now.year, now.month


def filter_events_to_month(events: List[Dict[str, Any]], year: int, month: int) -> List[Dict[str, Any]]:
    start_utc, end_utc = month_bounds(year, month)
    out: List[Dict[str, Any]] = []
    for e in events:
        s = iso_to_dt(e.get("start_utc"))
        if s and (start_utc <= s < end_utc):
            out.append(e)
    return out


def human_dur(td: dt.timedelta) -> str:
    total_h = max(0, int(td.total_seconds() // 3600))
    if total_h >= 48:
        d = total_h // 24
        h = total_h % 24
        return f"{d}d {h}h"
    return f"{total_h}h"


def human_ago_precise(from_dt: Optional[dt.datetime]) -> str:
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


def human_ago(from_dt: Optional[dt.datetime]) -> str:
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


# ---------------- Cache helpers ----------------
def normalize_cached_events(raw) -> List[Dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
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
    if isinstance(raw, dict) and isinstance(raw.get("events"), list):
        return raw["events"]
    return []


def read_cache_meta() -> Dict[str, Any]:
    raw = read_events_cache(ROLLING_SCOPE)
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list):
        return {"events": raw}
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return {"events": data}
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {"events": []}


def write_cache_meta(meta: Dict[str, Any]) -> None:
    if "events" not in meta or not isinstance(meta["events"], list):
        meta["events"] = []
    overwrite_events_cache(ROLLING_SCOPE, meta)


# ---------------- Parsing helpers ----------------
REPORT_RE = re.compile(r"\bReport:\s*(\d{3,4})L?\b", re.IGNORECASE)
LEG_RE = re.compile(r"\b(\d{3,4})\s+([A-Z]{3})-([A-Z]{3})\s+(\d{3,4})-(\d{3,4})\b")
HOTEL_RE = re.compile(
    r"(Westin|Element|Embassy|Marriott|Hilton|Hyatt|Holiday|Sheraton|Aloft|Courtyard)[^\n]*",
    re.IGNORECASE,
)


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
    h12 = h % 12 or 12
    return f"{h12}:{m} {ampm}"


def time_display(hhmm: Optional[str], is_24h: bool) -> str:
    if not hhmm:
        return ""
    if is_24h:
        return ensure_hhmm(hhmm)
    return to_12h(hhmm)


def parse_with_regex(text: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"days": []}
    m = REPORT_RE.search(text or "")
    report = ensure_hhmm(m.group(1)) if m else None

    legs = []
    for m in LEG_RE.finditer(text or ""):
        num, dep, arr, t_dep, t_arr = m.groups()
        legs.append(
            {
                "flight": f"FFT{num}",
                "dep": dep,
                "arr": arr,
                "dep_time": ensure_hhmm(t_dep),
                "arr_time": ensure_hhmm(t_arr),
            }
        )

    hotel = None
    hm = HOTEL_RE.search(text or "")
    if hm:
        hotel = hm.group(0).strip()

    if report or legs or hotel:
        release = None
        if legs:
            last_arr = legs[-1]["arr_time"]
            release = hhmm_from_minutes(minutes_from_hhmm(last_arr) + 15)
        out["days"].append(
            {
                "report": report,
                "legs": legs,
                "release": release,
                "hotel": hotel,
            }
        )
    return out


def parse_pairing_days(description: Optional[str]) -> Dict[str, Any]:
    text = description or ""
    try:
        from llm_parser import parse_pairing_days as parse_pairing_days_llm  # optional
    except Exception:
        parse_pairing_days_llm = None
    if parse_pairing_days_llm:
        try:
            return parse_pairing_days_llm(text)
        except Exception:
            pass
    return parse_with_regex(text)


# ---------------- Pairing rows + OFF rows ----------------
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

    pairings: List[Dict[str, Any]] = []

    for pairing_id, evs in groups.items():
        evs_sorted = sorted(evs, key=lambda x: iso_to_dt(x.get("start_utc")) or dt.datetime.min)

        parsed_days: List[Dict[str, Any]] = []
        for e in evs_sorted:
            parsed = parse_pairing_days(e.get("description"))
            days = parsed.get("days") or []
            if only_reports:
                days = [d for d in days if d.get("report")]
            for d in days:
                for leg in d.get("legs", []):
                    if not str(leg.get("flight", "")).startswith("FFT"):
                        nums = re.findall(r"\d{3,4}", str(leg.get("flight", ""))) or []
                        if nums:
                            leg["flight"] = f"FFT{nums[0]}"
                    leg["dep_time_str"] = time_display(leg.get("dep_time"), is_24h)
                    leg["arr_time_str"] = time_display(leg.get("arr_time"), is_24h)
            parsed_days.extend(days)

        first_evt_local = to_local(iso_to_dt(evs_sorted[0].get("start_utc"))) if evs_sorted else None
        first_report_hhmm = parsed_days[0].get("report") if parsed_days else None
        last_release_hhmm = parsed_days[-1].get("release") if parsed_days else None

        def combine_local(date_obj: Optional[dt.date], hhmm: Optional[str]) -> Optional[dt.datetime]:
            if not date_obj or not hhmm:
                return None
            return dt.datetime(date_obj.year, date_obj.month, date_obj.day, int(hhmm[:2]), int(hhmm[2:]), tzinfo=LOCAL_TZ)

        start_anchor_date = first_evt_local.date() if first_evt_local else None
        end_anchor_date = (start_anchor_date + dt.timedelta(days=max(len(parsed_days) - 1, 0))) if start_anchor_date else None

        pairing_report_local = combine_local(start_anchor_date, first_report_hhmm) or first_evt_local
        pairing_release_local = combine_local(end_anchor_date, last_release_hhmm) or (to_local(iso_to_dt(evs_sorted[-1].get("end_utc"))) if evs_sorted else None)

        if pairing_report_local and pairing_release_local and pairing_release_local < pairing_report_local:
            pairing_release_local += dt.timedelta(days=1)

        now_local = dt.datetime.now(LOCAL_TZ)
        in_progress = bool(pairing_report_local and pairing_release_local and pairing_report_local <= now_local <= pairing_release_local)

        days_with_flags: List[Dict[str, Any]] = []
        for idx, d in enumerate(parsed_days, start=1):
            anchor_date = start_anchor_date + dt.timedelta(days=idx - 1) if start_anchor_date else None
            legs = d.get("legs", [])
            for leg in legs:
                dep_dt = arr_dt = None
                if anchor_date:
                    if leg.get("dep_time"):
                        dep_dt = dt.datetime(anchor_date.year, anchor_date.month, anchor_date.day, int(leg["dep_time"][:2]), int(leg["dep_time"][2:]), tzinfo=LOCAL_TZ)
                    if leg.get("arr_time"):
                        arr_dt = dt.datetime(anchor_date.year, anchor_date.month, anchor_date.day, int(leg["arr_time"][:2]), int(leg["arr_time"][2:]), tzinfo=LOCAL_TZ)
                        if dep_dt and arr_dt and arr_dt < dep_dt:
                            arr_dt += dt.timedelta(days=1)
                leg["done"] = bool(arr_dt and now_local >= arr_dt)
            days_with_flags.append({**d, "day_index": idx})

        def dword(d: Optional[dt.datetime]) -> str:
            return d.strftime("%a %b %d") if d else ""

        report_disp = f"{dword(pairing_report_local)} {time_display(pairing_report_local.strftime('%H%M'), is_24h)}".strip() if pairing_report_local else ""
        release_disp = f"{dword(pairing_release_local)} {time_display(pairing_release_local.strftime('%H%M'), is_24h)}".strip() if pairing_release_local else ""

        pairings.append(
            {
                "kind": "pairing",
                "pairing_id": pairing_id,
                "in_progress": int(in_progress),
                "report_local_iso": pairing_report_local.isoformat() if pairing_report_local else None,
                "release_local_iso": pairing_release_local.isoformat() if pairing_release_local else None,
                "display": {"report_str": report_disp, "release_str": release_disp},
                "days": days_with_flags,
            }
        )

    pairings.sort(key=lambda r: r.get("report_local_iso") or "")

    rows: List[Dict[str, Any]] = []
    for i, p in enumerate(pairings):
        rows.append(p)
        if i + 1 < len(pairings):
            release = iso_to_dt(p.get("release_local_iso"))
            nxt_report = iso_to_dt(pairings[i + 1].get("report_local_iso"))
            gap = (nxt_report - release) if (release and nxt_report) else dt.timedelta(0)
            rows.append({"kind": "off", "display": {"off_dur": human_dur(gap if gap.total_seconds() >= 0 else dt.timedelta(0))}})
    return rows


# ---------------- Calendar fetch ----------------
def fetch_current_to_next_eom() -> List[Dict[str, Any]]:
    now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    y, m = now.year, now.month
    end = dt.datetime(y + (1 if m >= 11 else 0), ((m + 1) % 12) + 1, 1, tzinfo=dt.timezone.utc)
    if hasattr(cal, "fetch_events_between"):
        return cal.fetch_events_between(now.isoformat(), end.isoformat())
    hours = int((end - now).total_seconds() // 3600) + 1
    return cal.fetch_upcoming_events(hours_ahead=hours)


# ---------------- SSE + Poller ----------------
async def _emit(event_type: str, payload: dict | None = None):
    try:
        await state.sse_queue.put({"type": event_type, "payload": payload or {}, "version": state.version})
    except Exception:
        pass


def _next_from_meta(meta: Dict[str, Any]) -> dt.datetime:
    mins = int(meta.get("refresh_minutes", max(1, state.refresh_seconds // 60)))
    nxt = iso_to_dt(meta.get("next_run_utc"))
    if not nxt or nxt.tzinfo is None:
        nxt = _now_utc()
    if nxt <= _now_utc():
        nxt = (_now_utc() + dt.timedelta(minutes=mins)).replace(microsecond=0)
    return nxt


async def pull_and_update_once() -> bool:
    try:
        events = await run_in_threadpool(fetch_current_to_next_eom)
        meta = await run_in_threadpool(read_cache_meta)

        changed = json.dumps(meta.get("events", []), sort_keys=True) != json.dumps(events, sort_keys=True)

        meta["events"] = events
        meta["last_pull_utc"] = _now_utc().isoformat()
        meta.setdefault("refresh_minutes", max(1, state.refresh_seconds // 60))
        mins = int(meta.get("refresh_minutes", 5))
        meta["next_run_utc"] = (_now_utc() + dt.timedelta(minutes=mins)).replace(microsecond=0).isoformat()
        await run_in_threadpool(write_cache_meta, meta)

        if changed:
            state.version += 1

        # Always emit so UI leaves "(refreshing…)"
        await _emit("change", {"changed": changed})
        return changed
    except Exception as e:
        print(f"[pull] error: {e}")
        return False


async def poller_loop():
    while not state.shutdown_event.is_set():
        meta = await run_in_threadpool(read_cache_meta)
        nxt = _next_from_meta(meta)
        delay = max(1.0, (nxt - _now_utc()).total_seconds())
        try:
            await asyncio.wait_for(state.wake.wait(), timeout=delay)
            state.wake.clear()
            # loop to recompute next
            continue
        except asyncio.TimeoutError:
            pass
        await pull_and_update_once()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    state.poll_task = asyncio.create_task(poller_loop())
    try:
        yield
    finally:
        state.shutdown_event.set()
        if state.poll_task:
            state.poll_task.cancel()
            with suppress(asyncio.CancelledError):
                await state.poll_task


app.router.lifespan_context = lifespan


# ---------------- Routes ----------------
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
@app.get("/calendar/pairings", response_class=HTMLResponse)
async def pairings_page(request: Request):
    meta = await run_in_threadpool(read_cache_meta)
    saved_minutes = int(meta.get("refresh_minutes", max(1, state.refresh_seconds // 60)))
    dw_boot = {
        "clockMode": "24",
        "onlyReports": True,
        "tz": "CT",
        "refreshMinutes": saved_minutes,
        "apiBase": "",
    }
    return templates.TemplateResponse(
        "pairings.html",
        {"request": request, "dw_boot_json": json.dumps(dw_boot, ensure_ascii=False)},
    )


@app.get("/api/pairings")
async def api_pairings(
    year: Optional[int] = Query(default=None, ge=1970, le=2100),
    month: Optional[int] = Query(default=None, ge=1, le=12),
    only_reports: int = Query(default=1),
    is_24h: int = Query(default=0),
):
    meta = await run_in_threadpool(read_cache_meta)
    events = normalize_cached_events(meta)
    y, m = (year, month) if (year and month) else pick_default_month(events)
    month_events = filter_events_to_month(events, y, m)
    rows = await run_in_threadpool(build_pairing_rows, month_events, bool(is_24h), bool(only_reports))

    lp_iso = meta.get("last_pull_utc")
    nr_iso = meta.get("next_run_utc")

    lp_local = dt.datetime.fromisoformat(lp_iso.replace("Z", "+00:00")).astimezone(LOCAL_TZ) if lp_iso else None
    nr_local = dt.datetime.fromisoformat(nr_iso.replace("Z", "+00:00")).astimezone(LOCAL_TZ) if nr_iso else None
    now_local = dt.datetime.now(LOCAL_TZ)

    last_pull_local = human_ago_precise(lp_local)
    last_pull_human_simple = human_ago(lp_local)

    next_refresh_local_clock = nr_local.strftime("%I:%M %p").lstrip("0") if nr_local else ""
    seconds_to_next = max(0, int((nr_local - now_local).total_seconds())) if nr_local else 0

    looking_end = end_of_next_month_local()
    looking_through = f"Today – {looking_end.strftime('%b %d (%a)')}"

    return {
        "looking_through": looking_through,
        "last_pull_local": last_pull_local,
        "last_pull_local_simple": last_pull_human_simple,
        "last_pull_local_iso": lp_local.isoformat() if lp_local else "",
        "next_pull_local": next_refresh_local_clock,
        "next_pull_local_iso": nr_local.isoformat() if nr_local else "",
        "seconds_to_next": seconds_to_next,
        "tz_label": "CT",
        "rows": rows,
        "version": state.version,
        "year": y,
        "month": m,
    }


@app.get("/api/status")
async def api_status():
    meta = await run_in_threadpool(read_cache_meta)
    return {
        "version": state.version,
        "refresh_minutes": int(meta.get("refresh_minutes", max(1, state.refresh_seconds // 60))),
        "now": dt.datetime.now(LOCAL_TZ).isoformat(),
    }


@app.post("/api/settings/refresh-seconds")
async def api_set_refresh_seconds(payload: Dict[str, Any]):
    try:
        secs = int(payload.get("seconds"))
        if secs < 15 or secs > 21600:
            raise ValueError
    except Exception:
        raise HTTPException(status_code=400, detail="seconds must be 15..21600")
    state.refresh_seconds = secs

    meta = await run_in_threadpool(read_cache_meta)
    meta["refresh_minutes"] = max(1, secs // 60)
    meta["next_run_utc"] = (_now_utc() + dt.timedelta(minutes=meta["refresh_minutes"])).replace(microsecond=0).isoformat()
    await run_in_threadpool(write_cache_meta, meta)

    # wake poller to recompute immediately
    state.wake.set()
    await _emit("schedule_update", {"refresh_seconds": secs})
    return {"ok": True, "refresh_seconds": state.refresh_seconds}


@app.post("/api/refresh")
async def api_refresh():
    changed = await pull_and_update_once()
    # poller should recompute against new next_run_utc
    state.wake.set()
    return {"ok": True, "changed": changed, "version": state.version}


@app.get("/api/events")
async def sse_events():
    async def event_stream():
        yield f"event: hello\ndata: {json.dumps({'version': state.version})}\n\n"
        while not state.shutdown_event.is_set():
            msg = await state.sse_queue.get()
            evt_type = msg.get("type", "change")
            yield f"event: {evt_type}\ndata: {json.dumps(msg)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
