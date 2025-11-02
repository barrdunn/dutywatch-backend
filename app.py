from __future__ import annotations

import os
import asyncio
import datetime as dt
import json
import hashlib
import logging
import time
import traceback
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from fastapi import FastAPI, Query, HTTPException, Body
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool
from zoneinfo import ZoneInfo

import cal_client as cal
from cache_meta import read_cache_meta, write_cache_meta, normalize_cached_events
from rows import build_pairing_rows, end_of_next_month_local, grouping_key
from db import get_db, init_db

# ---- Optional DB helpers (server-side hidden + sticky rows) ----
try:
    # Hidden-store is keyed by individual iCloud VEVENT UIDs
    from db import hide_uid, list_hidden_uids, hidden_count, unhide_all, hidden_all
except Exception:  # safe fallbacks if db.py hasn't added these yet
    def hide_uid(uid: str) -> None:  # type: ignore
        pass
    def list_hidden_uids() -> List[str]:  # type: ignore
        return []
    def hidden_count() -> int:  # type: ignore
        return 0
    def unhide_all() -> None:  # type: ignore
        pass
    def hidden_all() -> List[str]:  # type: ignore
        return []

try:
    # Sticky "live" rows so an in-progress pairing isn't dropped mid-fly
    from db import upsert_live_row, list_live_rows, purge_expired_live, delete_live_row
except Exception:  # safe fallbacks
    def upsert_live_row(row: Dict[str, Any]) -> None:  # type: ignore
        pass
    def list_live_rows() -> List[Dict[str, Any]]:  # type: ignore
        return []
    def purge_expired_live(now_iso: str) -> None:  # type: ignore
        pass
    def delete_live_row(pairing_id: str) -> None:  # type: ignore
        pass

# ---------------- Paths / Static ----------------
BASE_DIR = Path(__file__).parent.resolve()
PAIRINGS_DIR = BASE_DIR / "public" / "pairings"

app = FastAPI(title="DutyWatch Backend (Viewer-Only)")
app.mount("/pairings", StaticFiles(directory=PAIRINGS_DIR, html=True), name="pairings")

@app.get("/pairings", include_in_schema=False)
def pairings_index_direct():
    return FileResponse(PAIRINGS_DIR / "index.html")

@app.get("/", include_in_schema=False)
def root_index():
    return FileResponse(PAIRINGS_DIR / "index.html")

# ---------------- Logging + middleware ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("dutywatch")
logger.setLevel(logging.INFO)

@app.middleware("http")
async def timing_and_errors(request, call_next):
    start = time.time()
    try:
        resp = await call_next(request)
        return resp
    except Exception:
        logger.exception("Unhandled error on %s %s", request.method, request.url.path)
        raise
    finally:
        dur_ms = int((time.time() - start) * 1000)
        logger.info("%s %s -> %dms", request.method, request.url.path, dur_ms)

# ---------------- Config / Time ----------------
LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "America/Chicago"))
VIEW_WINDOW_MODE = os.getenv("VIEW_WINDOW_MODE", "TODAY_TO_END_OF_NEXT_MONTH").upper().strip()
# Default True: only show sticky rows if the pairing_id is still present in the current feed
STICKY_REQUIRE_FEED = os.getenv("STICKY_REQUIRE_FEED", "1") not in ("0", "false", "False")
# Lookback so late-evening pairings are still fetched after UTC midnight
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))

class State:
    refresh_seconds: int = int(os.getenv("REFRESH_SECONDS", "1800"))  # default 30 min
    poll_task: Optional[asyncio.Task] = None
    version: int = 0
    sse_queue: "asyncio.Queue[dict]" = asyncio.Queue()
    shutdown_event: asyncio.Event = asyncio.Event()
    wake: asyncio.Event = asyncio.Event()

state = State()

def _now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

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

def to_utc(d: dt.datetime) -> dt.datetime:
    if d.tzinfo is None:
        d = d.replace(tzinfo=LOCAL_TZ)
    return d.astimezone(dt.timezone.utc)

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

def _fmt_time(hhmm: str, use_24h: bool) -> str:
    if not hhmm:
        return ""
    s = str(hhmm).zfill(4)
    h = int(s[:2]); m = int(s[2:])
    if use_24h:
        return f"{h:02d}:{m:02d}"
    ampm = "AM" if h < 12 else "PM"
    hh = h % 12 or 12
    return f"{hh}:{m:02d} {ampm}"

# ---------------- Calendar fetch ----------------
def fetch_current_to_next_eom() -> List[Dict[str, Any]]:
    """
    Fetch events from the beginning of LAST month through end of next month.
    This ensures we get October events (when in November) for calendar display.
    """
    now_utc = _now_utc()
    now_local = now_utc.astimezone(LOCAL_TZ)
    
    # Start from beginning of LAST month to get October when we're in November
    if now_local.month == 1:
        start_month = 12
        start_year = now_local.year - 1
    else:
        start_month = now_local.month - 1
        start_year = now_local.year
    
    start_local = dt.datetime(start_year, start_month, 1, tzinfo=LOCAL_TZ)
    start_utc = start_local.astimezone(dt.timezone.utc)
    
    # Calculate end of next month properly
    y, m = now_local.year, now_local.month
    
    # Next month
    if m == 12:
        next_month = 1
        next_year = y + 1
    else:
        next_month = m + 1
        next_year = y
    
    # Month after next
    if next_month == 12:
        end_month = 1
        end_year = next_year + 1
    else:
        end_month = next_month + 1
        end_year = next_year
    
    # First day of month after next
    end_utc = dt.datetime(end_year, end_month, 1, tzinfo=dt.timezone.utc)

    logger.info(f"FETCH DEBUG: Fetching events from {start_utc.isoformat()} to {end_utc.isoformat()}")
    logger.info(f"FETCH DEBUG: That's {start_local.strftime('%B %d, %Y')} to {end_utc.astimezone(LOCAL_TZ).strftime('%B %d, %Y')} in local time")

    if hasattr(cal, "fetch_events_between"):
        logger.info("FETCH DEBUG: Using fetch_events_between")
        result = cal.fetch_events_between(start_utc.isoformat(), end_utc.isoformat())
    else:
        # Fallback: calculate hours from start to end
        hours = int((end_utc - start_utc).total_seconds() // 3600) + 1
        logger.info(f"FETCH DEBUG: Using fetch_upcoming_events with hours_ahead={hours}")
        result = cal.fetch_upcoming_events(hours_ahead=hours)
    
    logger.info(f"FETCH DEBUG: Got {len(result)} events back")
    for evt in result[:5]:  # Log first 5 events
        logger.info(f"FETCH DEBUG: Event '{evt.get('summary')}' starts {evt.get('start_utc')}")
    
    return result

# ---------------- SSE + Poller ----------------
async def _emit(event_type: str, payload: dict | None = None):
    try:
        await state.sse_queue.put({"type": event_type, "payload": payload or {}, "version": state.version})
    except Exception:
        pass

def _hash_event(ev: Dict[str, Any]) -> str:
    """
    Stable hash over event fields that affect parsing / rendering.
    """
    material = {
        "uid": ev.get("uid") or "",
        "start_utc": ev.get("start_utc") or "",
        "end_utc": ev.get("end_utc") or "",
        "last_modified": ev.get("last_modified") or "",
        "summary": ev.get("summary") or "",
        "location": ev.get("location") or "",
        "description": ev.get("description") or "",
    }
    blob = json.dumps(material, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()

def _events_digest(events: List[Dict[str, Any]]) -> str:
    keyed = sorted(
        [{"k": f"{e.get('uid','')}|{e.get('start_utc','')}", "h": _hash_event(e)} for e in events],
        key=lambda x: x["k"],
    )
    blob = json.dumps(keyed, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()

def _next_from_meta(meta: Dict[str, Any]) -> dt.datetime:
    mins = int(meta.get("refresh_minutes", max(1, state.refresh_seconds // 60)))
    nxt = iso_to_dt(meta.get("next_run_utc"))
    if not nxt or nxt.tzinfo is None:
        nxt = _now_utc()
    if nxt <= _now_utc():
        nxt = (_now_utc() + dt.timedelta(minutes=mins)).replace(microsecond=0)
    return nxt

async def pull_and_update_once() -> bool:
    """
    Pull fresh iCloud events and mark 'changed' if *any* parse-relevant field differs.
    Also scrubs any mock events (uid starts with 'mock-') so refresh is the single source of truth.
    """
    try:
        events = await run_in_threadpool(fetch_current_to_next_eom)
        logger.info(f"DEBUG pull_and_update_once: Fetched {len(events)} events from calendar")

        # --- scrub any mock events by UID prefix (belt-and-suspenders) ---
        events_before_scrub = len(events)
        events = [e for e in events if not str(e.get("uid", "")).startswith("mock-")]
        logger.info(f"DEBUG pull_and_update_once: After scrubbing mock events: {len(events)} events (removed {events_before_scrub - len(events)})")

        meta = await run_in_threadpool(read_cache_meta)

        old_digest = meta.get("events_digest", "")
        new_digest = _events_digest(events)
        changed = (new_digest != old_digest)

        logger.info(f"DEBUG pull_and_update_once: About to save {len(events)} events to cache")
        meta["events"] = events
        meta["events_digest"] = new_digest
        meta["last_pull_utc"] = _now_utc().isoformat()
        meta.setdefault("refresh_minutes", max(1, state.refresh_seconds // 60))
        mins = int(meta.get("refresh_minutes", 30))
        meta["next_run_utc"] = (_now_utc() + dt.timedelta(minutes=mins)).replace(microsecond=0).isoformat()
        await run_in_threadpool(write_cache_meta, meta)
        
        # Verify what got saved
        verify_meta = await run_in_threadpool(read_cache_meta)
        logger.info(f"DEBUG pull_and_update_once: Verified cache now has {len(verify_meta.get('events', []))} events")

        if changed:
            state.version += 1
        await _emit("change", {"changed": changed})
        return changed
    except Exception as e:
        logger.exception("[pull] error: %s", e)
        return False

async def poller_loop():
    while not state.shutdown_event.is_set():
        meta = await run_in_threadpool(read_cache_meta)
        nxt = _next_from_meta(meta)
        delay = max(1.0, (nxt - _now_utc()).total_seconds())
        try:
            await asyncio.wait_for(state.wake.wait(), timeout=delay)
            state.wake.clear()
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

# ---------------- Ack policy ----------------
ACK_POLICY = {
    "push_start_hours": 12,
    "push_stop_hours": 4,
    "push_interval_minutes": 60,
    "call_start_hours": 4,
    "call_interval_minutes": 30,
    "calls_per_attempt": 2,
    "quiet_start_hour": 0,
    "quiet_end_hour": 6,
    "quiet_last_hour_minutes": 60,
    "quiet_interval_minutes": 15,
}

def _ack_id(pairing_id: str, report_local_iso: str) -> str:
    base = f"{pairing_id}|{report_local_iso}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]

def _read_ack_state(ack_id: str) -> Optional[str]:
    with get_db() as c:
        row = c.execute("SELECT state FROM acks WHERE ack_id=?", (ack_id,)).fetchone()
        return row["state"] if row else None

def _write_ack_state(ack_id: str, state_val: str, deadline_utc: Optional[str] = None):
    now_iso = _now_utc().isoformat()
    with get_db() as c:
        cur = c.execute("SELECT 1 FROM acks WHERE ack_id=?", (ack_id,)).fetchone()  # tuple comma bug fixed
        if cur:
            c.execute("UPDATE acks SET state=?, last_update_utc=? WHERE ack_id=?", (state_val, now_iso, ack_id))
        else:
            c.execute(
                "INSERT INTO acks(ack_id, event_uid, deadline_utc, state, last_update_utc) VALUES (?,?,?,?,?)",
                (ack_id, "", deadline_utc or "", state_val, now_iso),
            )

def _is_quiet_hour(t_local: dt.datetime) -> bool:
    qs = ACK_POLICY["quiet_start_hour"]
    qe = ACK_POLICY["quiet_end_hour"]
    h = t_local.hour
    if qs <= qe:
        return qs <= h < qe
    else:
        return h >= qs or h < qe

def _plan_attempts(report_local: dt.datetime) -> List[Dict[str, Any]]:
    p = ACK_POLICY
    items: List[Dict[str, Any]] = []
    t_start = report_local - dt.timedelta(hours=p["push_start_hours"])
    t_stop  = report_local - dt.timedelta(hours=p["push_stop_hours"])
    t = t_start
    while t < t_stop:
        items.append({"kind": "push", "label": "Push reminder", "at_iso": t.isoformat(), "meta": {}})
        t += dt.timedelta(minutes=p["push_interval_minutes"])

    base_interval = dt.timedelta(minutes=p["call_interval_minutes"])
    quiet_interval = dt.timedelta(minutes=p["quiet_interval_minutes"])
    last_hour = dt.timedelta(minutes=p["quiet_last_hour_minutes"])
    call_window_start = report_local - dt.timedelta(hours=p["call_start_hours"])

    t = call_window_start
    while t < report_local:
        t_local = t.astimezone(LOCAL_TZ)
        until_report = report_local - t
        if _is_quiet_hour(t_local) and until_report > last_hour:
            t += base_interval
            continue
        for ring in range(1, p["calls_per_attempt"] + 1):
            ts = t if ring == 1 else (t + dt.timedelta(minutes=1))
            items.append({"kind": "call", "label": f"Call attempt (ring {ring}/{p['calls_per_attempt']})", "at_iso": ts.isoformat(), "meta": {"ring": ring}})
        t += base_interval

    t0 = max(call_window_start, report_local - last_hour)
    t = t0
    seen = set(i["at_iso"] for i in items)
    while t < report_local:
        t_local = t.astimezone(LOCAL_TZ)
        if _is_quiet_hour(t_local):
            for ring in range(1, p["calls_per_attempt"] + 1):
                ts = t if ring == 1 else (t + dt.timedelta(minutes=1))
                iso = ts.isoformat()
                if iso not in seen:
                    seen.add(iso)
                    items.append({"kind": "call", "label": f"Call attempt (ring {ring}/{p['calls_per_attempt']})", "at_iso": iso, "meta": {"ring": ring}})
        t += quiet_interval

    items.sort(key=lambda x: x["at_iso"])
    return items

def _window_state(report_local: Optional[dt.datetime]) -> Dict[str, Any]:
    now_local = dt.datetime.now(LOCAL_TZ)
    if not report_local:
        return {"window_open": False, "seconds_until_open": None, "seconds_until_report": None}
    open_at = report_local - dt.timedelta(hours=ACK_POLICY["push_start_hours"])
    return {
        "window_open": open_at <= now_local <= report_local,
        "seconds_until_open": max(0, int((open_at - now_local).total_seconds())) if now_local < open_at else 0,
        "seconds_until_report": max(0, int((report_local - now_local).total_seconds())) if now_local < report_local else 0,
    }

# ---------------- Viewing window ----------------
def compute_window_bounds_local() -> Tuple[dt.datetime, dt.datetime, str]:
    """
    Compute viewing window bounds. Start from beginning of LAST month (October when in November)
    so calendar can show past month's events in the calendar (grayed out).
    """
    mode = VIEW_WINDOW_MODE
    now_local = dt.datetime.now(LOCAL_TZ)

    # Calculate start from PREVIOUS month to include October events
    if now_local.month == 1:
        start_month = 12
        start_year = now_local.year - 1
    else:
        start_month = now_local.month - 1
        start_year = now_local.year
    
    if mode == "TODAY_TO_END_OF_NEXT_MONTH":
        # Start from beginning of LAST month for calendar display
        start_local = dt.datetime(start_year, start_month, 1, tzinfo=LOCAL_TZ)
        end_local = end_of_next_month_local()
        label = f"{start_local.strftime('%b 1')} – {end_local.strftime('%b %d')}"
        return start_local, end_local, label

    # Default: also start from beginning of LAST month  
    start_local = dt.datetime(start_year, start_month, 1, tzinfo=LOCAL_TZ)
    end_local = end_of_next_month_local()
    label = f"{start_local.strftime('%b 1')} – {end_local.strftime('%b %d')}"
    return start_local, end_local, label

def window_bounds_utc() -> Tuple[dt.datetime, dt.datetime, dt.datetime, dt.datetime, str]:
    start_local, end_local, label = compute_window_bounds_local()
    start_utc = to_utc(start_local)
    end_utc = to_utc(end_local)
    return start_utc, end_utc, start_local, end_local, label

# ---------------- API Routes ----------------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/api/pairings")
async def api_pairings(
    year: Optional[int] = Query(default=None, ge=1970, le=2100),
    month: Optional[int] = Query(default=None, ge=1, le=12),
    only_reports: int = Query(default=1),
    is_24h: int = Query(default=0),
):
    """
    Builds visible rows:
      - Filters hidden zero-leg items (by UID) and pairing_id hides.
      - Keeps in-progress pairings sticky unless the release time has passed.
      - Recomputes OFF rows after hide/unhide and on every paint.
      - TOP OFF row when 'now' < first report shows: 'OFF (Now)' and remaining.
      - OFF rows show the previous pairing's release time under the "Report" column.
    """
    try:
        meta = await run_in_threadpool(read_cache_meta)
        events = normalize_cached_events(meta)

        start_utc, end_utc, start_local, end_local, window_label = window_bounds_utc()
        UTC_MIN = dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        UTC_MAX = dt.datetime.max.replace(tzinfo=dt.timezone.utc)

        def safe_start(ev: Dict[str, Any]) -> dt.datetime:
            s = iso_to_dt(ev.get("start_utc"))
            return s if (s and s.tzinfo) else (s or UTC_MIN).replace(tzinfo=dt.timezone.utc)

        def safe_end(ev: Dict[str, Any]) -> dt.datetime:
            e = iso_to_dt(ev.get("end_utc"))
            return e if (e and e.tzinfo) else (e or UTC_MAX).replace(tzinfo=dt.timezone.utc)

        window_events = [e for e in events if (safe_end(e) > start_utc) and (safe_start(e) < end_utc)]

        # Build rows fresh every time.
        rows = await run_in_threadpool(build_pairing_rows, window_events, bool(is_24h), bool(only_reports))
        
        # Map pairing -> uids
        pid_to_uids: Dict[str, List[str]] = {}
        for ev in window_events:
            pid = grouping_key(ev)
            uid = str(ev.get("uid") or "")
            if uid:
                pid_to_uids.setdefault(pid, []).append(uid)

        hidden_uids = await run_in_threadpool(list_hidden_uids)
        hidden_pids = set(await run_in_threadpool(hidden_all))
        use_24h = bool(is_24h)

        def fmt_top(disp_map: Dict[str, Any], r: Dict[str, Any]):
            rep = disp_map.get("report_hhmm") or r.get("report_hhmm") or r.get("report")
            rel = disp_map.get("release_hhmm") or r.get("release_hhmm") or r.get("release")
            if isinstance(rep, str) and rep.isdigit():
                disp_map["report_str"] = _fmt_time(rep, use_24h)
            if isinstance(rel, str) and rel.isdigit():
                disp_map["release_str"] = _fmt_time(rel, use_24h)
        
        # Keep ALL rows for the calendar (including past events, including hidden)
        # Just format them for display
        calendar_rows = []
        for r in rows:
            r_copy = dict(r)  # Make a copy for calendar
            if r_copy.get("kind") == "pairing":
                pid = str(r_copy.get("pairing_id") or "")
                r_copy["uids"] = pid_to_uids.get(pid, [])
                total_legs = sum(len((d.get("legs") or [])) for d in (r_copy.get("days") or []))
                r_copy["can_hide"] = (total_legs == 0)

                fmt_top(r_copy.setdefault("display", {}), r_copy)

                for d in (r_copy.get("days") or []):
                    for leg in (d.get("legs") or []):
                        if leg.get("dep_time") and not leg.get("dep_time_str"):
                            leg["dep_time_str"] = _fmt_time(str(leg["dep_time"]).zfill(4), use_24h)
                        if leg.get("arr_time") and not leg.get("arr_time_str"):
                            leg["arr_time_str"] = _fmt_time(str(leg["arr_time"]).zfill(4), use_24h)
            
            calendar_rows.append(r_copy)
        
        logger.info(f"DEBUG: calendar_rows has {len(calendar_rows)} items")
        logger.info(f"DEBUG: calendar_rows sample: {calendar_rows[:2] if calendar_rows else 'empty'}")

        # Process rows for the TABLE display (filter by time and visibility)
        visible: List[Dict[str, Any]] = []
        now_local = dt.datetime.now(LOCAL_TZ)
        
        for r in rows:
            if r.get("kind") != "pairing":
                continue
            
            # Check if this pairing is in the past
            release_iso = r.get("release_local_iso")
            if release_iso:
                release_time = iso_to_dt(release_iso)
                release_local = to_local(release_time) if release_time else None
                # Skip past pairings unless in progress
                if release_local and release_local < now_local and not r.get("in_progress"):
                    continue
            
            pid = str(r.get("pairing_id") or "")
            r["uids"] = pid_to_uids.get(pid, [])
            total_legs = sum(len((d.get("legs") or [])) for d in (r.get("days") or []))
            r["can_hide"] = (total_legs == 0)

            fmt_top(r.setdefault("display", {}), r)

            for d in (r.get("days") or []):
                for leg in (d.get("legs") or []):
                    if leg.get("dep_time") and not leg.get("dep_time_str"):
                        leg["dep_time_str"] = _fmt_time(str(leg["dep_time"]).zfill(4), use_24h)
                    if leg.get("arr_time") and not leg.get("arr_time_str"):
                        leg["arr_time_str"] = _fmt_time(str(leg["arr_time"]).zfill(4), use_24h)

            # hide logic: hide if ALL UIDs are hidden OR if pairing_id is hidden
            all_hidden = (len(r["uids"]) > 0) and all(uid in hidden_uids for uid in r["uids"])
            pid_hidden = pid in hidden_pids
            if (pid_hidden or all_hidden) and (r["can_hide"] or not r.get("in_progress")):
                continue

            # IMPORTANT: Only add real pairings (with legs) to visible array
            # Non-pairing events are handled separately
            if total_legs > 0:
                visible.append(r)

        # Sticky in-progress pairings: keep them even if missing from feed,
        # unless the release time has already passed (covers fetch gaps around UTC midnight).
        live_rows = await run_in_threadpool(list_live_rows)
        current_feed_pids = set(str(p.get("pairing_id") or "") for p in visible if p.get("kind") == "pairing")
        live_to_delete: List[str] = []

        for lr in live_rows:
            pid = str(lr.get("pairing_id") or "")
            if not pid:
                continue

            rel_iso = lr.get("release_local_iso")
            rel_dt = to_local(iso_to_dt(rel_iso)) if rel_iso else None

            if STICKY_REQUIRE_FEED and pid not in current_feed_pids:
                if rel_dt and rel_dt < now_local:
                    live_to_delete.append(pid)
                    continue
                # else: keep sticky visible

            if pid in current_feed_pids:
                continue
            if rel_dt and rel_dt >= now_local and not lr.get("can_hide"):
                visible.append(lr)

        # Remove live rows that actually expired
        for pid in live_to_delete:
            await run_in_threadpool(delete_live_row, pid)

        # Keep live rows updated; purge expired by time
        for r in visible:
            if r.get("in_progress") and not r.get("can_hide"):
                await run_in_threadpool(upsert_live_row, r)
        await run_in_threadpool(purge_expired_live, now_local.isoformat())

        # ---- Rebuild OFF rows across final visible
        def _dt(s):
            return iso_to_dt(s) if s else None

        visible.sort(key=lambda x: x.get("report_local_iso") or "")
        final_rows: List[Dict[str, Any]] = []

        # (A) TOP OFF
        if visible:
            first_report_iso = visible[0].get("report_local_iso")
            first_report = to_local(iso_to_dt(first_report_iso)) if first_report_iso else None
            if first_report and now_local < first_report:
                remaining = first_report - now_local
                hrs = int(remaining.total_seconds() // 3600)
                if hrs >= 24:
                    d = hrs // 24
                    h = hrs % 24
                    off_str = f"{d}d {h}h (Remaining)"
                else:
                    off_str = f"{hrs}h (Remaining)"
                final_rows.append({
                    "kind": "off",
                    "display": {"off_dur": off_str, "off_label": "OFF (Now)"}
                })

        # (B) Rebuild OFF rows between VISIBLE pairings only
        # We need to filter out past pairings and rebuild OFF rows between what's actually shown
        
        # Just pass through the rows from rows.py which already has correct OFF calculations
        # Filter out past events and hidden items, but keep the OFF structure intact
        
        # First, find the first future pairing
        first_future_pairing_index = None
        for i, r in enumerate(rows):
            if r.get("kind") == "pairing":
                total_legs = sum(len((d.get("legs") or [])) for d in (r.get("days") or []))
                
                # Check if this is a future event
                if total_legs > 0:
                    release_iso = r.get("release_local_iso")
                    if release_iso:
                        release_time = iso_to_dt(release_iso)
                        release_local = to_local(release_time) if release_time else None
                        if release_local and release_local > now_local:
                            first_future_pairing_index = i
                            break
                        elif r.get("in_progress"):
                            first_future_pairing_index = i
                            break
                else:
                    # Non-pairing event - check report time
                    report_iso = r.get("report_local_iso")
                    if report_iso:
                        report_time = iso_to_dt(report_iso)
                        report_local = to_local(report_time) if report_time else None
                        if report_local and report_local > now_local:
                            # This is a future non-pairing event, but keep looking for actual pairing
                            continue
        
        # Now process rows starting from just before the first future pairing
        start_index = max(0, first_future_pairing_index - 1) if first_future_pairing_index is not None else len(rows)
        
        for i in range(start_index, len(rows)):
            r = rows[i]
            
            if r.get("kind") == "off":
                # Only include OFF rows between future pairings
                final_rows.append(r)
                
            elif r.get("kind") == "pairing":
                # Check if this should be shown
                release_iso = r.get("release_local_iso")
                report_iso = r.get("report_local_iso")
                
                # For pairings with legs, check release time
                total_legs = sum(len((d.get("legs") or [])) for d in (r.get("days") or []))
                if total_legs > 0:
                    if release_iso:
                        release_time = iso_to_dt(release_iso)
                        release_local = to_local(release_time) if release_time else None
                        # Skip past pairings unless in progress
                        if release_local and release_local < now_local and not r.get("in_progress"):
                            continue
                else:
                    # For non-pairing events, check report time
                    if report_iso:
                        report_time = iso_to_dt(report_iso)
                        report_local = to_local(report_time) if report_time else None
                        # Skip past non-pairing events
                        if report_local and report_local < now_local:
                            continue
                
                pid = str(r.get("pairing_id") or "")
                
                # Check if hidden
                all_hidden = (len(r.get("uids", [])) > 0) and all(uid in hidden_uids for uid in r.get("uids", []))
                pid_hidden = pid in hidden_pids
                if (pid_hidden or all_hidden) and (r.get("can_hide") or not r.get("in_progress")):
                    continue
                
                # Include this pairing
                final_rows.append(r)

        # ---- Header meta
        lp_iso = meta.get("last_pull_utc")
        nr_iso = meta.get("next_run_utc")
        lp_local = to_local(iso_to_dt(lp_iso)) if lp_iso else None
        nr_local = to_local(iso_to_dt(nr_iso)) if nr_iso else None
        guard_now = dt.datetime.now(LOCAL_TZ)
        if not nr_local or nr_local <= guard_now:
            mins = int(meta.get("refresh_minutes", max(1, state.refresh_seconds // 60)))
            nr_local = guard_now + dt.timedelta(minutes=mins)
            meta["next_run_utc"] = to_utc(nr_local).replace(microsecond=0).isoformat()
            await run_in_threadpool(write_cache_meta, meta)

        last_pull_local = human_ago_precise(lp_local)
        last_pull_human_simple = human_ago(lp_local)
        next_refresh_local_clock = nr_local.strftime("%I:%M %p").lstrip("0") if nr_local else ""
        seconds_to_next = max(0, int((nr_local - dt.datetime.now(LOCAL_TZ)).total_seconds())) if nr_local else 0

        window_obj = {
            "start_local_iso": start_local.isoformat(),
            "end_local_iso": end_local.isoformat(),
            "label": window_label,
        }

        hc = await run_in_threadpool(hidden_count)

        return {
            "window": window_obj,
            "window_config": {"mode": VIEW_WINDOW_MODE, "tz": str(LOCAL_TZ)},
            "looking_through": window_label,
            "last_pull_local": last_pull_local,
            "last_pull_local_simple": last_pull_human_simple,
            "last_pull_local_iso": lp_local.isoformat() if lp_local else "",
            "next_pull_local": next_refresh_local_clock,
            "next_pull_local_iso": nr_local.isoformat() if nr_local else "",
            "seconds_to_next": seconds_to_next,
            "tz_label": "CT",
            "rows": final_rows,  # Table rows (future events only)
            "calendar_rows": calendar_rows,  # All rows for calendar (including past)
            "version": state.version,
            "refresh_minutes": int(meta.get("refresh_minutes", max(1, state.refresh_seconds // 60))),
            "ack_policy": ACK_POLICY,
            "is_24h": use_24h,
            "hidden_count": int(hc),
        }
    except Exception as e:
        tb = traceback.format_exc(limit=12)
        logger.error("api_pairings failed: %s\n%s", e, tb)
        return JSONResponse(
            status_code=500,
            content={"error": "pairings_failed", "message": str(e), "trace": tb},
        )
async def api_pairings(
    year: Optional[int] = Query(default=None, ge=1970, le=2100),
    month: Optional[int] = Query(default=None, ge=1, le=12),
    only_reports: int = Query(default=1),
    is_24h: int = Query(default=0),
):
    """
    Builds visible rows:
      - Filters hidden zero-leg items (by UID) and pairing_id hides.
      - Keeps in-progress pairings sticky unless the release time has passed.
      - Recomputes OFF rows after hide/unhide and on every paint.
      - TOP OFF row when 'now' < first report shows: 'OFF (Now)' and remaining.
      - OFF rows show the previous pairing's release time under the "Report" column.
    """
    try:
        meta = await run_in_threadpool(read_cache_meta)
        events = normalize_cached_events(meta)

        start_utc, end_utc, start_local, end_local, window_label = window_bounds_utc()
        UTC_MIN = dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        UTC_MAX = dt.datetime.max.replace(tzinfo=dt.timezone.utc)

        def safe_start(ev: Dict[str, Any]) -> dt.datetime:
            s = iso_to_dt(ev.get("start_utc"))
            return s if (s and s.tzinfo) else (s or UTC_MIN).replace(tzinfo=dt.timezone.utc)

        def safe_end(ev: Dict[str, Any]) -> dt.datetime:
            e = iso_to_dt(ev.get("end_utc"))
            return e if (e and e.tzinfo) else (e or UTC_MAX).replace(tzinfo=dt.timezone.utc)

        window_events = [e for e in events if (safe_end(e) > start_utc) and (safe_start(e) < end_utc)]

        # Build rows fresh every time.
        rows = await run_in_threadpool(build_pairing_rows, window_events, bool(is_24h), bool(only_reports))

        # Map pairing -> uids
        pid_to_uids: Dict[str, List[str]] = {}
        for ev in window_events:
            pid = grouping_key(ev)
            uid = str(ev.get("uid") or "")
            if uid:
                pid_to_uids.setdefault(pid, []).append(uid)

        hidden_uids = await run_in_threadpool(list_hidden_uids)
        hidden_pids = set(await run_in_threadpool(hidden_all))
        use_24h = bool(is_24h)

        visible: List[Dict[str, Any]] = []

        def fmt_top(disp_map: Dict[str, Any], r: Dict[str, Any]):
            rep = disp_map.get("report_hhmm") or r.get("report_hhmm") or r.get("report")
            rel = disp_map.get("release_hhmm") or r.get("release_hhmm") or r.get("release")
            if isinstance(rep, str) and rep.isdigit():
                disp_map["report_str"] = _fmt_time(rep, use_24h)
            if isinstance(rel, str) and rel.isdigit():
                disp_map["release_str"] = _fmt_time(rel, use_24h)

        for r in rows:
            if r.get("kind") != "pairing":
                continue
            pid = str(r.get("pairing_id") or "")
            r["uids"] = pid_to_uids.get(pid, [])
            total_legs = sum(len((d.get("legs") or [])) for d in (r.get("days") or []))
            r["can_hide"] = (total_legs == 0)

            fmt_top(r.setdefault("display", {}), r)

            for d in (r.get("days") or []):
                for leg in (d.get("legs") or []):
                    if leg.get("dep_time") and not leg.get("dep_time_str"):
                        leg["dep_time_str"] = _fmt_time(str(leg["dep_time"]).zfill(4), use_24h)
                    if leg.get("arr_time") and not leg.get("arr_time_str"):
                        leg["arr_time_str"] = _fmt_time(str(leg["arr_time"]).zfill(4), use_24h)

            # hide logic: hide if ALL UIDs are hidden OR if pairing_id is hidden
            all_hidden = (len(r["uids"]) > 0) and all(uid in hidden_uids for uid in r["uids"])
            pid_hidden = pid in hidden_pids
            if (pid_hidden or all_hidden) and (r["can_hide"] or not r.get("in_progress")):
                continue

            # IMPORTANT: Only add real pairings (with legs) to visible array
            # Non-pairing events are already properly positioned in rows from rows.py
            if total_legs > 0:
                visible.append(r)

        # Sticky in-progress pairings: keep them even if missing from feed,
        # unless the release time has already passed (covers fetch gaps around UTC midnight).
        now_local = dt.datetime.now(LOCAL_TZ)
        live_rows = await run_in_threadpool(list_live_rows)
        current_feed_pids = set(str(p.get("pairing_id") or "") for p in visible if p.get("kind") == "pairing")
        live_to_delete: List[str] = []

        for lr in live_rows:
            pid = str(lr.get("pairing_id") or "")
            if not pid:
                continue

            rel_iso = lr.get("release_local_iso")
            rel_dt = to_local(iso_to_dt(rel_iso)) if rel_iso else None

            if STICKY_REQUIRE_FEED and pid not in current_feed_pids:
                if rel_dt and rel_dt < now_local:
                    live_to_delete.append(pid)
                    continue
                # else: keep sticky visible

            if pid in current_feed_pids:
                continue
            if rel_dt and rel_dt >= now_local and not lr.get("can_hide"):
                visible.append(lr)

        # Remove live rows that actually expired
        for pid in live_to_delete:
            await run_in_threadpool(delete_live_row, pid)

        # Keep live rows updated; purge expired by time
        for r in visible:
            if r.get("in_progress") and not r.get("can_hide"):
                await run_in_threadpool(upsert_live_row, r)
        await run_in_threadpool(purge_expired_live, now_local.isoformat())

        # ---- Rebuild OFF rows across final visible
        def _dt(s):
            return iso_to_dt(s) if s else None

        visible.sort(key=lambda x: x.get("report_local_iso") or "")
        final_rows: List[Dict[str, Any]] = []

        # (A) TOP OFF
        if visible:
            first_report_iso = visible[0].get("report_local_iso")
            first_report = to_local(iso_to_dt(first_report_iso)) if first_report_iso else None
            if first_report and now_local < first_report:
                remaining = first_report - now_local
                hrs = int(remaining.total_seconds() // 3600)
                if hrs >= 24:
                    d = hrs // 24
                    h = hrs % 24
                    off_str = f"{d}d {h}h (Remaining)"
                else:
                    off_str = f"{hrs}h (Remaining)"
                final_rows.append({
                    "kind": "off",
                    "display": {"off_dur": off_str, "off_label": "OFF (Now)"}
                })

        # (B) Add visible pairings and OFF rows as already built by rows.py
        # rows.py has already calculated OFF times and inserted non-pairing events correctly
        # We just need to pass through what's not hidden
        
        # The original 'rows' from rows.py has everything correctly positioned
        # We just filtered some out for hiding. Now reconstruct the final output.
        
        # Since we need to maintain the order and OFF rows from rows.py, let's rebuild differently
        # Get all the pairing IDs that are visible
        visible_pids = set(str(v.get("pairing_id") or "") for v in visible)
        
        # Go through the original rows and include items that should be shown
        for r in rows:
            if r.get("kind") == "off":
                # Always include OFF rows
                final_rows.append(r)
            elif r.get("kind") == "pairing":
                pid = str(r.get("pairing_id") or "")
                total_legs = sum(len((d.get("legs") or [])) for d in (r.get("days") or []))
                
                # Include if it's a visible pairing OR a non-pairing event (which rows.py positioned correctly)
                if pid in visible_pids or total_legs == 0:
                    final_rows.append(r)

        # ---- Header meta
        lp_iso = meta.get("last_pull_utc")
        nr_iso = meta.get("next_run_utc")
        lp_local = to_local(iso_to_dt(lp_iso)) if lp_iso else None
        nr_local = to_local(iso_to_dt(nr_iso)) if nr_iso else None
        guard_now = dt.datetime.now(LOCAL_TZ)
        if not nr_local or nr_local <= guard_now:
            mins = int(meta.get("refresh_minutes", max(1, state.refresh_seconds // 60)))
            nr_local = guard_now + dt.timedelta(minutes=mins)
            meta["next_run_utc"] = to_utc(nr_local).replace(microsecond=0).isoformat()
            await run_in_threadpool(write_cache_meta, meta)

        last_pull_local = human_ago_precise(lp_local)
        last_pull_human_simple = human_ago(lp_local)
        next_refresh_local_clock = nr_local.strftime("%I:%M %p").lstrip("0") if nr_local else ""
        seconds_to_next = max(0, int((nr_local - dt.datetime.now(LOCAL_TZ)).total_seconds())) if nr_local else 0

        window_obj = {
            "start_local_iso": start_local.isoformat(),
            "end_local_iso": end_local.isoformat(),
            "label": window_label,
        }

        hc = await run_in_threadpool(hidden_count)

        return {
            "window": window_obj,
            "window_config": {"mode": VIEW_WINDOW_MODE, "tz": str(LOCAL_TZ)},
            "looking_through": window_label,
            "last_pull_local": last_pull_local,
            "last_pull_local_simple": last_pull_human_simple,
            "last_pull_local_iso": lp_local.isoformat() if lp_local else "",
            "next_pull_local": next_refresh_local_clock,
            "next_pull_local_iso": nr_local.isoformat() if nr_local else "",
            "seconds_to_next": seconds_to_next,
            "tz_label": "CT",
            "rows": final_rows,
            "version": state.version,
            "refresh_minutes": int(meta.get("refresh_minutes", max(1, state.refresh_seconds // 60))),
            "ack_policy": ACK_POLICY,
            "is_24h": use_24h,
            "hidden_count": int(hc),
        }
    except Exception as e:
        tb = traceback.format_exc(limit=12)
        logger.error("api_pairings failed: %s\n%s", e, tb)
        return JSONResponse(
            status_code=500,
            content={"error": "pairings_failed", "message": str(e), "trace": tb},
        )

@app.get("/api/ack/plan")
def api_ack_plan(pairing_id: str, report_local_iso: str):
    report_local = to_local(iso_to_dt(report_local_iso))
    if not report_local:
        raise HTTPException(400, "Invalid report_local_iso")
    attempts = _plan_attempts(report_local)
    return {"pairing_id": pairing_id, "report_local_iso": report_local_iso, "attempts": attempts, "policy": ACK_POLICY}

@app.post("/api/ack/acknowledge")
def api_ack_acknowledge(payload: Dict[str, Any] = Body(...)):
    pairing_id = str(payload.get("pairing_id") or "")
    report_local_iso = str(payload.get("report_local_iso") or "")
    if not pairing_id or not report_local_iso:
        raise HTTPException(400, "pairing_id and report_local_iso required")
    ackid = _ack_id(pairing_id, report_local_iso)
    report_local = to_local(iso_to_dt(report_local_iso))
    deadline_utc = report_local.astimezone(dt.timezone.utc).isoformat() if report_local else None
    _write_ack_state(ackid, "ack", deadline_utc)
    return {"ok": True, "ack_id": ackid}

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

    state.wake.set()
    await _emit("schedule_update", {"refresh_seconds": secs})
    return {"ok": True, "refresh_seconds": state.refresh_seconds}

@app.post("/api/refresh")
async def api_refresh():
    changed = await pull_and_update_once()
    state.wake.set()
    return {"ok": True, "changed": changed, "version": state.version}

@app.get("/api/events")
async def sse_events():
    async def event_stream():
        yield f"event: hello\ndata: {{\"version\": {state.version}}}\n\n"
        while not state.shutdown_event.is_set():
            msg = await state.sse_queue.get()
            evt_type = msg.get("type", "change")
            yield f"event: {evt_type}\ndata: {json.dumps(msg)}\n\n"
    return StreamingResponse(event_stream(), media_type="text/event-stream")

# ----- Hide/Unhide endpoints -----
@app.post("/api/hide")
async def api_hide(payload: Dict[str, Any] = Body(...)):
    """
    Hide a single VEVENT by UID (server-side).
    Frontend should send one UID (e.g., from a zero-leg event).
    """
    uid = str(payload.get("uid") or "").strip()
    if not uid:
        raise HTTPException(400, "uid required")
    await run_in_threadpool(hide_uid, uid)
    hc = await run_in_threadpool(hidden_count)
    state.version += 1
    await _emit("hidden_update", {"uid": uid, "hidden_count": int(hc)})
    return {"ok": True, "hidden_count": int(hc)}

@app.post("/api/unhide_all")
async def api_unhide_all():
    """
    Clear all hidden UIDs.
    """
    await run_in_threadpool(unhide_all)
    state.version += 1
    await _emit("hidden_update", {"cleared": True})
    return {"ok": True}

# ---- Hidden endpoints (pairing_id) -----------------------------------------
from pydantic import BaseModel
from fastapi import HTTPException as _HTTPExceptionAlias  # avoid shadowing
import logging as _logging

# DB helpers (pairing_id-based hide)
from db import hidden_add, hidden_clear_all, hidden_count as hidden_pairing_count

log = _logging.getLogger("dutywatch")

class _HideReq(BaseModel):
    pairing_id: str
    report_local_iso: str | None = None

# Support both with and without trailing slash to prevent 404s
@app.post("/api/hidden/hide")
@app.post("/api/hidden/hide/")
def api_hidden_hide(req: _HideReq):
    if not req.pairing_id:
        raise _HTTPExceptionAlias(status_code=400, detail="pairing_id required")
    log.info("POST /api/hidden/hide -> %s", req.pairing_id)
    hidden_add(req.pairing_id, req.report_local_iso)
    return {"ok": True, "hidden_count": hidden_pairing_count()}

@app.post("/api/hidden/unhide_all")
@app.post("/api/hidden/unhide_all/")
def api_hidden_unhide_all():
    before = hidden_pairing_count()
    log.info("POST /api/hidden/unhide_all -> %d", before)
    hidden_clear_all()
    return {"ok": True, "cleared": before, "hidden_count": 0}