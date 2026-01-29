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
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool
from zoneinfo import ZoneInfo

from modules import cal_client as cal
from modules.cache import read_cache_meta, write_cache_meta, normalize_cached_events
from modules.rows import build_pairing_rows, end_of_next_month_local, grouping_key
from modules.db import get_db, init_db
from modules.utils import iso_to_dt, to_local, to_utc, human_ago, human_ago_precise, fmt_time

# ---- Optional DB helpers (server-side hidden + sticky rows) ----
try:
    # Hidden-store is keyed by individual iCloud VEVENT UIDs
    from modules.db import hide_uid, list_hidden_uids, hidden_count, unhide_all, hidden_all
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
    from modules.db import upsert_live_row, list_live_rows, purge_expired_live, delete_live_row
except Exception:  # safe fallbacks
    def upsert_live_row(row: Dict[str, Any]) -> None:  # type: ignore
        pass
    def list_live_rows() -> List[Dict[str, Any]]:  # type: ignore
        return []
    def purge_expired_live(now_iso: str) -> None:  # type: ignore
        pass
    def delete_live_row(pairing_id: str) -> None:  # type: ignore
        pass

# --- Profile table helpers (functions only - endpoints are below after app is created) ---

def _ensure_profile_table():
    """Create profile table if it doesn't exist."""
    with get_db() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS profile (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                first_name TEXT DEFAULT 'Barry',
                last_name TEXT DEFAULT 'Dunn',
                photo TEXT DEFAULT NULL
            )
        """)
        # Ensure there's exactly one row
        existing = c.execute("SELECT 1 FROM profile WHERE id = 1").fetchone()
        if not existing:
            c.execute("INSERT INTO profile (id, first_name, last_name) VALUES (1, 'Barry', 'Dunn')")


def get_profile() -> Dict[str, Any]:
    """Get the user profile."""
    _ensure_profile_table()
    with get_db() as c:
        row = c.execute("SELECT first_name, last_name, photo FROM profile WHERE id = 1").fetchone()
        if row:
            return {
                "firstName": row["first_name"] or "Barry",
                "lastName": row["last_name"] or "Dunn",
                "photo": row["photo"]
            }
        return {"firstName": "Barry", "lastName": "Dunn", "photo": None}


def save_profile(first_name: str, last_name: str, photo: Optional[str] = None):
    """Save the user profile."""
    _ensure_profile_table()
    with get_db() as c:
        c.execute("""
            UPDATE profile 
            SET first_name = ?, last_name = ?, photo = ?
            WHERE id = 1
        """, (first_name or "Barry", last_name or "Dunn", photo))


# ---------------- Paths / Static ----------------
BASE_DIR = Path(__file__).parent.resolve()
PAIRINGS_DIR = BASE_DIR / "public" / "pairings"
MED_PORTAL_DIR = BASE_DIR / "public" / "med_portal"

app = FastAPI(title="DutyWatch Backend (Viewer-Only)")
app.mount("/pairings", StaticFiles(directory=PAIRINGS_DIR, html=True), name="pairings")
app.mount("/med_portal", StaticFiles(directory=MED_PORTAL_DIR, html=True), name="med_portal")

@app.get("/pairings", include_in_schema=False)
def pairings_index_direct():
    return FileResponse(PAIRINGS_DIR / "index.html")

@app.get("/", include_in_schema=False)
def root_index():
    return FileResponse(PAIRINGS_DIR / "index.html")

@app.get("/medical", include_in_schema=False)
def medical_portal():
    """Serve the medical portal page"""
    return FileResponse(MED_PORTAL_DIR / "index.html")

# --- Profile API endpoints ---

@app.get("/api/profile")
async def api_get_profile():
    """Get user profile."""
    profile = await run_in_threadpool(get_profile)
    return profile


@app.post("/api/profile")
async def api_save_profile(payload: Dict[str, Any] = Body(...)):
    """Save user profile."""
    first_name = str(payload.get("firstName") or "").strip() or "Barry"
    last_name = str(payload.get("lastName") or "").strip() or "Dunn"
    photo = payload.get("photo")  # base64 string or None
    
    # Limit photo size (roughly 500KB base64 = ~375KB image)
    if photo and len(photo) > 500_000:
        raise HTTPException(400, "Photo too large. Please use a smaller image.")
    
    await run_in_threadpool(save_profile, first_name, last_name, photo)
    return {"ok": True}

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
HOME_BASE = os.getenv("HOME_BASE", "DFW").upper()
VIEW_WINDOW_MODE = os.getenv("VIEW_WINDOW_MODE", "TODAY_TO_END_OF_NEXT_MONTH").upper().strip()
STICKY_REQUIRE_FEED = os.getenv("STICKY_REQUIRE_FEED", "1") not in ("0", "false", "False")
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))

# Toggle to show/hide non-pairing events (CBT, VAC, meetings, etc.)
INCLUDE_NON_PAIRING_EVENTS = False  # Set to False to hide non-flying events

class State:
    refresh_seconds: int = int(os.getenv("REFRESH_SECONDS", "1800"))
    poll_task: Optional[asyncio.Task] = None
    version: int = 0
    sse_queue: "asyncio.Queue[dict]" = asyncio.Queue()
    shutdown_event: asyncio.Event = asyncio.Event()
    wake: asyncio.Event = asyncio.Event()

state = State()

def _now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

# ---------------- Calendar fetch ----------------
def fetch_current_to_next_eom() -> List[Dict[str, Any]]:
    """
    Fetch events from the beginning of LAST month through end of next month.
    """
    now_utc = _now_utc()
    now_local = now_utc.astimezone(LOCAL_TZ)
    
    if now_local.month == 1:
        start_month = 12
        start_year = now_local.year - 1
    else:
        start_month = now_local.month - 1
        start_year = now_local.year
    
    start_local = dt.datetime(start_year, start_month, 1, tzinfo=LOCAL_TZ)
    start_utc = start_local.astimezone(dt.timezone.utc)
    
    y, m = now_local.year, now_local.month
    
    if m == 12:
        next_month = 1
        next_year = y + 1
    else:
        next_month = m + 1
        next_year = y
    
    if next_month == 12:
        end_month = 1
        end_year = next_year + 1
    else:
        end_month = next_month + 1
        end_year = next_year
    
    end_utc = dt.datetime(end_year, end_month, 1, tzinfo=dt.timezone.utc)

    logger.info(f"Fetching events from {start_utc.isoformat()} to {end_utc.isoformat()}")

    if hasattr(cal, "fetch_events_between"):
        result = cal.fetch_events_between(start_utc.isoformat(), end_utc.isoformat())
    else:
        hours = int((end_utc - start_utc).total_seconds() // 3600) + 1
        result = cal.fetch_upcoming_events(hours_ahead=hours)
    
    return result

# ---------------- SSE + Poller ----------------
async def _emit(event_type: str, payload: dict | None = None):
    try:
        await state.sse_queue.put({"type": event_type, "payload": payload or {}, "version": state.version})
    except Exception:
        pass

def _hash_event(ev: Dict[str, Any]) -> str:
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
    try:
        events = await run_in_threadpool(fetch_current_to_next_eom)
        logger.info(f"Fetched {len(events)} events from calendar")

        events_before_scrub = len(events)
        events = [e for e in events if not str(e.get("uid", "")).startswith("mock-")]
        logger.info(f"After scrubbing mock events: {len(events)} events (removed {events_before_scrub - len(events)})")

        meta = await run_in_threadpool(read_cache_meta)

        old_digest = meta.get("events_digest", "")
        new_digest = _events_digest(events)
        changed = (new_digest != old_digest)

        # ALWAYS replace all events - clean slate on every refresh
        meta["events"] = events
        meta["events_digest"] = new_digest
        meta["last_pull_utc"] = _now_utc().isoformat()
        meta.setdefault("refresh_minutes", max(1, state.refresh_seconds // 60))
        mins = int(meta.get("refresh_minutes", 30))
        meta["next_run_utc"] = (_now_utc() + dt.timedelta(minutes=mins)).replace(microsecond=0).isoformat()
        await run_in_threadpool(write_cache_meta, meta)

        if changed:
            state.version += 1
            # Clear stale live rows when calendar changes
            await run_in_threadpool(_clear_stale_live_rows)
            
        await _emit("change", {"changed": changed})
        return changed
    except Exception as e:
        logger.exception("[pull] error: %s", e)
        return False


def _clear_stale_live_rows():
    """Clear all live rows - they'll be rebuilt from fresh data."""
    try:
        with get_db() as c:
            c.execute("DELETE FROM live_rows")
        logger.info("Cleared live_rows table")
    except Exception as e:
        logger.warning(f"Could not clear live_rows: {e}")

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
    # Initialize multiuser tables
    _init_multiuser_tables()
    # Clear stale live rows on startup
    _clear_stale_live_rows()
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
        cur = c.execute("SELECT 1 FROM acks WHERE ack_id=?", (ack_id,)).fetchone()
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
    mode = VIEW_WINDOW_MODE
    now_local = dt.datetime.now(LOCAL_TZ)

    if now_local.month == 1:
        start_month = 12
        start_year = now_local.year - 1
    else:
        start_month = now_local.month - 1
        start_year = now_local.year
    
    if mode == "TODAY_TO_END_OF_NEXT_MONTH":
        start_local = dt.datetime(start_year, start_month, 1, tzinfo=LOCAL_TZ)
        end_local = end_of_next_month_local()
        label = f"{start_local.strftime('%b 1')} – {end_local.strftime('%b %d')}"
        return start_local, end_local, label

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
    Returns TWO arrays:
    - calendar_rows: ALL events including past (for calendar widget) - NO OFF rows
    - rows: Filtered events for table display (future only, WITH OFF calculations between pairings)
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

        # Build ALL rows including past events for calendar (NO OFF rows, NO past filtering)
        all_rows = await run_in_threadpool(
            build_pairing_rows,
            window_events,
            bool(is_24h),
            False,  # only_reports
            False,  # include_off_rows - NO for calendar
            HOME_BASE,
            False,  # filter_past - NO for calendar, show all
            INCLUDE_NON_PAIRING_EVENTS,
        )
        
        # Build table rows from ALL events with OFF rows and past filtering
        # Pass ALL window_events - the pairing builder needs complete multi-day pairings
        # filter_past=True will remove completed pairings after building
        table_rows = await run_in_threadpool(
            build_pairing_rows,
            window_events,  # ALL events so multi-day pairings are complete
            bool(is_24h),
            bool(only_reports),
            True,   # include_off_rows
            HOME_BASE,
            True,   # filter_past - removes completed pairings, keeps in-progress
            INCLUDE_NON_PAIRING_EVENTS,
        )
        
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
        now_local = dt.datetime.now(LOCAL_TZ)

        def fmt_row(r: Dict[str, Any]):
            """Format time displays for a row"""
            if r.get("kind") not in ("pairing", "other"):
                return
            
            pid = str(r.get("pairing_id") or "")
            r["uids"] = pid_to_uids.get(pid, [])
            total_legs = r.get("total_legs", 0)
            r["can_hide"] = (total_legs == 0)
            
            # Backend already determined out_of_base_airport
            # Just set the boolean flag based on whether it exists
            oob_airport = r.get("out_of_base_airport")
            r["out_of_base"] = bool(oob_airport)

            # Format report/release times
            disp = r.setdefault("display", {})
            for key in ["report_hhmm", "release_hhmm"]:
                val = disp.get(key) or r.get(key)
                if val and isinstance(val, str) and val.isdigit():
                    disp[f"{key.split('_')[0]}_str"] = fmt_time(val, use_24h)

            # Format leg times
            for d in (r.get("days") or []):
                for leg in (d.get("legs") or []):
                    for t in ["dep_time", "arr_time"]:
                        if leg.get(t) and not leg.get(f"{t}_str"):
                            leg[f"{t}_str"] = fmt_time(str(leg[t]).zfill(4), use_24h)

        # Format ALL rows for calendar
        calendar_rows = []
        for r in all_rows:
            r_copy = dict(r)
            fmt_row(r_copy)
            calendar_rows.append(r_copy)

        logger.info(f"Built {len(calendar_rows)} calendar rows (including past)")

        # Filter table rows by visibility - OFF rows already calculated correctly
        visible: List[Dict[str, Any]] = []
        
        for r in table_rows:
            fmt_row(r)
            
            # Always keep OFF rows - they were calculated by build_pairing_rows
            if r.get("kind") == "off":
                visible.append(r)
                continue
            
            # Keep other (non-pairing) events
            if r.get("kind") == "other":
                visible.append(r)
                continue
                
            if r.get("kind") != "pairing":
                continue
            
            pid = str(r.get("pairing_id") or "")
            
            # Check if hidden
            all_hidden = (len(r.get("uids", [])) > 0) and all(uid in hidden_uids for uid in r.get("uids", []))
            pid_hidden = pid in hidden_pids
            if (pid_hidden or all_hidden) and (r.get("can_hide") or not r.get("in_progress")):
                continue
            
            visible.append(r)

        # Handle sticky in-progress pairings
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

            if pid in current_feed_pids:
                continue
            if rel_dt and rel_dt >= now_local and not lr.get("can_hide"):
                visible.append(lr)

        for pid in live_to_delete:
            await run_in_threadpool(delete_live_row, pid)

        # Keep live rows updated
        for r in visible:
            if r.get("in_progress") and not r.get("can_hide"):
                await run_in_threadpool(upsert_live_row, r)
        await run_in_threadpool(purge_expired_live, now_local.isoformat())

        # Header meta
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
            "rows": visible,  # Table rows (future events WITH OFF rows between pairings)
            "calendar_rows": calendar_rows,  # All rows for calendar (including past, NO OFF rows)
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

@app.post("/api/hide")
async def api_hide(payload: Dict[str, Any] = Body(...)):
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
    await run_in_threadpool(unhide_all)
    state.version += 1
    await _emit("hidden_update", {"cleared": True})
    return {"ok": True}

from pydantic import BaseModel
from fastapi import HTTPException as _HTTPExceptionAlias
import logging as _logging

from modules.db import hidden_add, hidden_clear_all, hidden_count as hidden_pairing_count

log = _logging.getLogger("dutywatch")

class _HideReq(BaseModel):
    pairing_id: str
    report_local_iso: str | None = None

# ---------------- Hidden pairing endpoints ----------------

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


# ============================================================================
# MULTI-USER SUPPORT
# ============================================================================
# Import multiuser functions from db.py and cal_client.py
from modules.db import (
    init_multiuser_tables,
    create_user, get_user_by_username, get_user_by_id, update_user_credentials,
    get_user_decrypted_credentials, list_all_users,
    get_user_events_cache, set_user_events_cache,
    user_hidden_add, user_hidden_list, user_hidden_clear, user_hidden_count,
    get_user_profile, save_user_profile
)
from modules.cal_client import (
    list_calendars_for_user, diagnose_user_connection, fetch_events_for_user
)

multiuser_logger = logging.getLogger("dutywatch.multiuser")

def _init_multiuser_tables():
    """Initialize multi-user tables on startup."""
    with get_db() as conn:
        init_multiuser_tables(conn)

# List of reserved paths that should NOT be treated as usernames
RESERVED_PATHS = {
    'api', 'pairings', 'medical', 'med_portal', 'health', 'static',
    'favicon.ico', 'robots.txt', 'setup', 'admin', 'login', 'register'
}

# ==========================================
# User Setup / Registration
# ==========================================

@app.post("/api/users/register")
async def register_user(payload: Dict[str, Any] = Body(...)):
    """Register a new user."""
    username = str(payload.get("username") or "").strip().lower()
    if not username:
        raise HTTPException(400, "username is required")
    
    display_name = payload.get("display_name") or username
    timezone = payload.get("timezone", "America/Chicago")
    home_base = payload.get("home_base", "DFW").upper()
    
    def do_create():
        with get_db() as conn:
            existing = get_user_by_username(conn, username)
            if existing:
                raise HTTPException(409, "Username already exists")
            
            user_id = create_user(
                conn, username, display_name,
                timezone=timezone, home_base=home_base
            )
            return user_id
    
    try:
        user_id = await run_in_threadpool(do_create)
        return {
            "ok": True,
            "user_id": user_id,
            "username": username,
            "url": f"/{username}"
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        multiuser_logger.exception("Failed to create user")
        raise HTTPException(500, f"Failed to create user: {e}")


@app.post("/api/users/{username}/setup-credentials")
async def setup_user_credentials(username: str, payload: Dict[str, Any] = Body(...)):
    """Set up iCloud credentials for a user."""
    icloud_user = str(payload.get("icloud_user") or "").strip()
    icloud_app_pw = str(payload.get("icloud_app_pw") or "").strip()
    
    if not icloud_user or not icloud_app_pw:
        raise HTTPException(400, "icloud_user and icloud_app_pw are required")
    
    def do_setup():
        with get_db() as conn:
            user = get_user_by_username(conn, username)
            if not user:
                raise HTTPException(404, "User not found")
            
            diag = diagnose_user_connection(icloud_user, icloud_app_pw)
            if not diag.get("ok"):
                raise HTTPException(400, f"iCloud connection failed: {diag.get('error')}")
            
            update_user_credentials(conn, user["id"], icloud_user, icloud_app_pw)
            
            return {
                "ok": True,
                "calendars": diag.get("calendars", [])
            }
    
    try:
        return await run_in_threadpool(do_setup)
    except HTTPException:
        raise
    except Exception as e:
        multiuser_logger.exception("Failed to setup credentials")
        raise HTTPException(500, f"Setup failed: {e}")


@app.get("/api/users/{username}/calendars")
async def list_user_calendars(username: str):
    """List available calendars for a user."""
    def do_list():
        with get_db() as conn:
            user = get_user_by_username(conn, username)
            if not user:
                raise HTTPException(404, "User not found")
            
            creds = get_user_decrypted_credentials(conn, user["id"])
            if not creds or not creds.get("icloud_user"):
                raise HTTPException(400, "Credentials not configured")
            
            calendars = list_calendars_for_user(
                creds["icloud_user"],
                creds["icloud_app_pw"],
                creds.get("caldav_url", "https://caldav.icloud.com/")
            )
            return calendars
    
    try:
        calendars = await run_in_threadpool(do_list)
        return {"calendars": calendars}
    except HTTPException:
        raise
    except Exception as e:
        multiuser_logger.exception("Failed to list calendars")
        raise HTTPException(500, f"Failed to list calendars: {e}")


@app.post("/api/users/{username}/select-calendar")
async def select_user_calendar(username: str, payload: Dict[str, Any] = Body(...)):
    """Select which calendar to use for this user."""
    calendar_name = str(payload.get("calendar_name") or "").strip()
    if not calendar_name:
        raise HTTPException(400, "calendar_name is required")
    
    def do_select():
        with get_db() as conn:
            user = get_user_by_username(conn, username)
            if not user:
                raise HTTPException(404, "User not found")
            
            update_user_credentials(conn, user["id"], calendar_name=calendar_name)
            return True
    
    try:
        await run_in_threadpool(do_select)
        return {"ok": True, "calendar_name": calendar_name}
    except HTTPException:
        raise
    except Exception as e:
        multiuser_logger.exception("Failed to select calendar")
        raise HTTPException(500, f"Failed to select calendar: {e}")


# ==========================================
# Per-User Pairings API
# ==========================================

@app.get("/api/u/{username}/pairings")
async def get_user_pairings(
    username: str,
    is_24h: int = Query(default=0),
    only_reports: int = Query(default=1)
):
    """Get pairings for a specific user."""
    def do_fetch():
        with get_db() as conn:
            user = get_user_by_username(conn, username)
            if not user:
                raise HTTPException(404, "User not found")
            
            creds = get_user_decrypted_credentials(conn, user["id"])
            if not creds or not creds.get("icloud_user"):
                raise HTTPException(400, "Credentials not configured. Please complete setup.")
            
            # Calculate date range
            tz = ZoneInfo(user.get("timezone", "America/Chicago"))
            now_local = dt.datetime.now(tz)
            
            # Start from beginning of last month
            if now_local.month == 1:
                start_month, start_year = 12, now_local.year - 1
            else:
                start_month, start_year = now_local.month - 1, now_local.year
            
            start_local = dt.datetime(start_year, start_month, 1, tzinfo=tz)
            
            # End at end of next month
            if now_local.month == 12:
                next_month, next_year = 1, now_local.year + 1
            else:
                next_month, next_year = now_local.month + 1, now_local.year
            
            if next_month == 12:
                end_month, end_year = 1, next_year + 1
            else:
                end_month, end_year = next_month + 1, next_year
            
            end_utc = dt.datetime(end_year, end_month, 1, tzinfo=dt.timezone.utc)
            start_utc = start_local.astimezone(dt.timezone.utc)
            
            # Fetch events
            events = fetch_events_for_user(
                creds["icloud_user"],
                creds["icloud_app_pw"],
                creds.get("caldav_url", "https://caldav.icloud.com/"),
                creds.get("calendar_name"),
                start_utc.isoformat(),
                end_utc.isoformat()
            )
            
            # Cache events
            set_user_events_cache(conn, user["id"], events)
            
            # Get hidden items
            hidden_pids = user_hidden_list(conn, user["id"])
            hidden_cnt = user_hidden_count(conn, user["id"])
            
            # Get profile
            profile = get_user_profile(conn, user["id"])
            
            return {
                "user": user,
                "events": events,
                "hidden_pids": set(hidden_pids),
                "hidden_count": hidden_cnt,
                "profile": profile,
                "tz": tz,
                "home_base": user.get("home_base", "DFW")
            }
    
    try:
        data = await run_in_threadpool(do_fetch)
        
        # Build rows using the existing build_pairing_rows function
        rows = await run_in_threadpool(
            build_pairing_rows,
            data["events"],
            bool(is_24h),
            bool(only_reports),
            True,  # include_off_rows
            data["home_base"],
            True,  # filter_past
            False  # include_non_pairing_events
        )
        
        # Filter out hidden
        visible = [r for r in rows if r.get("pairing_id") not in data["hidden_pids"] or r.get("kind") == "off"]
        
        return {
            "rows": visible,
            "calendar_rows": rows,
            "profile": data["profile"],
            "hidden_count": data["hidden_count"],
            "tz_label": str(data["tz"]).split("/")[-1],
            "home_base": data["home_base"]
        }
    except HTTPException:
        raise
    except Exception as e:
        multiuser_logger.exception("Failed to get pairings")
        return JSONResponse(
            status_code=500,
            content={"error": "pairings_failed", "message": str(e)}
        )


@app.post("/api/u/{username}/refresh")
async def refresh_user_pairings(username: str):
    """Force refresh pairings for a user."""
    return await get_user_pairings(username)


@app.post("/api/u/{username}/hidden/hide")
async def hide_user_pairing(username: str, payload: Dict[str, Any] = Body(...)):
    """Hide a pairing for a user."""
    pairing_id = str(payload.get("pairing_id") or "").strip()
    if not pairing_id:
        raise HTTPException(400, "pairing_id required")
    
    def do_hide():
        with get_db() as conn:
            user = get_user_by_username(conn, username)
            if not user:
                raise HTTPException(404, "User not found")
            
            user_hidden_add(conn, user["id"], pairing_id, payload.get("report_local_iso"))
            return user_hidden_count(conn, user["id"])
    
    count = await run_in_threadpool(do_hide)
    return {"ok": True, "hidden_count": count}


@app.post("/api/u/{username}/hidden/unhide_all")
async def unhide_all_user_pairings(username: str):
    """Unhide all pairings for a user."""
    def do_unhide():
        with get_db() as conn:
            user = get_user_by_username(conn, username)
            if not user:
                raise HTTPException(404, "User not found")
            
            count = user_hidden_clear(conn, user["id"])
            return count
    
    count = await run_in_threadpool(do_unhide)
    return {"ok": True, "cleared": count}


# ==========================================
# Per-User Profile API
# ==========================================

@app.get("/api/u/{username}/profile")
async def get_user_profile_api(username: str):
    """Get user profile."""
    def do_get():
        with get_db() as conn:
            user = get_user_by_username(conn, username)
            if not user:
                raise HTTPException(404, "User not found")
            return get_user_profile(conn, user["id"])
    
    return await run_in_threadpool(do_get)


@app.post("/api/u/{username}/profile")
async def update_user_profile(username: str, payload: Dict[str, Any] = Body(...)):
    """Update user profile."""
    def do_update():
        with get_db() as conn:
            user = get_user_by_username(conn, username)
            if not user:
                raise HTTPException(404, "User not found")
            
            save_user_profile(
                conn, user["id"],
                str(payload.get("firstName") or "").strip(),
                str(payload.get("lastName") or "").strip(),
                payload.get("photo")
            )
            return True
    
    await run_in_threadpool(do_update)
    return {"ok": True}


# ==========================================
# Admin Routes
# ==========================================

@app.get("/api/admin/users")
async def admin_list_users():
    """List all users (admin only - add auth as needed)."""
    def do_list():
        with get_db() as conn:
            return list_all_users(conn)
    
    users = await run_in_threadpool(do_list)
    return {"users": users}


# ==========================================
# User Page Routes (HTML) - MUST BE LAST!
# ==========================================

@app.get("/{username}/setup", response_class=HTMLResponse)
async def user_setup_page(username: str):
    """Serve the setup page for a new user."""
    if username.lower() in RESERVED_PATHS:
        raise HTTPException(404, "Not found")
    
    def check_user():
        with get_db() as conn:
            return get_user_by_username(conn, username)
    
    user = await run_in_threadpool(check_user)
    if not user:
        raise HTTPException(404, "User not found")
    
    # Inline setup page HTML
    return f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>Setup - DutyWatch</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            :root {{ color-scheme: dark; --bg: #0b0f14; --card: #121821; --border: #1e2a38; --text: #e8edf2; --accent: #6fb1ff; }}
            * {{ box-sizing: border-box; }}
            body {{ margin: 0; background: var(--bg); color: var(--text); font: 14px/1.5 system-ui, sans-serif; padding: 20px; }}
            .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 24px; max-width: 500px; margin: 40px auto; }}
            h1 {{ margin: 0 0 20px; font-size: 24px; }}
            .field {{ margin-bottom: 16px; }}
            label {{ display: block; margin-bottom: 6px; color: #9fb0c0; }}
            input, select {{ width: 100%; padding: 10px 12px; background: var(--bg); border: 1px solid var(--border); border-radius: 8px; color: var(--text); font-size: 14px; }}
            .btn {{ background: var(--accent); color: #031323; border: 0; border-radius: 10px; padding: 12px 20px; font-weight: 600; cursor: pointer; width: 100%; font-size: 16px; }}
            .btn:disabled {{ opacity: 0.6; cursor: not-allowed; }}
            .help {{ font-size: 12px; color: #9fb0c0; margin-top: 6px; }}
            .help a {{ color: var(--accent); }}
            .error {{ color: #ff6b6b; margin: 12px 0; }}
            .success {{ color: #98f5a7; margin: 12px 0; }}
            .step {{ display: none; }}
            .step.active {{ display: block; }}
            .logo {{ text-align: center; margin-bottom: 20px; font-size: 28px; }}
        </style>
    </head>
    <body>
        <div class="card">
            <div class="logo">✈️ DutyWatch</div>
            <h1>Welcome, {user.get('display_name', username)}!</h1>
            <p>Let's connect your iCloud calendar to DutyWatch.</p>
            
            <div id="step1" class="step active">
                <div class="field">
                    <label>iCloud Email</label>
                    <input type="email" id="icloud-user" placeholder="your@icloud.com" autocomplete="email">
                </div>
                <div class="field">
                    <label>App-Specific Password</label>
                    <input type="text" id="icloud-pw" placeholder="xxxx-xxxx-xxxx-xxxx" autocomplete="off" 
                           style="font-family: monospace; letter-spacing: 1px;">
                    <div class="help">
                        Create one at <a href="https://appleid.apple.com/account/manage" target="_blank">appleid.apple.com</a><br>
                        Sign-In and Security → App-Specific Passwords → Generate
                    </div>
                </div>
                <div id="step1-error" class="error" style="display:none"></div>
                <button class="btn" id="connect-btn">Connect to iCloud</button>
            </div>
            
            <div id="step2" class="step">
                <div class="success">✓ Connected to iCloud!</div>
                <div class="field">
                    <label>Select Calendar</label>
                    <select id="calendar-select">
                        <option value="">Loading calendars...</option>
                    </select>
                    <div class="help">Choose the calendar containing your flight pairings</div>
                </div>
                <div id="step2-error" class="error" style="display:none"></div>
                <button class="btn" id="finish-btn">Finish Setup</button>
            </div>
            
            <div id="step3" class="step">
                <div class="success" style="font-size: 18px; text-align: center; padding: 20px 0;">
                    ✓ Setup Complete!
                </div>
                <p style="text-align: center;">Your DutyWatch is ready. Redirecting...</p>
            </div>
        </div>
        
        <script>
            const username = '{username}';
            const step1 = document.getElementById('step1');
            const step2 = document.getElementById('step2');
            const step3 = document.getElementById('step3');
            
            document.getElementById('connect-btn').addEventListener('click', async () => {{
                const user = document.getElementById('icloud-user').value.trim();
                const pw = document.getElementById('icloud-pw').value.trim();
                const errEl = document.getElementById('step1-error');
                
                if (!user || !pw) {{
                    errEl.textContent = 'Please enter both email and password';
                    errEl.style.display = 'block';
                    return;
                }}
                
                const btn = document.getElementById('connect-btn');
                btn.disabled = true;
                btn.textContent = 'Connecting...';
                errEl.style.display = 'none';
                
                try {{
                    const res = await fetch(`/api/users/${{username}}/setup-credentials`, {{
                        method: 'POST',
                        headers: {{'Content-Type': 'application/json'}},
                        body: JSON.stringify({{icloud_user: user, icloud_app_pw: pw}})
                    }});
                    const data = await res.json();
                    
                    if (!res.ok) {{
                        throw new Error(data.detail || 'Connection failed');
                    }}
                    
                    const select = document.getElementById('calendar-select');
                    select.innerHTML = data.calendars.map(c => 
                        `<option value="${{c}}">${{c}}</option>`
                    ).join('');
                    
                    step1.classList.remove('active');
                    step2.classList.add('active');
                }} catch (e) {{
                    errEl.textContent = e.message;
                    errEl.style.display = 'block';
                    btn.disabled = false;
                    btn.textContent = 'Connect to iCloud';
                }}
            }});
            
            document.getElementById('finish-btn').addEventListener('click', async () => {{
                const cal = document.getElementById('calendar-select').value;
                const errEl = document.getElementById('step2-error');
                
                if (!cal) {{
                    errEl.textContent = 'Please select a calendar';
                    errEl.style.display = 'block';
                    return;
                }}
                
                const btn = document.getElementById('finish-btn');
                btn.disabled = true;
                btn.textContent = 'Saving...';
                errEl.style.display = 'none';
                
                try {{
                    const res = await fetch(`/api/users/${{username}}/select-calendar`, {{
                        method: 'POST',
                        headers: {{'Content-Type': 'application/json'}},
                        body: JSON.stringify({{calendar_name: cal}})
                    }});
                    
                    if (!res.ok) {{
                        const data = await res.json();
                        throw new Error(data.detail || 'Failed to save');
                    }}
                    
                    step2.classList.remove('active');
                    step3.classList.add('active');
                    
                    setTimeout(() => {{
                        window.location.href = `/${{username}}`;
                    }}, 1500);
                }} catch (e) {{
                    errEl.textContent = e.message;
                    errEl.style.display = 'block';
                    btn.disabled = false;
                    btn.textContent = 'Finish Setup';
                }}
            }});
            
            document.getElementById('icloud-pw').addEventListener('keypress', (e) => {{
                if (e.key === 'Enter') document.getElementById('connect-btn').click();
            }});
        </script>
    </body>
    </html>
    """


@app.get("/{username}", response_class=HTMLResponse)
async def user_pairings_page(username: str):
    """
    Serve the pairings page for a specific user.
    IMPORTANT: This route must be registered LAST!
    """
    if username.lower() in RESERVED_PATHS:
        raise HTTPException(404, "Not found")
    
    def check_user():
        with get_db() as conn:
            return get_user_by_username(conn, username)
    
    user = await run_in_threadpool(check_user)
    if not user:
        raise HTTPException(404, "User not found")
    
    # Check if setup is complete
    has_creds = bool(user.get("icloud_user") and user.get("icloud_app_pw_encrypted"))
    has_calendar = bool(user.get("calendar_name"))
    
    if not has_creds or not has_calendar:
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=f"/{username}/setup", status_code=302)
    
    # Redirect to pairings page with user context
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>DutyWatch - {user.get('display_name', username)}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
    </head>
    <body>
        <script>
            window.location.href = '/pairings/?user={username}';
        </script>
        <noscript>
            <meta http-equiv="refresh" content="0;url=/pairings/?user={username}">
        </noscript>
    </body>
    </html>
    """