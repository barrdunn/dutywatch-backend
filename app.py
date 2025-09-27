from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import time
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

import cal_client as cal
from cache_meta import read_cache_meta, write_cache_meta, normalize_cached_events
from rows import build_pairing_rows, end_of_next_month_local
from db import init_db

# ---- central settings (single source of truth) ----
from config import (
    TIMEZONE,
    VIEW_WINDOW_MODE,
    DEFAULT_REFRESH_MINUTES,
    REFRESH_SECONDS,
)

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

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger("dutywatch")

@app.middleware("http")
async def timing_and_errors(request, call_next):
    start = time.time()
    try:
        return await call_next(request)
    except Exception:
        logger.exception("Unhandled error on %s %s", request.method, request.url.path)
        raise
    finally:
        dur = int((time.time() - start) * 1000)
        logger.info("%s %s -> %dms", request.method, request.url.path, dur)

# ---------------- Config / Time ----------------
LOCAL_TZ = ZoneInfo(TIMEZONE)

class State:
    refresh_seconds: int = REFRESH_SECONDS
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
    secs = int((now - from_dt).total_seconds())
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

# ---------------- Viewing window (SOT) ----------------
def compute_window_bounds_local() -> Tuple[dt.datetime, dt.datetime, str]:
    """
    Computes the viewing window in LOCAL time from VIEW_WINDOW_MODE.
    Returns (start_local, end_local, label).
    """
    mode = VIEW_WINDOW_MODE
    now_local = dt.datetime.now(LOCAL_TZ)

    if mode == "TODAY_TO_END_OF_NEXT_MONTH":
        start_local = now_local
        end_local = end_of_next_month_local()
        label = f"Today – {end_local.strftime('%b %d (%a)')}"
        return start_local, end_local, label

    # Fallback to default
    start_local = now_local
    end_local = end_of_next_month_local()
    label = f"Today – {end_local.strftime('%b %d (%a)')}"
    return start_local, end_local, label

def window_bounds_utc() -> Tuple[dt.datetime, dt.datetime, dt.datetime, dt.datetime, str]:
    """Return (start_utc, end_utc, start_local, end_local, label)."""
    start_local, end_local, label = compute_window_bounds_local()
    return to_utc(start_local), to_utc(end_local), start_local, end_local, label

# ---------------- Calendar fetch (uses window SOT) ----------------
def fetch_for_window() -> List[Dict[str, Any]]:
    start_utc, end_utc, _, _, _ = window_bounds_utc()
    if hasattr(cal, "fetch_events_between"):
        return cal.fetch_events_between(start_utc.isoformat(), end_utc.isoformat())
    hours = int((end_utc - start_utc).total_seconds() // 3600) + 1
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
        events = await run_in_threadpool(fetch_for_window)
        meta = await run_in_threadpool(read_cache_meta)

        changed = json.dumps(meta.get("events", []), sort_keys=True) != json.dumps(events, sort_keys=True)

        meta["events"] = events
        meta["last_pull_utc"] = _now_utc().isoformat()
        meta.setdefault("refresh_minutes", max(1, state.refresh_seconds // 60))
        mins = int(meta.get("refresh_minutes", DEFAULT_REFRESH_MINUTES))
        meta["next_run_utc"] = (_now_utc() + dt.timedelta(minutes=mins)).replace(microsecond=0).isoformat()
        await run_in_threadpool(write_cache_meta, meta)

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

# ---------------- API Routes ----------------
@app.get("/api/config")
def api_config():
    from config import TIMEZONE as TZ, VIEW_WINDOW_MODE as MODE, DEFAULT_REFRESH_MINUTES as REF_MINS
    start_utc, end_utc, start_local, end_local, label = window_bounds_utc()
    return {
        "timezone": TZ,
        "window_mode": MODE,
        "window": {
            "start_local_iso": start_local.isoformat(),
            "end_local_iso": end_local.isoformat(),
            "label": label,
        },
        "default_refresh_minutes": REF_MINS,
    }

@app.get("/api/pairings")
async def api_pairings(
    # kept for compatibility; window is authoritative
    year: Optional[int] = Query(default=None, ge=1970, le=2100),
    month: Optional[int] = Query(default=None, ge=1, le=12),
    only_reports: int = Query(default=1),
    is_24h: int = Query(default=0),
):
    try:
        meta = await run_in_threadpool(read_cache_meta)
        events = normalize_cached_events(meta)

        # EXACT window used for fetching and display
        start_utc, end_utc, start_local, end_local, window_label = window_bounds_utc()

        UTC_MIN = dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        def safe_start(ev: Dict[str, Any]) -> dt.datetime:
            s = iso_to_dt(ev.get("start_utc"))
            try:
                return s if (s and s.tzinfo) else (s or UTC_MIN).replace(tzinfo=dt.timezone.utc)
            except Exception:
                return UTC_MIN

        window_events = [e for e in events if start_utc <= safe_start(e) < end_utc]
        rows = await run_in_threadpool(build_pairing_rows, window_events, bool(is_24h), bool(only_reports))

        lp_iso = meta.get("last_pull_utc")
        nr_iso = meta.get("next_run_utc")

        lp_local = to_local(iso_to_dt(lp_iso)) if lp_iso else None
        nr_local = to_local(iso_to_dt(nr_iso)) if nr_iso else None

        last_pull_local = human_ago_precise(lp_local)
        last_pull_human_simple = human_ago(lp_local)

        next_refresh_local_clock = nr_local.strftime("%I:%M %p").lstrip("0") if nr_local else ""
        seconds_to_next = max(0, int((nr_local - dt.datetime.now(LOCAL_TZ)).total_seconds())) if nr_local else 0

        window_obj = {
            "start_local_iso": start_local.isoformat(),
            "end_local_iso": end_local.isoformat(),
            "label": window_label,
        }

        return {
            "window": window_obj,                    # <-- source of truth the UI shows
            "looking_through": window_label,         # legacy field
            "last_pull_local": last_pull_local,
            "last_pull_local_simple": last_pull_human_simple,
            "last_pull_local_iso": lp_local.isoformat() if lp_local else "",
            "next_pull_local": next_refresh_local_clock,
            "next_pull_local_iso": nr_local.isoformat() if nr_local else "",
            "seconds_to_next": seconds_to_next,
            "tz_label": "CT",
            "rows": rows,
            "version": state.version,
            "refresh_minutes": int(meta.get("refresh_minutes", max(1, state.refresh_seconds // 60))),
        }
    except Exception as e:
        logger.exception("api_pairings failed: %s", e)
        return JSONResponse(status_code=500, content={"error":"pairings_failed","message":str(e)})

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
    secs = int(payload.get("seconds", 0))
    if secs < 15 or secs > 21600:
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
