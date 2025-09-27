from __future__ import annotations

import os
import asyncio
import datetime as dt
import json
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from fastapi import FastAPI, Query, HTTPException, Body
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool
from zoneinfo import ZoneInfo

import cal_client as cal
from cache_meta import read_cache_meta, write_cache_meta, normalize_cached_events
from rows import build_pairing_rows, end_of_next_month_local
from db import get_db, init_db  # for ack state + boot DB

# ---------------- Paths / Static ----------------
BASE_DIR = Path(__file__).parent.resolve()
PAIRINGS_DIR = BASE_DIR / "public" / "pairings"

app = FastAPI(title="DutyWatch Backend (Viewer-Only)")

# Serve the pairings folder at /pairings (serves index.html at /pairings/)
app.mount("/pairings", StaticFiles(directory=PAIRINGS_DIR, html=True), name="pairings")

# Also serve /pairings (no trailing slash) directly without redirect
@app.get("/pairings", include_in_schema=False)
def pairings_index_direct():
    return FileResponse(PAIRINGS_DIR / "index.html")

# Optionally make it the root page too (comment out if undesired)
@app.get("/", include_in_schema=False)
def root_index():
    return FileResponse(PAIRINGS_DIR / "index.html")


# ---------------- Config / Time ----------------
LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "America/Chicago"))

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

        # update meta
        meta["events"] = events
        meta["last_pull_utc"] = _now_utc().isoformat()
        meta.setdefault("refresh_minutes", max(1, state.refresh_seconds // 60))
        mins = int(meta.get("refresh_minutes", 30))
        meta["next_run_utc"] = (_now_utc() + dt.timedelta(minutes=mins)).replace(microsecond=0).isoformat()
        await run_in_threadpool(write_cache_meta, meta)

        if changed:
            state.version += 1

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


# ---------------- Ack / Check-in policy (simulation) ----------------
ACK_POLICY = {
    "window_open_hours": 12,
    "second_push_at_hours": 6,
    "call_start_hours": 3,
    "call_interval_minutes": 15,
    "calls_per_attempt": 2,
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

def _plan_attempts(report_local: dt.datetime) -> List[Dict[str, Any]]:
    p = ACK_POLICY
    at: List[Dict[str, Any]] = []
    at.append({"kind":"push","label":"Initial push","at_iso":(report_local - dt.timedelta(hours=p["window_open_hours"])).isoformat(),"meta":{}})
    at.append({"kind":"push","label":"Second push","at_iso":(report_local - dt.timedelta(hours=p["second_push_at_hours"])).isoformat(),"meta":{}})
    t = report_local - dt.timedelta(hours=p["call_start_hours"])
    while t < report_local:
        at.append({"kind":"call","label":"Call attempt (ring 1/2)","at_iso":t.isoformat(),"meta":{"ring":1}})
        at.append({"kind":"call","label":"Call attempt (ring 2/2)","at_iso":(t + dt.timedelta(minutes=1)).isoformat(),"meta":{"ring":2}})
        t = t + dt.timedelta(minutes=p["call_interval_minutes"])
    at.sort(key=lambda x: x["at_iso"])
    return at

def _window_state(report_local: Optional[dt.datetime]) -> Dict[str, Any]:
    now_local = dt.datetime.now(LOCAL_TZ)
    if not report_local:
        return {"window_open": False, "seconds_until_open": None, "seconds_until_report": None}
    open_at = report_local - dt.timedelta(hours=ACK_POLICY["window_open_hours"])
    return {
        "window_open": open_at <= now_local <= report_local,
        "seconds_until_open": max(0, int((open_at - now_local).total_seconds())) if now_local < open_at else 0,
        "seconds_until_report": max(0, int((report_local - now_local).total_seconds())) if now_local < report_local else 0,
    }


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
    meta = await run_in_threadpool(read_cache_meta)
    events = normalize_cached_events(meta)

    def month_bounds(y: int, m: int) -> Tuple[dt.datetime, dt.datetime]:
        start = dt.datetime(y, m, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(y + (m // 12), (m % 12) + 1, 1, tzinfo=dt.timezone.utc)
        return start, end

    # ---- FIX: use UTC-aware minimum when an event has no/invalid start time
    UTC_MIN = dt.datetime.min.replace(tzinfo=dt.timezone.utc)

    def start_utc_of(ev: Dict[str, Any]) -> dt.datetime:
        s = iso_to_dt(ev.get("start_utc"))
        if not s:
            return UTC_MIN
        return s if s.tzinfo else s.replace(tzinfo=dt.timezone.utc)

    now_local = dt.datetime.now(LOCAL_TZ)
    y, m = (year or now_local.year, month or now_local.month)
    start_utc, end_utc = month_bounds(y, m)

    month_events = [e for e in events if start_utc <= start_utc_of(e) < end_utc]

    rows = await run_in_threadpool(build_pairing_rows, month_events, bool(is_24h), bool(only_reports))

    # Attach ack/check-in info to pairing rows
    enriched: List[Dict[str, Any]] = []
    for r in rows:
        if r.get("kind") != "pairing":
            enriched.append(r)
            continue
        pairing_id = str(r.get("pairing_id") or "")
        report_iso = r.get("report_local_iso") or ""
        report_local = to_local(iso_to_dt(report_iso)) if report_iso else None

        win = _window_state(report_local)
        ackid = _ack_id(pairing_id, report_iso)
        state = _read_ack_state(ackid)
        r["ack"] = {
            "ack_id": ackid,
            "window_open": bool(win["window_open"]),
            "acknowledged": (state == "ack"),
            "seconds_until_open": win["seconds_until_open"],
            "seconds_until_report": win["seconds_until_report"],
        }
        enriched.append(r)

    lp_iso = meta.get("last_pull_utc")
    nr_iso = meta.get("next_run_utc")

    lp_local = to_local(iso_to_dt(lp_iso)) if lp_iso else None
    nr_local = to_local(iso_to_dt(nr_iso)) if nr_iso else None

    last_pull_local = human_ago_precise(lp_local)
    last_pull_human_simple = human_ago(lp_local)

    next_refresh_local_clock = nr_local.strftime("%I:%M %p").lstrip("0") if nr_local else ""
    seconds_to_next = max(0, int((nr_local - dt.datetime.now(LOCAL_TZ)).total_seconds())) if nr_local else 0

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
        "rows": enriched,
        "version": state.version,
        "year": y,
        "month": m,
        "refresh_minutes": int(meta.get("refresh_minutes", max(1, state.refresh_seconds // 60))),
        "ack_policy": ACK_POLICY,
    }

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
        yield f"event: hello\ndata: {json.dumps({'version': state.version})}\n\n"
        while not state.shutdown_event.is_set():
            msg = await state.sse_queue.get()
            evt_type = msg.get("type", "change")
            yield f"event: {evt_type}\ndata: {json.dumps(msg)}\n\n"
    return StreamingResponse(event_stream(), media_type="text/event-stream")
