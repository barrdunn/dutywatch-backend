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
from rows import build_pairing_rows, end_of_next_month_local
from db import get_db, init_db

# ---------------- Paths / Static ----------------
BASE_DIR = Path(__file__).parent.resolve()
PAIRINGS_DIR = BASE_DIR / "public" / "pairings"

app = FastAPI(title="DutyWatch Backend (Viewer-Only)")

# Serve static front-end
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

# SINGLE SOURCE OF TRUTH for the viewing window
# Supported:
#   - TODAY_TO_END_OF_NEXT_MONTH  (default)
VIEW_WINDOW_MODE = os.getenv("VIEW_WINDOW_MODE", "TODAY_TO_END_OF_NEXT_MONTH").upper().strip()

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

# --- time formatting helper (added) ---
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
        mins = int(meta.get("refresh_minutes", 30))
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
    # /* ADDED */ _ensure_hidden_table()
    _ensure_hidden_table()  # /* ADDED */
    # /* ADDED END */
    state.poll_task = asyncio.create_task(poller_loop())
    try:
        yield
    finally:
        state.shutdown_event.set()
        if state.poll_task:
            state.poll_task.cancel()
            with suppress(asyncio.CancelledError):
                await state.poll_task

# ---------------- Ack / Check-in policy (updated + quiet hours) ----------------
ACK_POLICY = {
    # Push notifications: hourly from T-12h until T-4h (exclusive)
    "push_start_hours": 12,            # begin pushes at T-12h
    "push_stop_hours": 4,              # stop pushes at T-4h
    "push_interval_minutes": 60,       # hourly pushes

    # Calls (default cadence)
    "call_start_hours": 4,             # begin calls at T-4h
    "call_interval_minutes": 30,       # every 30 minutes
    "calls_per_attempt": 2,            # ring twice per attempt

    # Quiet-hours rule (local time)
    "quiet_start_hour": 0,             # 00:00 local
    "quiet_end_hour": 6,               # 06:00 local (exclusive)
    "quiet_last_hour_minutes": 60,     # within last 60 min to report we allow calls
    "quiet_interval_minutes": 15,      # cadence during quiet last-hour window
}

def _ack_id(pairing_id: str, report_local_iso: str) -> str:
    base = f"{pairing_id}|{report_local_iso}"
    return hashlib.sha256(base.encode("utf-8")).encode("utf-8").hex()[:16]  # /* CHANGED */  /* CHANGED */

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
    """Return True if local time is within [quiet_start_hour, quiet_end_hour)."""
    qs = ACK_POLICY["quiet_start_hour"]
    qe = ACK_POLICY["quiet_end_hour"]
    h = t_local.hour
    # supports ranges that may cross midnight (not used here, but future-proof)
    if qs <= qe:
        return qs <= h < qe
    else:
        return h >= qs or h < qe

def _plan_attempts(report_local: dt.datetime) -> List[Dict[str, Any]]:
    p = ACK_POLICY
    items: List[Dict[str, Any]] = []

    # ---- Pushes every hour: [report - push_start, report - push_stop)
    t_start = report_local - dt.timedelta(hours=p["push_start_hours"])
    t_stop  = report_local - dt.timedelta(hours=p["push_stop_hours"])
    t = t_start
    while t < t_stop:
        items.append({"kind": "push", "label": "Push reminder", "at_iso": t.isoformat(), "meta": {}})
        t += dt.timedelta(minutes=p["push_interval_minutes"])

    # ---- Calls:
    # Base cadence: every call_interval_minutes from T-4h to report (two rings per attempt).
    # Quiet hours override:
    #   - If candidate call time is between 00:00–06:00 local, skip it unless it's within the last 60 min.
    #   - Within last 60 min AND in quiet hours, switch to 15-minute cadence.
    base_interval = dt.timedelta(minutes=p["call_interval_minutes"])
    quiet_interval = dt.timedelta(minutes=p["quiet_interval_minutes"])
    last_hour = dt.timedelta(minutes=p["quiet_last_hour_minutes"])

    call_window_start = report_local - dt.timedelta(hours=p["call_start_hours"])

    # Phase A: standard 30-min calls across the whole window (skipping quiet-hour times > 60m from report)
    t = call_window_start
    while t < report_local:
        t_local = t.astimezone(LOCAL_TZ)
        until_report = report_local - t
        if _is_quiet_hour(t_local) and until_report > last_hour:
            # In quiet hours and not within last hour: skip
            t += base_interval
            continue

        # schedule this attempt (two rings, 1 min apart)
        for ring in range(1, p["calls_per_attempt"] + 1):
            ts = t if ring == 1 else (t + dt.timedelta(minutes=1))
            items.append({
                "kind": "call",
                "label": f"Call attempt (ring {ring}/{p['calls_per_attempt']})",
                "at_iso": ts.isoformat(),
                "meta": {"ring": ring}
            })

        t += base_interval

    # Phase B: ensure 15-min cadence inside the last hour *when the time falls in quiet hours*
    t0 = max(call_window_start, report_local - last_hour)
    t = t0
    seen = set(i["at_iso"] for i in items)  # dedupe
    while t < report_local:
        t_local = t.astimezone(LOCAL_TZ)
        if _is_quiet_hour(t_local):
            for ring in range(1, p["calls_per_attempt"] + 1):
                ts = t if ring == 1 else (t + dt.timedelta(minutes=1))
                iso = ts.isoformat()
                if iso not in seen:
                    seen.add(iso)
                    items.append({
                        "kind": "call",
                        "label": f"Call attempt (ring {ring}/{p['calls_per_attempt']})",
                        "at_iso": iso,
                        "meta": {"ring": ring}
                    })
        t += quiet_interval

    items.sort(key=lambda x: x["at_iso"])
    return items

def _window_state(report_local: Optional[dt.datetime]) -> Dict[str, Any]:
    """
    Window is considered "open" from the first push time (report - push_start_hours)
    until the report time.
    """
    now_local = dt.datetime.now(LOCAL_TZ)
    if not report_local:
        return {"window_open": False, "seconds_until_open": None, "seconds_until_report": None}

    open_at = report_local - dt.timedelta(hours=ACK_POLICY["push_start_hours"])
    return {
        "window_open": open_at <= now_local <= report_local,
        "seconds_until_open": max(0, int((open_at - now_local).total_seconds())) if now_local < open_at else 0,
        "seconds_until_report": max(0, int((report_local - now_local).total_seconds())) if now_local < report_local else 0,
    }

# ---------------- Viewing window (single source of truth) ----------------
def compute_window_bounds_local() -> Tuple[dt.datetime, dt.datetime, str]:
    """
    Computes the viewing window in LOCAL time based on VIEW_WINDOW_MODE.
    Returns (start_local, end_local, label).
    """
    mode = VIEW_WINDOW_MODE
    now_local = dt.datetime.now(LOCAL_TZ)

    if mode == "TODAY_TO_END_OF_NEXT_MONTH":
        start_local = now_local
        end_local = end_of_next_month_local()
        label = f"Today – {end_local.strftime('%b %d (%a)')}"
        return start_local, end_local, label

    # Fallback: default behavior
    start_local = now_local
    end_local = end_of_next_month_local()
    label = f"Today – {end_local.strftime('%b %d (%a)')}"
    return start_local, end_local, label

def window_bounds_utc() -> Tuple[dt.datetime, dt.datetime, dt.datetime, dt.datetime, str]:
    """Return (start_utc, end_utc, start_local, end_local, label)."""
    start_local, end_local, label = compute_window_bounds_local()
    start_utc = to_utc(start_local)
    end_utc = to_utc(end_local)
    return start_utc, end_utc, start_local, end_local, label

# ============================
# /* ADDED: Hidden items (server-side persistence) */
# ============================
def _ensure_hidden_table():  # /* ADDED */
    with get_db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS hidden_items (
            key TEXT PRIMARY KEY,
            fpr TEXT NOT NULL,
            created_utc TEXT NOT NULL,
            last_update_utc TEXT NOT NULL
        )""")

def _stable_row_key(row: Dict[str, Any]) -> Optional[str]:  # /* ADDED */
    uid = row.get("uid") or row.get("event_uid")
    if uid:
        return str(uid)
    ack = row.get("ack") or {}
    if ack.get("ack_id"):
        return str(ack["ack_id"])
    pid = str(row.get("pairing_id") or "")
    riso = str((row.get("ack") or {}).get("report_local_iso") or row.get("report_local_iso") or "")
    if pid or riso:
        base = f"{pid}|{riso}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]
    return None

def _row_fingerprint(row: Dict[str, Any]) -> str:  # /* ADDED */
    def legs_of(day):
        legs = day.get("legs") or []
        return [[
            (l.get("flight") or ""),
            (l.get("dep") or ""),
            (l.get("arr") or ""),
            (l.get("dep_hhmm") or l.get("dep_time") or ""),
            (l.get("arr_hhmm") or l.get("arr_time") or "")
        ] for l in legs]
    pick = {
        "pairing_id": row.get("pairing_id") or "",
        "display": {
            "report": ((row.get("display") or {}).get("report_str") or ""),
            "release": ((row.get("display") or {}).get("release_str") or ""),
        },
        "days": [{
            "hotel": (d.get("hotel") or ""),
            "legs": legs_of(d)
        } for d in (row.get("days") or [])]
    }
    blob = json.dumps(pick, sort_keys=True, separators=(",", ":"))
    h = 2166136261  # FNV-like simple hash
    for ch in blob:
        h ^= ord(ch)
        h = (h * 16777619) & 0xFFFFFFFF
    return format(h, "08x")

def _hidden_map() -> Dict[str, str]:  # /* ADDED */
    with get_db() as c:
        rows = c.execute("SELECT key, fpr FROM hidden_items").fetchall()
    return {r["key"]: r["fpr"] for r in rows}

def _hidden_set(key: str, fpr: str):  # /* ADDED */
    now_iso = _now_utc().isoformat()
    with get_db() as c:
        c.execute("""
            INSERT INTO hidden_items(key, fpr, created_utc, last_update_utc)
            VALUES (?,?,?,?)
            ON CONFLICT(key) DO UPDATE SET fpr=excluded.fpr, last_update_utc=excluded.last_update_utc
        """, (key, fpr, now_iso, now_iso))

def _hidden_clear_all():  # /* ADDED */
    with get_db() as c:
        c.execute("DELETE FROM hidden_items")

# ---------------- API Routes ----------------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/api/pairings")
async def api_pairings(
    # legacy params kept; ignored for windowed view
    year: Optional[int] = Query(default=None, ge=1970, le=2100),
    month: Optional[int] = Query(default=None, ge=1, le=12),
    only_reports: int = Query(default=1),
    is_24h: int = Query(default=0),
):
    try:
        meta = await run_in_threadpool(read_cache_meta)
        events = normalize_cached_events(meta)

        # EXACT window used for both filtering and UI, from single source of truth
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

        # --- normalize display strings to requested clock mode (added) ---
        use_24h = bool(is_24h)
        for r in rows:
            if r.get("kind") != "pairing":
                continue
            disp = r.setdefault("display", {})
            # pairing-level report/release display (if raw HHMM present)
            rep = disp.get("report_hhmm") or r.get("report_hhmm") or r.get("report")
            rel = disp.get("release_hhmm") or r.get("release_hhmm") or r.get("release")
            if isinstance(rep, str) and rep.isdigit():
                disp["report_str"] = _fmt_time(rep, use_24h)
            if isinstance(rel, str) and rel.isdigit():
                disp["release_str"] = _fmt_time(rel, use_24h)
            # legs per day
            for d in (r.get("days") or []):
                for leg in (d.get("legs") or []):
                    dep_raw = leg.get("dep_time") or leg.get("dep_hhmm")
                    arr_raw = leg.get("arr_time") or leg.get("arr_hhmm")
                    if dep_raw and not leg.get("dep_time_str"):
                        leg["dep_time_str"] = _fmt_time(dep_raw, use_24h)
                    if arr_raw and not leg.get("arr_time_str"):
                        leg["arr_time_str"] = _fmt_time(arr_raw, use_24h)

        # ack enrichment
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
            ack_state = _read_ack_state(ackid)
            r["ack"] = {
                "ack_id": ackid,
                "window_open": bool(win["window_open"]),
                "acknowledged": (ack_state == "ack"),
                "seconds_until_open": win["seconds_until_open"],
                "seconds_until_report": win["seconds_until_report"],
                "report_local_iso": report_iso,
            }
            enriched.append(r)

        # /* ADDED: server-side hidden filtering */
        hidden_map = _hidden_map()  # {key: fpr}
        visible: List[Dict[str, Any]] = []
        for r in enriched:
            key = _stable_row_key(r)
            if not key:
                visible.append(r)
                continue
            fpr = _row_fingerprint(r)
            if hidden_map.get(key) == fpr:
                continue  # stay hidden (unchanged)
            visible.append(r)
        # /* ADDED END */

        # meta display — header shows the SAME window we used
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

        # /* ADDED */
        hidden_count = len(hidden_map)
        # /* ADDED END */

        return {
            "window": window_obj,
            "window_config": {"mode": VIEW_WINDOW_MODE, "tz": str(LOCAL_TZ)},
            "looking_through": window_label,  # legacy compatibility
            "last_pull_local": last_pull_local,
            "last_pull_local_simple": last_pull_human_simple,
            "last_pull_local_iso": lp_local.isoformat() if lp_local else "",
            "next_pull_local": next_refresh_local_clock,
            "next_pull_local_iso": nr_local.isoformat() if nr_local else "",
            "seconds_to_next": seconds_to_next,
            "tz_label": "CT",
            "rows": visible,  # /* CHANGED */
            "version": state.version,
            "refresh_minutes": int(meta.get("refresh_minutes", max(1, state.refresh_seconds // 60))),
            "ack_policy": ACK_POLICY,
            "is_24h": use_24h,  # debug/visibility
            "hidden_count": hidden_count,  # /* ADDED */
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

# /* ADDED: Hidden items API */
@app.post("/api/hide")
def api_hide_item(payload: Dict[str, Any] = Body(...)):  # /* ADDED */
    """
    Payload:
      - uid: Apple event UID (preferred)
      - ack_id: fallback key
      - row: full pairing row (used to compute fingerprint)
    """
    uid = (payload.get("uid") or "").strip()
    ack_id = (payload.get("ack_id") or "").strip()
    row = payload.get("row") or {}

    key = uid or ack_id or _stable_row_key(row)
    if not key:
        raise HTTPException(400, "uid or ack_id or identifiable row is required")
    if not isinstance(row, dict) or not row:
        raise HTTPException(400, "row object required to compute fingerprint")

    fpr = _row_fingerprint(row)
    _hidden_set(key, fpr)
    state.version += 1
    asyncio.create_task(_emit("hidden_update", {"key": key}))
    return {"ok": True, "key": key, "fpr": fpr}

@app.post("/api/unhide_all")
def api_unhide_all():  # /* ADDED */
    _hidden_clear_all()
    state.version += 1
    asyncio.create_task(_emit("hidden_update", {"cleared": True}))
    return {"ok": True}

@app.get("/api/hidden")
def api_hidden_list():  # /* ADDED */
    m = _hidden_map()
    return {"count": len(m), "hidden": m}
# /* ADDED END */

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
