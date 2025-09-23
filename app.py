"""
DutyWatch Backend – Main FastAPI Application (minimal boot)
"""
import datetime as dt
from fastapi import FastAPI, HTTPException
from db import init_db, get_db
from models import DeviceIn, ReminderPolicy, NaturalPrompt, AckIn
from policy_llm import natural_to_policy
from scheduler_jobs import install_scheduler
from twilio_client import router as twilio_router
from fastapi import Query
from fastapi.responses import JSONResponse
from cal_client import fetch_upcoming_events, diagnose
from schedule_builder import build_schedule_table
from db import load_schedule_cache, save_schedule_cache

app = FastAPI(title="DutyWatch Backend")
app.include_router(twilio_router)

@app.on_event("startup")
async def startup():
    init_db()
    install_scheduler(app)

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/calendar/debug")
async def calendar_debug():
    # Always returns JSON with ok/error and calendar names
    return diagnose()

@app.get("/calendar/upcoming")
async def calendar_upcoming(hours: int = Query(24, ge=1, le=168)):
    try:
        events = fetch_upcoming_events(hours_ahead=hours)
        return {"count": len(events), "events": events}
    except Exception as e:
        # Return a clear error message instead of 500 plaintext
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": f"{type(e).__name__}: {e}"}
        )

@app.post("/register_device")
async def register_device(d: DeviceIn):
    with get_db() as c:
        c.execute(
            "INSERT OR IGNORE INTO devices(device_token, created_at) VALUES(?,?)",
            (d.device_token, dt.datetime.utcnow().isoformat()),
        )
    return {"ok": True}

@app.post("/policy/save")
async def policy_save(p: ReminderPolicy):
    with get_db() as c:
        c.execute(
            "INSERT OR REPLACE INTO policy(id,json,updated_at) VALUES(1,?,?)",
            (p.model_dump_json(), dt.datetime.utcnow().isoformat()),
        )
    return {"ok": True, "policy": p}

@app.post("/policy/from_natural")
async def from_natural(np: NaturalPrompt):
    policy_dict = natural_to_policy(np.prompt)
    p = ReminderPolicy.model_validate(policy_dict)
    return p

@app.post("/ack")
async def ack(ai: AckIn):
    with get_db() as c:
        cur = c.execute("SELECT 1 FROM acks WHERE ack_id=?", (ai.ack_id,)).fetchone()
        if not cur:
            raise HTTPException(404, "ack_id not found")

        new_state = "ack" if ai.action == "ack" else ("snoozed" if ai.action == "snooze_5" else "pending")
        c.execute(
            "UPDATE acks SET state=?, last_update_utc=? WHERE ack_id=?",
            (new_state, dt.datetime.utcnow().isoformat(), ai.ack_id),
        )
    return {"ok": True}

@app.get("/schedule/table")
async def schedule_table():
    cache = load_schedule_cache()
    if cache:
        return cache
    built = build_schedule_table()
    save_schedule_cache(built)
    return built

@app.post("/schedule/refresh")
async def schedule_refresh():
    built = build_schedule_table()
    save_schedule_cache(built)
    return {"ok": True, "hash": built["hash"], "rows": built["rows"]}