"""
DutyWatch FastAPI app (with HTML table views)

Endpoints:

- GET  /health
- POST /register_device
- POST /policy/save
- POST /policy/from_natural
- POST /ack  (ack | snooze_5)

Calendar (JSON):
- GET  /calendar/debug
- GET  /calendar/upcoming?hours=...
- GET  /calendar/month?year=YYYY&month=MM

Calendar (HTML table):
- GET  /calendar/upcoming/table?hours=...
- GET  /calendar/month/table?year=YYYY&month=MM

Schedule cache:
- GET  /schedule/table
"""

import datetime as dt
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, HTMLResponse

from db import init_db, get_db, load_schedule_cache
from models import DeviceIn, ReminderPolicy, NaturalPrompt, AckIn
from policy_llm import natural_to_policy
from scheduler_jobs import install_scheduler
from twilio_client import router as twilio_router
import cal_client as cal


app = FastAPI(title="DutyWatch Backend")
app.include_router(twilio_router)


# -------------------------
# Helpers
# -------------------------

def _html_escape(s: str | None) -> str:
    if not s:
        return ""
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
    )

def _events_to_table_html(title: str, events: list[dict]) -> str:
    # Expect each event dict to have keys you already return: uid, calendar, summary,
    # location, description, start_utc, end_utc, last_modified (any missing become blank).
    head = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>{_html_escape(title)}</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Arial, sans-serif; padding: 24px; color: #111; }}
  h1 {{ font-size: 20px; margin: 0 0 16px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
  th, td {{ border-bottom: 1px solid #eee; padding: 8px 10px; vertical-align: top; }}
  th {{ text-align: left; background: #fafafa; position: sticky; top: 0; }}
  tr:hover td {{ background: #fcfcff; }}
  .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size: 12px; color: #444; }}
  .muted {{ color: #666; }}
  .wrap {{ white-space: pre-wrap; word-break: break-word; }}
</style>
</head>
<body>
<h1>{_html_escape(title)}</h1>
<table>
  <thead>
    <tr>
      <th>Start (UTC)</th>
      <th>End (UTC)</th>
      <th>Summary</th>
      <th>Calendar</th>
      <th>Location</th>
      <th>Description</th>
      <th class="mono">UID</th>
      <th class="muted">Last Modified</th>
    </tr>
  </thead>
  <tbody>
"""
    rows = []
    # sort by start_utc (nulls at bottom)
    def _key(e):
        s = e.get("start_utc") or "9999-12-31T00:00:00Z"
        return s
    for e in sorted(events, key=_key):
        rows.append(
            "<tr>"
            f"<td>{_html_escape(e.get('start_utc'))}</td>"
            f"<td>{_html_escape(e.get('end_utc'))}</td>"
            f"<td>{_html_escape(e.get('summary'))}</td>"
            f"<td>{_html_escape(e.get('calendar'))}</td>"
            f"<td>{_html_escape(e.get('location'))}</td>"
            f"<td class='wrap'>{_html_escape(e.get('description'))}</td>"
            f"<td class='mono'>{_html_escape(e.get('uid'))}</td>"
            f"<td class='muted'>{_html_escape(e.get('last_modified'))}</td>"
            "</tr>"
        )
    tail = """
  </tbody>
</table>
</body>
</html>
"""
    return head + "\n".join(rows) + tail


# -------------------------
# Startup
# -------------------------

@app.on_event("startup")
async def startup():
    init_db()
    install_scheduler(app)


# -------------------------
# Basic
# -------------------------

@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/register_device")
async def register_device(d: DeviceIn):
    with get_db() as c:
        c.execute(
            "INSERT OR IGNORE INTO devices(device_token, created_at) VALUES(?,?)",
            (d.device_token, dt.datetime.utcnow().isoformat()),
        )
    return {"ok": True}


# -------------------------
# Policy
# -------------------------

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


# -------------------------
# Acks
# -------------------------

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

        if ai.action == "snooze_5":
            fire = (dt.datetime.utcnow() + dt.timedelta(minutes=5)).isoformat()
            c.execute(
                """
                INSERT INTO notifications(ack_id,event_uid,event_start_utc,fire_at_utc,kind,attempt,sent)
                SELECT ack_id, event_uid, event_start_utc, ?, 'push', 0, 0
                FROM notifications WHERE ack_id=? LIMIT 1
                """,
                (fire, ai.ack_id),
            )

    return {"ok": True}


# -------------------------
# Calendar: diagnostic + upcoming + month (JSON)
# -------------------------

@app.get("/calendar/debug")
async def calendar_debug():
    try:
        return cal.diagnose()
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.get("/calendar/upcoming")
async def calendar_upcoming(hours: Optional[int] = Query(default=None, ge=1, le=24 * 30)):
    try:
        events = cal.fetch_upcoming_events(hours_ahead=hours)
        return {"count": len(events), "events": events}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.get("/calendar/month")
async def calendar_month(
    year: int = Query(..., ge=1970, le=2100),
    month: int = Query(..., ge=1, le=12),
):
    try:
        if not hasattr(cal, "fetch_month"):
            raise RuntimeError("cal_client.fetch_month(year, month) is not available")
        events = cal.fetch_month(year, month)
        return {"count": len(events), "events": events}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"{type(e).__name__}: {e}"})


# -------------------------
# Calendar: HTML tables
# -------------------------

@app.get("/calendar/upcoming/table", response_class=HTMLResponse)
async def calendar_upcoming_table(hours: Optional[int] = Query(default=None, ge=1, le=24 * 30)):
    try:
        events = cal.fetch_upcoming_events(hours_ahead=hours)
        title = f"Upcoming events (next {hours or 'default'} hours)"
        return HTMLResponse(_events_to_table_html(title, events))
    except Exception as e:
        return HTMLResponse(f"<pre>Error: {type(e).__name__}: {e}</pre>", status_code=500)


@app.get("/calendar/month/table", response_class=HTMLResponse)
async def calendar_month_table(
    year: int = Query(..., ge=1970, le=2100),
    month: int = Query(..., ge=1, le=12),
):
    try:
        if not hasattr(cal, "fetch_month"):
            raise RuntimeError("cal_client.fetch_month(year, month) is not available")
        events = cal.fetch_month(year, month)
        title = f"Events for {year:04d}-{month:02d}"
        return HTMLResponse(_events_to_table_html(title, events))
    except Exception as e:
        return HTMLResponse(f"<pre>Error: {type(e).__name__}: {e}</pre>", status_code=500)


# -------------------------
# Schedule table cache viewer (JSON)
# -------------------------

@app.get("/schedule/table")
async def schedule_table():
    cache = load_schedule_cache()
    if not cache:
        return {"ok": False, "error": "No schedule_cache yet."}
    return {"ok": True, **cache}
