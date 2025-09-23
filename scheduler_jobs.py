"""
DutyWatch APScheduler Jobs

Jobs included:

- import_and_schedule()  [async, every 1 min]
    • Fetches upcoming calendar events (from iCloud via cal_client.fetch_upcoming_events)
    • Creates ack rows + scheduled push/call notifications per policy

- fire_due_items()       [async, every 20 sec]
    • Checks notifications due at/before now
    • Sends APNs pushes if still pending ack
    • Places Twilio call escalations if still pending ack

- _rebuild_schedule_if_changed()  [sync, every 30 min]
    • Builds pairings + time-off table (schedule_builder.build_schedule_table)
    • Stores in schedule_cache only when the content hash changes

- install_scheduler(app)
    • Creates a singleton AsyncIOScheduler attached to app.state.scheduler
    • Schedules: import (1m), fire (20s), rebuild_schedule (30m)
    • Also does a one-time kick on startup:
        - import_and_schedule()
        - _rebuild_schedule_if_changed()
"""

from __future__ import annotations

import os
import json
import logging
import datetime as dt
import pytz
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from db import get_db
from models import ReminderPolicy
from apns_client import send_push
from twilio_client import place_ack_call
from cal_client import fetch_upcoming_events
from utils import deterministic_ack_id, to_local
from config import TIMEZONE

# schedule table (pairings + time off)
from schedule_builder import build_schedule_table
from db import save_schedule_cache, load_schedule_cache

log = logging.getLogger(__name__)
TZ = pytz.timezone(TIMEZONE)


# ---------------------------
# Policy & filtering helpers
# ---------------------------

def _load_policy() -> ReminderPolicy:
    with get_db() as c:
        row = c.execute("SELECT json FROM policy WHERE id=1").fetchone()
        if not row:
            return ReminderPolicy()  # defaults
        return ReminderPolicy.model_validate_json(row["json"])


def _is_work_event(summary: str, policy: ReminderPolicy) -> bool:
    s = (summary or "").lower()
    if any(x.lower() in s for x in policy.detect_events.exclude_keywords):
        return False
    return any(x.lower() in s for x in policy.detect_events.include_keywords)


def _schedule_for_event(con, event: dict, policy: ReminderPolicy) -> None:
    uid = event["uid"]
    start_utc = dt.datetime.fromisoformat(event["start_utc"])
    ack_id = deterministic_ack_id(uid, start_utc)

    # Ack row (idempotent)
    con.execute(
        """
        INSERT OR IGNORE INTO acks(ack_id, event_uid, deadline_utc, state, last_update_utc)
        VALUES(?,?,?,?,?)
        """,
        (
            ack_id,
            uid,
            (start_utc - dt.timedelta(minutes=policy.ack.deadline_min_before_event)).isoformat(),
            "pending",
            dt.datetime.utcnow().isoformat(),
        ),
    )

    # Push notifications at configured lead times (idempotent)
    for lead in policy.lead_times_min:
        fire_at = (start_utc - dt.timedelta(minutes=lead)).isoformat()
        con.execute(
            """
            INSERT OR IGNORE INTO notifications(ack_id,event_uid,event_start_utc,fire_at_utc,kind,attempt,sent)
            VALUES(?,?,?,?,?,?,?)
            """,
            (ack_id, uid, start_utc.isoformat(), fire_at, "push", 0, 0),
        )

    # Call escalations after deadline (idempotent)
    if policy.escalation.twilio_call_if_no_ack:
        for i in range(policy.escalation.max_repeats):
            fire_at = (
                start_utc
                - dt.timedelta(minutes=policy.ack.deadline_min_before_event)
                + dt.timedelta(minutes=i * policy.escalation.repeat_every_min)
            ).isoformat()
            con.execute(
                """
                INSERT OR IGNORE INTO notifications(ack_id,event_uid,event_start_utc,fire_at_utc,kind,attempt,sent)
                VALUES(?,?,?,?,?,?,?)
                """,
                (ack_id, uid, start_utc.isoformat(), fire_at, "call", i, 0),
            )


# ---------------------------
# Jobs
# ---------------------------

async def import_and_schedule():
    """Scan upcoming events and create acks/notifications (idempotent)."""
    try:
        policy = _load_policy()
        events = fetch_upcoming_events()  # uses your existing cal_client
        with get_db() as c:
            for ev in events:
                if _is_work_event(ev.get("summary", ""), policy):
                    _schedule_for_event(c, ev, policy)
    except Exception as e:
        log.exception("DutyWatch: import_and_schedule failed: %s", e)


async def fire_due_items():
    """Send pending pushes/calls for notifications whose fire_at_utc <= now."""
    try:
        now = dt.datetime.utcnow().replace(tzinfo=pytz.utc)
        policy = _load_policy()

        with get_db() as c:
            due = c.execute(
                """
                SELECT n.*, a.state
                FROM notifications n
                LEFT JOIN acks a ON a.ack_id = n.ack_id
                WHERE n.sent = 0 AND n.fire_at_utc <= ?
                ORDER BY n.fire_at_utc ASC
                """,
                (now.isoformat(),),
            ).fetchall()

            if not due:
                return

            # All device tokens (APNs)
            devs = [r["device_token"] for r in c.execute("SELECT device_token FROM devices").fetchall()]

            for row in due:
                # If already acked/snoozed/etc. then mark sent and skip
                if row["state"] != "pending":
                    c.execute("UPDATE notifications SET sent=1 WHERE id=?", (row["id"],))
                    continue

                if row["kind"] == "push":
                    start_utc = dt.datetime.fromisoformat(row["event_start_utc"]).replace(tzinfo=pytz.utc)
                    start_local = to_local(start_utc)
                    body = f"Report at {start_local.strftime('%a %b %d, %I:%M %p')}."
                    payload = {"ack_id": row["ack_id"], "event_uid": row["event_uid"]}

                    for token in devs:
                        try:
                            await send_push(token, "DutyWatch", body, payload)
                        except Exception:
                            log.exception("DutyWatch: send_push failed (token redacted)")
                    c.execute("UPDATE notifications SET sent=1 WHERE id=?", (row["id"],))

                elif row["kind"] == "call":
                    try:
                        # For now, use TEST_CALL_TO (verified in Twilio console)
                        to_number = os.getenv("TEST_CALL_TO", None)
                        if to_number:
                            place_ack_call(to_number, row["ack_id"], policy.escalation.say_text)
                    except Exception:
                        log.exception("DutyWatch: place_ack_call failed")
                    c.execute("UPDATE notifications SET sent=1 WHERE id=?", (row["id"],))

    except Exception as e:
        log.exception("DutyWatch: fire_due_items failed: %s", e)


def _rebuild_schedule_if_changed():
    """
    Build pairings + time-off table, and cache it if the content hash changed.
    Safe to call often; writes only when different.
    """
    try:
        new_cache = build_schedule_table()
        old = load_schedule_cache()
        if not old or old.get("hash") != new_cache["hash"]:
            save_schedule_cache(new_cache)
            log.info("DutyWatch: schedule cache updated (hash=%s)", new_cache["hash"][:8])
        else:
            log.info("DutyWatch: no schedule changes")
    except Exception as e:
        log.exception("DutyWatch: schedule rebuild failed: %s", e)


# ---------------------------
# Scheduler wiring
# ---------------------------

def install_scheduler(app) -> AsyncIOScheduler:
    """
    Create (or return) a singleton AsyncIOScheduler, and register jobs.
    Idempotent on hot reload (reuses app.state.scheduler if present).
    """
    # Reuse existing scheduler if already attached (prevents duplicate jobs on reload)
    if getattr(app.state, "scheduler", None):
        return app.state.scheduler  # type: ignore[return-value]

    scheduler = AsyncIOScheduler(timezone=str(TZ))
    scheduler.start()

    # Every 1 minute: import events & schedule notifications
    scheduler.add_job(
        import_and_schedule,
        "interval",
        minutes=1,
        id="import",
        replace_existing=True,
    )

    # Every 20 seconds: fire due pushes/calls
    scheduler.add_job(
        fire_due_items,
        "interval",
        seconds=20,
        id="fire",
        replace_existing=True,
    )

    # Every 30 minutes: rebuild schedule table if changed
    scheduler.add_job(
        _rebuild_schedule_if_changed,
        "interval",
        minutes=30,
        id="rebuild_schedule",
        replace_existing=True,
    )

    # One-time kick on startup to fill the cache & seed notifications quickly
    @app.on_event("startup")
    async def _kick_once():
        # run import first (await since it's async), then rebuild table
        await import_and_schedule()
        _rebuild_schedule_if_changed()

    app.state.scheduler = scheduler
    log.info("DutyWatch: scheduler installed with jobs: import(1m), fire(20s), rebuild_schedule(30m)")
    return scheduler
