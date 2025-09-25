"""
DutyWatch APScheduler Jobs

New behavior:
- Every 30 minutes, look from NOW -> end of NEXT month.
- If the UID set in that window changed, CLEAR cache and repopulate with fresh events.
- Cache scope is fixed to 'rolling:active'.

You can still bolt on your import/notify jobs later.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
import pytz

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from db import (
    read_uid_hash,
    overwrite_events_cache,
    clear_events_cache,
)
from config import TIMEZONE
from cal_client import list_uids_between, fetch_events_between

log = logging.getLogger(__name__)
TZ = pytz.timezone(TIMEZONE)
ROLLING_SCOPE = "rolling:active"


def _end_of_next_month(now: dt.datetime) -> dt.datetime:
    """
    Given a timezone-aware UTC datetime 'now',
    return the UTC timestamp for the start of the month after next (exclusive end).
    """
    # Convert to local tz to compute month boundaries
    local_now = now.astimezone(TZ)
    y = local_now.year
    m = local_now.month
    # next month:
    if m == 12:
        ny, nm = y + 1, 1
    else:
        ny, nm = y, m + 1
    # month after next:
    if nm == 12:
        ay, am = ny + 1, 1
    else:
        ay, am = ny, nm + 1
    end_local = dt.datetime(ay, am, 1, tzinfo=TZ)
    return end_local.astimezone(pytz.utc)


def _hash_uids(uids: set[str]) -> str:
    return hashlib.sha256(",".join(sorted(uids)).encode("utf-8")).hexdigest()


def monitor_rolling_window():
    """
    Every 30 minutes:
      - Window: now (UTC) -> end of next month (UTC)
      - Compare UID hash with cached scope 'rolling:active'
      - If changed: clear the cache, fetch full window, repopulate with fresh events
    """
    try:
        now_utc = dt.datetime.utcnow().replace(tzinfo=pytz.utc)
        end_utc = _end_of_next_month(now_utc)

        start_iso = now_utc.isoformat()
        end_iso = end_utc.isoformat()

        fresh_uids = list_uids_between(start_iso, end_iso)
        fresh_hash = _hash_uids(fresh_uids)

        current_hash = read_uid_hash(ROLLING_SCOPE)

        if fresh_hash != current_hash:
            log.info("Rolling window changed; refreshing cache…")
            events = fetch_events_between(start_iso, end_iso)

            # Clear entire cache (your request), then repopulate with current events
            clear_events_cache()
            overwrite_events_cache(ROLLING_SCOPE, events, uid_hash=fresh_hash)
            log.info("Cache replaced: %s (%d events)", ROLLING_SCOPE, len(events))
        else:
            log.debug("Rolling window unchanged; no action.")
    except Exception as e:
        log.exception("monitor_rolling_window failed: %s", e)


# Optional stubs left for future notification flow
async def import_and_schedule():
    return

async def fire_due_items():
    return


def install_scheduler(app):
    scheduler = AsyncIOScheduler(timezone=str(TZ))
    scheduler.add_job(monitor_rolling_window, "interval", minutes=30, id="rolling_monitor")
    # Keep these placeholders so your previous pipeline stays intact if needed
    scheduler.add_job(import_and_schedule, "interval", minutes=1, id="import")
    scheduler.add_job(fire_due_items, "interval", seconds=20, id="fire")
    scheduler.start()
    app.state.scheduler = scheduler

    # Run the rolling check once at startup
    try:
        monitor_rolling_window()
    except Exception:
        log.exception("Initial rolling window monitor failed")
