"""
Builds DutyWatch schedule rows from calendar events:
- Groups legs by pairing id (e.g., W3134) found in summary
- Pairing row has report_time, checkout_time, legs grouped by local day
- Inserts time_off rows between pairings
- Returns {"hash": ..., "rows": [...]}
"""

import re, json, hashlib, datetime as dt
from typing import List, Dict, Any, DefaultDict
from collections import defaultdict

from config import (
    REPORT_LEAD_MINUTES, CHECKOUT_PAD_MINUTES,
    NOTIFY_BEFORE_REPORT_MINUTES, SCHEDULE_LOOKAHEAD_HOURS, TIMEZONE
)
from cal_client import fetch_upcoming_events
from utils import to_utc, to_local_iso, humanize_gap_hours

PAIRING_RE = re.compile(r"\b([A-Z]\d{3,6})\b")  # e.g., W3134, A12345

def _parse_pairing_id(summary: str) -> str | None:
    if not summary:
        return None
    m = PAIRING_RE.search(summary.upper())
    return m.group(1) if m else None

def _iso_to_dt(s: str | None) -> dt.datetime | None:
    return dt.datetime.fromisoformat(s) if s else None

def _legs_by_day(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Show "HH:MM  summary" grouped by local day
    by_day: DefaultDict[str, List[str]] = defaultdict(list)
    for ev in events:
        sdt = _iso_to_dt(ev["start_utc"])
        if not sdt:
            continue
        day = to_local_iso(sdt, TIMEZONE).split(" ")[0]
        tme = to_local_iso(sdt, TIMEZONE).split(" ")[1]
        by_day[day].append(f"{tme}  {ev['summary']}")
    out = []
    for d in sorted(by_day.keys()):
        out.append({"day": d, "legs": by_day[d]})
    return out

def build_schedule_table() -> Dict[str, Any]:
    # 1) Pull Work calendar events for the window
    events = fetch_upcoming_events(hours_ahead=SCHEDULE_LOOKAHEAD_HOURS)

    # 2) Keep only legs with a pairing id in the summary
    legs: List[Dict[str, Any]] = []
    for ev in events:
        pid = _parse_pairing_id(ev.get("summary", ""))
        if pid:
            legs.append({**ev, "pairing_id": pid})

    # 3) Group by pairing id
    grouped: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    for leg in legs:
        grouped[leg["pairing_id"]].append(leg)

    # 4) Build pairing rows
    pairing_rows: List[Dict[str, Any]] = []
    for pid, evs in grouped.items():
        evs_sorted = sorted(evs, key=lambda e: e["start_utc"] or "9999")
        first = evs_sorted[0]
        last  = max(evs_sorted, key=lambda e: e["end_utc"] or "0000")

        first_start = _iso_to_dt(first["start_utc"])
        last_end    = _iso_to_dt(last["end_utc"])
        if not first_start or not last_end:
            continue

        report_time  = to_utc(first_start - dt.timedelta(minutes=REPORT_LEAD_MINUTES))
        checkout_time= to_utc(last_end    + dt.timedelta(minutes=CHECKOUT_PAD_MINUTES))
        notify_at    = to_utc(report_time - dt.timedelta(minutes=NOTIFY_BEFORE_REPORT_MINUTES))

        legs_day = _legs_by_day(evs_sorted)

        pairing_rows.append({
            "kind": "pairing",
            "pairing_id": pid,
            "report_time_utc": report_time.isoformat(),
            "checkout_time_utc": checkout_time.isoformat(),
            "legs_by_day": legs_day,
            "notifiable": True,
            "notify_at_utc": notify_at.isoformat(),
            "title": f"{pid}  (report {to_local_iso(report_time, TIMEZONE)})",
        })

    # 5) Insert time_off rows between pairings
    pairing_rows.sort(key=lambda r: r["report_time_utc"])
    rows: List[Dict[str, Any]] = []
    for i, row in enumerate(pairing_rows):
        rows.append(row)
        if i < len(pairing_rows) - 1:
            this_checkout = _iso_to_dt(row["checkout_time_utc"])
            next_report   = _iso_to_dt(pairing_rows[i+1]["report_time_utc"])
            if this_checkout and next_report and next_report > this_checkout:
                gap_hours = (next_report - this_checkout).total_seconds() / 3600.0
                rows.append({
                    "kind": "time_off",
                    "start_utc": this_checkout.isoformat(),
                    "end_utc": next_report.isoformat(),
                    "off_hours": round(gap_hours, 2),
                    "off_human": humanize_gap_hours(gap_hours),
                    "notifiable": False,
                    "notify_at_utc": None,
                    "title": f"Time Off  ({humanize_gap_hours(gap_hours)})",
                })

    # 6) Stable hash for change detection
    sig_src = [
        (leg["uid"], leg["start_utc"], leg["end_utc"], leg.get("summary", ""))
        for leg in sorted(legs, key=lambda x: (x["uid"], x["start_utc"] or ""))
    ]
    digest = hashlib.sha256(json.dumps(sig_src).encode("utf-8")).hexdigest()

    return {"hash": digest, "rows": rows}
