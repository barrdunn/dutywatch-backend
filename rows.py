"""
Builds pairing/off rows for the frontend table.
"""

from __future__ import annotations
import os
import datetime as dt
import re
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from parser import parse_pairing_days

LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "America/Chicago"))

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

def end_of_next_month_local() -> dt.datetime:
    now_local = dt.datetime.now(LOCAL_TZ)
    y, m = now_local.year, now_local.month
    first_next = dt.datetime(y + (1 if m == 12 else 0), (m % 12) + 1, 1, tzinfo=LOCAL_TZ)
    y2 = first_next.year + (1 if first_next.month == 12 else 0)
    m2 = (first_next.month % 12) + 1
    first_after_next = dt.datetime(y2, m2, 1, tzinfo=LOCAL_TZ)
    return (first_after_next - dt.timedelta(seconds=1)).replace(microsecond=0)

def _ensure_hhmm(s: str) -> str:
    return s if len(s) == 4 else s.zfill(4)

def _to_12h(hhmm: str) -> str:
    hhmm = _ensure_hhmm(hhmm)
    h = int(hhmm[:2]); m = hhmm[2:]
    ampm = "AM" if h < 12 else "PM"
    h12 = h % 12 or 12
    return f"{h12}:{m} {ampm}"

def time_display(hhmm: Optional[str], is_24h: bool) -> str:
    if not hhmm:
        return ""
    return _ensure_hhmm(hhmm) if is_24h else _to_12h(hhmm)

def grouping_key(e: Dict[str, Any]) -> str:
    pid = (e.get("summary") or "").strip()
    if pid:
        return pid
    uid = (e.get("uid") or "")[:8]
    return f"PAIR-{uid}"

def _human_dur(td: dt.timedelta) -> str:
    total_h = max(0, int(td.total_seconds() // 3600))
    if total_h >= 48:
        d = total_h // 24
        h = total_h % 24
        return f"{d}d {h}h"
    return f"{total_h}h"

def build_pairing_rows(
    events: List[Dict[str, Any]],
    is_24h: bool,
    only_reports: bool,
) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for e in events:
        groups.setdefault(grouping_key(e), []).append(e)

    pairings: List[Dict[str, Any]] = []

    for pairing_id, evs in groups.items():
        evs_sorted = sorted(evs, key=lambda x: iso_to_dt(x.get("start_utc")) or dt.datetime.min)

        parsed_days: List[Dict[str, Any]] = []
        for e in evs_sorted:
            parsed = parse_pairing_days(e.get("description") or "")
            days = parsed.get("days") or []
            if only_reports:
                days = [d for d in days if d.get("report")]
            for d in days:
                for leg in d.get("legs", []):
                    if not str(leg.get("flight", "")).startswith("FFT"):
                        nums = re.findall(r"\d{3,4}", str(leg.get("flight", ""))) or []
                        if nums:
                            leg["flight"] = f"FFT{nums[0]}"
                    leg["dep_time_str"] = time_display(leg.get("dep_time"), is_24h)
                    leg["arr_time_str"] = time_display(leg.get("arr_time"), is_24h)
            parsed_days.extend(days)

        first_evt_local = to_local(iso_to_dt(evs_sorted[0].get("start_utc"))) if evs_sorted else None
        first_report_hhmm = parsed_days[0].get("report") if parsed_days else None
        last_release_hhmm = parsed_days[-1].get("release") if parsed_days else None

        def combine_local(date_obj: Optional[dt.date], hhmm: Optional[str]) -> Optional[dt.datetime]:
            if not date_obj or not hhmm:
                return None
            return dt.datetime(date_obj.year, date_obj.month, date_obj.day, int(hhmm[:2]), int(hhmm[2:]), tzinfo=LOCAL_TZ)

        start_anchor_date = first_evt_local.date() if first_evt_local else None
        end_anchor_date = (start_anchor_date + dt.timedelta(days=max(len(parsed_days) - 1, 0))) if start_anchor_date else None

        pairing_report_local = combine_local(start_anchor_date, first_report_hhmm) or first_evt_local
        pairing_release_local = combine_local(end_anchor_date, last_release_hhmm) or (to_local(iso_to_dt(evs_sorted[-1].get("end_utc"))) if evs_sorted else None)

        if pairing_report_local and pairing_release_local and pairing_release_local < pairing_report_local:
            pairing_release_local += dt.timedelta(days=1)

        now_local = dt.datetime.now(LOCAL_TZ)
        in_progress = bool(pairing_report_local and pairing_release_local and pairing_report_local <= now_local <= pairing_release_local)

        days_with_flags: List[Dict[str, Any]] = []
        for idx, d in enumerate(parsed_days, start=1):
            anchor_date = start_anchor_date + dt.timedelta(days=idx - 1) if start_anchor_date else None
            legs = d.get("legs", [])
            for leg in legs:
                dep_dt = arr_dt = None
                if anchor_date:
                    if leg.get("dep_time"):
                        dep_dt = dt.datetime(anchor_date.year, anchor_date.month, anchor_date.day, int(leg["dep_time"][:2]), int(leg["dep_time"][2:]), tzinfo=LOCAL_TZ)
                    if leg.get("arr_time"):
                        arr_dt = dt.datetime(anchor_date.year, anchor_date.month, anchor_date.day, int(leg["arr_time"][:2]), int(leg["arr_time"][2:]), tzinfo=LOCAL_TZ)
                        if dep_dt and arr_dt and arr_dt < dep_dt:
                            arr_dt += dt.timedelta(days=1)
                leg["done"] = bool(arr_dt and now_local >= arr_dt)
            days_with_flags.append({**d, "day_index": idx})

        def dword(d: Optional[dt.datetime]) -> str:
            return d.strftime("%a %b %d") if d else ""

        def hhmm_or_blank(d: Optional[dt.datetime]) -> str:
            if not d:
                return ""
            hhmm = d.strftime("%H%M")
            return hhmm

        report_disp = f"{dword(pairing_report_local)} {(_to_12h(hhmm_or_blank(pairing_report_local)) if pairing_report_local else '')}".strip() if pairing_report_local else ""
        release_disp = f"{dword(pairing_release_local)} {(_to_12h(hhmm_or_blank(pairing_release_local)) if pairing_release_local else '')}".strip() if pairing_release_local else ""

        pairings.append(
            {
                "kind": "pairing",
                "pairing_id": pairing_id,
                "in_progress": int(in_progress),
                "report_local_iso": pairing_report_local.isoformat() if pairing_report_local else None,
                "release_local_iso": pairing_release_local.isoformat() if pairing_release_local else None,
                "display": {"report_str": report_disp, "release_str": release_disp},
                "days": days_with_flags,
            }
        )

    pairings.sort(key=lambda r: r.get("report_local_iso") or "")

    rows: List[Dict[str, Any]] = []
    for i, p in enumerate(pairings):
        rows.append(p)
        if i + 1 < len(pairings):
            release = iso_to_dt(p.get("release_local_iso"))
            nxt_report = iso_to_dt(pairings[i + 1].get("report_local_iso"))
            gap = (nxt_report - release) if (release and nxt_report) else dt.timedelta(0)
            rows.append({"kind": "off", "display": {"off_dur": _human_dur(gap if gap.total_seconds() >= 0 else dt.timedelta(0))}})
    return rows
