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
    """
    Group events by pairing ID + start date.
    This prevents separate trips with the same pairing number (e.g., W3086 on different weeks)
    from being merged together.
    """
    pid = (e.get("summary") or "").strip()
    start_utc = iso_to_dt(e.get("start_utc"))
    
    if pid and start_utc:
        # Use the date (YYYY-MM-DD) as part of the key to separate same-numbered pairings on different dates
        date_key = start_utc.strftime("%Y-%m-%d")
        return f"{pid}|{date_key}"
    elif pid:
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

def _day_signature(day: Dict[str, Any]) -> str:
    """Create a unique signature for a day based on report time and legs."""
    report = day.get("report") or ""
    legs = day.get("legs") or []
    leg_sig = "|".join(f"{leg.get('flight')}:{leg.get('dep')}:{leg.get('arr')}:{leg.get('dep_time')}:{leg.get('arr_time')}" for leg in legs)
    return f"{report}||{leg_sig}"

def build_pairing_rows(
    events: List[Dict[str, Any]],
    is_24h: bool,
    only_reports: bool,
) -> List[Dict[str, Any]]:
    # First pass: group by pairing ID only
    initial_groups: Dict[str, List[Dict[str, Any]]] = {}
    for e in events:
        pid = (e.get("summary") or "").strip()
        if not pid:
            uid = (e.get("uid") or "")[:8]
            pid = f"PAIR-{uid}"
        initial_groups.setdefault(pid, []).append(e)
    
    # Second pass: split groups if there's a gap > 12 hours between consecutive events
    final_groups: Dict[str, List[Dict[str, Any]]] = {}
    group_counter = 0
    
    for pid, evs in initial_groups.items():
        evs_sorted = sorted(evs, key=lambda x: iso_to_dt(x.get("start_utc")) or dt.datetime.min)
        
        current_batch = []
        for e in evs_sorted:
            if not current_batch:
                current_batch.append(e)
            else:
                prev_end = iso_to_dt(current_batch[-1].get("end_utc"))
                curr_start = iso_to_dt(e.get("start_utc"))
                
                # If gap is > 12 hours, this is a separate pairing
                if prev_end and curr_start and (curr_start - prev_end).total_seconds() > 12 * 3600:
                    # Save current batch
                    final_groups[f"{pid}#{group_counter}"] = current_batch
                    group_counter += 1
                    current_batch = [e]
                else:
                    current_batch.append(e)
        
        if current_batch:
            final_groups[f"{pid}#{group_counter}"] = current_batch
            group_counter += 1

    pairings: List[Dict[str, Any]] = []

    for group_key, evs in final_groups.items():
        # Extract original pairing_id (before the #number suffix)
        pairing_id = group_key.rsplit('#', 1)[0]
        evs_sorted = sorted(evs, key=lambda x: iso_to_dt(x.get("start_utc")) or dt.datetime.min)

        parsed_days: List[Dict[str, Any]] = []
        seen_signatures = set()
        
        for e in evs_sorted:
            parsed = parse_pairing_days(e.get("description") or "")
            days = parsed.get("days") or []
            
            for d in days:
                # Deduplicate days with identical report times and legs
                sig = _day_signature(d)
                if sig in seen_signatures:
                    continue
                seen_signatures.add(sig)
                
                for leg in d.get("legs", []):
                    if not str(leg.get("flight", "")).startswith("FFT"):
                        nums = re.findall(r"\d{3,4}", str(leg.get("flight", ""))) or []
                        if nums:
                            leg["flight"] = f"{nums[0]}"
                    leg["dep_time_str"] = time_display(leg.get("dep_time"), is_24h)
                    leg["arr_time_str"] = time_display(leg.get("arr_time"), is_24h)
                parsed_days.append(d)

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

        # IMPORTANT FIX #1: Only filter by report if NOT in progress
        # This keeps all days of in-progress pairings intact
        if only_reports and not in_progress:
            parsed_days = [d for d in parsed_days if d.get("report")]

        days_with_flags: List[Dict[str, Any]] = []
        for idx, d in enumerate(parsed_days, start=1):
            anchor_date = start_anchor_date + dt.timedelta(days=idx - 1) if start_anchor_date else None
            legs = d.get("legs", [])
            
            for leg in legs:
                dep_dt = arr_dt = None
                if anchor_date:
                    if leg.get("dep_time"):
                        dep_dt = dt.datetime(anchor_date.year, anchor_date.month, anchor_date.day, 
                                           int(leg["dep_time"][:2]), int(leg["dep_time"][2:]), tzinfo=LOCAL_TZ)
                    if leg.get("arr_time"):
                        arr_dt = dt.datetime(anchor_date.year, anchor_date.month, anchor_date.day, 
                                           int(leg["arr_time"][:2]), int(leg["arr_time"][2:]), tzinfo=LOCAL_TZ)
                        if dep_dt and arr_dt and arr_dt < dep_dt:
                            arr_dt += dt.timedelta(days=1)
                
                # Mark legs as done if arrival time has passed
                leg["done"] = bool(arr_dt and now_local >= arr_dt)
                
                # FIX #2: Add tracking availability info
                # Tracking is available 24 hours before departure
                if dep_dt:
                    tracking_available_dt = dep_dt - dt.timedelta(hours=24)
                    leg["tracking_available"] = now_local >= tracking_available_dt
                    leg["tracking_available_time"] = tracking_available_dt
                    
                    # Add a display message for when tracking will be available
                    if not leg.get("tracking_available"):
                        # Show the date when tracking will be available (the day of the flight)
                        leg["tracking_message"] = f"Tracking available {dep_dt.strftime('%b %d')}"
                    else:
                        leg["tracking_message"] = "Tracking available"
                else:
                    leg["tracking_available"] = False
                    leg["tracking_message"] = ""
                    
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

        uid = (evs_sorted[0].get("uid") if evs_sorted else None)

        pairings.append(
            {
                "kind": "pairing",
                "pairing_id": pairing_id,
                "in_progress": int(in_progress),
                "report_local_iso": pairing_report_local.isoformat() if pairing_report_local else None,
                "release_local_iso": pairing_release_local.isoformat() if pairing_release_local else None,
                "display": {"report_str": report_disp, "release_str": release_disp},
                "days": days_with_flags,
                "uid": uid,
            }
        )

    pairings.sort(key=lambda r: r.get("report_local_iso") or "")

    # STEP 3: Build rows with OFF times between ACTUAL pairings ONLY
    print(f"\n=== DEBUG: Building rows from {len(pairings)} pairings ===")
    
    # Separate actual pairings (with legs) from non-pairing events
    actual_pairings = []
    non_pairing_events = []
    
    for p in pairings:
        days = p.get("days", [])
        has_legs = any(day.get("legs") for day in days)
        
        if has_legs:
            actual_pairings.append(p)
            print(f"DEBUG: Actual pairing: {p.get('pairing_id')}")
        else:
            non_pairing_events.append(p)
            print(f"DEBUG: Non-pairing event: {p.get('pairing_id')}")
    
    print(f"DEBUG: Separated into {len(actual_pairings)} actual pairings and {len(non_pairing_events)} non-pairing events")
    
    # Build the final rows array
    rows: List[Dict[str, Any]] = []
    
    for i, p in enumerate(actual_pairings):
        print(f"DEBUG: Adding pairing {p.get('pairing_id')} at position {len(rows)}")
        rows.append(p)
        
        # Check for OFF period and any non-pairing events within it
        if i + 1 < len(actual_pairings):
            release = iso_to_dt(p.get("release_local_iso"))
            nxt_report = iso_to_dt(actual_pairings[i + 1].get("report_local_iso"))
            
            if release and nxt_report:
                gap = nxt_report - release
                if gap.total_seconds() > 0:
                    gap_str = _human_dur(gap)
                    print(f"DEBUG: OFF {gap_str} between {p.get('pairing_id')} and {actual_pairings[i+1].get('pairing_id')}")
                    
                    # Add the OFF row
                    rows.append({"kind": "off", "display": {"off_dur": gap_str}})
                    
                    # Find and add any non-pairing events that fall within this OFF period
                    for npe in non_pairing_events:
                        npe_time = iso_to_dt(npe.get("report_local_iso"))
                        if npe_time and release <= npe_time < nxt_report:
                            print(f"DEBUG: Adding non-pairing {npe.get('pairing_id')} within OFF period")
                            rows.append(npe)
    
    # Handle any non-pairing events that come after all pairings
    if actual_pairings:
        last_release = iso_to_dt(actual_pairings[-1].get("release_local_iso"))
        for npe in non_pairing_events:
            npe_time = iso_to_dt(npe.get("report_local_iso"))
            if npe_time and last_release and npe_time >= last_release:
                print(f"DEBUG: Adding non-pairing {npe.get('pairing_id')} after all pairings")
                rows.append(npe)
    
    print(f"=== DEBUG: Final output has {len(rows)} rows ===\n")
    return rows