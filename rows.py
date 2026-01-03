"""
Row Builder - Builds display rows from pairings.

This is the display/formatting layer:
- Time formatting (12h/24h)
- OFF period calculations
- Tracking info (FlightAware links)
- In-progress status
"""

from __future__ import annotations
import os
import datetime as dt
import re
import logging
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from pairing_builder import build_pairings
from parser import extract_pairing_id
from utils import iso_to_dt, to_local, ensure_hhmm, to_12h, time_display

logger = logging.getLogger("rows")

LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "America/Chicago"))


# =============================================================================
# Utility functions needed by app.py
# =============================================================================

def end_of_next_month_local() -> dt.datetime:
    """Get the end of next month in local timezone."""
    now_local = dt.datetime.now(LOCAL_TZ)
    y, m = now_local.year, now_local.month
    first_next = dt.datetime(y + (1 if m == 12 else 0), (m % 12) + 1, 1, tzinfo=LOCAL_TZ)
    y2 = first_next.year + (1 if first_next.month == 12 else 0)
    m2 = (first_next.month % 12) + 1
    first_after_next = dt.datetime(y2, m2, 1, tzinfo=LOCAL_TZ)
    return (first_after_next - dt.timedelta(seconds=1)).replace(microsecond=0)


def grouping_key(e: Dict[str, Any]) -> str:
    """
    Generate a grouping key for an event.
    Used for compatibility with existing hiding/caching logic.
    """
    summary = (e.get("summary") or "").strip()
    pid = extract_pairing_id(summary) or summary
    
    start_utc = iso_to_dt(e.get("start_utc"))
    if pid and start_utc:
        date_key = start_utc.strftime("%Y-%m-%d")
        return f"{pid}|{date_key}"
    elif pid:
        return pid
    
    uid = (e.get("uid") or "")[:8]
    return f"PAIR-{uid}"


# =============================================================================
# Main entry point for app.py
# =============================================================================

def build_pairing_rows(
    events: List[Dict[str, Any]],
    is_24h: bool = False,
    only_reports: bool = False,
    include_off_rows: bool = True,
    home_base: str = "DFW",
    filter_past: bool = False,
    include_non_pairing_events: bool = True,
) -> List[Dict[str, Any]]:
    """
    Build display rows from calendar events.
    
    ALWAYS rebuilds from scratch - no caching, no merging.
    
    Args:
        include_non_pairing_events: If False, excludes CBT, VAC, meetings, etc.
    """
    logger.info(f"Building rows from {len(events)} events (fresh rebuild)")
    
    # Build pairings from ALL events
    pairings = build_pairings(events)
    
    # Filter out non-pairing events if requested
    if not include_non_pairing_events:
        pairings = [p for p in pairings if p.is_pairing]
        logger.info(f"Filtered to {len(pairings)} actual pairings (excluded non-pairing events)")
    
    # Convert to pairing dicts
    pairing_dicts = [_pairing_to_dict(p) for p in pairings]
    
    # Build display rows
    rows = build_rows(pairing_dicts, is_24h, include_off_rows, home_base)
    
    # Filter past rows if requested
    if filter_past:
        rows = _filter_past_rows(rows)
    
    logger.info(f"Built {len(rows)} rows")
    return rows


def _filter_past_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filter out past pairings, keeping in-progress ones.
    Then rebuild OFF rows for the remaining pairings.
    """
    now_local = dt.datetime.now(LOCAL_TZ)
    
    # First pass: keep future/in-progress pairings and other events
    kept_pairings = []
    kept_other = []
    
    for r in rows:
        kind = r.get("kind")
        
        # Skip OFF rows - we'll rebuild them
        if kind == "off":
            continue
        
        # Check if past
        release_iso = r.get("release_local_iso")
        pid = r.get("pairing_id", "")
        
        # Debug C3075F
        if pid == "C3075F":
            logger.info(f"DEBUG C3075F: release_iso={release_iso}, in_progress={r.get('in_progress')}")
        
        if release_iso:
            release_local = to_local(iso_to_dt(release_iso))
            if release_local and release_local < now_local and not r.get("in_progress"):
                # Past and not in progress - skip
                if pid == "C3075F":
                    logger.info(f"DEBUG C3075F: FILTERED OUT (release={release_local}, now={now_local})")
                continue
        
        if r.get("has_legs"):
            kept_pairings.append(r)
        else:
            kept_other.append(r)
    
    # Rebuild OFF rows between kept pairings
    result = []
    
    # Sort kept pairings by report time
    kept_pairings.sort(key=lambda r: r.get("report_local_iso") or "")
    
    # Check for initial OFF (before first pairing)
    if kept_pairings:
        first_report = to_local(iso_to_dt(kept_pairings[0].get("report_local_iso")))
        
        # Check if we're currently in a pairing
        in_pairing = any(p.get("in_progress") for p in kept_pairings)
        
        if not in_pairing and first_report and first_report > now_local:
            gap = first_report - now_local
            if gap.total_seconds() > 0:  # Show any remaining OFF time
                result.append({
                    "kind": "off",
                    "is_current": True,
                    "display": {
                        "off_label": "OFF",
                        "off_duration": format_off_duration(gap, show_minutes=True),
                        "show_remaining": True,
                    }
                })
    
    # Add pairings with OFF between them
    for i, pairing_row in enumerate(kept_pairings):
        result.append(pairing_row)
        
        if i + 1 < len(kept_pairings):
            this_end = to_local(iso_to_dt(pairing_row.get("release_local_iso")))
            next_start = to_local(iso_to_dt(kept_pairings[i + 1].get("report_local_iso")))
            
            if this_end and next_start and next_start > this_end:
                gap = next_start - this_end
                if gap.total_seconds() > 3600:
                    is_current = (now_local >= this_end and now_local < next_start)
                    result.append({
                        "kind": "off",
                        "is_current": is_current,
                        "display": {
                            "off_label": "OFF",
                            "off_duration": format_off_duration(gap, show_minutes=is_current),
                            "show_remaining": is_current,
                        }
                    })
    
    # Insert non-flying events chronologically
    for nfe in kept_other:
        nfe_time = iso_to_dt(nfe.get("report_local_iso"))
        if nfe_time:
            inserted = False
            for i, row in enumerate(result):
                if row.get("kind") == "off":
                    continue
                row_time = iso_to_dt(row.get("report_local_iso"))
                if row_time and nfe_time < row_time:
                    result.insert(i, nfe)
                    inserted = True
                    break
            if not inserted:
                result.append(nfe)
    
    return result


def _pairing_to_dict(pairing) -> Dict[str, Any]:
    """Convert a Pairing dataclass to a dict for build_rows."""
    return {
        "pairing_id": pairing.pairing_id,
        "base_airports": pairing.base_airports,
        "is_pairing": pairing.is_pairing,
        "is_complete": pairing.is_complete,
        "starts_at_base": pairing.starts_at_base,
        "ends_at_base": pairing.ends_at_base,
        "num_days": pairing.num_days,
        "first_departure": pairing.first_departure,
        "last_arrival": pairing.last_arrival,
        "events": [
            {
                "uid": ev.uid,
                "summary": ev.summary,
                "pairing_id": ev.pairing_id,
                "start_utc": ev.start_utc,
                "end_utc": ev.end_utc,
                "description": ev.description,
                "location": ev.location,
                "is_pairing": ev.is_pairing,
                "legs": ev.legs,
                "report_time": ev.report_time,
                "report_date": ev.report_date,
                "release_time": ev.release_time,
                "hotel": ev.hotel,
                "first_departure": ev.first_departure,
                "last_arrival": ev.last_arrival,
            }
            for ev in pairing.events
        ],
    }


# =============================================================================
# Display formatting
# =============================================================================


def format_off_duration(td: dt.timedelta, show_minutes: bool = False) -> str:
    """Format duration for OFF rows. 
    - If show_minutes=True (for current/remaining): shows hours+minutes when under 24h
    - If show_minutes=False: rounds to nearest hour
    """
    total_seconds = int(td.total_seconds())
    if total_seconds < 0:
        return "0h"
    
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    
    if show_minutes:
        if days == 0:
            if minutes == 0:
                return f"{hours}h"
            return f"{hours}h {minutes}m"
        else:
            if hours == 0:
                return f"{days}d"
            return f"{days}d {hours}h"
    else:
        total_hours = total_seconds / 3600
        rounded_hours = round(total_hours)
        
        if rounded_hours < 24:
            return f"{rounded_hours}h"
        else:
            days = rounded_hours // 24
            remaining_hours = rounded_hours % 24
            if remaining_hours == 0:
                return f"{days}d"
            return f"{days}d {remaining_hours}h"


def _parse_report_date(report_date_str: str, reference_date: dt.datetime) -> Optional[dt.date]:
    """Parse a report date like '15NOV' using reference date for the year."""
    if not report_date_str:
        return None
    
    try:
        day_num = int(re.match(r'(\d+)', report_date_str).group(1))
        month_str = re.search(r'([A-Z]{3})', report_date_str.upper()).group(1)
        
        month_map = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
        }
        month_num = month_map.get(month_str)
        
        if not month_num:
            return None
        
        year = reference_date.year
        ref_month = reference_date.month
        
        if month_num == 12 and ref_month <= 2:
            year -= 1
        elif month_num <= 2 and ref_month == 12:
            year += 1
        
        return dt.date(year, month_num, day_num)
    except Exception as e:
        logger.error(f"Failed to parse report date '{report_date_str}': {e}")
        return None


def _combine_local(date_obj: Optional[dt.date], hhmm: Optional[str]) -> Optional[dt.datetime]:
    """Combine a date and HHMM time string into a local datetime."""
    if not date_obj or not hhmm:
        return None
    hhmm = ensure_hhmm(hhmm)
    return dt.datetime(date_obj.year, date_obj.month, date_obj.day, 
                       int(hhmm[:2]), int(hhmm[2:]), tzinfo=LOCAL_TZ)


def _build_day_row(event: Dict[str, Any], day_index: int, is_24h: bool) -> Dict[str, Any]:
    """Build a day row from a stored event dict."""
    
    # Get reference date from calendar event
    reference_date = to_local(iso_to_dt(event.get("start_utc"))) or dt.datetime.now(LOCAL_TZ)
    
    # Determine the actual date for this day
    report_date_str = event.get("report_date")
    if report_date_str:
        # Date is in the description like "02JAN"
        actual_date = _parse_report_date(report_date_str, reference_date)
    else:
        # Use date from calendar event
        actual_date = reference_date.date()
    
    if not actual_date:
        actual_date = reference_date.date()
    
    now_local = dt.datetime.now(LOCAL_TZ)
    
    # Format legs
    formatted_legs = []
    for leg in event.get("legs", []):
        is_deadhead = leg.get("deadhead", False)
        
        # Format flight number
        flight_num = str(leg.get("flight", ""))
        if not flight_num.startswith("FFT"):
            nums = re.findall(r"\d+", flight_num) or []
            if nums:
                flight_num = nums[0]
                if is_deadhead:
                    flight_num = f"*{flight_num}"
        
        dep = leg.get("dep", "")
        arr = leg.get("arr", "")
        
        # Route display
        if is_deadhead:
            route_display = f"*DH {dep}–{arr}"
        else:
            route_display = f"{dep}–{arr}"
        
        # Time display
        dep_time = leg.get("dep_time", "")
        arr_time = leg.get("arr_time", "")
        
        if dep_time and arr_time:
            dep_formatted = time_display(dep_time, is_24h)
            arr_formatted = time_display(arr_time, is_24h)
            block_display = f"{dep_formatted} → {arr_formatted}"
        elif dep_time:
            block_display = time_display(dep_time, is_24h)
        else:
            block_display = ""
        
        # Calculate departure/arrival datetimes for tracking
        dep_dt = None
        arr_dt = None
        
        if actual_date and dep_time:
            dep_dt = dt.datetime(actual_date.year, actual_date.month, actual_date.day,
                                 int(dep_time[:2]), int(dep_time[2:]), tzinfo=LOCAL_TZ)
        if actual_date and arr_time:
            arr_dt = dt.datetime(actual_date.year, actual_date.month, actual_date.day,
                                 int(arr_time[:2]), int(arr_time[2:]), tzinfo=LOCAL_TZ)
            if dep_dt and arr_dt and arr_dt < dep_dt:
                arr_dt += dt.timedelta(days=1)
        
        # Tracking info
        tracking_info = {}
        if is_deadhead:
            tracking_info = {
                "tracking_display": "Check FLICA",
                "tracking_available": False,
                "tracking_message": "Check FLICA",
            }
        elif dep_dt and flight_num:
            tracking_available_dt = dep_dt - dt.timedelta(hours=24)
            tracking_available = now_local >= tracking_available_dt
            
            if tracking_available:
                clean_num = re.sub(r"[^0-9]", "", flight_num)
                if clean_num:
                    tracking_info = {
                        "tracking_display": f"FFT{clean_num}",
                        "tracking_url": f"https://flightaware.com/live/flight/FFT{clean_num}",
                        "tracking_message": "Track →",
                        "tracking_clickable": True,
                        "tracking_available": True,
                        "tracking_available_time": tracking_available_dt.isoformat(),
                    }
            else:
                tracking_info = {
                    "tracking_available": False,
                    "tracking_message": f"Avail. {dep_dt.strftime('%-m/%d')}",
                    "tracking_display": f"Avail. {dep_dt.strftime('%-m/%d')}",
                    "tracking_available_time": tracking_available_dt.isoformat(),
                }
        
        formatted_legs.append({
            "flight": flight_num,
            "dep": dep,
            "arr": arr,
            "dep_time": dep_time,
            "arr_time": arr_time,
            "dep_time_str": time_display(dep_time, is_24h),
            "arr_time_str": time_display(arr_time, is_24h),
            "deadhead": is_deadhead,
            "route_display": route_display,
            "block_display": block_display,
            "done": bool(arr_dt and now_local >= arr_dt),
            **tracking_info,
        })
    
    # Day report/release times
    report_time = event.get("report_time")
    release_time = event.get("release_time")
    
    day_report_dt = _combine_local(actual_date, report_time)
    day_release_dt = _combine_local(actual_date, release_time)
    
    if day_report_dt and day_release_dt and day_release_dt < day_report_dt:
        day_release_dt += dt.timedelta(days=1)
    
    return {
        "day_index": day_index,
        "actual_date": actual_date.isoformat() if actual_date else None,
        "date_local_iso": day_report_dt.isoformat() if day_report_dt else None,
        "day_report_dt": day_report_dt.isoformat() if day_report_dt else None,
        "day_release_dt": day_release_dt.isoformat() if day_release_dt else None,
        "report": report_time,
        "release": release_time,
        "legs": formatted_legs,
        "hotel": event.get("hotel"),
        "is_layover": len(formatted_legs) == 0,
    }


def _pairing_to_row(pairing: Dict[str, Any], is_24h: bool, home_base: str) -> Dict[str, Any]:
    """Convert a stored pairing dict to a display row."""
    
    now_local = dt.datetime.now(LOCAL_TZ)
    events = pairing.get("events", [])
    
    # Calculate report and release times from first/last events
    pairing_report_local = None
    pairing_release_local = None
    
    if events:
        first_event = events[0]
        last_event = events[-1]
        
        # Get reference date from calendar event
        ref_date = to_local(iso_to_dt(first_event.get("start_utc"))) or now_local
        
        # Report time from first event
        report_time = first_event.get("report_time")
        report_date_str = first_event.get("report_date")
        
        if report_time:
            # We have a report time - determine the date
            if report_date_str:
                # Date is in the description like "02JAN"
                report_date = _parse_report_date(report_date_str, ref_date)
            else:
                # Use date from calendar event
                report_date = ref_date.date()
            
            if report_date:
                pairing_report_local = _combine_local(report_date, report_time)
        
        # Fall back to calendar event start if no report time parsed
        if not pairing_report_local:
            pairing_report_local = to_local(iso_to_dt(first_event.get("start_utc")))
        
        # Release time from last event
        release_time = last_event.get("release_time")
        if release_time:
            last_ref_date = to_local(iso_to_dt(last_event.get("start_utc"))) or now_local
            release_date_str = last_event.get("report_date")
            
            if release_date_str:
                release_date = _parse_report_date(release_date_str, last_ref_date)
            else:
                release_date = last_ref_date.date()
            
            if release_date:
                # Check if release is next day (overnight flight)
                # Compare release time to last arrival time
                legs = last_event.get("legs", [])
                report_time_val = last_event.get("report_time")
                
                if legs and release_time and report_time_val:
                    try:
                        # If release time is earlier than report time, it's next day
                        rel_mins = int(release_time[:2]) * 60 + int(release_time[2:])
                        rep_mins = int(report_time_val[:2]) * 60 + int(report_time_val[2:])
                        
                        if rel_mins < rep_mins:
                            # Release is on the next day
                            release_date = release_date + dt.timedelta(days=1)
                    except (ValueError, IndexError):
                        pass
                
                pairing_release_local = _combine_local(release_date, release_time)
        
        # Fall back to calendar event end if no release time
        if not pairing_release_local:
            pairing_release_local = to_local(iso_to_dt(last_event.get("end_utc")))
    
    # Check if in progress
    in_progress = bool(
        pairing_report_local and pairing_release_local and 
        pairing_report_local <= now_local <= pairing_release_local
    )
    
    # Build day rows
    days_with_flags = []
    for idx, event in enumerate(events, start=1):
        day_row = _build_day_row(event, idx, is_24h)
        days_with_flags.append(day_row)
    
    # Calculate number of days
    num_days = pairing.get("num_days", len(events))
    if pairing_report_local and pairing_release_local:
        delta = (pairing_release_local.date() - pairing_report_local.date()).days + 1
        num_days = max(num_days, delta)
    
    # Format display strings
    def dword(d: Optional[dt.datetime]) -> str:
        return d.strftime("%a %b %d") if d else ""
    
    def hhmm_or_blank(d: Optional[dt.datetime]) -> str:
        return d.strftime("%H%M") if d else ""
    
    report_disp = ""
    if pairing_report_local:
        report_disp = f"{dword(pairing_report_local)} {to_12h(hhmm_or_blank(pairing_report_local))}".strip()
    
    release_disp = ""
    if pairing_release_local:
        release_disp = f"{dword(pairing_release_local)} {to_12h(hhmm_or_blank(pairing_release_local))}".strip()
    
    # Count legs
    total_legs = sum(len(event.get("legs", [])) for event in events)
    
    # Determine out-of-base status (only for actual pairings)
    is_pairing = pairing.get("is_pairing", True)
    base_airports = pairing.get("base_airports", [])
    out_of_base_airport = None
    
    if is_pairing and base_airports and home_base not in base_airports:
        out_of_base_airport = base_airports[0]
    
    # Determine the row kind
    kind = "pairing" if is_pairing else "other"
    
    return {
        "kind": kind,
        "pairing_id": pairing.get("pairing_id", ""),
        "is_pairing": is_pairing,
        "in_progress": int(in_progress),
        "report_local_iso": pairing_report_local.isoformat() if pairing_report_local else None,
        "release_local_iso": pairing_release_local.isoformat() if pairing_release_local else None,
        "display": {"report_str": report_disp, "release_str": release_disp},
        "days": days_with_flags,
        "num_days": num_days,
        "uid": events[0].get("uid") if events else None,
        "total_legs": total_legs,
        "has_legs": total_legs > 0,
        "first_dep_airport": pairing.get("first_departure"),
        "out_of_base": bool(out_of_base_airport),
        "out_of_base_airport": out_of_base_airport,
        "base_airports": base_airports,
        "is_complete": pairing.get("is_complete", False),
    }


def build_rows(
    pairings: List[Dict[str, Any]],
    is_24h: bool = False,
    include_off_rows: bool = True,
    home_base: str = "DFW",
) -> List[Dict[str, Any]]:
    """
    Build display rows from stored pairings.
    
    Args:
        pairings: List of pairing dicts from the store
        is_24h: Use 24-hour time format
        include_off_rows: Include OFF period rows between pairings
        home_base: Pilot's home base for out-of-base detection
    
    Returns:
        List of row dicts ready for the frontend
    """
    logger.info(f"Building rows from {len(pairings)} pairings")
    
    # Convert pairings to rows
    pairing_rows = []
    for pairing in pairings:
        row = _pairing_to_row(pairing, is_24h, home_base)
        pairing_rows.append(row)
    
    # Sort by report time
    pairing_rows.sort(key=lambda r: r.get("report_local_iso") or "")
    
    if not include_off_rows:
        return pairing_rows
    
    # Build rows with OFF times between pairings
    now_local = dt.datetime.now(LOCAL_TZ)
    
    # Separate actual pairings (with legs) from non-flying events
    actual_pairings = [p for p in pairing_rows if p.get("has_legs")]
    non_flying_events = [p for p in pairing_rows if not p.get("has_legs")]
    
    logger.info(f"Building OFF rows: {len(actual_pairings)} flying pairings, {len(non_flying_events)} non-flying")
    
    # Build final rows with OFF periods
    rows = []
    
    # Check for initial OFF period (currently off before first pairing)
    if actual_pairings:
        first_report = iso_to_dt(actual_pairings[0].get("report_local_iso"))
        if first_report:
            first_report_local = to_local(first_report)
            
            # Check if we're currently in a pairing
            in_pairing = False
            for p in actual_pairings:
                p_start = to_local(iso_to_dt(p.get("report_local_iso")))
                p_end = to_local(iso_to_dt(p.get("release_local_iso")))
                if p_start and p_end and p_start <= now_local <= p_end:
                    in_pairing = True
                    break
            
            if not in_pairing and first_report_local and first_report_local > now_local:
                gap = first_report_local - now_local
                if gap.total_seconds() > 3600:  # More than 1 hour
                    rows.append({
                        "kind": "off",
                        "is_current": True,
                        "display": {
                            "off_label": "OFF",
                            "off_duration": format_off_duration(gap, show_minutes=True),
                            "show_remaining": True,
                        }
                    })
    
    # Add pairings with OFF periods between them
    for i, pairing_row in enumerate(actual_pairings):
        rows.append(pairing_row)
        
        if i + 1 < len(actual_pairings):
            this_end = to_local(iso_to_dt(pairing_row.get("release_local_iso")))
            next_start = to_local(iso_to_dt(actual_pairings[i + 1].get("report_local_iso")))
            
            if this_end and next_start and next_start > this_end:
                gap = next_start - this_end
                if gap.total_seconds() > 3600:  # More than 1 hour
                    is_current = (now_local >= this_end and now_local < next_start)
                    
                    rows.append({
                        "kind": "off",
                        "is_current": is_current,
                        "display": {
                            "off_label": "OFF",
                            "off_duration": format_off_duration(gap, show_minutes=is_current),
                            "show_remaining": is_current,
                        }
                    })
    
    # Insert non-flying events in chronological order
    for nfe in non_flying_events:
        nfe_time = iso_to_dt(nfe.get("report_local_iso"))
        if nfe_time:
            inserted = False
            for i, row in enumerate(rows):
                if row.get("kind") == "off":
                    continue
                row_time = iso_to_dt(row.get("report_local_iso"))
                if row_time and nfe_time < row_time:
                    rows.insert(i, nfe)
                    inserted = True
                    break
            if not inserted:
                rows.append(nfe)
    
    return rows