"""
Builds pairing/off rows for the frontend table.
WORKING VERSION: Properly handles pairings without commute rows.
"""

from __future__ import annotations
import os
import datetime as dt
import re
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from parser import parse_pairing_days
from utils import iso_to_dt, to_local, ensure_hhmm, to_12h, time_display, human_duration

LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "America/Chicago"))


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
        # Show precise hours and minutes (for "Remaining" row)
        if days == 0:
            if minutes == 0:
                return f"{hours}h"
            return f"{hours}h {minutes}m"
        else:
            if hours == 0:
                return f"{days}d"
            return f"{days}d {hours}h"
    else:
        # Round to nearest hour
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


def end_of_next_month_local() -> dt.datetime:
    now_local = dt.datetime.now(LOCAL_TZ)
    y, m = now_local.year, now_local.month
    first_next = dt.datetime(y + (1 if m == 12 else 0), (m % 12) + 1, 1, tzinfo=LOCAL_TZ)
    y2 = first_next.year + (1 if first_next.month == 12 else 0)
    m2 = (first_next.month % 12) + 1
    first_after_next = dt.datetime(y2, m2, 1, tzinfo=LOCAL_TZ)
    return (first_after_next - dt.timedelta(seconds=1)).replace(microsecond=0)


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


def _day_signature(day: Dict[str, Any]) -> str:
    """Create a unique signature for a day based on report time and legs."""
    report = day.get("report") or ""
    legs = day.get("legs") or []
    leg_sig = "|".join(f"{leg.get('flight')}:{leg.get('dep')}:{leg.get('arr')}:{leg.get('dep_time')}:{leg.get('arr_time')}" for leg in legs)
    return f"{report}||{leg_sig}"


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
        
        result = dt.date(year, month_num, day_num)
        import logging
        logging.info(f"Parsed report date '{report_date_str}' as {result} ({result.strftime('%A')})")
        return result
    except Exception as e:
        import logging
        logging.error(f"Failed to parse report date '{report_date_str}': {e}")
        return None


def _parse_day_prefix(day_prefix: str, reference_date: dt.datetime) -> Optional[dt.date]:
    """Parse a day prefix like 'SU16' or 'MO17' to get the actual date."""
    if not day_prefix:
        return None
    
    try:
        day_num = int(re.search(r'(\d+)', day_prefix).group(1))
        year = reference_date.year
        month = reference_date.month
        date = dt.date(year, month, day_num)
        
        if day_num < 15 and reference_date.day > 15:
            if month == 12:
                date = dt.date(year + 1, 1, day_num)
            else:
                date = dt.date(year, month + 1, day_num)
        elif day_num > 15 and reference_date.day < 15:
            if month == 1:
                date = dt.date(year - 1, 12, day_num)
            else:
                date = dt.date(year, month - 1, day_num)
        
        return date
    except Exception:
        return None


def build_pairing_rows(
    events: List[Dict[str, Any]],
    is_24h: bool,
    only_reports: bool,
    include_off_rows: bool = False,
    home_base: str = "DFW",
) -> List[Dict[str, Any]]:
    """Build pairing rows from calendar events."""
    
    # Group events by pairing ID
    initial_groups: Dict[str, List[Dict[str, Any]]] = {}
    for e in events:
        pid = (e.get("summary") or "").strip()
        if not pid:
            uid = (e.get("uid") or "")[:8]
            pid = f"PAIR-{uid}"
        initial_groups.setdefault(pid, []).append(e)
    
    # Split groups based on whether events end at home base
    final_groups: Dict[str, List[Dict[str, Any]]] = {}
    group_counter = 0
    
    for pid, evs in initial_groups.items():
        evs_sorted = sorted(evs, key=lambda x: iso_to_dt(x.get("start_utc")) or dt.datetime.min)
        
        current_batch = []
        for e in evs_sorted:
            if not current_batch:
                current_batch.append(e)
            else:
                prev_event = current_batch[-1]
                prev_desc = (prev_event.get("description") or "")
                prev_parsed = parse_pairing_days(prev_desc)
                prev_days = prev_parsed.get("days") or []
                
                ended_at_home = False
                if prev_days:
                    last_day = prev_days[-1]
                    last_legs = last_day.get("legs") or []
                    if last_legs:
                        last_arr = last_legs[-1].get("arr", "").upper()
                        ended_at_home = (last_arr == home_base)
                
                if ended_at_home:
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
        pairing_id = group_key.rsplit('#', 1)[0]
        evs_sorted = sorted(evs, key=lambda x: iso_to_dt(x.get("start_utc")) or dt.datetime.min)

        parsed_days: List[Dict[str, Any]] = []
        seen_signatures = set()
        
        all_parsed_events = []
        for e in evs_sorted:
            parsed = parse_pairing_days(e.get("description") or "")
            event_start_local = to_local(iso_to_dt(e.get("start_utc")))
            all_parsed_events.append((e, parsed, event_start_local))
        
        # Find the first report date
        first_report_date = None
        first_report_hhmm = None
        
        for e, parsed, event_start_local in all_parsed_events:
            days = parsed.get("days") or []
            if days and days[0].get("report_date"):
                report_date = _parse_report_date(days[0]["report_date"], event_start_local)
                if report_date:
                    first_report_date = report_date
                    first_report_hhmm = days[0].get("report")
                    break
        
        # Build the complete list of days
        day_prefix_to_date = {}
        
        for e, parsed, event_start_local in all_parsed_events:
            days = parsed.get("days") or []
            
            for d in days:
                sig = _day_signature(d)
                if sig in seen_signatures:
                    continue
                seen_signatures.add(sig)
                
                day_date = None
                day_prefix = None
                
                if d.get("legs"):
                    first_leg = d["legs"][0]
                    if first_leg.get("day_prefix"):
                        day_prefix = first_leg["day_prefix"]
                        day_date = _parse_day_prefix(day_prefix, event_start_local)
                        
                        if day_prefix and day_date:
                            day_prefix_to_date[day_prefix] = day_date
                
                if day_prefix and not day_date and day_prefix in day_prefix_to_date:
                    day_date = day_prefix_to_date[day_prefix]
                
                if not parsed_days and first_report_date:
                    day_date = first_report_date
                
                d["actual_date"] = day_date
                
                for leg in d.get("legs", []):
                    is_deadhead = leg.get("deadhead", False)
                    
                    if not str(leg.get("flight", "")).startswith("FFT"):
                        nums = re.findall(r"\d+", str(leg.get("flight", ""))) or []
                        if nums:
                            flight_num = nums[0]
                            if is_deadhead:
                                leg["flight"] = f"*{flight_num}"
                            else:
                                leg["flight"] = flight_num
                    
                    dep = leg.get("dep", "")
                    arr = leg.get("arr", "")
                    if is_deadhead:
                        leg["route_display"] = f"*DH {dep}–{arr}"
                    else:
                        leg["route_display"] = f"{dep}–{arr}"
                    
                    dep_time = leg.get("dep_time", "")
                    arr_time = leg.get("arr_time", "")
                    if dep_time and arr_time:
                        dep_formatted = time_display(dep_time, is_24h)
                        arr_formatted = time_display(arr_time, is_24h)
                        leg["block_display"] = f"{dep_formatted} → {arr_formatted}"
                    elif dep_time:
                        leg["block_display"] = time_display(dep_time, is_24h)
                    else:
                        leg["block_display"] = ""
                    
                    leg["dep_time_str"] = time_display(dep_time, is_24h)
                    leg["arr_time_str"] = time_display(arr_time, is_24h)
                    
                    if is_deadhead:
                        leg["tracking_display"] = "Check FLICA"
                        leg["tracking_available"] = False
                        leg["tracking_message"] = "Check FLICA"
                
                parsed_days.append(d)

        # Calculate report and release times
        def combine_local(date_obj: Optional[dt.date], hhmm: Optional[str]) -> Optional[dt.datetime]:
            if not date_obj or not hhmm:
                return None
            hhmm = ensure_hhmm(hhmm)
            return dt.datetime(date_obj.year, date_obj.month, date_obj.day, 
                             int(hhmm[:2]), int(hhmm[2:]), tzinfo=LOCAL_TZ)

        pairing_report_local = None
        if parsed_days and parsed_days[0].get("actual_date") and parsed_days[0].get("report"):
            import logging
            logging.info(f"Building report for {pairing_id} from date {parsed_days[0]['actual_date']} and time {parsed_days[0]['report']}")
            pairing_report_local = combine_local(parsed_days[0]["actual_date"], parsed_days[0]["report"])
            logging.info(f"Report datetime for {pairing_id}: {pairing_report_local} ({pairing_report_local.strftime('%A %H:%M') if pairing_report_local else 'None'})")
        elif not parsed_days or not parsed_days[0].get("report"):
            if evs_sorted:
                pairing_report_local = to_local(iso_to_dt(evs_sorted[0].get("start_utc")))
                import logging
                logging.info(f"Using calendar start_utc for {pairing_id}: {pairing_report_local}")
        
        pairing_release_local = None
        if parsed_days and parsed_days[-1].get("actual_date") and parsed_days[-1].get("release"):
            last_day = parsed_days[-1]
            release_date = last_day["actual_date"]
            
            if last_day.get("legs"):
                last_arr = last_day["legs"][-1].get("arr_time", "")
                release = last_day.get("release", "")
                if last_arr and release:
                    arr_hour = int(last_arr[:2])
                    rel_hour = int(release[:2])
                    if arr_hour >= 23 and rel_hour < 2:
                        release_date = release_date + dt.timedelta(days=1)
            
            pairing_release_local = combine_local(release_date, last_day["release"])
        elif not parsed_days or not parsed_days[-1].get("release"):
            if evs_sorted:
                pairing_release_local = to_local(iso_to_dt(evs_sorted[-1].get("end_utc")))

        now_local = dt.datetime.now(LOCAL_TZ)
        in_progress = bool(pairing_report_local and pairing_release_local and pairing_report_local <= now_local <= pairing_release_local)

        if only_reports and not in_progress:
            parsed_days = [d for d in parsed_days if d.get("report")]

        days_with_flags: List[Dict[str, Any]] = []
        for idx, d in enumerate(parsed_days, start=1):
            anchor_date = d.get("actual_date")
            
            for leg in d.get("legs", []):
                dep_dt = None
                arr_dt = None
                
                if anchor_date:
                    if leg.get("dep_time"):
                        dep_dt = dt.datetime(anchor_date.year, anchor_date.month, anchor_date.day, 
                                           int(leg["dep_time"][:2]), int(leg["dep_time"][2:]), tzinfo=LOCAL_TZ)
                    if leg.get("arr_time"):
                        arr_dt = dt.datetime(anchor_date.year, anchor_date.month, anchor_date.day, 
                                           int(leg["arr_time"][:2]), int(leg["arr_time"][2:]), tzinfo=LOCAL_TZ)
                        if dep_dt and arr_dt and arr_dt < dep_dt:
                            arr_dt += dt.timedelta(days=1)
                
                leg["done"] = bool(arr_dt and now_local >= arr_dt)
                
                if not leg.get("deadhead", False):
                    if dep_dt and leg.get("flight"):
                        tracking_available_dt = dep_dt - dt.timedelta(hours=24)
                        leg["tracking_available"] = now_local >= tracking_available_dt
                        leg["tracking_available_time"] = tracking_available_dt
                        
                        if leg.get("tracking_available"):
                            flight_num = str(leg.get("flight", "")).replace("FFT", "")
                            flight_num = re.sub(r"[^0-9]", "", flight_num)
                            if flight_num:
                                leg["tracking_display"] = f"FFT{flight_num}"
                                leg["tracking_url"] = f"https://flightaware.com/live/flight/FFT{flight_num}"
                                leg["tracking_message"] = "Track →"
                                leg["tracking_clickable"] = True
                        else:
                            leg["tracking_message"] = f"Avail. {dep_dt.strftime('%-m/%d')}"
                            leg["tracking_display"] = leg["tracking_message"]
                    else:
                        leg["tracking_available"] = False
                        leg["tracking_message"] = ""
                        leg["tracking_display"] = ""
            
            day_report_dt = None
            if anchor_date and d.get("report"):
                day_report_dt = combine_local(anchor_date, d.get("report"))
            
            day_release_dt = None
            if anchor_date and d.get("release"):
                day_release_dt = combine_local(anchor_date, d.get("release"))
                if day_report_dt and day_release_dt and day_release_dt < day_report_dt:
                    day_release_dt += dt.timedelta(days=1)
            
            days_with_flags.append({
                **d, 
                "day_index": idx,
                "date_local_iso": day_report_dt.isoformat() if day_report_dt else None,
                "day_report_dt": day_report_dt.isoformat() if day_report_dt else None,
                "day_release_dt": day_release_dt.isoformat() if day_release_dt else None,
            })

        # Calculate number of days
        num_days = 1
        if pairing_report_local and pairing_release_local:
            report_date = pairing_report_local.date()
            release_date = pairing_release_local.date()
            delta = (release_date - report_date).days + 1
            num_days = max(1, delta)
        
        # Handle layovers for multi-day trips
        if num_days > 1 and days_with_flags:
            legs_by_date = {}
            for d in days_with_flags:
                if d.get("actual_date"):
                    date_key = d["actual_date"].strftime("%Y-%m-%d")
                    if d.get("legs"):
                        legs_by_date[date_key] = d
            
            new_days = []
            current_date = pairing_report_local.date() if pairing_report_local else report_date
            prev_arrival = None
            prev_hotel = None
            
            for day_num in range(num_days):
                date_key = current_date.strftime("%Y-%m-%d")
                
                if date_key in legs_by_date:
                    day = legs_by_date[date_key]
                    day["day_index"] = day_num + 1
                    new_days.append(day)
                    
                    if day.get("legs"):
                        last_leg = day["legs"][-1]
                        prev_arrival = last_leg.get("arr", "")
                    if day.get("hotel"):
                        prev_hotel = day.get("hotel")
                else:
                    layover_day = {
                        "actual_date": current_date,
                        "day_index": day_num + 1,
                        "legs": [],
                        "is_layover": True,
                        "layover_location": prev_arrival or "Unknown",
                        "no_flights_message": "No flights scheduled"
                    }
                    
                    if prev_hotel:
                        layover_day["hotel"] = prev_hotel
                    
                    if day_num == 0 and parsed_days and parsed_days[0].get("report"):
                        layover_day["report"] = parsed_days[0].get("report")
                    
                    if day_num == num_days - 1 and parsed_days and parsed_days[-1].get("release"):
                        layover_day["release"] = parsed_days[-1].get("release")
                    
                    new_days.append(layover_day)
                
                current_date = current_date + dt.timedelta(days=1)
            
            days_with_flags = new_days
        
        # Format display strings
        def dword(d: Optional[dt.datetime]) -> str:
            return d.strftime("%a %b %d") if d else ""

        def hhmm_or_blank(d: Optional[dt.datetime]) -> str:
            if not d:
                return ""
            return d.strftime("%H%M")

        report_disp = f"{dword(pairing_report_local)} {(to_12h(hhmm_or_blank(pairing_report_local)) if pairing_report_local else '')}".strip() if pairing_report_local else ""
        release_disp = f"{dword(pairing_release_local)} {(to_12h(hhmm_or_blank(pairing_release_local)) if pairing_release_local else '')}".strip() if pairing_release_local else ""

        uid = (evs_sorted[0].get("uid") if evs_sorted else None)
        total_legs = sum(len(d.get("legs", [])) for d in days_with_flags)
        
        # Find first departure airport
        first_dep_airport = None
        for d in days_with_flags:
            legs = d.get("legs", [])
            if legs and legs[0].get("dep"):
                first_dep_airport = str(legs[0]["dep"]).upper()
                break
        
        
        # Determine if out-of-base
        # D-prefix = Denver-based pairing (out-of-base for DFW pilots)
        # W, X, Y, Z prefix = DFW home pairings
        is_home_pairing = False
        out_of_base_airport = None
        
        if pairing_id:
            # Check for D-prefix (Denver pairings - always out-of-base for DFW pilots)
            if re.match(r'^D\d', pairing_id, re.IGNORECASE):
                out_of_base_airport = "DEN"
            # Check for W, X, Y, Z prefix patterns that indicate DFW home base
            elif re.match(r'^[WXYZ]\d', pairing_id, re.IGNORECASE):
                is_home_pairing = True
        
        # For non-prefixed pairings, check the first departure airport
        if not out_of_base_airport and not is_home_pairing and first_dep_airport and first_dep_airport != home_base:
            out_of_base_airport = first_dep_airport

        pairings.append({
            "kind": "pairing",
            "pairing_id": pairing_id,
            "in_progress": int(in_progress),
            "report_local_iso": pairing_report_local.isoformat() if pairing_report_local else None,
            "release_local_iso": pairing_release_local.isoformat() if pairing_release_local else None,
            "display": {"report_str": report_disp, "release_str": release_disp},
            "days": days_with_flags,
            "num_days": num_days,
            "uid": uid,
            "total_legs": total_legs,
            "has_legs": total_legs > 0,
            "first_dep_airport": first_dep_airport,
            "out_of_base": bool(out_of_base_airport),
            "out_of_base_airport": out_of_base_airport,
        })

    pairings.sort(key=lambda r: r.get("report_local_iso") or "")

    if not include_off_rows:
        return pairings

    # Build rows with OFF times (no commutes)
    actual_pairings = []
    non_pairing_events = []
    now_local = dt.datetime.now(LOCAL_TZ)
    
    for p in pairings:
        days = p.get("days", [])
        has_legs = any(day.get("legs") for day in days)
        
        if has_legs:
            actual_pairings.append(p)
        else:
            non_pairing_events.append(p)
    
    import logging
    logging.info(f"Building rows with {len(actual_pairings)} actual pairings and {len(non_pairing_events)} non-pairing events")
    
    # Build all events (pairings only, no commutes)
    all_events = []
    
    for p in actual_pairings:
        all_events.append(p)
    
    # Add non-flying events chronologically
    for npe in non_pairing_events:
        npe_time = iso_to_dt(npe.get("report_local_iso"))
        if npe_time:
            inserted = False
            for i, event in enumerate(all_events):
                event_time = iso_to_dt(event.get("report_local_iso"))
                if event_time and npe_time < event_time:
                    all_events.insert(i, npe)
                    inserted = True
                    break
            if not inserted:
                all_events.append(npe)
    
    # Build final rows with OFF periods
    rows = []
    
    # Common airport codes pattern for detecting commute flights
    AIRPORT_CODE_PATTERN = re.compile(r'^[A-Z]{3}-[A-Z]{3}$')
    
    def is_commute_flight(event):
        """Detect commute flights by naming pattern (e.g., LAS-ORD, DEN-DFW)."""
        pid = event.get("pairing_id", "")
        if AIRPORT_CODE_PATTERN.match(pid):
            return True
        return False
    
    # Helper to find the next actual pairing WITH LEGS (skipping commutes and non-pairing events)
    def get_next_pairing_report(events_list, start_idx):
        """Find the report time of the next actual pairing with legs, skipping commutes and non-pairing events."""
        for j in range(start_idx, len(events_list)):
            event = events_list[j]
            # Skip commutes by kind
            if event.get("kind") == "commute":
                continue
            # Skip commute flights detected by pattern (e.g., LAS-ORD)
            if is_commute_flight(event):
                continue
            # Skip non-pairing events (no legs) - these are things like CBT, meetings, etc.
            if not event.get("has_legs"):
                continue
            return iso_to_dt(event.get("report_local_iso")), event.get("pairing_id")
        return None, None
    
    # Check for initial OFF period - only consider actual pairings with legs
    if all_events:
        # Find first actual pairing with legs (not commute)
        first_pairing_report, first_pid = get_next_pairing_report(all_events, 0)
        
        logging.info(f"OFF CALC: now_local={now_local}, first_pairing_report={first_pairing_report}, first_pid={first_pid}")
        
        # Check if currently in a pairing (only consider real pairings with legs, not commutes)
        in_pairing = False
        for event in all_events:
            if event.get("kind") == "commute":
                continue
            if is_commute_flight(event):
                continue
            if not event.get("has_legs"):
                continue
            event_start = iso_to_dt(event.get("report_local_iso"))
            event_end = iso_to_dt(event.get("release_local_iso"))
            if event_start and event_end and event_start <= now_local <= event_end:
                in_pairing = True
                logging.info(f"OFF CALC: Currently in pairing {event.get('pairing_id')}")
                break
        
        if not in_pairing and first_pairing_report and first_pairing_report > now_local:
            gap = first_pairing_report - now_local
            logging.info(f"OFF CALC: gap={gap}, gap_hours={gap.total_seconds()/3600:.2f}")
            if gap.total_seconds() > 3600:
                rows.append({
                    "kind": "off",
                    "is_current": True,
                    "display": {
                        "off_label": "OFF",
                        "off_duration": format_off_duration(gap, show_minutes=True),
                        "show_remaining": True
                    }
                })
    
    # Add all events with OFF periods between them
    for i, event in enumerate(all_events):
        rows.append(event)
        
        if i + 1 < len(all_events):
            # Skip OFF calculation for commutes and non-pairing events
            if event.get("kind") == "commute":
                continue
            # Skip commute flights (e.g., LAS-ORD)
            if is_commute_flight(event):
                continue
            if not event.get("has_legs"):
                # Non-pairing event (like CBT, etc.) - don't create OFF after it
                continue
            
            this_end = iso_to_dt(event.get("release_local_iso"))
            
            # Find next actual pairing with legs (skip commutes and non-pairing events)
            next_start, next_pid = get_next_pairing_report(all_events, i + 1)
            
            logging.info(f"OFF CALC between: {event.get('pairing_id')} ends {this_end}, next {next_pid} starts {next_start}")
            
            if this_end and next_start and next_start > this_end:
                gap = next_start - this_end
                logging.info(f"OFF CALC between: gap={gap}, gap_hours={gap.total_seconds()/3600:.2f}")
                if gap.total_seconds() > 3600:
                    is_current = (now_local >= this_end and now_local < next_start)
                    
                    rows.append({
                        "kind": "off",
                        "is_current": is_current,
                        "display": {
                            "off_label": "OFF",
                            "off_duration": format_off_duration(gap, show_minutes=is_current),
                            "show_remaining": is_current
                        }
                    })
    
    return rows