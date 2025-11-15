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

def _parse_report_date(report_date_str: str, reference_date: dt.datetime) -> Optional[dt.date]:
    """
    Parse a report date like "15NOV" using reference date for the year.
    Handles year boundaries intelligently.
    """
    if not report_date_str:
        return None
    
    try:
        # Parse format: "15NOV" -> day=15, month=NOV
        day_num = int(re.match(r'(\d+)', report_date_str).group(1))
        month_str = re.search(r'([A-Z]{3})', report_date_str.upper()).group(1)
        
        # Map month abbreviation to number
        month_map = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
        }
        month_num = month_map.get(month_str)
        
        if not month_num:
            return None
        
        # Use the reference year, but handle year boundaries
        year = reference_date.year
        ref_month = reference_date.month
        
        # If report month is December and reference is January, use previous year
        if month_num == 12 and ref_month <= 2:
            year -= 1
        # If report month is January and reference is December, use next year
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
    """
    Parse a day prefix like "SU16" or "MO17" to get the actual date.
    """
    if not day_prefix:
        return None
    
    try:
        # Extract day number (e.g., "16" from "SU16")
        day_num = int(re.search(r'(\d+)', day_prefix).group(1))
        
        # Use reference month/year
        year = reference_date.year
        month = reference_date.month
        
        # Create the date
        date = dt.date(year, month, day_num)
        
        # Handle month boundaries
        if day_num < 15 and reference_date.day > 15:
            # Day is probably in next month
            if month == 12:
                date = dt.date(year + 1, 1, day_num)
            else:
                date = dt.date(year, month + 1, day_num)
        elif day_num > 15 and reference_date.day < 15:
            # Day is probably in previous month
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
    """
    Build pairing rows from calendar events.
    
    Args:
        events: List of calendar events
        is_24h: Whether to use 24-hour time format
        only_reports: Whether to filter out days without report times
        include_off_rows: Whether to insert OFF rows between pairings
        home_base: Home base airport code (default DFW)
    
    Returns:
        List of row dictionaries with 'kind' field ('pairing', 'off', or 'non-pairing')
    """
    # First pass: group by pairing ID only
    initial_groups: Dict[str, List[Dict[str, Any]]] = {}
    for e in events:
        pid = (e.get("summary") or "").strip()
        if not pid:
            uid = (e.get("uid") or "")[:8]
            pid = f"PAIR-{uid}"
        initial_groups.setdefault(pid, []).append(e)
    
    # Second pass: split groups based on whether events end at home base
    # Events with the same pairing ID are connected UNLESS they end at home base
    final_groups: Dict[str, List[Dict[str, Any]]] = {}
    group_counter = 0
    
    for pid, evs in initial_groups.items():
        evs_sorted = sorted(evs, key=lambda x: iso_to_dt(x.get("start_utc")) or dt.datetime.min)
        
        current_batch = []
        for e in evs_sorted:
            if not current_batch:
                current_batch.append(e)
            else:
                # Check if previous event ended at home base
                prev_event = current_batch[-1]
                prev_desc = (prev_event.get("description") or "")
                
                # Parse the previous event to see where it ended
                prev_parsed = parse_pairing_days(prev_desc)
                prev_days = prev_parsed.get("days") or []
                
                # Get last leg's arrival airport from previous event
                ended_at_home = False
                if prev_days:
                    last_day = prev_days[-1]
                    last_legs = last_day.get("legs") or []
                    if last_legs:
                        last_arr = last_legs[-1].get("arr", "").upper()
                        ended_at_home = (last_arr == home_base)
                
                # If previous event ended at home base, start a new group
                if ended_at_home:
                    final_groups[f"{pid}#{group_counter}"] = current_batch
                    group_counter += 1
                    current_batch = [e]
                else:
                    # Still away from base, continue the same trip
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
        
        all_parsed_events = []
        for e in evs_sorted:
            parsed = parse_pairing_days(e.get("description") or "")
            event_start_local = to_local(iso_to_dt(e.get("start_utc")))
            all_parsed_events.append((e, parsed, event_start_local))
        
        # Find the first report date across all events
        first_report_date = None
        first_report_hhmm = None
        
        for e, parsed, event_start_local in all_parsed_events:
            days = parsed.get("days") or []
            if days and days[0].get("report_date"):
                # Parse the explicit report date like "15NOV"
                report_date = _parse_report_date(days[0]["report_date"], event_start_local)
                if report_date:
                    first_report_date = report_date
                    first_report_hhmm = days[0].get("report")
                    break
        
        # Build the complete list of days across all events
        # Use a dictionary to track dates by day prefix (e.g., "SU16" -> date)
        day_prefix_to_date = {}
        
        for e, parsed, event_start_local in all_parsed_events:
            days = parsed.get("days") or []
            
            for d in days:
                # Deduplicate days with identical report times and legs
                sig = _day_signature(d)
                if sig in seen_signatures:
                    continue
                seen_signatures.add(sig)
                
                # Determine this day's date based on leg prefixes
                day_date = None
                day_prefix = None
                
                if d.get("legs"):
                    first_leg = d["legs"][0]
                    if first_leg.get("day_prefix"):
                        day_prefix = first_leg["day_prefix"]
                        # Parse day prefix like "SU16" to get the actual date
                        day_date = _parse_day_prefix(day_prefix, event_start_local)
                        
                        # Store this mapping for consistency
                        if day_prefix and day_date:
                            day_prefix_to_date[day_prefix] = day_date
                
                # If we have a day prefix but couldn't parse it, check our mapping
                if day_prefix and not day_date and day_prefix in day_prefix_to_date:
                    day_date = day_prefix_to_date[day_prefix]
                
                # If this is the first day and we have a report date, use it
                if not parsed_days and first_report_date:
                    day_date = first_report_date
                
                # Store the date for this day
                d["actual_date"] = day_date
                
                for leg in d.get("legs", []):
                    # Check if this is a deadhead leg
                    is_deadhead = leg.get("deadhead", False)
                    
                    # Format flight number (add * prefix for deadheads)
                    if not str(leg.get("flight", "")).startswith("FFT"):
                        # Accept any number of digits for flight numbers
                        nums = re.findall(r"\d+", str(leg.get("flight", ""))) or []
                        if nums:
                            flight_num = nums[0]
                            # Add asterisk prefix for deadheads
                            if is_deadhead:
                                leg["flight"] = f"*{flight_num}"
                            else:
                                leg["flight"] = flight_num
                    
                    # Format route with *DH prefix for deadheads
                    dep = leg.get("dep", "")
                    arr = leg.get("arr", "")
                    if is_deadhead:
                        leg["route_display"] = f"*DH {dep}–{arr}"
                    else:
                        leg["route_display"] = f"{dep}–{arr}"
                    
                    # Format block times (departure → arrival)
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
                    
                    # Store raw times for frontend clock mode switching
                    leg["dep_time_str"] = time_display(dep_time, is_24h)
                    leg["arr_time_str"] = time_display(arr_time, is_24h)
                    
                    # Set tracking info
                    if is_deadhead:
                        leg["tracking_display"] = "Check FLICA"
                        leg["tracking_available"] = False
                        leg["tracking_message"] = "Check FLICA"
                    # else: tracking info will be set later based on departure time
                
                parsed_days.append(d)

        # Calculate actual report and release times from parsed data
        def combine_local(date_obj: Optional[dt.date], hhmm: Optional[str]) -> Optional[dt.datetime]:
            if not date_obj or not hhmm:
                return None
            hhmm = _ensure_hhmm(hhmm)
            return dt.datetime(date_obj.year, date_obj.month, date_obj.day, 
                             int(hhmm[:2]), int(hhmm[2:]), tzinfo=LOCAL_TZ)

        # Get first report time
        pairing_report_local = None
        if parsed_days and parsed_days[0].get("actual_date") and parsed_days[0].get("report"):
            import logging
            logging.info(f"Building report from date {parsed_days[0]['actual_date']} and time {parsed_days[0]['report']}")
            pairing_report_local = combine_local(parsed_days[0]["actual_date"], parsed_days[0]["report"])
            logging.info(f"Report datetime: {pairing_report_local} ({pairing_report_local.strftime('%A') if pairing_report_local else 'None'})")
        elif not parsed_days or not parsed_days[0].get("report"):
            # For non-flying events with no parsed report time, use the calendar event start time
            if evs_sorted:
                pairing_report_local = to_local(iso_to_dt(evs_sorted[0].get("start_utc")))
        
        # Get last release time
        pairing_release_local = None
        if parsed_days and parsed_days[-1].get("actual_date") and parsed_days[-1].get("release"):
            last_day = parsed_days[-1]
            release_date = last_day["actual_date"]
            
            # Check if release crosses midnight
            if last_day.get("legs"):
                last_arr = last_day["legs"][-1].get("arr_time", "")
                release = last_day.get("release", "")
                if last_arr and release:
                    # If arrival is late (>= 23:00) and release is early (< 02:00), release is next day
                    arr_hour = int(last_arr[:2])
                    rel_hour = int(release[:2])
                    if arr_hour >= 23 and rel_hour < 2:
                        release_date = release_date + dt.timedelta(days=1)
            
            pairing_release_local = combine_local(release_date, last_day["release"])
        elif not parsed_days or not parsed_days[-1].get("release"):
            # For non-flying events with no parsed release time, use the calendar event end time
            if evs_sorted:
                pairing_release_local = to_local(iso_to_dt(evs_sorted[-1].get("end_utc")))

        now_local = dt.datetime.now(LOCAL_TZ)
        in_progress = bool(pairing_report_local and pairing_release_local and pairing_report_local <= now_local <= pairing_release_local)

        # Only filter by report if NOT in progress
        if only_reports and not in_progress:
            parsed_days = [d for d in parsed_days if d.get("report")]

        days_with_flags: List[Dict[str, Any]] = []
        for idx, d in enumerate(parsed_days, start=1):
            anchor_date = d.get("actual_date")
            
            # Add tracking availability for legs
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
                
                # Mark legs as done if arrival time has passed
                leg["done"] = bool(arr_dt and now_local >= arr_dt)
                
                # Add tracking availability info (skip for deadheads)
                if not leg.get("deadhead", False):
                    if dep_dt and leg.get("flight"):
                        tracking_available_dt = dep_dt - dt.timedelta(hours=24)
                        leg["tracking_available"] = now_local >= tracking_available_dt
                        leg["tracking_available_time"] = tracking_available_dt
                        
                        # Build FlightAware link if tracking is available
                        if leg.get("tracking_available"):
                            flight_num = str(leg.get("flight", "")).replace("FFT", "")
                            # Remove any non-digits
                            flight_num = re.sub(r"[^0-9]", "", flight_num)
                            if flight_num:
                                leg["tracking_display"] = f"FFT{flight_num}"
                                leg["tracking_url"] = f"https://flightaware.com/live/flight/FFT{flight_num}"
                                leg["tracking_message"] = "Tracking available"
                        else:
                            # Not yet available
                            leg["tracking_message"] = f"Tracking available {dep_dt.strftime('%b %d')}"
                            leg["tracking_display"] = leg["tracking_message"]
                    else:
                        leg["tracking_available"] = False
                        leg["tracking_message"] = ""
                        leg["tracking_display"] = ""
            
            # Store report/release datetimes for this day
            day_report_dt = None
            if anchor_date and d.get("report"):
                day_report_dt = combine_local(anchor_date, d.get("report"))
            
            day_release_dt = None
            if anchor_date and d.get("release"):
                day_release_dt = combine_local(anchor_date, d.get("release"))
                # Handle release after midnight
                if day_report_dt and day_release_dt and day_release_dt < day_report_dt:
                    day_release_dt += dt.timedelta(days=1)
            
            days_with_flags.append({
                **d, 
                "day_index": idx,
                "date_local_iso": day_report_dt.isoformat() if day_report_dt else None,
                "day_report_dt": day_report_dt.isoformat() if day_report_dt else None,
                "day_release_dt": day_release_dt.isoformat() if day_release_dt else None,
            })

        # Calculate number of days based on report and release dates
        num_days = 1  # Default to 1 day
        if pairing_report_local and pairing_release_local:
            # Calculate the span in days (partial days count as full days)
            report_date = pairing_report_local.date()
            release_date = pairing_release_local.date()
            delta = (release_date - report_date).days + 1  # +1 because we include both start and end days
            num_days = max(1, delta)
        
        # For multi-day trips, ensure we have entries for ALL days including layovers
        if num_days > 1 and days_with_flags:
            # Map existing days to their dates
            legs_by_date = {}
            for d in days_with_flags:
                if d.get("actual_date"):
                    date_key = d["actual_date"].strftime("%Y-%m-%d")
                    if d.get("legs"):
                        legs_by_date[date_key] = d
            
            # Rebuild the days list with ALL days in the span
            new_days = []
            current_date = pairing_report_local.date() if pairing_report_local else report_date
            prev_arrival = None
            prev_hotel = None
            
            for day_num in range(num_days):
                date_key = current_date.strftime("%Y-%m-%d")
                
                # Check if we have flight data for this day
                if date_key in legs_by_date:
                    # Use the existing day with flights
                    day = legs_by_date[date_key]
                    day["day_index"] = day_num + 1
                    new_days.append(day)
                    
                    # Track where we end up
                    if day.get("legs"):
                        last_leg = day["legs"][-1]
                        prev_arrival = last_leg.get("arr", "")
                    if day.get("hotel"):
                        prev_hotel = day.get("hotel")
                else:
                    # This is a layover day with no flights
                    layover_day = {
                        "actual_date": current_date,
                        "day_index": day_num + 1,
                        "legs": [],
                        "is_layover": True,
                        "layover_location": prev_arrival or "Unknown",
                        "no_flights_message": "No flights scheduled"
                    }
                    
                    # Add hotel info if available
                    if prev_hotel:
                        layover_day["hotel"] = prev_hotel
                    
                    # Add report time for first day if missing
                    if day_num == 0 and parsed_days and parsed_days[0].get("report"):
                        layover_day["report"] = parsed_days[0].get("report")
                    
                    # Add release time for last day if missing
                    if day_num == num_days - 1 and parsed_days and parsed_days[-1].get("release"):
                        layover_day["release"] = parsed_days[-1].get("release")
                    
                    new_days.append(layover_day)
                
                current_date = current_date + dt.timedelta(days=1)
            
            # Use the complete days list with layovers
            days_with_flags = new_days
        
        # Format display strings
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
        
        # Calculate total legs
        total_legs = sum(len(d.get("legs", [])) for d in days_with_flags)
        
        # Find first departure airport
        first_dep_airport = None
        for d in days_with_flags:
            legs = d.get("legs", [])
            if legs and legs[0].get("dep"):
                first_dep_airport = str(legs[0]["dep"]).upper()
                break
        
        # Determine if out-of-base using pairing prefix logic
        # W prefix = DFW home pairing, not out-of-base even if starting elsewhere
        is_home_pairing = False
        if pairing_id:
            # Check for W, X, Y, Z prefix patterns that indicate home base
            if re.match(r'^[WXYZ]\d', pairing_id, re.IGNORECASE):
                is_home_pairing = True
        
        # Only mark as out-of-base if:
        # 1. NOT a home pairing prefix, AND
        # 2. First departure is different from home base
        out_of_base_airport = None
        if not is_home_pairing and first_dep_airport and first_dep_airport != home_base:
            out_of_base_airport = first_dep_airport

        pairings.append(
            {
                "kind": "pairing",
                "pairing_id": pairing_id,
                "in_progress": int(in_progress),
                "report_local_iso": pairing_report_local.isoformat() if pairing_report_local else None,
                "release_local_iso": pairing_release_local.isoformat() if pairing_release_local else None,
                "display": {"report_str": report_disp, "release_str": release_disp},
                "days": days_with_flags,
                "num_days": num_days,  # Calculated span between report and release
                "uid": uid,
                "total_legs": total_legs,
                "has_legs": total_legs > 0,
                "first_dep_airport": first_dep_airport,
                "out_of_base_airport": out_of_base_airport,
            }
        )

    pairings.sort(key=lambda r: r.get("report_local_iso") or "")

    # If not including OFF rows, just return the pairings
    if not include_off_rows:
        return pairings

    # Build rows with OFF times between ACTUAL pairings ONLY
    # Separate actual pairings (with legs) from non-pairing events
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
    
    # Build the final rows array with OFF rows interspersed and non-pairing events in chronological order
    rows: List[Dict[str, Any]] = []
    
    import logging
    logging.info(f"Building rows with {len(actual_pairings)} actual pairings and {len(non_pairing_events)} non-pairing events")
    
    # First, add any non-pairing events that come before the first pairing
    if actual_pairings:
        first_report = iso_to_dt(actual_pairings[0].get("report_local_iso"))
        logging.info(f"First pairing reports at: {first_report}, current time: {now_local}")
        
        # Add OFF row if the first pairing is in the future
        if first_report and first_report > now_local:
            gap = first_report - now_local
            logging.info(f"First pairing is in future, gap: {gap}")
            # Only create OFF row if gap is meaningful (> 1 hour)
            if gap.total_seconds() > 3600:
                gap_str = _human_dur(gap)
                
                off_row = {
                    "kind": "off",
                    "is_current": True,  # Flag to indicate this is the current OFF period
                    "display": {
                        "off_label": "OFF",
                        "off_duration": gap_str,
                        "show_remaining": True  # Just a flag to show "(Remaining)"
                    }
                }
                rows.append(off_row)
                logging.info(f"Added OFF row before first pairing: {off_row}")
        
        # Add non-pairing events before first pairing
        for npe in non_pairing_events:
            npe_time = iso_to_dt(npe.get("report_local_iso"))
            if npe_time and first_report and npe_time < first_report:
                rows.append(npe)
    
    # Now add pairings with OFF rows between them
    for i, p in enumerate(actual_pairings):
        rows.append(p)
        
        # Check for OFF period between this pairing and the next
        if i + 1 < len(actual_pairings):
            release = iso_to_dt(p.get("release_local_iso"))
            nxt_report = iso_to_dt(actual_pairings[i + 1].get("report_local_iso"))
            
            if release and nxt_report and release < nxt_report:
                gap = nxt_report - release
                
                # Only create OFF row if gap is positive and meaningful (> 1 hour)
                if gap.total_seconds() > 3600:
                    # Calculate time remaining if OFF period is currently active
                    if now_local > release and now_local < nxt_report:
                        remaining = nxt_report - now_local
                        gap_str = f"{_human_dur(gap)}"
                        remaining_str = f"{_human_dur(remaining)}"
                        off_label = "OFF"
                        is_current = True
                        # Provide both for frontend formatting
                        off_dur_full = f"{gap_str} (Remaining: {remaining_str})"
                    else:
                        gap_str = _human_dur(gap)
                        off_label = "OFF"
                        is_current = False
                        off_dur_full = gap_str
                    
                    # Add the OFF row
                    rows.append({
                        "kind": "off",
                        "is_current": is_current,
                        "display": {
                            "off_dur": off_dur_full,
                            "off_label": off_label,
                            "off_duration": gap_str,  # Raw duration without "(Remaining)"
                            "off_remaining": remaining_str if is_current else None
                        }
                    })
                    
                    # Find and add any non-pairing events that fall within this OFF period
                    for npe in non_pairing_events:
                        npe_time = iso_to_dt(npe.get("report_local_iso"))
                        if npe_time and release <= npe_time < nxt_report:
                            rows.append(npe)
    
    # Handle any non-pairing events that come after all pairings
    if actual_pairings:
        last_release = iso_to_dt(actual_pairings[-1].get("release_local_iso"))
        for npe in non_pairing_events:
            npe_time = iso_to_dt(npe.get("report_local_iso"))
            if npe_time and last_release and npe_time >= last_release:
                rows.append(npe)
    # If there are no actual pairings, just add all non-pairing events
    elif non_pairing_events:
        rows.extend(non_pairing_events)
    
    return rows