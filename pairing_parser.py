# pairing_parser.py
from __future__ import annotations
import re
from typing import Dict, List, Optional

# ---- simple helpers ----

def _hhmm_from_local(s: str) -> Optional[str]:
    """
    Accepts '0700L', '7:00', '0700', '800', etc. Returns 'HH:MM' or None.
    """
    s = s.strip().upper().replace("L", "")
    # 07:00
    m = re.match(r"^(\d{1,2}):?(\d{2})$", s)
    if not m:
        return None
    h = int(m.group(1))
    mnt = int(m.group(2))
    if h < 0 or h > 23 or mnt < 0 or mnt > 59:
        return None
    return f"{h:02d}:{mnt:02d}"

def _mins(hhmm: Optional[str]) -> Optional[int]:
    if not hhmm: return None
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)

def _mins_to_hhmm(total: int) -> str:
    total %= (24 * 60)
    h = total // 60
    m = total % 60
    return f"{h:02d}:{m:02d}"

def _block(dep: Optional[str], arr: Optional[str]) -> Optional[str]:
    if not dep or not arr: return None
    d = _mins(dep); a = _mins(arr)
    if d is None or a is None: return None
    if a < d:  # overnight
        a += 24 * 60
    return _mins_to_hhmm(a - d)

def _add15(hhmm: Optional[str]) -> Optional[str]:
    if not hhmm: return None
    return _mins_to_hhmm((_mins(hhmm) or 0) + 15)

def _first_group(s: str, pattern: str, flags=0) -> Optional[str]:
    m = re.search(pattern, s, flags)
    return m.group(1) if m else None

# ---- core parsing ----

REPORT_RE = re.compile(r"Report:\s*([0-2]?\d[:.]?\d{2})\s*L?", re.I)
LEG_RE = re.compile(
    r"\b([A-Z]{3})-([A-Z]{3})\s+([0-2]?\d[:.]?\d{2})-([0-2]?\d[:.]?\d{2})\b"
)
HOTEL_RE = re.compile(
    r"(?:Hotel|Element|Embassy|Hilton|Hyatt|Marriott|Westin|Inn|Suites)[^\.]*",
    re.I,
)

def _parse_day(idx: int, description: str) -> Dict:
    txt = (description or "")
    # normalize times like 0800 or 8:00 -> HH:MM
    report_raw = _first_group(txt, REPORT_RE)
    report = _hhmm_from_local(report_raw) if report_raw else None

    legs = []
    last_arr = None
    for m in LEG_RE.finditer(txt):
        orig, dest, dep_raw, arr_raw = m.groups()
        dep = _hhmm_from_local(dep_raw)
        arr = _hhmm_from_local(arr_raw)
        blk = _block(dep, arr)
        legs.append({
            "origin": orig,
            "destination": dest,
            "dep_local": dep,
            "arr_local": arr,
            "block_time": blk,
        })
        if arr:
            last_arr = arr

    release = _add15(last_arr) if last_arr else None

    hotel_match = HOTEL_RE.search(txt)
    hotel = hotel_match.group(0).strip() if hotel_match else None

    return {
        "day_index": idx,
        "report_time_local": report,
        "release_time_local": release,
        "hotel": hotel,
        "legs": legs,
    }

def parse_pairing_days(events_for_pairing: List[Dict]) -> Dict:
    """
    Input: list of calendar events (same pairing) sorted by start_utc.
    Each event dict should at least have 'summary' and 'description'.
    Output matches the LLM schema:
      { pairing_id, pairing_start_local, pairing_end_local, days: [...] }
    """
    if not events_for_pairing:
        return {"pairing_id": "", "pairing_start_local": None, "pairing_end_local": None, "days": []}

    pairing_id = (events_for_pairing[0].get("summary") or "").strip()
    days = []
    first_report = None
    last_release = None

    for i, ev in enumerate(events_for_pairing, start=1):
        d = _parse_day(i, ev.get("description") or "")
        days.append(d)
        if not first_report and d.get("report_time_local"):
            first_report = d["report_time_local"]
        if d.get("release_time_local"):
            last_release = d["release_time_local"]

    return {
        "pairing_id": pairing_id,
        "pairing_start_local": first_report,
        "pairing_end_local": last_release,
        "days": days,
    }
