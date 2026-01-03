"""
Regex-only parser for a single event description -> {"days":[...]}.

What it extracts per *day*:
- report: "HHMM" (e.g., "0715")
- legs: [{ flight, dep, arr, dep_time, arr_time }]
- hotel: a human-looking place string (from event location if present, else from description)
- release: last arrival + 15 minutes (HHMM)

Hotel detection is brand-agnostic:
1) If the event 'location' looks like a place name, use it.
2) Else scan description lines and pick the first plausible "place-like" line:
   - skip report lines, leg lines, phone-only lines, boilerplate
   - prefer lines containing hotel-ish words (Hotel/Inn/Suites/Resort/Lodge/Residence/Place/Element/etc.)
   - otherwise accept the first human-looking line (letters + a space).
No LLMs, just simple regex + heuristics.
"""

from __future__ import annotations
import re
from typing import Any, Dict, Optional, List

# --- core patterns ---
# Report can be either "Report: 15NOV 0545L" or just "Report: 0500L"
REPORT_RE = re.compile(r"\bReport:\s*(?:(\d{1,2}[A-Z]{3})\s+)?(\d{3,4})L?\b", re.IGNORECASE)

# Leg format: "FR26 1596 ORD-ATL 0555-0853" or "MO29 DH 2066 ATL-ORD 1020-1141"
# Day prefix (FR26, SA27, SU28, MO29) is optional
# DH prefix indicates deadhead
# Also handle "N NTR" which seems to be a special non-revenue/deadhead indicator
LEG_RE = re.compile(
    r"\b(?:([A-Z]{2}\d{1,2})\s+)?(?:(DH|N\s*NTR)\s+)?(\d+)\s+([A-Z]{3})-([A-Z]{3})\s+(\d{3,4})-(\d{3,4})\b",
    re.IGNORECASE
)

# Pattern for valid pairing IDs: letter + numbers + optional letter suffix
# Examples: W1234, C3075, D5678F, A1234B
PAIRING_ID_PATTERN = re.compile(r'^[A-Z]\d+[A-Z]?$', re.IGNORECASE)

# Soft keywords to *prefer* but not require (kept broad and brand-agnostic)
HOTELISH_KEYWORDS = re.compile(
    r"\b(Hotel|Inn|Suites?|Resort|Lodge|Residence|Place|Element|Embassy|Hilton|Hyatt|Westin|Marriott|Plaza|Centre|Center|Aloft|Courtyard|Stay)\b",
    re.IGNORECASE,
)

PHONE_ONLY_RE = re.compile(r"^\s*[\d\-\s().+]{7,}\s*$")
BOILERPLATE_RE = re.compile(r"Created by the Flight Crew View App", re.IGNORECASE)


def is_valid_pairing_id(pairing_id: str) -> bool:
    """
    Check if a string is a valid pairing ID.
    
    Valid pairing IDs:
    - Start with a letter (the base prefix)
    - Followed by numbers
    - Optionally end with a letter suffix
    
    Examples of valid: W1234, C3075, D5678F, A1234B
    Examples of invalid: CBT, VAC, SICK, RDO, Training, 1234
    """
    if not pairing_id:
        return False
    return bool(PAIRING_ID_PATTERN.match(pairing_id.strip()))


def extract_pairing_id(summary: str) -> str:
    """
    Extract the pairing ID from event summary.
    
    Pairing IDs are like W1234, C3075F, D5678.
    Returns the full ID including any trailing letter suffix.
    """
    if not summary:
        return ""
    # Match letter + numbers + optional trailing letter
    match = re.match(r'^([A-Z]\d+[A-Z]?)', summary.strip().upper())
    if match:
        return match.group(1)
    return summary.strip().upper()


def _ensure_hhmm(s: str) -> str:
    """Normalize '800' -> '0800'."""
    return s if len(s) == 4 else s.zfill(4)


def _mins_from_hhmm(hhmm: str) -> int:
    """Convert 'HHMM' to total minutes since midnight."""
    hhmm = _ensure_hhmm(hhmm)
    return int(hhmm[:2]) * 60 + int(hhmm[2:])


def _hhmm_from_mins(total: int) -> str:
    """Convert total minutes to 'HHMM' (wrap 24h)."""
    total = total % (24 * 60)
    h = total // 60
    m = total % 60
    return f"{h:02d}{m:02d}"


def _looks_like_place(s: str) -> bool:
    """
    Heuristic "place" check (not AI):
    - must contain at least one letter
    - must contain at least one space (prefers human names over codes)
    - must not be phone-only
    """
    if not s:
        return False
    if PHONE_ONLY_RE.match(s):
        return False
    has_letters = re.search(r"[A-Za-z]", s) is not None
    has_space = " " in s.strip()
    return has_letters and has_space


def _extract_hotel(description: str, location: Optional[str]) -> Optional[str]:
    """
    Choose a hotel / lodging string:
    1) Prefer event location if it looks like a place.
    2) Else scan description lines; prefer hotel-ish keyword lines, otherwise
       first human-looking line.
    """
    # 1) Prefer event location
    if location and _looks_like_place(location.strip()):
        return location.strip()

    # 2) Scan description lines
    lines: List[str] = [ln.strip() for ln in (description or "").splitlines() if ln.strip()]
    candidates_pref: List[str] = []
    candidates_fallback: List[str] = []

    for ln in lines:
        if REPORT_RE.search(ln):
            continue
        if LEG_RE.search(ln):
            continue
        if PHONE_ONLY_RE.match(ln):
            continue
        if BOILERPLATE_RE.search(ln):
            continue

        if HOTELISH_KEYWORDS.search(ln):
            candidates_pref.append(ln)
        elif _looks_like_place(ln):
            candidates_fallback.append(ln)

    if candidates_pref:
        return candidates_pref[0]
    if candidates_fallback:
        return candidates_fallback[0]
    return None


def parse_pairing_days(text: str, location: Optional[str] = None) -> Dict[str, Any]:
    """
    Parse one event's description (and optional location) into:
      {"days":[{ "report","legs":[...],"release","hotel" }]}
    """
    out: Dict[str, Any] = {"days": []}

    # Report time (HHMM) and optional date (e.g., "15NOV")
    m = REPORT_RE.search(text or "")
    report_date_str = m.group(1) if m and m.group(1) else None  # e.g., "15NOV"
    report = _ensure_hhmm(m.group(2)) if m else None  # e.g., "2334"

    # Legs
    legs: List[Dict[str, Any]] = []
    for m in LEG_RE.finditer(text or ""):
        day_prefix, dh_prefix, num, dep, arr, t_dep, t_arr = m.groups()
        # DH or "N NTR" both indicate deadhead
        is_deadhead = bool(dh_prefix)
        legs.append(
            {
                # Keep numeric flight; prefixing (e.g., 'FFT') can be added by the caller.
                "flight": num,
                "dep": dep.upper(),
                "arr": arr.upper(),
                "dep_time": _ensure_hhmm(t_dep),
                "arr_time": _ensure_hhmm(t_arr),
                "deadhead": is_deadhead,
                "day_prefix": day_prefix.upper() if day_prefix else None,  # e.g., "FR26" or None
            }
        )

    # Hotel / lodging
    hotel = _extract_hotel(text or "", (location or "").strip() or None)

    # Release time = last arrival + 15 min (if any legs)
    release = None
    if legs:
        last_arr = legs[-1]["arr_time"]
        release = _hhmm_from_mins(_mins_from_hhmm(last_arr) + 15)

    # Only emit a day if we found *something* useful
    if report or legs or hotel:
        out["days"].append(
            {
                "report": report,
                "report_date": report_date_str,  # e.g., "15NOV" or None
                "legs": legs,
                "release": release,
                "hotel": hotel,
            }
        )

    return out