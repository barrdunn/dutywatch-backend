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
REPORT_RE = re.compile(r"\bReport:\s*(\d{3,4})L?\b", re.IGNORECASE)
LEG_RE = re.compile(r"\b(\d{3,4})\s+([A-Z]{3})-([A-Z]{3})\s+(\d{3,4})-(\d{3,4})\b")

# Soft keywords to *prefer* but not require (kept broad and brand-agnostic)
HOTELISH_KEYWORDS = re.compile(
    r"\b(Hotel|Inn|Suites?|Resort|Lodge|Residence|Place|Element|Embassy|Hilton|Hyatt|Westin|Marriott|Plaza|Centre|Center|Aloft|Courtyard|Stay)\b",
    re.IGNORECASE,
)

PHONE_ONLY_RE = re.compile(r"^\s*[\d\-\s().+]{7,}\s*$")
BOILERPLATE_RE = re.compile(r"Created by the Flight Crew View App", re.IGNORECASE)


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

    # Report time (HHMM)
    m = REPORT_RE.search(text or "")
    report = _ensure_hhmm(m.group(1)) if m else None

    # Legs
    legs: List[Dict[str, Any]] = []
    for m in LEG_RE.finditer(text or ""):
        num, dep, arr, t_dep, t_arr = m.groups()
        legs.append(
            {
                # Keep numeric flight; prefixing (e.g., 'FFT') can be added by the caller.
                "flight": num,
                "dep": dep,
                "arr": arr,
                "dep_time": _ensure_hhmm(t_dep),
                "arr_time": _ensure_hhmm(t_arr),
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
                "legs": legs,
                "release": release,
                "hotel": hotel,
            }
        )

    return out
