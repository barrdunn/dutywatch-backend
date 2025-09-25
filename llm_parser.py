"""
llm_parser.py
--------------
Lightweight parser for Flight Crew View descriptions.

- Default: fast regex-based parsing (no API calls)
- Optional: if OPENAI_API_KEY is set AND you pass use_llm=True,
  we call OpenAI to refine/normalize the same fields.

Fields we extract per *day* (one calendar event):
- report_local (e.g., "0700L")
- legs: list of { day_code, flight_no, dep, arr, dep_time, arr_time }
- hotel (free text, best-effort)
- release_local (arr_time + 15min, e.g., "2315L")

Public API:
- parse_pairing_day(desc: str, use_llm: bool=False) -> dict
"""

from __future__ import annotations
import os, re, datetime as dt
from typing import List, Dict, Any, Optional

# Optional OpenAI client (v1+). If not present or key not set, we skip LLM.
try:
    from openai import OpenAI  # openai>=1.40
except Exception:
    OpenAI = None

TIME_SUFFIX = "L"

LEG_LINE_RE = re.compile(
    r"\b([A-Z]{2}\d{2})\s+(\d{3,4})\s+([A-Z]{3})-([A-Z]{3})\s+(\d{3,4})-(\d{3,4})"
)
REPORT_RE = re.compile(r"Report:\s*([0-2]?\d[0-5]\d)"+re.escape(TIME_SUFFIX), re.IGNORECASE)

def _add_15(hhmm: str) -> str:
    hh = int(hhmm[:-2])
    mm = int(hhmm[-2:])
    total = hh * 60 + mm + 15
    total %= 24 * 60
    nh, nm = divmod(total, 60)
    return f"{nh:02d}{nm:02d}"

def _parse_regex(desc: str) -> Dict[str, Any]:
    report_local = None
    hotel = None
    legs: List[Dict[str, Any]] = []

    lines = [ln.strip() for ln in (desc or "").splitlines() if ln.strip()]
    for ln in lines:
        mrep = REPORT_RE.search(ln)
        if mrep:
            report_local = mrep.group(1) + TIME_SUFFIX
            continue
        mleg = LEG_LINE_RE.search(ln)
        if mleg:
            legs.append({
                "day_code": mleg.group(1),
                "flight_no": mleg.group(2),
                "dep": mleg.group(3),
                "arr": mleg.group(4),
                "dep_time": mleg.group(5),
                "arr_time": mleg.group(6),
            })
            continue

    # crude hotel pick: first non-leg, non-report line that has letters and not a phone-only line
    for ln in lines:
        if REPORT_RE.search(ln): 
            continue
        if LEG_LINE_RE.search(ln):
            continue
        # phone lines are usually () or digits-heavy; skip those
        if re.fullmatch(r"[\d\-\s\(\)]+", ln):
            continue
        if "Created by the Flight Crew View App" in ln:
            continue
        # keep first plausible hotel-ish line
        hotel = ln
        break

    # release = last arr_time + 15
    release_local = None
    if legs:
        last_arr = legs[-1]["arr_time"]
        release_local = _add_15(last_arr) + TIME_SUFFIX

    return {
        "report_local": report_local,        # "0700L"
        "legs": legs,                        # list
        "hotel": hotel,                      # string or None
        "release_local": release_local,      # "2315L"
    }

_LLM_SYS = (
    "You convert airline crew pairing day text into structured JSON for a single day. "
    "Return only JSON with keys: report_local (string like '0700L'), "
    "legs (array of {day_code, flight_no, dep, arr, dep_time, arr_time}), "
    "hotel (string or null), release_local (string like '2315L'). "
    "Times keep 'L' suffix. release_local is last arr_time + 15 minutes."
)

def _call_openai(desc: str) -> Optional[Dict[str, Any]]:
    if OpenAI is None:
        return None
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    client = OpenAI(api_key=api_key)
    prompt = (
        "TEXT:\n" + desc + "\n\n"
        "Extract the JSON now."
    )
    try:
        # uses the Responses API (new SDK). You can swap to chat.completions if you prefer.
        resp = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": _LLM_SYS},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
        )
        content = resp.choices[0].message.content
        # Extract JSON (best effort)
        import json
        # If it returns fenced block:
        m = re.search(r"\{.*\}", content, re.S)
        if m:
            return json.loads(m.group(0))
        return json.loads(content)
    except Exception:
        return None

def parse_pairing_day(desc: str, use_llm: bool=False) -> Dict[str, Any]:
    """
    Parse one day's description. If use_llm and OPENAI_API_KEY present,
    try LLM; else fallback to regex.
    """
    base = _parse_regex(desc or "")
    if not use_llm:
        return base

    refined = _call_openai(desc or "")
    if not refined:
        return base

    # merge: keep refined keys if present, else fallback to base
    for k, v in base.items():
        refined.setdefault(k, v)
    return refined
