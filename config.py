"""
DutyWatch config loader (env + tracked JSON)

- Secrets: from .env (NOT tracked)
- User knobs: from settings.json (tracked in repo); env can override
- Exposes both the new knobs (VIEW_WINDOW_MODE, HOME_BASE, etc.) and
  legacy constants for back-compat (e.g., CALENDAR_NAME_FILTER).
"""

from __future__ import annotations
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent / ".env")

import os, json
from pathlib import Path

# ---------- secrets via .env ----------
ICLOUD_USER    = os.getenv("ICLOUD_USER")
ICLOUD_APP_PW  = os.getenv("ICLOUD_APP_PW")
CALDAV_URL     = os.getenv("CALDAV_URL", "https://caldav.icloud.com/")

TWILIO_SID     = os.getenv("TWILIO_SID")
TWILIO_AUTH    = os.getenv("TWILIO_AUTH")
TWILIO_FROM    = os.getenv("TWILIO_FROM")
PUBLIC_BASE_URL= os.getenv("PUBLIC_BASE_URL")

APNS_TEAM_ID   = os.getenv("APNS_TEAM_ID")
APNS_KEY_ID    = os.getenv("APNS_KEY_ID")
APNS_KEY_PEM   = os.getenv("APNS_KEY_PEM")
APNS_BUNDLE_ID = os.getenv("APNS_BUNDLE_ID")

_apns_env = (os.getenv("APNS_ENV", "sandbox") or "").lower()
APNS_ENV  = "sandbox" if _apns_env in ("dev", "sandbox") else "production"
APNS_HOST = "https://api.sandbox.push.apple.com" if APNS_ENV == "sandbox" else "https://api.push.apple.com"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # optional

# ---------- user knobs via tracked settings.json ----------
BASE_DIR = Path(__file__).parent
SETTINGS_PATH = BASE_DIR / "settings.json"

def _load_settings() -> dict:
    try:
        if SETTINGS_PATH.exists():
            with open(SETTINGS_PATH, "r") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}

_SETTINGS = _load_settings()

def _knob(env_name: str, json_path: str, default):
    """
    env overrides -> settings.json -> default
    json_path like "frontend.default_refresh_minutes"
    """
    if env_name in os.environ and os.environ[env_name] != "":
        val = os.environ[env_name]
        # cast to default's type when possible
        try:
            if isinstance(default, int): return int(val)
            if isinstance(default, bool): return val == "1" or val.lower() in ("true","yes","on")
            return val
        except Exception:
            return val
    # dig into settings.json
    cur = _SETTINGS
    for part in json_path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            cur = None
            break
        cur = cur[part]
    return cur if cur is not None else default

# New knobs
TIMEZONE         = _knob("TIMEZONE",          "timezone", "America/Chicago")
VIEW_WINDOW_MODE = (_knob("VIEW_WINDOW_MODE", "view_window_mode", "TODAY_TO_END_OF_NEXT_MONTH") or "").upper()
HOME_BASE        = (_knob("HOME_BASE",        "home_base", "DFW") or "").upper()

DEFAULT_CLOCK_MODE      = str(_knob("DEFAULT_CLOCK_MODE",      "frontend.default_clock_mode", "12"))
DEFAULT_REFRESH_MINUTES = int(_knob("DEFAULT_REFRESH_MINUTES", "frontend.default_refresh_minutes", 30))

# Backend refresh cadence (allow override via env)
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", str(DEFAULT_REFRESH_MINUTES * 60)))

# ---------- Legacy constants (back-compat) ----------
CALENDAR_NAME_FILTER         = (os.getenv("CALENDAR_NAME_FILTER") or "Work").lower()
LOOKAHEAD_HOURS              = int(os.getenv("LOOKAHEAD_HOURS", "24"))
SCHEDULE_LOOKAHEAD_HOURS     = int(os.getenv("SCHEDULE_LOOKAHEAD_HOURS", "336"))
REPORT_LEAD_MINUTES          = int(os.getenv("REPORT_LEAD_MINUTES", "60"))
CHECKOUT_PAD_MINUTES         = int(os.getenv("CHECKOUT_PAD_MINUTES", "15"))
NOTIFY_BEFORE_REPORT_MINUTES = int(os.getenv("NOTIFY_BEFORE_REPORT_MINUTES", "15"))

# Simulation toggles
SIMULATE_PUSH = (os.getenv("SIMULATE_PUSH", "1") == "1")
SIMULATE_CALL = (os.getenv("SIMULATE_CALL", "1") == "1")

# For easy introspection by /api/config if you add it
def effective_settings() -> dict:
    return {
        "timezone": TIMEZONE,
        "view_window_mode": VIEW_WINDOW_MODE,
        "home_base": HOME_BASE,
        "frontend": {
            "default_clock_mode": DEFAULT_CLOCK_MODE,
            "default_refresh_minutes": DEFAULT_REFRESH_MINUTES,
        },
        "refresh_seconds": REFRESH_SECONDS,
        "legacy": {
            "calendar_name_filter": CALENDAR_NAME_FILTER,
            "lookahead_hours": LOOKAHEAD_HOURS,
            "schedule_lookahead_hours": SCHEDULE_LOOKAHEAD_HOURS,
            "report_lead_minutes": REPORT_LEAD_MINUTES,
            "checkout_pad_minutes": CHECKOUT_PAD_MINUTES,
            "notify_before_report_minutes": NOTIFY_BEFORE_REPORT_MINUTES,
        },
        "apns": {"env": APNS_ENV},
        "simulate": {"push": SIMULATE_PUSH, "call": SIMULATE_CALL},
    }
