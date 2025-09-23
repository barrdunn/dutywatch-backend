"""
DutyWatch config loader

Reads settings from .env (python-dotenv) and exposes them as module constants.
Safe defaults let the app boot even if some creds are missing (simulation mode).
"""

from dotenv import load_dotenv
load_dotenv()

import os

# --- iCloud CalDAV ---
ICLOUD_USER    = os.getenv("ICLOUD_USER")
ICLOUD_APP_PW  = os.getenv("ICLOUD_APP_PW")
CALDAV_URL     = os.getenv("CALDAV_URL", "https://caldav.icloud.com/")

# --- Twilio (optional while simulating) ---
TWILIO_SID     = os.getenv("TWILIO_SID")         # None is OK in simulation
TWILIO_AUTH    = os.getenv("TWILIO_AUTH")
TWILIO_FROM    = os.getenv("TWILIO_FROM")
PUBLIC_BASE_URL= os.getenv("PUBLIC_BASE_URL")

# --- APNs (optional; unused while simulating) ---
APNS_TEAM_ID   = os.getenv("APNS_TEAM_ID")
APNS_KEY_ID    = os.getenv("APNS_KEY_ID")
APNS_KEY_PEM   = os.getenv("APNS_KEY_PEM")       # path or inline PEM
APNS_BUNDLE_ID = os.getenv("APNS_BUNDLE_ID")
APNS_ENV       = os.getenv("APNS_ENV", "sandbox")  # "sandbox" or "production"
APNS_HOST      = "https://api.sandbox.push.apple.com" if APNS_ENV == "sandbox" else "https://api.push.apple.com"

# --- App settings ---
TIMEZONE                     = os.getenv("TIMEZONE", "America/Chicago")
LOOKAHEAD_HOURS              = int(os.getenv("LOOKAHEAD_HOURS", "24"))
CALENDAR_NAME_FILTER         = (os.getenv("CALENDAR_NAME_FILTER") or "").lower()
SCHEDULE_LOOKAHEAD_HOURS     = int(os.getenv("SCHEDULE_LOOKAHEAD_HOURS", "336"))
REPORT_LEAD_MINUTES          = int(os.getenv("REPORT_LEAD_MINUTES", "60"))
CHECKOUT_PAD_MINUTES         = int(os.getenv("CHECKOUT_PAD_MINUTES", "15"))
NOTIFY_BEFORE_REPORT_MINUTES = int(os.getenv("NOTIFY_BEFORE_REPORT_MINUTES", "15"))

# --- Simulation toggles ---
SIMULATE_PUSH = os.getenv("SIMULATE_PUSH", "1") == "1"
SIMULATE_CALL = os.getenv("SIMULATE_CALL", "1") == "1"
