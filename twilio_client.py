"""
DutyWatch Twilio Voice Call Integration

- Provides FastAPI router with endpoints for Twilio webhooks:
    • /twilio/voice/entry – TwiML greeting + <Gather> DTMF input
    • /twilio/voice/gather – handles pressed digits (ack/snooze)
- Function place_ack_call() triggers outbound calls via Twilio REST API
"""

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse
from twilio.rest import Client as TwilioClient
from config import TWILIO_SID, TWILIO_AUTH, TWILIO_FROM, PUBLIC_BASE_URL
from db import get_db
import datetime as dt

router = APIRouter(prefix="/twilio", tags=["twilio"])

_twilio = TwilioClient(TWILIO_SID, TWILIO_AUTH)

def place_ack_call(to_number: str, ack_id: str, say_text: str):
    # Twilio will fetch TwiML from our endpoint
    url = f"{PUBLIC_BASE_URL}/twilio/voice/entry?ack_id={ack_id}"
    _twilio.calls.create(to=to_number, from_=TWILIO_FROM, url=url)

@router.post("/voice/entry")
async def voice_entry(request: Request):
    ack_id = request.query_params.get("ack_id","")
    # Immediate greeting + Gather
    twiml = f"""
<Response>
  <Say voice="Polly.Joanna">This is DutyWatch calling.</Say>
  <Pause length="1"/>
  <Say>Please press 1 to acknowledge your report reminder.</Say>
  <Say>Press 2 to snooze for five minutes.</Say>
  <Gather input="dtmf" timeout="10" numDigits="1" action="/twilio/voice/gather?ack_id={ack_id}" method="POST"/>
  <Say>No input received. We will call back shortly if not acknowledged.</Say>
</Response>
""".strip()
    return PlainTextResponse(content=twiml, media_type="application/xml")

@router.post("/voice/gather")
async def voice_gather(request: Request):
    form = await request.form()
    digits = form.get("Digits", "")
    ack_id = request.query_params.get("ack_id","")
    action = "ack" if digits == "1" else ("snooze_5" if digits == "2" else "noop")
    if action in ("ack","snooze_5"):
        with get_db() as c:
            state = "ack" if action == "ack" else "snoozed"
            c.execute("UPDATE acks SET state=?, last_update_utc=? WHERE ack_id=?",
                      (state, dt.datetime.utcnow().isoformat(), ack_id))
    # Final TwiML
    msg = "Acknowledged. Have a great day." if action=="ack" else (
          "Snoozed for five minutes." if action=="snooze_5" else
          "No valid input received.")
    twiml = f"<Response><Say>{msg}</Say></Response>"
    return PlainTextResponse(content=twiml, media_type="application/xml")
