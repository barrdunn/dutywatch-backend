"""
DutyWatch Pydantic Models (minimal)
"""
from pydantic import BaseModel, Field
from typing import List

class DetectEvents(BaseModel):
    include_keywords: List[str] = Field(default_factory=lambda: ["report","show","sign in","check in"])
    exclude_keywords: List[str] = Field(default_factory=list)

class QuietHours(BaseModel):
    start: str = "22:30"
    end: str = "05:30"
    override_for_work: bool = True

class AckPolicy(BaseModel):
    require_ack: bool = True
    deadline_min_before_event: int = 10
    actions: List[str] = ["acknowledge","snooze_5","call_me"]

class EscalationPolicy(BaseModel):
    twilio_call_if_no_ack: bool = True
    repeat_every_min: int = 5
    max_repeats: int = 3
    say_text: str = "This is DutyWatch. Please acknowledge your report time."

class ReminderPolicy(BaseModel):
    version: int = 1
    detect_events: DetectEvents = DetectEvents()
    lead_times_min: List[int] = [120,90,60,30,15,5]
    quiet_hours_local: QuietHours = QuietHours()
    ack: AckPolicy = AckPolicy()
    escalation: EscalationPolicy = EscalationPolicy()
    notification_text_template: str = "Report at {event_start_local}. Leave by {leave_time_local}."

class DeviceIn(BaseModel):
    device_token: str

class NaturalPrompt(BaseModel):
    prompt: str

class AckIn(BaseModel):
    ack_id: str
    action: str  # "ack" | "call_me" | "snooze_5"
