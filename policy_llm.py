"""
DutyWatch Policy LLM Translator (Stub)

- Converts natural-language prompt ("notify me 90/60/30…")
  into a ReminderPolicy JSON object
- Currently returns default policy until LLM function-calling is integrated
"""

import json
from models import ReminderPolicy

def natural_to_policy(prompt: str) -> dict:
    # TODO: replace with function-call to your LLM; validate with pydantic
    # For now, just return defaults and you can edit in the app.
    return json.loads(ReminderPolicy().model_dump_json())
