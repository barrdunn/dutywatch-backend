"""
DutyWatch APNs Push Sender

- Handles creation and caching of APNs JWT token for auth
- Sends push notifications over HTTP/2 to Apple APNs endpoint
- Accepts custom payloads and notification category (DUTYWATCH_DUTY)
"""

import time, httpx, jwt
from pathlib import Path
from config import APNS_TEAM_ID, APNS_KEY_ID, APNS_KEY_PEM, APNS_BUNDLE_ID, APNS_HOST

_cached_token = {"jwt": None, "exp": 0}

def _get_jwt():
    now = int(time.time())
    if _cached_token["jwt"] and now < _cached_token["exp"] - 60:
        return _cached_token["jwt"]
    key = Path(APNS_KEY_PEM).read_text()
    token = jwt.encode(
        {"iss": APNS_TEAM_ID, "iat": now},
        key,
        algorithm="ES256",
        headers={"alg":"ES256","kid":APNS_KEY_ID}
    )
    _cached_token["jwt"] = token
    _cached_token["exp"] = now + 50*60  # 50 minutes
    return token

async def send_push(device_token: str, title: str, body: str, payload: dict, category: str = "DUTYWATCH_DUTY"):
    jwt_token = _get_jwt()
    url = f"{APNS_HOST}/3/device/{device_token}"
    headers = {
        "authorization": f"bearer {jwt_token}",
        "apns-topic": APNS_BUNDLE_ID,
        "apns-push-type": "alert",
        "content-type": "application/json"
    }
    # Merge custom payload + aps
    data = {
        "aps": {
            "alert": {"title": title, "body": body},
            "sound": "default",
            "category": category
        }
    }
    data.update(payload)
    async with httpx.AsyncClient(http2=True, timeout=10) as client:
        r = await client.post(url, headers=headers, json=data)
        r.raise_for_status()
        return True
