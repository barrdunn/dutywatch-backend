# DutyWatch (backend)

FastAPI backend that pulls iCloud CalDAV calendar events and builds a pilot-friendly schedule with simulated notifications.

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# copy and fill secrets
cp .env.example .env

# run
python3 -m uvicorn app:app --reload --host 0.0.0.0 --port 8000