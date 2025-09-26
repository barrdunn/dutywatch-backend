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

# DutyWatch Backend

## How to Deploy

### Local steps
```bash
cd /path/to/dutywatch-backend

git status
git git add -A
git commit -m ""
git push origin main

# optional tagging
# git tag -a v0.1.1 -m "Overnight release fix"
# git push origin v0.1.1

### Remote Steps (on the AWS server)
Run these to pull the latest code and restart the service:

cd /opt/dutywatch-backend
git pull --ff-only
sudo systemctl restart dutywatch
curl -s http://127.0.0.1:8001/health ; echo
