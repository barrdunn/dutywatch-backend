# Dockerfile
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000

WORKDIR /app

# System deps (optional but nice)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# If you use SQLite, make a writable dir (optional)
RUN mkdir -p /data && chmod 777 /data
# If your app can read DB path from env, consider:
# ENV DUTYWATCH_DB=/data/dutywatch.db

EXPOSE 8000

# Use gunicorn+uvicorn workers in prod
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-w", "2", "-b", "0.0.0.0:8000", "app:app"]
