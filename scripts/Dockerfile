FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all python scripts
COPY *.py .

# Run the web service with Gunicorn + Uvicorn workers for multi-core concurrency.
# WEB_CONCURRENCY: number of worker processes (default: 4, adjust to match CPU cores).
# -k uvicorn.workers.UvicornWorker: each worker runs an async event loop.
# --timeout 120: allow long-running requests (e.g. fetches) up to 2 minutes.
# WEB_CONCURRENCY: default bumped to 8 (8×200 = 1,600 thread capacity for sync paths).
# With async Motor endpoints the event-loop handles concurrency, so even fewer workers suffice.
CMD ["sh", "-c", "gunicorn app:app -w ${WEB_CONCURRENCY:-8} -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:10000 --timeout 120"]
