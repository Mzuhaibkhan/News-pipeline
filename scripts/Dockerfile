FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all python scripts
COPY *.py .

# Run the web service with Gunicorn + Uvicorn workers for multi-core concurrency.
# -w 4: spawn 4 worker processes (adjust to match available CPU cores).
# -k uvicorn.workers.UvicornWorker: each worker runs an async event loop.
# --timeout 120: allow long-running requests (e.g. fetches) up to 2 minutes.
CMD ["gunicorn", "app:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:10000", "--timeout", "120"]
