FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all python scripts
COPY *.py .

# Create non-root user for security — limits blast radius if an attacker
# gains code execution inside the container.
RUN adduser --disabled-password --no-create-home appuser
USER appuser

# Run the web service with Gunicorn + Uvicorn workers for multi-core concurrency.
# WEB_CONCURRENCY: number of worker processes (default: 4, adjust to match CPU cores).
# -k uvicorn.workers.UvicornWorker: each worker runs an async event loop.
# --timeout 120: allow long-running requests (e.g. fetches) up to 2 minutes.
# --limit-request-body: cap request bodies at 1MB to prevent memory exhaustion.
# WEB_CONCURRENCY: default set to 2 workers to optimize RAM and idle database bandwidth on Render.
CMD ["sh", "-c", "gunicorn app:app -w ${WEB_CONCURRENCY:-2} -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:${PORT:-10000} --timeout 120 --limit-request-body 1048576"]
