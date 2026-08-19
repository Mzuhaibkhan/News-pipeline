FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all python scripts and templates
COPY *.py .
COPY templates/ ./templates/

# Create non-root user for security — limits blast radius if an attacker
# gains code execution inside the container.
RUN adduser --disabled-password --no-create-home appuser
USER appuser

# Run the web service with Gunicorn + Uvicorn workers for multi-core concurrency.
# WEB_CONCURRENCY: number of worker processes (default: 2, adjust to match CPU cores).
# -k uvicorn.workers.UvicornWorker: each worker runs an async event loop.
# --timeout 120: allow long-running requests (e.g. fetches) up to 2 minutes.
CMD ["sh", "-c", "gunicorn app:app -w ${WEB_CONCURRENCY:-2} -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:${PORT:-10000} --timeout 120"]
