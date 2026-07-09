FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script
COPY fetch.py .

# Run the pipeline once and exit (Job, not a long-running service)
CMD ["python", "fetch.py"]
