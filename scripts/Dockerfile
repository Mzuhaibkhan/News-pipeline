FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all python scripts
COPY *.py .

# Run the web service
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "10000"]
