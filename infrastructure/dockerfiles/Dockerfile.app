FROM python:3.11-slim
# Install system dependencies including netcat for health checks
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*
WORKDIR /app
# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copy application code
COPY . .
# Copy entrypoint script
COPY infrastructure/dockerfiles/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
# Set environment to identify this as the app container
ENV SERVICE_TYPE=app
ENV PYTHONPATH=/app
# Use entrypoint for startup tasks and CMD for the actual command
ENTRYPOINT ["/entrypoint.sh"]
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]