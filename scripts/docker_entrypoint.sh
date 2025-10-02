#!/bin/bash
# Docker entrypoint script for IRP application
# This script runs startup tasks and then starts the main application

set -e

echo "=========================================="
echo "     __   __   "
echo "  _ |__| /  \  "
echo " |_) || | \\ | "
echo " |  |__| \__\\ "
echo " "
echo "Starting IRP Application"
echo "=========================================="

# Function to wait for a service
wait_for_service() {
    local service=$1
    local host=$2
    local port=$3
    local max_retries=30
    local retry_count=0
    
    echo "Waiting for $service at $host:$port..."
    
    while ! nc -z $host $port; do
        retry_count=$((retry_count + 1))
        if [ $retry_count -ge $max_retries ]; then
            echo "ERROR: $service not available after $max_retries attempts"
            exit 1
        fi
        echo "Waiting for $service... ($retry_count/$max_retries)"
        sleep 2
    done
    
    echo "✓ $service is ready"
}

# Wait for required services
wait_for_service "PostgreSQL" "postgres" "5432"
wait_for_service "Redis" "redis" "6379"

# Additional wait for database to be fully ready
sleep 3

# Run database initialization if needed
echo "Running database initialization..."
python -c "
from src.db.session import init_db
import logging
import os
logging.basicConfig(level=logging.INFO)

try:
    init_db()
    print('✓ Database tables initialized')        
except Exception as e:
    print(f'ERROR: Database initialization failed: {e}')
    exit(1)
"

# Check if this is a worker container (WORKER_TYPE environment variable)
if [ -n "$WORKER_TYPE" ]; then
    if [ "$WORKER_TYPE" = "beat" ]; then
        echo "Starting Celery Beat scheduler..."
        exec celery -A src.tasks.celery_app beat --loglevel=INFO
    else
        echo "Starting Celery Worker..."
        
        # Wait a bit for the main app to be ready
        sleep 5
        
        # Run startup tasks only from the first worker
        if [ "$WORKER_INSTANCE" = "1" ] || [ -z "$WORKER_INSTANCE" ]; then
            echo "Running startup tasks from worker..."
            python src/startup.py || echo "⚠ Startup tasks completed with warnings"
        fi
        
        # Start the worker
        exec celery -A src.tasks.celery_app worker \
            --loglevel=INFO \
            --concurrency=${CELERY_WORKER_CONCURRENCY:-4} \
            --queues=${CELERY_QUEUES:-default,jobs,polling,recovery}
    fi
else
    # This is the main API container
    echo "Starting API server..."
    exec uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
fi