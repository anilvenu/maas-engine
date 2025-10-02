#!/bin/bash
# Docker entrypoint script for IRP application
# This script runs startup tasks and then executes the CMD

set -e

echo "=========================================="
echo "     __   __   "
echo "  _ |__| /  \  "
echo " |_) || | || | "
echo " |  |__| \__\\\\ "
echo " "
echo "Starting IRP Automation Service: ${SERVICE_TYPE}"
echo "=========================================="

# Function to wait for a service
wait_for_service() {
    local service=$1
    local host=$2
    local port=$3
    local max_retries=30
    local retry_count=0
    
    echo "Waiting for $service at $host:$port..."
    
    while ! nc -z $host $port 2>/dev/null; do
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
sleep 2

# Run database initialization only from app or first worker
if [ "$SERVICE_TYPE" = "app" ] || [ "$SERVICE_TYPE" = "worker" ]; then
    echo "Running database initialization..."
    python -c "
import sys
import os
sys.path.insert(0, '/app')
os.environ['PYTHONPATH'] = '/app'
from src.db.session import init_db
import logging
logging.basicConfig(level=logging.INFO)

try:
    init_db()
    print('✓ Database tables initialized')
except Exception as e:
    print(f'ERROR: Database initialization failed: {e}')
    exit(1)
"
fi

# Run startup tasks only from the worker container
if [ "$SERVICE_TYPE" = "worker" ]; then
    echo "Running startup tasks from worker..."
    
    # Wait a bit more for everything to stabilize
    sleep 5
    
    # Run startup tasks
    python -c "
import sys
import os
sys.path.insert(0, '/app')
os.environ['PYTHONPATH'] = '/app'

try:
    from src.startup import run_startup_tasks
    success = run_startup_tasks()
    if success:
        print('✓ Startup tasks completed successfully')
    else:
        print('⚠ Startup tasks completed with warnings')
except Exception as e:
    print(f'⚠ Startup tasks error (non-fatal): {e}')
"
fi

# Message for each service type
case "$SERVICE_TYPE" in
    app)
        echo "Starting API server..."
        ;;
    worker)
        echo "Starting Celery worker..."
        ;;
    beat)
        echo "Starting Celery beat scheduler..."
        ;;
    *)
        echo "Starting service..."
        ;;
esac

# Execute the CMD passed to the container
exec "$@"