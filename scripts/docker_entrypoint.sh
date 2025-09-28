#!/bin/bash
# Docker entrypoint script with recovery

set -e

echo "Starting IRP application with recovery..."

# Wait for database
echo "Waiting for database..."
while ! nc -z postgres 5432; do
  sleep 1
done
echo "Database is ready"

# Wait for Redis
echo "Waiting for Redis..."
while ! nc -z redis 6379; do
  sleep 1
done
echo "Redis is ready"

# Run startup recovery
echo "Running startup recovery..."
python src/startup.py

# Start the main application
echo "Starting main application..."
exec "$@"