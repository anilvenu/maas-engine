from celery import Celery
import os

# Create the Celery instance
celery = Celery(
    'irp_tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/1'
)

# Basic configuration
celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    broker_connection_retry_on_startup=True
)
