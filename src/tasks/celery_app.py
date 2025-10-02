"""Celery application configuration."""

from celery import Celery
from kombu import Exchange, Queue
import os
from src.core.config import settings

# Create Celery instance
celery = Celery(
    'irp_tasks',
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        'src.tasks.job_tasks',
        'src.tasks.recovery_tasks',
        'src.tasks.batch_tasks'
    ]
)

# Configuration
celery.conf.update(
    # Serialization
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    
    # Task execution
    task_track_started=True,
    task_time_limit=300,  # 5 minutes hard limit
    task_soft_time_limit=240,  # 4 minutes soft limit
    task_acks_late=True,  # Acknowledge after completion
    task_reject_on_worker_lost=True,
    
    # Worker
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=100,
    
    # Results
    result_expires=86400,  # 24 hours
    result_persistent=True,
    
    # Retry
    task_autoretry_for=(Exception,),
    task_max_retries=3,
    task_retry_backoff=True,
    task_retry_backoff_max=600,
    task_retry_jitter=True,
)

# Queue configuration with priorities
celery.conf.task_queues = (
    Queue('default', Exchange('default'), routing_key='default', priority=5),
    Queue('jobs', Exchange('jobs'), routing_key='jobs', priority=5),
    Queue('polling', Exchange('polling'), routing_key='polling', priority=3),
    Queue('recovery', Exchange('recovery'), routing_key='recovery', priority=10),
)

# Task routing
celery.conf.task_routes = {
    # Job tasks
    'src.tasks.job_tasks.submit_job': {'queue': 'jobs'},
    'src.tasks.job_tasks.poll_job_status': {'queue': 'polling'},
    'src.tasks.job_tasks.cancel_job': {'queue': 'jobs'},
    
    # Batch tasks
    'src.tasks.batch_tasks.check_batch_completion': {'queue': 'default'},
    'src.tasks.batch_tasks.check_all_batch': {'queue': 'default'},
    'src.tasks.batch_tasks.startup_batch_check': {'queue': 'default'},
    
    # Recovery tasks - all go to recovery queue
    'src.tasks.recovery_tasks.*': {'queue': 'recovery'},
}

# Beat schedule for periodic tasks
from celery.schedules import crontab

celery.conf.beat_schedule = {
    # Batch completion check - every 2 minutes
    'check-batch-completion': {
        'task': 'src.tasks.batch_tasks.check_all_batch',
        'schedule': crontab(minute='*/2'),
        'options': {
            'queue': 'default',
            'expires': 60  # Expire if not executed within 60 seconds
        }
    },
    
    # Main recovery check - every 10 minutes
    'recovery-check': {
        'task': 'src.tasks.recovery_tasks.perform_recovery_check',
        'schedule': crontab(minute='*/10'),
        'args': ['scheduled'],
        'options': {
            'queue': 'recovery',
            'priority': 10,
            'expires': 300  # Expire if not executed within 5 minutes
        }
    },
    
    # Orphan detection - every 5 minutes
    'orphan-detection': {
        'task': 'src.tasks.recovery_tasks.detect_orphan_jobs',
        'schedule': crontab(minute='*/5'),
        'options': {
            'queue': 'recovery',
            'priority': 8,
            'expires': 120  # Expire if not executed within 2 minutes
        }
    },
    
    # Health check with auto-recovery - every 15 minutes
    'health-check-recovery': {
        'task': 'src.tasks.recovery_tasks.health_check_recovery',
        'schedule': crontab(minute='*/15'),
        'options': {
            'queue': 'recovery',
            'priority': 7,
            'expires': 300
        }
    },
}

# Custom task class for better error handling
from celery import Task

class ErrorHandlingTask(Task):
    """Custom task class with enhanced error handling."""
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Log task failures."""
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Task {self.name}[{task_id}] failed: {exc}")
        super().on_failure(exc, task_id, args, kwargs, einfo)
    
    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Log task retries."""
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Task {self.name}[{task_id}] retrying: {exc}")
        super().on_retry(exc, task_id, args, kwargs, einfo)

# Set as default task class
celery.Task = ErrorHandlingTask