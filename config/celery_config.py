"""Celery configuration settings."""

from kombu import Exchange, Queue
from datetime import timedelta

# Broker settings
broker_url = 'redis://redis:6379/0'
result_backend = 'redis://redis:6379/1'

# Task execution settings
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'UTC'
enable_utc = True

# Worker settings
worker_prefetch_multiplier = 1  # Don't prefetch tasks
task_acks_late = True  # Acknowledge after task completion
task_reject_on_worker_lost = True  # Return task to queue if worker dies
worker_max_tasks_per_child = 1000  # Restart worker after N tasks

# Task settings
task_track_started = True  # Track when task starts
task_time_limit = 300  # 5 minutes hard limit
task_soft_time_limit = 240  # 4 minutes soft limit
task_default_retry_delay = 60  # 1 minute

# Result backend settings
result_expires = 86400  # Results expire after 24 hours
result_persistent = True  # Store results persistently

# Task routing
task_default_queue = 'default'
task_default_exchange_type = 'direct'
task_default_routing_key = 'default'

# Queue configuration
task_queues = (
    Queue('default', Exchange('default'), routing_key='default'),
    Queue('jobs', Exchange('jobs'), routing_key='jobs'),
    Queue('polling', Exchange('polling'), routing_key='polling'),
    Queue('recovery', Exchange('recovery'), routing_key='recovery'),
)

# Task routes
task_routes = {
    'tasks.job_tasks.*': {'queue': 'jobs'},
    'tasks.job_tasks.poll_*': {'queue': 'polling'},
    'tasks.recovery_tasks.*': {'queue': 'recovery'},
}

# Beat schedule
beat_schedule = {
    'check-analysis-completion': {
        'task': 'tasks.analysis_tasks.check_all_analyses',
        'schedule': timedelta(minutes=2),
        'options': {'queue': 'default'}
    },
    'recovery-check': {
        'task': 'tasks.recovery_tasks.perform_recovery_check',
        'schedule': timedelta(minutes=10),
        'options': {'queue': 'recovery'}
    },
    'orphan-job-detection': {
        'task': 'tasks.recovery_tasks.detect_orphan_jobs',
        'schedule': timedelta(minutes=15),
        'options': {'queue': 'recovery'}
    },
}

# Beat settings
beat_scheduler = 'celery.beat:PersistentScheduler'
beat_schedule_filename = '/app/logs/celerybeat-schedule'
beat_sync_every = 10  # Sync schedule to disk every 10 beats

# Monitoring
worker_send_task_events = True  # Send events for monitoring
task_send_sent_event = True  # Send event when task is sent

# Error handling
task_autoretry_for = (Exception,)  # Auto-retry on any exception
task_max_retries = 3  # Maximum retry attempts
task_retry_backoff = True  # Exponential backoff
task_retry_backoff_max = 600  # Max backoff of 10 minutes
task_retry_jitter = True  # Add randomness to retry delay

# Optimization
worker_disable_rate_limits = True  # No rate limiting
task_ignore_result = False  # Store all results for tracking
task_store_eager_result = True  # Store results even in eager mode

# Development settings (disable in production)
task_always_eager = False  # Execute tasks synchronously (for debugging)
task_eager_propagates = False  # Propagate exceptions in eager mode

# Logging
worker_hijack_root_logger = False  # Don't hijack root logger
worker_log_format = '[%(asctime)s: %(levelname)s/%(processName)s] %(message)s'
worker_task_log_format = '[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s'

# Connection pool settings
broker_pool_limit = 10
broker_connection_retry_on_startup = True
broker_connection_retry = True
broker_connection_max_retries = 10

# Redis specific settings
redis_max_connections = 100
redis_socket_keepalive = True
redis_socket_keepalive_options = {
    1: 3,  # TCP_KEEPIDLE
    2: 3,  # TCP_KEEPINTVL
    3: 3,  # TCP_KEEPCNT
}