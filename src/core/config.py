"""Application configuration management."""

import os
from typing import Optional
from pydantic import BaseSettings, PostgresDsn, RedisDsn, validator
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings with validation."""
    
    # Application
    APP_NAME: str = "IRP Batch Processor"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"
    
    # Database
    DATABASE_URL: PostgresDsn = "postgresql://irp_user:irp_pass@localhost:5432/irp_db"
    DB_POOL_SIZE: int = 10
    DB_POOL_MAX_OVERFLOW: int = 20
    DB_POOL_PRE_PING: bool = True
    
    # Redis
    REDIS_URL: RedisDsn = "redis://localhost:6379/0"
    REDIS_MAX_CONNECTIONS: int = 100
    
    # Celery
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/1"
    CELERY_TASK_ACKS_LATE: bool = True
    CELERY_TASK_REJECT_ON_WORKER_LOST: bool = True
    CELERY_WORKER_PREFETCH_MULTIPLIER: int = 1
    CELERY_TASK_TIME_LIMIT: int = 300  # 5 minutes
    CELERY_TASK_SOFT_TIME_LIMIT: int = 240  # 4 minutes
    
    # Moody's API Configuration
    MOODY_API_BASE_URL: str = "http://localhost:8001/mock"
    MOODY_API_TIMEOUT: int = 30
    MOODY_API_MAX_RETRIES: int = 5
    MOODY_API_RETRY_BACKOFF: int = 2
    
    # Polling Configuration
    POLL_INITIAL_DELAY_SECONDS: int = 30
    POLL_INTERVAL_QUEUED_SECONDS: int = 120  # 2 minutes
    POLL_INTERVAL_RUNNING_SECONDS: int = 300  # 5 minutes
    POLL_MAX_DURATION_SECONDS: int = 86400  # 24 hours
    
    # Recovery Configuration
    RECOVERY_CHECK_INTERVAL_SECONDS: int = 600  # 10 minutes
    RECOVERY_STALE_JOB_THRESHOLD_SECONDS: int = 600  # 10 minutes
    RECOVERY_MAX_RESUBMISSIONS: int = 3
    
    # Retry Strategy
    RETRY_MAX_ATTEMPTS: int = 5
    RETRY_BACKOFF_BASE: int = 2
    RETRY_BACKOFF_MAX_SECONDS: int = 300

    # API Key for secure endpoints
    API_KEY: Optional[str] = None

    @validator("DATABASE_URL")
    def validate_postgres_url(cls, v):
        """Ensure PostgreSQL URL is properly formatted."""
        if not v or not v.startswith("postgresql://"):
            raise ValueError("Invalid PostgreSQL URL")
        return v
    
    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Create a single settings instance
settings = get_settings()