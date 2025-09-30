"""System management and monitoring endpoints."""

from fastapi import APIRouter, HTTPException, Depends, status
from typing import Dict, Any
from datetime import datetime, UTC
import httpx
from sqlalchemy.orm import Session

from src.api.dependencies.database import get_db
from src.api.dependencies.auth import verify_api_key

from src.api.models.schemas import SystemHealthResponse, RecoveryStatusResponse
from src.db.session import DatabaseManager
from src.tasks.recovery_tasks import perform_recovery_check
from src.core.config import settings
import redis
from celery import Celery

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


router = APIRouter(prefix="/api/system", tags=["System"])
celery_app = Celery(broker=settings.CELERY_BROKER_URL, backend=settings.CELERY_RESULT_BACKEND)

@router.get("/health", response_model=SystemHealthResponse)
def health_check():
    """Check system health status."""
    health = SystemHealthResponse(
        status="healthy",
        database=False,
        redis=False,
        celery_worker=False,
        moodys_api=False,
        timestamp=datetime.now(UTC)
    )
    
    # Check database
    try:
        health.database = DatabaseManager.health_check()
    except Exception as e:
        logger.exception(f"Database health check failed. Exception: {e}")
        health.status = "degraded"
    
    # Check Redis
    try:
        r = redis.Redis.from_url(settings.REDIS_URL)
        r.ping()
        health.redis = True
    except Exception as e:
        logger.exception(f"Redis health check failed. Exception: {e}")
        health.status = "degraded"
    
    # Check Celery workers
    try:
        insp = celery_app.control.inspect(timeout=5)  # short timeout so API doesn't hang
        logger.info(f"Celery inspect object: {insp}")
        stats = insp.stats()
        if stats:
            health.celery_worker = True
        else:
            health.status = "degraded"
            health.celery_worker = False
    except Exception as e:
        logger.exception(f"Celery health check failed. Exception: {e}")
        health.status = "degraded"
        health.celery_worker = False
    
    # Check Moody's API
    try:
        response = httpx.get(f"{settings.MOODYS_API_BASE_URL}", timeout=2)
        response.raise_for_status()
        health.moodys_api = True
    except:
        health.status = "degraded"
    
    # Set overall status
    if not all([health.database, health.redis]):
        health.status = "unhealthy"
    
    return health


@router.post("/recovery", 
             response_model=RecoveryStatusResponse,
             dependencies=[Depends(verify_api_key)])
def trigger_recovery():
    """Manually trigger recovery process."""
    task = perform_recovery_check.delay()
    
    return RecoveryStatusResponse(
        stale_jobs=0,
        recovered=0,
        resubmitted=0,
        resumed_polling=0,
        errors=0,
        recovery_id=None
    )


@router.get("/recovery/status")
def get_recovery_status(db: Session = Depends(get_db)):
    """Get status of last recovery."""
    from src.db.models import SystemRecovery
    
    last_recovery = db.query(SystemRecovery)\
        .order_by(SystemRecovery.started_at.desc())\
        .first()
    
    if not last_recovery:
        return {"message": "No recovery runs found"}
    
    return {
        "recovery_id": last_recovery.id,
        "type": last_recovery.recovery_type,
        "started_at": last_recovery.started_at,
        "completed_at": last_recovery.completed_at,
        "jobs_recovered": last_recovery.jobs_recovered,
        "jobs_resubmitted": last_recovery.jobs_resubmitted,
        "jobs_resumed_polling": last_recovery.jobs_resumed_polling,
        "metadata": last_recovery.recovery_metadata
    }


@router.get("/stats")
def get_system_stats(db: Session = Depends(get_db)):
    """Get system statistics."""
    from src.db.models import Analysis, Job, Configuration
    
    stats = {
        "analysis": {
            "total": db.query(Analysis).count(),
            "pending": db.query(Analysis).filter(Analysis.status == "pending").count(),
            "running": db.query(Analysis).filter(Analysis.status == "running").count(),
            "completed": db.query(Analysis).filter(Analysis.status == "completed").count(),
            "failed": db.query(Analysis).filter(Analysis.status == "failed").count(),
        },
        "jobs": {
            "total": db.query(Job).count(),
            "planned": db.query(Job).filter(Job.status == "planned").count(),
            "initiated": db.query(Job).filter(Job.status == "initiated").count(),
            "queued": db.query(Job).filter(Job.status == "queued").count(),
            "running": db.query(Job).filter(Job.status == "running").count(),
            "completed": db.query(Job).filter(Job.status == "completed").count(),
            "failed": db.query(Job).filter(Job.status == "failed").count(),
        },
        "configurations": {
            "total": db.query(Configuration).count(),
            "active": db.query(Configuration).filter(Configuration.is_active == True).count(),
        }
    }
    
    return stats