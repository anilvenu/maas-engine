"""System management and monitoring endpoints."""

from fastapi import APIRouter, HTTPException, Depends, status
from typing import Dict, Any
from datetime import datetime, UTC
import httpx
from sqlalchemy.orm import Session

from src.api.dependencies.database import get_db
from src.api.models.schemas import SystemHealthResponse, RecoveryStatusResponse
from src.db.session import DatabaseManager
from src.tasks.recovery_tasks import perform_recovery_check
from src.core.config import settings
import redis

router = APIRouter(prefix="/api/system", tags=["System"])


@router.get("/health", response_model=SystemHealthResponse)
def health_check():
    """Check system health status."""
    health = SystemHealthResponse(
        status="healthy",
        database=False,
        redis=False,
        celery_worker=False,
        mock_moody_api=False,
        timestamp=datetime.now(UTC)
    )
    
    # Check database
    try:
        health.database = DatabaseManager.health_check()
    except:
        health.status = "degraded"
    
    # Check Redis
    try:
        r = redis.Redis.from_url(settings.REDIS_URL)
        r.ping()
        health.redis = True
    except:
        health.status = "degraded"
    
    # Check Celery (via Redis queue check)
    try:
        r = redis.Redis.from_url(settings.CELERY_BROKER_URL)
        # Check if workers are registered
        workers = r.keys("celery-task-meta-*")
        health.celery_worker = len(workers) > 0 or r.exists("celery")
    except:
        health.status = "degraded"
    
    # Check Mock Moody's API
    try:
        response = httpx.get(f"{settings.MOODY_API_BASE_URL}/", timeout=2)
        health.mock_moody_api = response.status_code == 200
    except:
        health.status = "degraded"
    
    # Set overall status
    if not all([health.database, health.redis]):
        health.status = "unhealthy"
    
    return health


@router.get("/health/ready")
def readiness_check():
    """Readiness probe for Kubernetes/Docker."""
    health = health_check()
    
    if health.status == "unhealthy":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="System not ready"
        )
    
    return {"status": "ready"}


@router.get("/health/live")
def liveness_check():
    """Liveness probe for Kubernetes/Docker."""
    return {"status": "alive"}


@router.post("/recovery", response_model=RecoveryStatusResponse)
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
        "analyses": {
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