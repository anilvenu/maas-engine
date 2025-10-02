"""System management and monitoring endpoints."""

from fastapi import APIRouter, HTTPException, Depends, status, Query
from typing import Dict, Any, Optional
from datetime import datetime, UTC
import httpx
from sqlalchemy.orm import Session

from src.api.dependencies.database import get_db
from src.api.dependencies.auth import verify_api_key

from src.api.models.schemas import SystemHealthResponse, RecoveryStatusResponse
from src.db.session import DatabaseManager
from src.services.recovery_service import RecoveryService
from src.tasks.recovery_tasks import perform_recovery_check, detect_orphan_jobs
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
        insp = celery_app.control.inspect(timeout=5)
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


@router.post("/recovery/manual", 
             dependencies=[Depends(verify_api_key)])
def trigger_manual_recovery():
    """Manually trigger comprehensive recovery process."""
    service = RecoveryService()
    result = service.trigger_manual_recovery()
    
    return {
        "status": "recovery_triggered",
        "task_id": result["task_id"],
        "timestamp": result["timestamp"],
        "message": "Manual recovery started. Check /api/system/recovery/status for progress."
    }


@router.post("/recovery/health-check",
             dependencies=[Depends(verify_api_key)])
def trigger_health_check_recovery():
    """Trigger health check with automatic recovery if issues found."""
    service = RecoveryService()
    result = service.trigger_health_check_recovery()
    
    return {
        "status": "health_check_triggered",
        "task_id": result["task_id"],
        "timestamp": result["timestamp"],
        "message": "Health check started. Recovery will be triggered if issues are found."
    }


@router.get("/recovery/status")
def get_recovery_status(
    recovery_id: Optional[int] = Query(None, description="Specific recovery ID"),
    db: Session = Depends(get_db)
):
    """Get status of recovery operations."""
    service = RecoveryService()
    return service.get_recovery_status(recovery_id)


@router.get("/recovery/history")
def get_recovery_history(
    limit: int = Query(10, ge=1, le=100, description="Number of records to return"),
    db: Session = Depends(get_db)
):
    """Get history of recovery operations."""
    service = RecoveryService()
    history = service.get_recovery_history(limit)
    
    return {
        "count": len(history),
        "limit": limit,
        "history": history
    }


@router.get("/recovery/statistics")
def get_recovery_statistics(
    hours: int = Query(24, ge=1, le=168, description="Number of hours to look back"),
    db: Session = Depends(get_db)
):
    """Get recovery statistics for the specified time period."""
    service = RecoveryService()
    return service.get_recovery_statistics(hours)


@router.post("/recovery/orphan-check",
             dependencies=[Depends(verify_api_key)])
def check_orphan_jobs():
    """Detect and recover orphaned jobs."""
    task = detect_orphan_jobs.apply_async(queue='recovery')
    
    return {
        "status": "orphan_check_triggered",
        "task_id": task.id,
        "message": "Orphan job detection started"
    }


@router.get("/system-health")
def get_system_health(db: Session = Depends(get_db)):
    """Get comprehensive system health analysis."""
    service = RecoveryService()
    return service.check_system_health()


@router.get("/stats")
def get_system_stats(db: Session = Depends(get_db)):
    """Get system statistics."""
    from src.db.models import Batch, Job, Configuration, SystemRecovery
    from datetime import timedelta
    
    now = datetime.now(UTC)
    
    # Basic counts
    stats = {
        "timestamp": now.isoformat(),
        "batch": {
            "total": db.query(Batch).count(),
            "pending": db.query(Batch).filter(Batch.status == "pending").count(),
            "running": db.query(Batch).filter(Batch.status == "running").count(),
            "completed": db.query(Batch).filter(Batch.status == "completed").count(),
            "failed": db.query(Batch).filter(Batch.status == "failed").count(),
            "cancelled": db.query(Batch).filter(Batch.status == "cancelled").count(),
        },
        "jobs": {
            "total": db.query(Job).count(),
            "pending": db.query(Job).filter(Job.status == "pending").count(),
            "submitted": db.query(Job).filter(Job.status == "submitted").count(),
            "queued": db.query(Job).filter(Job.status == "queued").count(),
            "running": db.query(Job).filter(Job.status == "running").count(),
            "completed": db.query(Job).filter(Job.status == "completed").count(),
            "failed": db.query(Job).filter(Job.status == "failed").count(),
            "cancelled": db.query(Job).filter(Job.status == "cancelled").count(),
        },
        "configurations": {
            "total": db.query(Configuration).count(),
            "active": db.query(Configuration).filter(Configuration.is_active == True).count(),
            "skipped": db.query(Configuration).filter(Configuration.skip == True).count(),
        },
        "recoveries": {
            "total": db.query(SystemRecovery).count(),
            "last_24h": db.query(SystemRecovery).filter(
                SystemRecovery.started_at > now - timedelta(hours=24)
            ).count(),
            "last_7d": db.query(SystemRecovery).filter(
                SystemRecovery.started_at > now - timedelta(days=7)
            ).count(),
        }
    }
    
    # Recent activity (last hour)
    last_hour = now - timedelta(hours=1)
    stats["recent_activity"] = {
        "jobs_created": db.query(Job).filter(Job.created_ts > last_hour).count(),
        "jobs_completed": db.query(Job).filter(
            Job.completed_ts > last_hour,
            Job.status == "completed"
        ).count(),
        "jobs_failed": db.query(Job).filter(
            Job.completed_ts > last_hour,
            Job.status == "failed"
        ).count(),
        "batches_created": db.query(Batch).filter(Batch.created_ts > last_hour).count(),
    }
    
    # Performance metrics
    from sqlalchemy import func
    
    # Average job duration for completed jobs in last 24h
    last_day = now - timedelta(hours=24)
    avg_duration = db.query(
        func.avg(func.extract('epoch', Job.completed_ts - Job.initiation_ts))
    ).filter(
        Job.status == "completed",
        Job.completed_ts > last_day,
        Job.initiation_ts.isnot(None)
    ).scalar()
    
    stats["performance"] = {
        "avg_job_duration_seconds": round(avg_duration, 2) if avg_duration else None,
        "jobs_without_workflow": db.query(Job).filter(
            Job.status.in_(["submitted", "queued", "running"]),
            Job.workflow_id == None
        ).count(),
        "stale_jobs": db.query(Job).filter(
            Job.status.in_(["submitted", "queued", "running"]),
            Job.updated_ts < now - timedelta(minutes=30)
        ).count(),
    }
    
    return stats


@router.get("/metrics")
def get_system_metrics(
    time_range: str = Query("1h", regex="^(1h|6h|24h|7d|30d)$", description="Time range for metrics"),
    db: Session = Depends(get_db)
):
    """Get detailed system metrics for monitoring."""
    from src.db.models import Job, Batch, JobStatus
    from datetime import timedelta
    from sqlalchemy import func
    
    # Parse time range
    now = datetime.now(UTC)
    time_ranges = {
        "1h": timedelta(hours=1),
        "6h": timedelta(hours=6),
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30)
    }
    
    delta = time_ranges[time_range]
    start_time = now - delta
    
    metrics = {
        "time_range": time_range,
        "start_time": start_time.isoformat(),
        "end_time": now.isoformat(),
    }
    
    # Job metrics
    metrics["jobs"] = {
        "created": db.query(Job).filter(Job.created_ts > start_time).count(),
        "completed": db.query(Job).filter(
            Job.completed_ts > start_time,
            Job.status == "completed"
        ).count(),
        "failed": db.query(Job).filter(
            Job.completed_ts > start_time,
            Job.status == "failed"  
        ).count(),
        "success_rate": None
    }
    
    total_finished = metrics["jobs"]["completed"] + metrics["jobs"]["failed"]
    if total_finished > 0:
        metrics["jobs"]["success_rate"] = round(
            (metrics["jobs"]["completed"] / total_finished) * 100, 2
        )
    
    # Batch metrics
    metrics["batches"] = {
        "created": db.query(Batch).filter(Batch.created_ts > start_time).count(),
        "completed": db.query(Batch).filter(
            Batch.completed_ts > start_time,
            Batch.status == "completed"
        ).count(),
        "failed": db.query(Batch).filter(
            Batch.completed_ts > start_time,
            Batch.status == "failed"
        ).count(),
    }
    
    # Polling metrics
    poll_count = db.query(JobStatus).filter(
        JobStatus.polled_at > start_time
    ).count()
    
    avg_poll_duration = db.query(
        func.avg(JobStatus.poll_duration_ms)
    ).filter(
        JobStatus.polled_at > start_time,
        JobStatus.poll_duration_ms.isnot(None)
    ).scalar()
    
    metrics["polling"] = {
        "total_polls": poll_count,
        "avg_duration_ms": round(avg_poll_duration, 2) if avg_poll_duration else None
    }
    
    # Recovery metrics
    from src.db.models import SystemRecovery
    
    recoveries = db.query(SystemRecovery).filter(
        SystemRecovery.started_at > start_time
    ).all()
    
    metrics["recoveries"] = {
        "count": len(recoveries),
        "types": {},
        "total_jobs_recovered": sum(r.jobs_recovered or 0 for r in recoveries),
        "total_jobs_resubmitted": sum(r.jobs_resubmitted or 0 for r in recoveries)
    }
    
    for recovery in recoveries:
        recovery_type = recovery.recovery_type
        if recovery_type not in metrics["recoveries"]["types"]:
            metrics["recoveries"]["types"][recovery_type] = 0
        metrics["recoveries"]["types"][recovery_type] += 1
    
    return metrics