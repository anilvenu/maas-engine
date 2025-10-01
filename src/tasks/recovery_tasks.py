"""Celery tasks for recovery operations."""

import logging
from datetime import datetime, timedelta, UTC
from typing import Dict, Any, List
import httpx

from src.tasks.celery_app import celery
from src.tasks.job_tasks import poll_workflow_status, submit_job
from src.db.session import get_db_session
from src.db.models import Job, SystemRecovery, WorkflowStatus
from src.core.constants import JobStatus, RecoveryType
from src.core.config import settings

logger = logging.getLogger(__name__)


@celery.task(name='src.tasks.recovery_tasks.perform_recovery_check')
def perform_recovery_check(recovery_type: str = "scheduled") -> Dict[str, Any]:
    """
    Main recovery task that checks and recovers stale jobs.
    
    Args:
        recovery_type: Type of recovery (startup, manual, scheduled)
        
    Returns:
        Dict with recovery statistics
    """
    logger.info(f"Starting {recovery_type} recovery check")
    
    with get_db_session() as db:
        # Create recovery record
        recovery = SystemRecovery(
            recovery_type=recovery_type,
            recovery_metadata={"started_at": datetime.utcnow().isoformat()}
        )
        db.add(recovery)
        db.commit()
        
        try:
            # Find all incomplete jobs
            incomplete_jobs = db.query(Job).filter(
                Job.status.in_(['submitted', 'queued', 'running'])
            ).all()
            
            logger.info(f"Found {len(incomplete_jobs)} incomplete jobs to check")
            
            results = {
                "total_checked": len(incomplete_jobs),
                "still_active": 0,
                "completed": 0,
                "failed": 0,
                "not_found": 0,
                "resumed_polling": 0,
                "resubmitted": 0,
                "errors": 0
            }
            
            for job in incomplete_jobs:
                try:
                    job_result = recover_single_job.apply_async(
                        args=[job.id],
                        queue='recovery'
                    )          
                except Exception as e:
                    logger.error(f"Error recovering job {job.id}: {e}")
            
            # Update recovery record
            recovery.jobs_recovered = results["total_checked"]
            recovery.completed_at = datetime.utcnow()
            recovery.recovery_metadata = results
            db.commit()
            
            logger.info(f"Recovery complete: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Recovery failed: {e}")
            recovery.completed_at = datetime.utcnow()
            recovery.recovery_metadata = {"error": str(e)}
            db.commit()
            raise


@celery.task(name='src.tasks.recovery_tasks.recover_single_job')
def recover_single_job(job_id: int) -> Dict[str, Any]:
    """
    Recover a single job by checking its status and resuming operations.
    
    Args:
        job_id: Database job ID
        
    Returns:
        Dict with recovery action taken
    """
    logger.info(f"Recovering job {job_id}")
    
    with get_db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            logger.error(f"Job {job_id} not found")
            return {"job_id": job_id, "error": "not_found"}
        
        # Skip if job is already terminal
        if job.status in ['completed', 'failed', 'cancelled']:
            logger.info(f"Job {job_id} already in terminal state: {job.status}")
            return {"job_id": job_id, "action": "skipped", "status": job.status}
        
        # If no workflow_id, job was never submitted
        if not job.workflow_id:
            logger.info(f"Job {job_id} has no workflow_id, resubmitting")
            submit_job.apply_async(args=[job_id], queue='jobs')
            return {"job_id": job_id, "action": "resubmitted"}
        
        # Poll Moody's API to get current status
        try:
            with httpx.Client(timeout=settings.MOODYS_API_TIMEOUT) as client:
                response = client.get(
                    f"{settings.MOODYS_API_BASE_URL}/workflows/{job.workflow_id}/status"
                )
                
                if response.status_code == 404:
                    # Workflow not found in Moody's
                    logger.warning(f"Workflow {job.workflow_id} not found, resubmitting job {job_id}")
                    
                    # Clear old workflow_id and reset status
                    job.workflow_id = None
                    job.status = 'pending'
                    job.initiation_ts = None
                    db.commit()
                    
                    # Resubmit
                    submit_job.apply_async(args=[job_id], queue='jobs')
                    return {"job_id": job_id, "action": "not_found", "resubmitted": True}
                
                response.raise_for_status()
                result = response.json()
                current_status = result["status"]
                
                # Map Moody's status to our status
                status_map = {
                    "queued": "queued",
                    "running": "running", 
                    "completed": "completed",
                    "failed": "failed",
                    "cancelled": "cancelled"
                }
                
                our_status = status_map.get(current_status)
                
                # Update job status if different
                if our_status and our_status != job.status:
                    logger.info(f"Job {job_id} status mismatch. DB: {job.status}, Actual: {our_status}")
                    job.status = our_status
                    job.last_poll_ts = datetime.utcnow()
                    
                    if our_status in ['completed', 'failed', 'cancelled']:
                        job.completed_ts = datetime.utcnow()
                    
                    # Record the status update
                    workflow_status = WorkflowStatus(
                        job_id=job_id,
                        status=current_status,
                        response_data=result,
                        http_status_code=200
                    )
                    db.add(workflow_status)
                    db.commit()
                
                # Resume polling if still active
                if our_status in ['queued', 'running']:
                    logger.info(f"Resuming polling for job {job_id} (status: {our_status})")
                    
                    # Calculate next poll interval
                    if our_status == 'queued':
                        countdown = settings.POLL_INTERVAL_QUEUED_SECONDS
                    else:
                        countdown = settings.POLL_INTERVAL_RUNNING_SECONDS
                    
                    # Schedule next poll
                    poll_workflow_status.apply_async(
                        args=[job_id, job.workflow_id],
                        countdown=countdown,
                        queue='polling'
                    )
                    
                    return {"job_id": job_id, "action": "resumed_polling", "status": our_status}
                
                elif our_status == 'completed':
                    return {"job_id": job_id, "action": "marked_complete"}
                elif our_status == 'failed':
                    return {"job_id": job_id, "action": "marked_failed"}
                else:
                    return {"job_id": job_id, "action": "no_action", "status": our_status}
                
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error checking job {job_id}: {e}")
            return {"job_id": job_id, "error": f"HTTP {e.response.status_code}"}
        except Exception as e:
            logger.error(f"Error checking job {job_id}: {e}")
            return {"job_id": job_id, "error": str(e)}


@celery.task(name='src.tasks.recovery_tasks.startup_recovery')
def startup_recovery() -> Dict[str, Any]:
    """
    Special recovery task to run on system startup.
    Checks all incomplete jobs and resumes operations.
    
    Returns:
        Dict with recovery statistics
    """
    logger.info("Running startup recovery")
    
    # Wait a bit for system to stabilize
    import time
    time.sleep(5)
    
    # Run recovery with startup type
    return perform_recovery_check(recovery_type=RecoveryType.STARTUP.value)


@celery.task(name='src.tasks.recovery_tasks.detect_orphan_jobs')
def detect_orphan_jobs() -> Dict[str, Any]:
    """
    Detect jobs that haven't been updated recently and might be orphaned.
    
    Returns:
        Dict with orphan job information
    """
    logger.info("Detecting orphan jobs")
    
    with get_db_session() as db:
        # Find jobs not updated in the last threshold period
        threshold = datetime.utcnow() - timedelta(
            seconds=settings.RECOVERY_STALE_JOB_THRESHOLD_SECONDS
        )
        
        orphan_jobs = db.query(Job).filter(
            Job.status.in_(['submitted', 'queued', 'running']),
            Job.updated_ts < threshold
        ).all()
        
        if not orphan_jobs:
            logger.info("No orphan jobs detected")
            return {"orphan_count": 0}
        
        logger.warning(f"Found {len(orphan_jobs)} potential orphan jobs")
        
        orphan_ids = [j.id for j in orphan_jobs]
        
        # Trigger recovery for each orphan
        for job_id in orphan_ids:
            recover_single_job.apply_async(
                args=[job_id],
                queue='recovery'
            )
        
        return {
            "orphan_count": len(orphan_jobs),
            "orphan_job_ids": orphan_ids,
            "recovery_triggered": True
        }