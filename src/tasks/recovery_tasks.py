"""Celery tasks for recovery operations."""

import logging
from datetime import datetime, timedelta, UTC
from typing import Dict, Any, List, Optional
import httpx
from collections import defaultdict

from src.tasks.celery_app import celery
from src.tasks.job_tasks import poll_job_status, submit_job
from src.tasks.batch_tasks import check_all_batch
from src.db.session import get_db_session
from src.db.models import Job, SystemRecovery, JobStatus, Batch
import src.core.constants as constants
from src.core.config import settings

logger = logging.getLogger(__name__)


@celery.task(name='src.tasks.recovery_tasks.perform_recovery_check')
def perform_recovery_check(recovery_type: str = "scheduled") -> Dict[str, Any]:
    """
    Main recovery task that performs comprehensive system recovery.
    
    Recovery phases:
    1. Submit all pending jobs to Moody's
    2. Check status of all active jobs and resume polling
    3. Resubmit jobs that are missing from Moody's
    4. Trigger batch completion checks
    
    Args:
        recovery_type: Type of recovery (startup, manual, scheduled, crash)
        
    Returns:
        Dict with recovery statistics
    """
    logger.info("=" * 80)
    logger.info(f"Starting {recovery_type} recovery check")
    logger.info("=" * 80)
    
    with get_db_session() as db:
        # Create recovery record
        recovery = SystemRecovery(
            recovery_type=recovery_type,
            recovery_metadata={"started_at": datetime.now(UTC).isoformat()}
        )
        db.add(recovery)
        db.commit()
        
        try:
            results = {
                "recovery_id": recovery.id,
                "type": recovery_type,
                "pending_submitted": 0,
                "polling_resumed": 0,
                "resubmitted": 0,
                "status_updated": 0,
                "errors": 0,
                "batch_checked": 0,
                "details": {
                    "pending_jobs": [],
                    "active_jobs": [],
                    "resubmitted_jobs": [],
                    "failed_checks": []
                }
            }
            
            # Phase 1: Submit all pending jobs
            logger.info("-" * 60)
            logger.info("PHASE 1: Submitting pending jobs")
            logger.info("-" * 60)
            
            pending_jobs = db.query(Job).filter(
                Job.status == constants.JobStatus.PENDING.value
            ).all()
            
            logger.info(f"Found {len(pending_jobs)} pending jobs to submit")
            
            for job in pending_jobs:
                try:
                    # Check if job should be skipped (configuration marked as skip)
                    if job.configuration and job.configuration.skip:
                        logger.info(f"Skipping job {job.id} - configuration marked as skip")
                        continue
                    
                    logger.info(f"Submitting pending job {job.id}")
                    submit_job.apply_async(
                        args=[job.id],
                        queue='jobs'
                    )
                    results["pending_submitted"] += 1
                    results["details"]["pending_jobs"].append(job.id)
                    
                except Exception as e:
                    logger.error(f"Error submitting job {job.id}: {e}")
                    results["errors"] += 1
                    results["details"]["failed_checks"].append({
                        "job_id": job.id,
                        "action": "submit",
                        "error": str(e)
                    })
            
            # Phase 2: Check and recover active jobs
            logger.info("-" * 60)
            logger.info("PHASE 2: Checking active jobs")
            logger.info("-" * 60)
            
            active_jobs = db.query(Job).filter(
                Job.status.in_(['submitted', 'queued', 'running'])
            ).all()
            
            logger.info(f"Found {len(active_jobs)} active jobs to check")
            
            for job in active_jobs:
                try:
                    recovery_result = recover_single_job_sync(job, db)
                    
                    if recovery_result["action"] == "resumed_polling":
                        results["polling_resumed"] += 1
                        results["details"]["active_jobs"].append(job.id)
                    elif recovery_result["action"] == "resubmitted":
                        results["resubmitted"] += 1
                        results["details"]["resubmitted_jobs"].append(job.id)
                    elif recovery_result["action"] in ["marked_complete", "marked_failed", "status_updated"]:
                        results["status_updated"] += 1
                        
                except Exception as e:
                    logger.error(f"Error recovering job {job.id}: {e}")
                    results["errors"] += 1
                    results["details"]["failed_checks"].append({
                        "job_id": job.id,
                        "action": "recover",
                        "error": str(e)
                    })
            
            # Phase 3: Check for stale jobs that haven't been polled recently
            logger.info("-" * 60)
            logger.info("PHASE 3: Checking for stale jobs")
            logger.info("-" * 60)
            
            stale_threshold = datetime.now(UTC) - timedelta(
                seconds=settings.RECOVERY_STALE_JOB_THRESHOLD_SECONDS
            )
            
            stale_jobs = db.query(Job).filter(
                Job.status.in_(['submitted', 'queued', 'running']),
                Job.updated_ts < stale_threshold
            ).all()
            
            if stale_jobs:
                logger.warning(f"Found {len(stale_jobs)} stale jobs")
                for job in stale_jobs:
                    if job.workflow_id:
                        logger.info(f"Resuming polling for stale job {job.id}")
                        poll_job_status.apply_async(
                            args=[job.id, job.workflow_id],
                            countdown=5,
                            queue='polling'
                        )
                        results["polling_resumed"] += 1
            
            # Phase 4: Trigger batch completion checks
            logger.info("-" * 60)
            logger.info("PHASE 4: Triggering batch checks")
            logger.info("-" * 60)
            
            # Get all non-terminal batches
            active_batches = db.query(Batch).filter(
                Batch.status.in_([constants.BatchStatus.PENDING.value, 
                                 constants.BatchStatus.RUNNING.value])
            ).all()
            
            logger.info(f"Found {len(active_batches)} active batches to check")
            
            if active_batches:
                # Trigger check_all_batch task
                check_all_batch.apply_async(queue='default', countdown=10)
                results["batch_checked"] = len(active_batches)
                logger.info(f"Triggered batch check for {len(active_batches)} batches")
            
            # Update recovery record
            recovery.jobs_recovered = results["status_updated"]
            recovery.jobs_resubmitted = results["pending_submitted"] + results["resubmitted"]
            recovery.jobs_resumed_polling = results["polling_resumed"]
            recovery.completed_at = datetime.now(UTC)
            recovery.recovery_metadata = results
            db.commit()
            
            # Log summary
            logger.info("=" * 80)
            logger.info("RECOVERY COMPLETE")
            logger.info(f"  Pending jobs submitted: {results['pending_submitted']}")
            logger.info(f"  Jobs resubmitted: {results['resubmitted']}")
            logger.info(f"  Polling resumed: {results['polling_resumed']}")
            logger.info(f"  Status updated: {results['status_updated']}")
            logger.info(f"  Batches checked: {results['batch_checked']}")
            logger.info(f"  Errors: {results['errors']}")
            logger.info("=" * 80)
            
            return results
            
        except Exception as e:
            logger.error(f"Recovery failed: {e}")
            recovery.completed_at = datetime.now(UTC)
            recovery.recovery_metadata = {"error": str(e)}
            db.commit()
            raise


def recover_single_job_sync(job: Job, db) -> Dict[str, Any]:
    """
    Synchronously recover a single job by checking its status.
    
    Args:
        job: Job object to recover
        db: Database session
        
    Returns:
        Dict with recovery action taken
    """
    logger.info(f"Recovering job {job.id} (status: {job.status}, workflow: {job.workflow_id})")
    
    # If no workflow_id, job was never submitted
    if not job.workflow_id:
        if job.status == constants.JobStatus.PENDING.value:
            # Will be handled in Phase 1
            logger.info(f"Job {job.id} is pending, will be submitted in Phase 1")
            return {"job_id": job.id, "action": "pending"}
        else:
            # Job has status but no workflow - resubmit
            logger.warning(f"Job {job.id} has status {job.status} but no workflow_id, resetting")
            job.status = constants.JobStatus.PENDING.value
            job.initiation_ts = None
            db.commit()
            
            submit_job.apply_async(args=[job.id], queue='jobs')
            return {"job_id": job.id, "action": "resubmitted"}
    
    # Poll Moody's API to get current status
    try:
        with httpx.Client(timeout=settings.MOODYS_API_TIMEOUT) as client:
            response = client.get(
                f"{settings.MOODYS_API_BASE_URL}/workflows/{job.workflow_id}/status"
            )
            
            if response.status_code == 404:
                # Job not found in Moody's - resubmit
                logger.warning(f"Job {job.workflow_id} not found in Moody's, resubmitting job {job.id}")
                
                # Clear old workflow_id and reset status
                job.workflow_id = None
                job.status = constants.JobStatus.PENDING.value
                job.initiation_ts = None
                db.commit()
                
                # Resubmit
                submit_job.apply_async(args=[job.id], queue='jobs')
                return {"job_id": job.id, "action": "resubmitted", "reason": "not_found"}
            
            response.raise_for_status()
            result = response.json()
            current_status = result["status"]
            
            # Map Moody's status to our status
            status_map = {
                "submitted": constants.JobStatus.SUBMITTED.value,
                "queued": constants.JobStatus.QUEUED.value,
                "running": constants.JobStatus.RUNNING.value, 
                "completed": constants.JobStatus.COMPLETED.value,
                "failed": constants.JobStatus.FAILED.value,
                "cancelled": constants.JobStatus.CANCELLED.value
            }
            
            our_status = status_map.get(current_status)
            
            # Update job status if different
            status_changed = False
            if our_status and our_status != job.status:
                logger.info(f"Job {job.id} status mismatch. DB: {job.status}, Moody's: {our_status}")
                job.status = our_status
                job.last_poll_ts = datetime.now(UTC)
                status_changed = True
                
                if our_status in [constants.JobStatus.COMPLETED.value, 
                                 constants.JobStatus.FAILED.value, 
                                 constants.JobStatus.CANCELLED.value]:
                    job.completed_ts = datetime.now(UTC)
                
                # Record the status update
                job_status = JobStatus(
                    job_id=job.id,
                    status=current_status,
                    response_data=result,
                    http_status_code=200,
                    poll_duration_ms=0
                )
                db.add(job_status)
                db.commit()
            
            # Resume polling if still active
            if our_status in [constants.JobStatus.SUBMITTED.value,
                            constants.JobStatus.QUEUED.value,
                            constants.JobStatus.RUNNING.value]:
                
                logger.info(f"Resuming polling for job {job.id} (status: {our_status})")
                
                # Calculate next poll interval
                if our_status == constants.JobStatus.QUEUED.value:
                    countdown = settings.POLL_INTERVAL_QUEUED_SECONDS
                else:
                    countdown = settings.POLL_INTERVAL_RUNNING_SECONDS
                
                # Check if we should still poll (not exceeded max duration)
                should_poll = True
                if job.initiation_ts:
                    elapsed = (datetime.now(UTC) - job.initiation_ts).total_seconds()
                    if elapsed > settings.POLL_MAX_DURATION_SECONDS:
                        logger.warning(f"Job {job.id} exceeded max poll duration")
                        job.status = constants.JobStatus.FAILED.value
                        job.last_error = "Exceeded maximum polling duration"
                        job.completed_ts = datetime.now(UTC)
                        db.commit()
                        should_poll = False
                
                if should_poll:
                    # Schedule next poll
                    poll_job_status.apply_async(
                        args=[job.id, job.workflow_id],
                        countdown=countdown,
                        queue='polling'
                    )
                    
                    return {
                        "job_id": job.id, 
                        "action": "resumed_polling", 
                        "status": our_status,
                        "status_changed": status_changed
                    }
                else:
                    return {"job_id": job.id, "action": "marked_failed", "reason": "timeout"}
            
            elif our_status == constants.JobStatus.COMPLETED.value:
                return {"job_id": job.id, "action": "marked_complete", "status_changed": status_changed}
            elif our_status == constants.JobStatus.FAILED.value:
                return {"job_id": job.id, "action": "marked_failed", "status_changed": status_changed}
            else:
                return {"job_id": job.id, "action": "status_updated" if status_changed else "no_action", "status": our_status}
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error checking job {job.id}: {e}")
        
        if e.response.status_code in constants.HTTPStatusCode.RETRYABLE:
            # Schedule a retry
            poll_job_status.apply_async(
                args=[job.id, job.workflow_id],
                countdown=60,
                queue='polling'
            )
            return {"job_id": job.id, "action": "retry_scheduled", "error": f"HTTP {e.response.status_code}"}
        
        return {"job_id": job.id, "action": "error", "error": f"HTTP {e.response.status_code}"}
        
    except Exception as e:
        logger.error(f"Error checking job {job.id}: {e}")
        return {"job_id": job.id, "action": "error", "error": str(e)}


@celery.task(name='src.tasks.recovery_tasks.recover_single_job')
def recover_single_job(job_id: int) -> Dict[str, Any]:
    """
    Async task to recover a single job.
    
    Args:
        job_id: Database job ID
        
    Returns:
        Dict with recovery action taken
    """
    logger.info(f"Async recovery for job {job_id}")
    
    with get_db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            logger.error(f"Job {job_id} not found")
            return {"job_id": job_id, "error": "not_found"}
        
        return recover_single_job_sync(job, db)


@celery.task(name='src.tasks.recovery_tasks.startup_recovery')
def startup_recovery() -> Dict[str, Any]:
    """
    Special recovery task to run on system startup.
    Performs comprehensive recovery and system initialization.
    
    Returns:
        Dict with recovery statistics
    """
    logger.info("*" * 80)
    logger.info("STARTUP RECOVERY: Initializing system")
    logger.info("*" * 80)
    
    # Wait for system to stabilize
    import time
    time.sleep(5)
    
    # Run comprehensive recovery
    result = perform_recovery_check(recovery_type=constants.RecoveryType.STARTUP.value)
    
    logger.info("*" * 80)
    logger.info("STARTUP RECOVERY: Complete")
    logger.info(f"  - Submitted {result.get('pending_submitted', 0)} pending jobs")
    logger.info(f"  - Resumed polling for {result.get('polling_resumed', 0)} jobs")
    logger.info(f"  - Resubmitted {result.get('resubmitted', 0)} missing jobs")
    logger.info(f"  - Triggered checks for {result.get('batch_checked', 0)} batches")
    logger.info("*" * 80)
    
    return result


@celery.task(name='src.tasks.recovery_tasks.detect_orphan_jobs')
def detect_orphan_jobs() -> Dict[str, Any]:
    """
    Detect and recover orphaned jobs that haven't been updated recently.
    
    Returns:
        Dict with orphan job information
    """
    logger.info("Detecting orphan jobs")
    
    with get_db_session() as db:
        # Find jobs not updated in the last threshold period
        threshold = datetime.now(UTC) - timedelta(
            seconds=settings.RECOVERY_STALE_JOB_THRESHOLD_SECONDS
        )
        
        orphan_jobs = db.query(Job).filter(
            Job.status.in_(['submitted', 'queued', 'running']),
            Job.updated_ts < threshold
        ).all()
        
        if not orphan_jobs:
            logger.debug("No orphan jobs detected")
            return {"orphan_count": 0}
        
        logger.warning(f"Found {len(orphan_jobs)} potential orphan jobs")
        
        results = {
            "orphan_count": len(orphan_jobs),
            "orphan_job_ids": [],
            "recovered": 0,
            "errors": 0
        }
        
        for job in orphan_jobs:
            try:
                results["orphan_job_ids"].append(job.id)
                
                # Directly recover the job
                recovery_result = recover_single_job_sync(job, db)
                
                if recovery_result.get("action") != "error":
                    results["recovered"] += 1
                else:
                    results["errors"] += 1
                    
            except Exception as e:
                logger.error(f"Error recovering orphan job {job.id}: {e}")
                results["errors"] += 1
        
        logger.info(f"Orphan recovery complete: {results['recovered']} recovered, {results['errors']} errors")
        return results


@celery.task(name='src.tasks.recovery_tasks.health_check_recovery')
def health_check_recovery() -> Dict[str, Any]:
    """
    Perform a health check and trigger recovery if issues are found.
    
    Returns:
        Dict with health status and actions taken
    """
    logger.info("Running health check recovery")
    
    with get_db_session() as db:
        health_status = {
            "healthy": True,
            "issues": [],
            "actions_taken": []
        }
        
        # Check for stuck jobs
        stuck_threshold = datetime.now(UTC) - timedelta(hours=2)
        stuck_jobs = db.query(Job).filter(
            Job.status.in_(['submitted', 'queued', 'running']),
            Job.updated_ts < stuck_threshold
        ).count()
        
        if stuck_jobs > 0:
            health_status["healthy"] = False
            health_status["issues"].append(f"{stuck_jobs} stuck jobs detected")
            
            # Trigger recovery
            perform_recovery_check.apply_async(
                args=[constants.RecoveryType.SCHEDULED.value],
                queue='recovery'
            )
            health_status["actions_taken"].append("Triggered recovery for stuck jobs")
        
        # Check for batches without recent updates
        batch_threshold = datetime.now(UTC) - timedelta(hours=1)
        stuck_batches = db.query(Batch).filter(
            Batch.status == constants.BatchStatus.RUNNING.value,
            Batch.updated_ts < batch_threshold
        ).count()
        
        if stuck_batches > 0:
            health_status["healthy"] = False
            health_status["issues"].append(f"{stuck_batches} stuck batches detected")
            
            # Trigger batch check
            check_all_batch.apply_async(queue='default')
            health_status["actions_taken"].append("Triggered batch completion check")
        
        # Check for high failure rate
        recent_window = datetime.now(UTC) - timedelta(hours=1)
        recent_failures = db.query(Job).filter(
            Job.status == constants.JobStatus.FAILED.value,
            Job.completed_ts > recent_window
        ).count()
        
        if recent_failures > 10:
            health_status["healthy"] = False
            health_status["issues"].append(f"{recent_failures} recent failures detected")
            logger.warning(f"High failure rate detected: {recent_failures} failures in last hour")
        
        if health_status["healthy"]:
            logger.info("System health check passed")
        else:
            logger.warning(f"System health issues detected: {health_status['issues']}")
        
        return health_status