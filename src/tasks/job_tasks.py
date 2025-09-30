"""Celery tasks for job management."""

import httpx
import logging
from datetime import datetime, timedelta, UTC
from typing import Optional, Dict, Any

from src.tasks.celery_app import celery
from src.db.session import get_db_session
from src.db.models import Job, WorkflowStatus, RetryHistory
from src.core.constants import JobStatus, HTTPStatusCode
from src.core.config import settings

logger = logging.getLogger(__name__)


@celery.task(bind=True, name='src.tasks.job_tasks.submit_job')
def submit_job(self, job_id: int) -> Dict[str, Any]:
    """
    Submit a job to Moody's API.
    
    Args:
        job_id: Database job ID
        
    Returns:
        Dict with workflow_id and status
    """
    logger.info(f"Submitting job {job_id}")
    
    with get_db_session() as db:
        # Get job from database
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            logger.error(f"Job {job_id} not found")
            raise ValueError(f"Job {job_id} not found")
        
        # Check if already submitted
        if job.workflow_id:
            logger.info(f"Job {job_id} already submitted with workflow_id {job.workflow_id}")
            return {"workflow_id": job.workflow_id, "status": "already_submitted"}
        
        # Prepare submission data
        submission_data = {
            "analysis_id": job.analysis_id,
            "configuration_id": job.configuration_id,
            "model_name": job.configuration.config_data.get("model", "default_model"),
            "parameters": job.configuration.config_data.get("parameters", {})
        }
        
        try:
            # Submit to Moody's API
            with httpx.Client(timeout=settings.MOODYS_API_TIMEOUT) as client:
                logger.info(f"Submitting job {job_id} to Moody's API at {settings.MOODYS_API_BASE_URL}/workflows")
                logger.info(f"Submission data for job {job_id}: {submission_data}")

                response = client.post(
                    f"{settings.MOODYS_API_BASE_URL}/workflows",
                    json=submission_data
                )
                response.raise_for_status()
                result = response.json()
            
            # Update job with workflow ID
            job.workflow_id = result["workflow_id"]
            job.status = JobStatus.INITIATED.value
            job.initiation_ts = datetime.now(UTC)
            job.celery_task_id = self.request.id
            db.commit()
            
            logger.info(f"Job {job_id} submitted successfully: {job.workflow_id}")
            
            # Schedule first poll after initial delay
            poll_workflow_status.apply_async(
                args=[job_id, job.workflow_id],
                countdown=settings.POLL_INITIAL_DELAY_SECONDS
            )
            
            return {
                "workflow_id": job.workflow_id,
                "status": "submitted",
                "message": result.get("message")
            }
            
        except httpx.HTTPStatusError as e:
            # Handle HTTP errors
            status_code = e.response.status_code
            logger.error(f"HTTP error submitting job {job_id}: {status_code}")
            
            # Record retry history
            retry_record = RetryHistory(
                job_id=job_id,
                retry_attempt=self.request.retries + 1,
                error_code=str(status_code),
                error_message=e.response.text[:500],
                retry_after_seconds=int(e.response.headers.get("Retry-After", 60))
            )
            db.add(retry_record)
            
            # Update job error
            job.last_error = f"HTTP {status_code}: {e.response.text[:500]}"
            job.retry_count += 1
            db.commit()
            
            # Retry if retryable error
            if status_code in HTTPStatusCode.RETRYABLE:
                retry_after = int(e.response.headers.get("Retry-After", 60))
                raise self.retry(exc=e, countdown=retry_after)
            else:
                # Non-retryable error
                job.status = JobStatus.FAILED.value
                db.commit()
                raise
                
        except Exception as e:
            logger.error(f"Error submitting job {job_id}: {str(e)}")
            job.last_error = str(e)[:500]
            job.retry_count += 1
            db.commit()
            raise self.retry(exc=e, countdown=60)


@celery.task(bind=True, name='src.tasks.job_tasks.poll_workflow_status')
def poll_workflow_status(self, job_id: int, workflow_id: str) -> Dict[str, Any]:
    """
    Poll workflow status from Moody's API.
    
    Args:
        job_id: Database job ID
        workflow_id: Moody's workflow ID
        
    Returns:
        Dict with current status
    """
    logger.info(f"Polling status for job {job_id}, workflow {workflow_id}")
    
    with get_db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            logger.error(f"Job {job_id} not found")
            return {"error": "Job not found"}
        
        # Skip if job is already in terminal state
        if JobStatus.is_terminal(job.status):
            logger.info(f"Job {job_id} already in terminal state: {job.status}")
            return {"status": job.status, "terminal": True}
        
        poll_start = datetime.now(UTC)
        
        try:
            # Poll Moody's API
            with httpx.Client(timeout=settings.MOODYS_API_TIMEOUT) as client:
                response = client.get(
                    f"{settings.MOODYS_API_BASE_URL}/workflows/{workflow_id}/status"
                )
                response.raise_for_status()
                result = response.json()
            
            poll_duration_ms = int((datetime.now(UTC) - poll_start).total_seconds() * 1000)
            
            # Store workflow status
            workflow_status = WorkflowStatus(
                job_id=job_id,
                status=result["status"],
                response_data=result,
                http_status_code=response.status_code,
                poll_duration_ms=poll_duration_ms
            )
            db.add(workflow_status)
            
            # Map Moody's status to our job status
            moodys_to_job_status = {
                "queued": JobStatus.QUEUED.value,
                "running": JobStatus.RUNNING.value,
                "completed": JobStatus.COMPLETED.value,
                "failed": JobStatus.FAILED.value,
                "cancelled": JobStatus.CANCELLED.value
            }
            
            new_status = moodys_to_job_status.get(result["status"])
            if new_status and new_status != job.status:
                job.status = new_status
                logger.info(f"Job {job_id} status changed to {new_status}")
            
            job.last_poll_ts = datetime.now(UTC)
            
            # Update completed timestamp if terminal
            if JobStatus.is_terminal(new_status):
                job.completed_ts = datetime.now(UTC)
            
            db.commit()
            
            # Schedule next poll if not terminal
            if not JobStatus.is_terminal(new_status):
                # Determine poll interval based on status
                if new_status == JobStatus.QUEUED.value:
                    countdown = settings.POLL_INTERVAL_QUEUED_SECONDS
                else:
                    countdown = settings.POLL_INTERVAL_RUNNING_SECONDS
                
                # Check if we've been polling too long
                if job.initiation_ts:
                    elapsed = (datetime.now(UTC) - job.initiation_ts).total_seconds()
                    if elapsed > settings.POLL_MAX_DURATION_SECONDS:
                        logger.warning(f"Job {job_id} exceeded max poll duration")
                        job.status = JobStatus.FAILED.value
                        job.last_error = "Exceeded maximum polling duration"
                        db.commit()
                        return {"status": "timeout", "message": "Exceeded max poll duration"}
                
                # Schedule next poll
                poll_workflow_status.apply_async(
                    args=[job_id, workflow_id],
                    countdown=countdown
                )
                logger.info(f"Scheduled next poll for job {job_id} in {countdown} seconds")
            
            return {
                "job_id": job_id,
                "workflow_id": workflow_id,
                "status": result["status"],
                "progress": result.get("progress_percentage"),
                "terminal": JobStatus.is_terminal(new_status)
            }
            
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            logger.error(f"HTTP error polling job {job_id}: {status_code}")
            
            # Handle 404 - workflow not found
            if status_code == 404:
                job.status = JobStatus.FAILED.value
                job.last_error = "Workflow not found in Moody's system"
                db.commit()
                return {"status": "not_found", "error": "Workflow not found"}
            
            # Retry for temporary errors
            if status_code in HTTPStatusCode.RETRYABLE:
                raise self.retry(exc=e, countdown=60)
            
            return {"error": f"HTTP {status_code}"}
            
        except Exception as e:
            logger.error(f"Error polling job {job_id}: {str(e)}")
            raise self.retry(exc=e, countdown=60)


@celery.task(name='src.tasks.job_tasks.cancel_job')
def cancel_job(job_id: int) -> Dict[str, Any]:
    """
    Cancel a running job.
    
    Args:
        job_id: Database job ID
        
    Returns:
        Dict with cancellation result
    """
    logger.info(f"Cancelling job {job_id}")
    
    with get_db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            return {"error": "Job not found"}
        
        if not job.workflow_id:
            return {"error": "Job has no workflow ID"}
        
        if job.status in [JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value]:
            return {"status": job.status, "message": "Job already in terminal state"}
        
        try:
            # Call Moody's cancel API
            with httpx.Client(timeout=settings.MOODYS_API_TIMEOUT) as client:
                response = client.post(
                    f"{settings.MOODYS_API_BASE_URL}/workflows/{job.workflow_id}/cancel"
                )
                response.raise_for_status()
                result = response.json()
            
            # Update job status
            job.status = JobStatus.CANCELLED.value
            job.completed_ts = datetime.now(UTC)
            db.commit()
            
            logger.info(f"Job {job_id} cancelled successfully")
            return {"status": "cancelled", "message": result.get("message")}
            
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return {"error": f"Failed to cancel: HTTP {e.response.status_code}"}
        except Exception as e:
            logger.error(f"Error cancelling job {job_id}: {str(e)}")
            return {"error": str(e)}