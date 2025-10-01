"""Repository for Job operations."""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, UTC
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, or_

from src.db.models import Job, WorkflowStatus, RetryHistory
from src.db.repositories.base_repository import BaseRepository
from src.core.constants import JobStatus


class JobRepository(BaseRepository[Job]):
    """Repository for job-related database operations."""
    
    def __init__(self, db: Session):
        super().__init__(Job, db)
    
    def get(self, job_id: int) -> Optional[Job]:
        """Get job by ID."""
        return self.db.query(Job).filter(Job.id == job_id).first()

    def get_with_details(self, job_id: int) -> Optional[Job]:
        """Get job with all related data."""
        return self.db.query(Job)\
            .options(
                joinedload(Job.configuration),
                joinedload(Job.job_statuses),
                joinedload(Job.retry_history)
            )\
            .filter(Job.id == job_id)\
            .first()
    
    def get_jobs_by_batch(self, batch_id: int) -> List[Job]:
        """Get all jobs for an batch."""
        return self.db.query(Job)\
            .filter(Job.batch_id == batch_id)\
            .all()
    
    def get_active_jobs(self) -> List[Job]:
        """Get all jobs in active state."""
        return self.db.query(Job)\
            .filter(Job.status.in_(JobStatus.is_active()))\
            .all()
    
    def get_stale_jobs(self, stale_threshold_seconds: int = 600) -> List[Job]:
        """Get jobs that haven't been updated recently."""
        threshold = datetime.now(UTC) - timedelta(seconds=stale_threshold_seconds)
        return self.db.query(Job)\
            .filter(
                and_(
                    Job.status.in_(JobStatus.is_active()),
                    Job.updated_ts < threshold
                )
            )\
            .all()
    
    def create_job(self, batch_id: int, configuration_id: int) -> Job:
        """Create a new job."""
        return self.create(
            batch_id=batch_id,
            configuration_id=configuration_id,
            status=JobStatus.PENDING.value
        )
    
    def update_status(self, job_id: int, status: str, error: str = None) -> Optional[Job]:
        """Update job status."""
        updates = {"status": status}
        
        if error:
            updates["last_error"] = error
        
        if status == JobStatus.SUBMITTED.value:
            updates["initiation_ts"] = datetime.now(UTC)
        elif status in [JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value]:
            updates["completed_ts"] = datetime.now(UTC)
        
        return self.update(job_id, **updates)
    
    def record_job_status(self, job_id: int, status: str, 
                              response_data: Dict[str, Any],
                              http_status_code: int = 200,
                              poll_duration_ms: int = None) -> WorkflowStatus:
        """Record a workflow status poll result."""
        job_status = WorkflowStatus(
            job_id=job_id,
            status=status,
            response_data=response_data,
            http_status_code=http_status_code,
            poll_duration_ms=poll_duration_ms
        )
        self.db.add(job_status)
        self.db.commit()
        return job_status
    
    def record_retry(self, job_id: int, error_code: str, 
                    error_message: str, retry_after: int = None) -> RetryHistory:
        """Record a retry attempt."""
        # Get current retry count
        job = self.get(job_id)
        retry_count = job.retry_count + 1 if job else 1
        
        retry = RetryHistory(
            job_id=job_id,
            retry_attempt=retry_count,
            error_code=error_code,
            error_message=error_message,
            retry_after_seconds=retry_after
        )
        self.db.add(retry)
        
        # Update job retry count
        if job:
            job.retry_count = retry_count
        
        self.db.commit()
        return retry
    
    def get_job_metrics(self, job_id: int) -> Dict[str, Any]:
        """Get job metrics and timing information."""
        job = self.get_with_details(job_id)
        if not job:
            return None
        
        metrics = {
            "job_id": job.id,
            "status": job.status,
            "workflow_id": job.workflow_id,
            "age_minutes": 0,
            "time_in_status_minutes": 0,
            "retry_count": job.retry_count,
            "poll_count": len(job.job_statuses) if job.job_statuses else 0
        }
        
        if job.created_ts:
            age = datetime.now(UTC) - job.created_ts
            metrics["age_minutes"] = int(age.total_seconds() / 60)
        
        if job.updated_ts:
            time_in_status = datetime.now(UTC) - job.updated_ts
            metrics["time_in_status_minutes"] = int(time_in_status.total_seconds() / 60)
        
        if job.initiation_ts and job.completed_ts:
            duration = job.completed_ts - job.initiation_ts
            metrics["execution_minutes"] = int(duration.total_seconds() / 60)
        
        return metrics