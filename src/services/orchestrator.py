"""Orchestration service for managing job flow."""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from src.db.session import get_db_session
from src.db.repositories.batch_repository import BatchRepository
from src.db.repositories.job_repository import JobRepository
from src.db.repositories.configuration_repository import ConfigurationRepository
from src.tasks.job_tasks import submit_job, cancel_job
from src.tasks.batch_tasks import check_batch_completion
from src.core.constants import JobStatus, BatchStatus
from src.core.exceptions import (
    BatchNotFoundException, 
    JobNotFoundException,
    InvalidStatusTransitionException
)

logger = logging.getLogger(__name__)


class Orchestrator:
    """Main orchestration service for job management."""
    
    def __init__(self):
        """Initialize orchestrator."""
        self.logger = logger
    
    def submit_batch_jobs(self, batch_id: int) -> Dict[str, Any]:
        """
        Submit all jobs for an batch.
        
        Args:
            batch_id: Batch ID
            
        Returns:
            Dict with submission results
        """
        self.logger.info(f"Submitting jobs for batch {batch_id}")
        
        with get_db_session() as db:
            batch_repo = BatchRepository(db)
            job_repo = JobRepository(db)
            
            # Get batch
            batch = batch_repo.get(batch_id)
            if not batch:
                raise BatchNotFoundException(f"Batch {batch_id} not found")
            
            # Get all jobs for batch
            jobs = job_repo.get_jobs_by_batch(batch_id)
            if not jobs:
                self.logger.warning(f"No jobs found for batch {batch_id}")
                return {"submitted": 0, "skipped": 0, "errors": 0}
            
            results = {
                "submitted": 0,
                "skipped": 0,
                "errors": 0,
                "job_ids": []
            }
            
            # Update batch status to running
            batch_repo.update_status(batch_id, BatchStatus.RUNNING.value)
            
            # Submit each job
            for job in jobs:
                try:
                    if job.status != JobStatus.PENDING.value:
                        self.logger.info(f"Skipping job {job.id} - already {job.status}")
                        results["skipped"] += 1
                        continue
                    
                    # Submit job asynchronously
                    task = submit_job.delay(job.id)
                    self.logger.info(f"Submitted job {job.id}, task ID: {task.id}")
                    
                    results["submitted"] += 1
                    results["job_ids"].append(job.id)
                    
                except Exception as e:
                    self.logger.error(f"Error submitting job {job.id}: {e}")
                    results["errors"] += 1
            
            # Schedule batch completion check
            check_batch_completion.delay(batch_id)
            
            return results
    
    def cancel_batch(self, batch_id: int) -> Dict[str, Any]:
        """
        Cancel an batch and all its jobs.
        
        Args:
            batch_id: Batch ID
            
        Returns:
            Dict with cancellation results
        """
        self.logger.info(f"Cancelling batch {batch_id}")
        
        with get_db_session() as db:
            batch_repo = BatchRepository(db)
            job_repo = JobRepository(db)
            
            # Get batch
            batch = batch_repo.get(batch_id)
            if not batch:
                raise BatchNotFoundException(f"Batch {batch_id} not found")
            
            # Get active jobs
            jobs = job_repo.get_jobs_by_batch(batch_id)
            active_jobs = [j for j in jobs if j.status in 
                          ['submitted', 'queued', 'running']]
            
            results = {
                "cancelled": 0,
                "skipped": 0,
                "errors": 0
            }
            
            # Cancel each active job
            for job in active_jobs:
                try:
                    if job.workflow_id:
                        # Cancel via Celery task
                        cancel_job.delay(job.id)
                        results["cancelled"] += 1
                    else:
                        # Just update status
                        job_repo.update_status(job.id, JobStatus.CANCELLED.value)
                        results["cancelled"] += 1
                        
                except Exception as e:
                    self.logger.error(f"Error cancelling job {job.id}: {e}")
                    results["errors"] += 1
            
            # Cancel batch
            batch_repo.cancel_batch(batch_id)
            
            return results
    
    def resubmit_failed_job(self, job_id: int, 
                        config_override: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Resubmit a failed job.

        Resubmitting a job involves the following steps:
            1. Create a new configuration if override provided
            2. Create a new job with the same batch and configuration
            3. Set the parent job reference
            4. Update the original job to cancelled
            5. Update the batch to running if needed
            6. Submit the new job

        Args:
            job_id: Job ID to resubmit
            config_override: Optional configuration override
            
        Returns:
            Dict with new job information
        """
        self.logger.info(f"Resubmitting job {job_id}")
        
        with get_db_session() as db:
            job_repo = JobRepository(db)
            config_repo = ConfigurationRepository(db)
            
            # Get original job
            original_job = job_repo.get_with_details(job_id)
            if not original_job:
                raise JobNotFoundException(f"Job {job_id} not found")
            
            # Check if job is in terminal state
            if original_job.status not in ['failed', 'cancelled']:
                raise InvalidStatusTransitionException(
                    f"Can only resubmit failed or cancelled jobs, current status: {original_job.status}"
                )
                        
            # Create new configuration if override provided
            if config_override:
                logger.info(f"Creating new configuration {original_job.configuration.config_name}_resubmit for job {job_id} resubmit")
                config = config_repo.create_configuration(
                    original_job.batch_id,
                    f"{original_job.configuration.config_name}_resubmit",
                    config_override
                )
                config_id = config.id
            else:
                # Use original configuration if no override provided
                logger.info(f"Using original configuration {original_job.configuration_id} for job {job_id} resubmit")
                config_id = original_job.configuration_id
            
            # Create new job
            new_job = job_repo.create_job(
                original_job.batch_id,
                config_id
            )
            logger.info(f"Created new job {new_job.id} for job {job_id} resubmit")
            
            # Set parent reference
            new_job.parent_job_id = original_job.id

            # Update the original job to cancelled
            job_repo.update_status(original_job.id, JobStatus.CANCELLED.value)
            logger.info(f"Updated original job {original_job.id} status to cancelled")

            # Update the batch to running if needed
            if original_job.batch.status != BatchStatus.RUNNING.value:
                logger.info(f"Updating batch {original_job.batch_id} status to running")
                batch_repo = BatchRepository(db)
                batch_repo.update_status(original_job.batch_id, BatchStatus.RUNNING.value)
            else:
                logger.info(f"Batch {original_job.batch_id} already running")

            db.commit()

            # Submit new job
            submit_job.delay(new_job.id)
            logger.info(f"Submitted new job {new_job.id} for job {job_id} resubmit")
            
            return {
                "original_job_id": job_id,
                "new_job_id": new_job.id,
                "configuration_id": config_id,
                "status": "submitted"
            }
    
    def get_batch_progress(self, batch_id: int) -> Dict[str, Any]:
        """
        Get detailed progress of an batch.
        
        Args:
            batch_id: Batch ID
            
        Returns:
            Dict with progress information
        """
        with get_db_session() as db:
            batch_repo = BatchRepository(db)
            job_repo = JobRepository(db)
            
            # Get batch summary
            summary = batch_repo.get_batch_summary(batch_id)
            if not summary:
                raise BatchNotFoundException(f"Batch {batch_id} not found")
            
            # Get job details
            jobs = job_repo.get_jobs_by_batch(batch_id)
            job_details = []
            
            for job in jobs:
                metrics = job_repo.get_job_metrics(job.id)
                job_details.append({
                    "job_id": job.id,
                    "configuration": job.configuration.config_name if job.configuration else None,
                    "status": job.status,
                    "workflow_id": job.workflow_id,
                    "metrics": metrics
                })         
           
            return {
                "batch": summary,
                "jobs": job_details,
            }