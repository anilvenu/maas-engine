"""Orchestration service for managing job workflow."""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from src.db.session import get_db_session
from src.db.repositories.analysis_repository import AnalysisRepository
from src.db.repositories.job_repository import JobRepository
from src.db.repositories.configuration_repository import ConfigurationRepository
from src.tasks.job_tasks import submit_job, cancel_job
from src.tasks.analysis_tasks import check_analysis_completion
from src.core.constants import JobStatus, AnalysisStatus
from src.core.exceptions import (
    AnalysisNotFoundException, 
    JobNotFoundException,
    InvalidStatusTransitionException
)

logger = logging.getLogger(__name__)


class Orchestrator:
    """Main orchestration service for job management."""
    
    def __init__(self):
        """Initialize orchestrator."""
        self.logger = logger
    
    def submit_analysis_jobs(self, analysis_id: int) -> Dict[str, Any]:
        """
        Submit all jobs for an analysis.
        
        Args:
            analysis_id: Analysis ID
            
        Returns:
            Dict with submission results
        """
        self.logger.info(f"Submitting jobs for analysis {analysis_id}")
        
        with get_db_session() as db:
            analysis_repo = AnalysisRepository(db)
            job_repo = JobRepository(db)
            
            # Get analysis
            analysis = analysis_repo.get(analysis_id)
            if not analysis:
                raise AnalysisNotFoundException(f"Analysis {analysis_id} not found")
            
            # Get all jobs for analysis
            jobs = job_repo.get_jobs_by_analysis(analysis_id)
            if not jobs:
                self.logger.warning(f"No jobs found for analysis {analysis_id}")
                return {"submitted": 0, "skipped": 0, "errors": 0}
            
            results = {
                "submitted": 0,
                "skipped": 0,
                "errors": 0,
                "job_ids": []
            }
            
            # Update analysis status to running
            analysis_repo.update_status(analysis_id, AnalysisStatus.RUNNING.value)
            
            # Submit each job
            for job in jobs:
                try:
                    if job.status != JobStatus.PLANNED.value:
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
            
            # Schedule analysis completion check
            check_analysis_completion.delay(analysis_id)
            
            return results
    
    def cancel_analysis(self, analysis_id: int) -> Dict[str, Any]:
        """
        Cancel an analysis and all its jobs.
        
        Args:
            analysis_id: Analysis ID
            
        Returns:
            Dict with cancellation results
        """
        self.logger.info(f"Cancelling analysis {analysis_id}")
        
        with get_db_session() as db:
            analysis_repo = AnalysisRepository(db)
            job_repo = JobRepository(db)
            
            # Get analysis
            analysis = analysis_repo.get(analysis_id)
            if not analysis:
                raise AnalysisNotFoundException(f"Analysis {analysis_id} not found")
            
            # Get active jobs
            jobs = job_repo.get_jobs_by_analysis(analysis_id)
            active_jobs = [j for j in jobs if j.status in 
                          ['initiated', 'queued', 'running']]
            
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
            
            # Cancel analysis
            analysis_repo.cancel_analysis(analysis_id)
            
            return results
    
    def retry_failed_job(self, job_id: int, 
                        config_override: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Retry a failed job.
        
        Args:
            job_id: Job ID to retry
            config_override: Optional configuration override
            
        Returns:
            Dict with new job information
        """
        self.logger.info(f"Retrying job {job_id}")
        
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
                    f"Can only retry failed or cancelled jobs, current status: {original_job.status}"
                )
            
            # Create new configuration if override provided
            if config_override:
                config = config_repo.create_configuration(
                    original_job.analysis_id,
                    f"{original_job.configuration.config_name}_retry",
                    config_override
                )
                config_id = config.id
            else:
                config_id = original_job.configuration_id
            
            # Create new job
            new_job = job_repo.create_job(
                original_job.analysis_id,
                config_id
            )
            
            # Set parent reference
            new_job.parent_job_id = original_job.id
            db.commit()
            
            # Update the analysis to running if needed
            if original_job.analysis.status != AnalysisStatus.RUNNING.value:
                logger.info(f"Updating analysis {original_job.analysis_id} status to running")
                analysis_repo = AnalysisRepository(db)
                analysis_repo.update_status(original_job.analysis_id, AnalysisStatus.RUNNING.value)
            else:
                logger.info(f"Analysis {original_job.analysis_id} already running")

            # Submit new job
            submit_job.delay(new_job.id)
            
            return {
                "original_job_id": job_id,
                "new_job_id": new_job.id,
                "configuration_id": config_id,
                "status": "submitted"
            }
    
    def get_analysis_progress(self, analysis_id: int) -> Dict[str, Any]:
        """
        Get detailed progress of an analysis.
        
        Args:
            analysis_id: Analysis ID
            
        Returns:
            Dict with progress information
        """
        with get_db_session() as db:
            analysis_repo = AnalysisRepository(db)
            job_repo = JobRepository(db)
            
            # Get analysis summary
            summary = analysis_repo.get_analysis_summary(analysis_id)
            if not summary:
                raise AnalysisNotFoundException(f"Analysis {analysis_id} not found")
            
            # Get job details
            jobs = job_repo.get_jobs_by_analysis(analysis_id)
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
            
            # Calculate progress percentage
            total_jobs = len(jobs)
            completed_jobs = sum(1 for j in jobs if j.status == 'completed')
            cancelled_jobs = sum(1 for j in jobs if j.status == 'cancelled')
            failed_jobs = sum(1 for j in jobs if j.status == 'failed')

            print(f"Total jobs: {total_jobs}")  # Debugging line
            print(f"Completed jobs: {completed_jobs}")  # Debugging line
            print(f"Cancelled jobs: {cancelled_jobs}")  # Debugging line
            print(f"Failed jobs: {failed_jobs}")  # Debugging line

            progress_percentage = (completed_jobs / total_jobs * 100) if total_jobs > 0 else 0

            
            return {
                "analysis": summary,
                "jobs": job_details,
                "total_jobs": total_jobs,
                "completed_jobs": completed_jobs,
                "cancelled_jobs": cancelled_jobs,
                "failed_jobs": failed_jobs
            }