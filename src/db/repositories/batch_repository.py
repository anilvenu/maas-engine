"""Repository for Batch operations."""

from typing import List, Optional, Dict, Any
from datetime import datetime, UTC
from sqlalchemy.orm import Session
from sqlalchemy import and_

from src.db.models import Batch, Configuration, Job
from src.db.repositories.base_repository import BaseRepository
import src.core.constants as constants

class BatchRepository(BaseRepository[Batch]):
    """Repository for batch-related database operations."""
    
    def __init__(self, db: Session):
        super().__init__(Batch, db)
    
    def get_with_jobs(self, batch_id: int) -> Optional[Batch]:
        """Get batch with all related jobs."""
        return self.db.query(Batch)\
            .filter(Batch.id == batch_id)\
            .first()
    
    def get_active_batch(self) -> List[Batch]:
        """Get all non-terminal batch."""
        return self.db.query(Batch)\
            .filter(Batch.status.in_(['pending', 'running']))\
            .all()
    
    def get_batch_summary(self, batch_id: int) -> Dict[str, Any]:
        """Get batch summary with job statistics and configuration completion."""
        batch = self.get(batch_id)
        if not batch:
            return None
        
        print(f"Getting summary for batch {batch_id}")
        print(f"Batch details: {batch}")

        # Get all jobs and configurations
        jobs = self.db.query(Job).filter(Job.batch_id == batch_id).all()
        configurations = self.db.query(Configuration).filter(
            Configuration.batch_id == batch_id
        ).all()
        
        # Build job status counts
        status_counts = {}
        for job in jobs:
            status_counts[job.status] = status_counts.get(job.status, 0) + 1
        
        # Check configuration completion
        config_completion = {}
        configs_with_completed_jobs = 0
        total_active_configs = 0
        
        for config in configurations:
            if not config.is_active:
                continue  # Skip inactive configurations
            
            total_active_configs += 1
            config_key = f"{config.config_name}_v{config.version}"
            
            if config.skip:
                config_completion[config_key] = "skipped"
                configs_with_completed_jobs += 1  # Skipped counts as "completed"
            else:
                # Check if this configuration has a completed job
                completed_jobs = [
                    j for j in jobs 
                    if j.configuration_id == config.id 
                    and j.status == constants.JobStatus.COMPLETED.value
                ]
                
                if completed_jobs:
                    config_completion[config_key] = "completed"
                    configs_with_completed_jobs += 1
                else:
                    # Check for active jobs
                    active_jobs = [
                        j for j in jobs 
                        if j.configuration_id == config.id 
                        and j.status in [constants.JobStatus.PENDING.value,
                                         constants.JobStatus.SUBMITTED.value, 
                                         constants.JobStatus.QUEUED.value,
                                         constants.JobStatus.RUNNING.value,                                    
                                         ]
                    ]
                    
                    if active_jobs:
                        config_completion[config_key] = "in_progress"
                    else:
                        # Check for failed/cancelled jobs
                        failed_jobs = [
                            j for j in jobs 
                            if j.configuration_id == config.id 
                            and j.status in [constants.JobStatus.FAILED.value, constants.JobStatus.CANCELLED.value]
                        ]
                        
                        if failed_jobs:
                            config_completion[config_key] = "failed"
                        else:
                            config_completion[config_key] = "not_started"
        
        # Calculate progress percentage based on configuration completion
        progress_percentage = 0.0
        if total_active_configs > 0:
            progress_percentage = (configs_with_completed_jobs / total_active_configs) * 100
        elif jobs:
            # Fallback to job-based progress if no configurations
            completed_jobs = status_counts.get('completed', 0)
            cancelled_jobs = status_counts.get('cancelled', 0)
            progress_percentage = ((completed_jobs + cancelled_jobs) / len(jobs)) * 100

        return {
            "id": batch.id,
            "name": batch.name,
            "description": batch.description,
            "status": batch.status,
            "yaml_config": batch.yaml_config,
            "total_jobs": len(jobs),
            "total_configurations": len(configurations),
            "active_configurations": total_active_configs,
            "job_status_counts": status_counts,
            "configuration_completion": config_completion,
            "configs_completed": configs_with_completed_jobs,
            "configs_total": total_active_configs,
            "created_ts": batch.created_ts,
            "updated_ts": batch.updated_ts,
            "completed_ts": batch.completed_ts,
            "progress_percentage": round(progress_percentage, 2), 
        }
    
    def create_batch(self, name: str, yaml_config: Dict[str, Any], 
                        description: str = None) -> Batch:
        """Create batch from YAML configuration."""
        batch = self.create(
            name=name,
            description=description,
            yaml_config=yaml_config,
            status=constants.BatchStatus.PENDING.value
        )
        return batch
    
    def update_status(self, batch_id: int, status: str) -> Optional[Batch]:
        """Update batch status."""
        updates = {"status": status}
        if status in [constants.BatchStatus.COMPLETED.value, 
                      constants.BatchStatus.FAILED.value, 
                      constants.BatchStatus.CANCELLED.value]:
            updates["completed_ts"] = datetime.now(UTC)
        else:
            updates["completed_ts"] = None
        
        return self.update(batch_id, **updates)
    
    def cancel_batch(self, batch_id: int) -> bool:
        """Cancel a batch and all its jobs."""
        batch = self.get(batch_id)
        if not batch:
            return False
        
        # Update all non-terminal jobs to cancelled
        self.db.query(Job)\
            .filter(
                and_(
                    Job.batch_id == batch_id,
                    Job.status.notin_(['completed', 'failed', 'cancelled'])
                )
            )\
            .update(
                {"status": constants.JobStatus.CANCELLED.value, "completed_ts": datetime.now(UTC)},
                synchronize_session=False
            )
        
        # Update batch status
        batch.status = constants.BatchStatus.CANCELLED.value
        batch.completed_ts = datetime.now(UTC)
        self.db.commit()
        
        return True
    
    def get_batch_history(self, batch_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        """Get the history of batch status changes."""
        from src.db.models import BatchStatus
        
        history = self.db.query(BatchStatus)\
            .filter(BatchStatus.batch_id == batch_id)\
            .order_by(BatchStatus.updated_at.desc())\
            .limit(limit)\
            .all()
        
        return [
            {
                "id": record.id,
                "status": record.status,
                "updated_at": record.updated_at,
                "summary": record.batch_summary,
                "comments": record.comments
            }
            for record in history
        ]