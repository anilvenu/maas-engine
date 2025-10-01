"""Repository for Batch operations."""

from typing import List, Optional, Dict, Any
from datetime import datetime, UTC
from sqlalchemy.orm import Session
from sqlalchemy import and_

from src.db.models import Batch, Configuration, Job
from src.db.repositories.base_repository import BaseRepository
from src.core.constants import BatchStatus, JobStatus


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
        """Get batch summary with job statistics."""
        batch = self.get(batch_id)
        if not batch:
            return None
        
        jobs = self.db.query(Job).filter(Job.batch_id == batch_id).all()
        
        status_counts = {}
        for job in jobs:
            status_counts[job.status] = status_counts.get(job.status, 0) + 1
        
        progress_percentage = 0.0
        if jobs:
            completed_jobs = status_counts.get('completed', 0)
            cancelled_jobs = status_counts.get('cancelled', 0)

            # Consider both completed and cancelled jobs as progress
            progress_percentage = (completed_jobs + cancelled_jobs) / len(jobs) * 100

        return {
            "id": batch.id,
            "name": batch.name,
            "status": batch.status,
            "total_jobs": len(jobs),
            "job_status_counts": status_counts,
            "created_ts": batch.created_ts,
            "updated_ts": batch.updated_ts,
            "completed_ts": batch.completed_ts,
            "progress_percentage": progress_percentage, 
        }
    
    def create_batch(self, name: str, yaml_config: Dict[str, Any], 
                        description: str = None) -> Batch:
        """Create batch from YAML configuration."""
        batch = self.create(
            name=name,
            description=description,
            yaml_config=yaml_config,
            status=BatchStatus.PENDING.value
        )
        return batch
    
    def update_status(self, batch_id: int, status: str) -> Optional[Batch]:
        """Update batch status."""
        updates = {"status": status}
        if status in [BatchStatus.COMPLETED.value, BatchStatus.FAILED.value, 
                     BatchStatus.CANCELLED.value]:
            updates["completed_ts"] = datetime.now(UTC)
        else:
            updates["completed_ts"] = None
        
        return self.update(batch_id, **updates)
    
    def cancel_batch(self, batch_id: int) -> bool:
        """Cancel an batch and all its jobs."""
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
                {"status": JobStatus.CANCELLED.value, "completed_ts": datetime.now(UTC)},
                synchronize_session=False
            )
        
        # Update batch status
        batch.status = BatchStatus.CANCELLED.value
        batch.completed_ts = datetime.now(UTC)
        self.db.commit()
        
        return True