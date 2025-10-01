"""Celery tasks for batch management."""

import logging
from datetime import datetime, UTC
from typing import List, Dict, Any

from src.tasks.celery_app import celery
from src.db.session import get_db_session
from src.db.models import Batch, Job
from src.core.constants import BatchStatus, JobStatus

logger = logging.getLogger(__name__)


@celery.task(name='src.tasks.batch_tasks.check_batch_completion')
def check_batch_completion(batch_id: int) -> Dict[str, Any]:
    """
    Check if all jobs in an batch are complete.
    
    Args:
        batch_id: Database batch ID
        
    Returns:
        Dict with batch status
    """
    logger.info(f"Checking completion for batch {batch_id}")
    
    with get_db_session() as db:
        batch = db.query(Batch).filter(Batch.id == batch_id).first()
        if not batch:
            logger.error(f"Batch {batch_id} not found")
            return {"error": "Batch not found"}
        
        # Skip if already terminal
        if BatchStatus.is_terminal(batch.status):
            logger.info(f"Batch {batch_id} already in terminal state: {batch.status}")
            return {"status": batch.status, "terminal": True}
        
        # Get all jobs for this batch
        jobs = db.query(Job).filter(Job.batch_id == batch_id).all()
        
        if not jobs:
            logger.warning(f"Batch {batch_id} has no jobs")
            return {"status": "no_jobs"}
        
        # Count job statuses
        status_counts = {
            "total": len(jobs),
            "completed": 0,
            "failed": 0,
            "cancelled": 0,
            "active": 0
        }
        
        for job in jobs:
            if job.status == JobStatus.COMPLETED.value:
                status_counts["completed"] += 1
            elif job.status == JobStatus.FAILED.value:
                status_counts["failed"] += 1
            elif job.status == JobStatus.CANCELLED.value:
                status_counts["cancelled"] += 1
            elif JobStatus.is_active(job.status):
                status_counts["active"] += 1
        
        logger.info(f"Batch {batch_id} job counts: {status_counts}")
        
        # Determine batch status
        if status_counts["active"] > 0:
            # Still have active jobs
            if batch.status != BatchStatus.RUNNING.value:
                batch.status = BatchStatus.RUNNING.value
                db.commit()
            return {"status": "running", "counts": status_counts}
        
        # All jobs are terminal
        if status_counts["completed"] == status_counts["total"]:
            # All completed successfully --> set batch to completed
            batch.status = BatchStatus.COMPLETED.value
            batch.completed_ts = datetime.now(UTC)
            db.commit()
            logger.info(f"Batch {batch_id} completed successfully")
            return {"status": "completed", "counts": status_counts}
        
        elif status_counts["failed"] > 0:
            # Some jobs failed --> set batch to failed
            batch.status = BatchStatus.FAILED.value
            batch.completed_ts = datetime.now(UTC)
            db.commit()
            logger.info(f"Batch {batch_id} failed with {status_counts['failed']} failed jobs")
            return {"status": "failed", "counts": status_counts}
        
        elif status_counts["cancelled"] == status_counts["total"]:
            # All jobs cancelled --> set batch to cancelled
            batch.status = BatchStatus.CANCELLED.value
            batch.completed_ts = datetime.now(UTC)
            db.commit()
            logger.info(f"Batch {batch_id} cancelled")
            return {"status": "cancelled", "counts": status_counts}
        
        else:
            # Mixed terminal states (some completed, some cancelled) --> set to completed
            batch.status = BatchStatus.COMPLETED.value
            batch.completed_ts = datetime.now(UTC)
            db.commit()
            logger.info(f"Batch {batch_id} completed with mixed results")
            return {"status": "completed_with_cancellations", "counts": status_counts}


@celery.task(name='src.tasks.batch_tasks.check_all_batch')
def check_all_batch() -> Dict[str, Any]:
    """
    Periodic task to check all running batch.
    
    Returns:
        Dict with summary of checked batch
    """
    logger.info("Checking all running batch")
    
    with get_db_session() as db:
        # Get all non-terminal batch
        running_batch = db.query(Batch).filter(
            Batch.status.in_([BatchStatus.PENDING.value, BatchStatus.RUNNING.value])
        ).all()
        
        results = {
            "checked": 0,
            "completed": 0,
            "failed": 0,
            "still_running": 0
        }
        
        for batch in running_batch:
            result = check_batch_completion(batch.id)
            results["checked"] += 1
            
            if result.get("status") == "completed":
                results["completed"] += 1
            elif result.get("status") == "failed":
                results["failed"] += 1
            elif result.get("status") == "running":
                results["still_running"] += 1
        
        logger.info(f"Batch check complete: {results}")
        return results