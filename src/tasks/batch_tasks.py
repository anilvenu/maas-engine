"""Celery tasks for batch management."""

import logging
from datetime import datetime, UTC
from typing import List, Dict, Any

from src.tasks.celery_app import celery
from src.db.session import get_db_session
from src.db.models import Batch, Job, BatchStatus
import src.core.constants as constants

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
    logger.info("-----------------------------------------------------------------------------------")
    
    with get_db_session() as db:
        batch = db.query(Batch).filter(Batch.id == batch_id).first()
        if not batch:
            logger.error(f"Batch {batch_id} not found")
            return {"error": "Batch not found"}

        # Skip if already terminal
        if constants.BatchStatus.is_terminal(batch.status):
            logger.info(f"Batch {batch_id} already in terminal state: {batch.status}")

            batch_status = BatchStatus(
                batch_id=batch.id,
                status=batch.status,
                batch_summary=None,
                comments= f"Batch already in terminal state {batch.status}"
            )

            db.add(batch_status)
            db.commit()
            return {
                "batch_id": batch_id,
                "status": batch.status, 
                "previous_status": batch.status,
                "status_changed": False
            }
        else:
            # Get all jobs for this batch
            jobs = db.query(Job).filter(Job.batch_id == batch_id).all()
            
            if not jobs:
                logger.warning(f"Batch {batch_id} has no jobs")
                batch_status = BatchStatus(
                    batch_id=batch.id,
                    status=batch.status,
                    batch_summary=None,
                    comments= f"No jobs found in batch"
                )
                db.add(batch_status)
                db.commit()

                return {
                    "batch_id": batch_id,
                    "status": "ERROR: No jobs found", 
                    "previous_status": batch.status,
                    "status_changed": None
                }
            else:
                logger.info(f"Batch {batch_id} has {len(jobs)} jobs")            
                
                # Count job statuses using dictionary comprehension, dynamically handling all statuses without hardcoding
                status_counts = {
                    "total": len(jobs),
                    "pending": sum(1 for job in jobs if job.status == constants.JobStatus.PENDING.value),
                    "submitted": sum(1 for job in jobs if job.status == constants.JobStatus.SUBMITTED.value),
                    "queued": sum(1 for job in jobs if job.status == constants.JobStatus.QUEUED.value),
                    "running": sum(1 for job in jobs if job.status == constants.JobStatus.RUNNING.value),
                    "completed": sum(1 for job in jobs if job.status == constants.JobStatus.COMPLETED.value),
                    "failed": sum(1 for job in jobs if job.status == constants.JobStatus.FAILED.value),
                    "cancelled": sum(1 for job in jobs if job.status == constants.JobStatus.CANCELLED.value),
                    "active": sum(1 for job in jobs if job.status in constants.JobStatus.is_active()),
                    "terminal": sum(1 for job in jobs if job.status in constants.JobStatus.is_terminal())
                }
                
                logger.info(f"Batch {batch_id} job counts: {status_counts}")
                
                # Determine batch status
                if status_counts["active"] > 0:
                    logger.info(f"Batch {batch_id} still has active jobs")
                    batch.status = constants.BatchStatus.RUNNING.value
                    db.commit()

                elif (status_counts["completed"] + status_counts["cancelled"]) == status_counts["total"]:
                    logger.info(f"All jobs in batch {batch_id} are completed or cancelled")
                    batch.status = constants.BatchStatus.COMPLETED.value
                    batch.completed_ts = datetime.now(UTC)                
                    db.commit()

                elif status_counts["failed"] > 0:
                    logger.info(f"Some jobs in batch {batch_id} have failed")
                    batch.status = constants.BatchStatus.FAILED.value        
                    db.commit()    

                elif status_counts["cancelled"] == status_counts["total"]:
                    logger.info(f"All jobs in batch {batch_id} are cancelled")
                    batch.status = constants.BatchStatus.CANCELLED.value
                    batch.completed_ts = datetime.now(UTC)                
                    db.commit()

                else:
                    logging.error(f"Unhandled batch status case for batch {batch_id}. Status counts: {status_counts}")
                    raise Exception(f"Unhandled batch status case.")


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
            Batch.status.in_([constants.BatchStatus.PENDING.value, constants.BatchStatus.RUNNING.value])
        ).all()
        
        results = {
            "checked": 0,
            "completed": 0,
            "failed": 0,
            "active": 0
        }
        
        for batch in running_batch:
            result = check_batch_completion(batch.id)
            results["checked"] += 1
            
            if result.get("status") == "completed":
                results["completed"] += 1
            elif result.get("status") == "failed":
                results["failed"] += 1
            elif result.get("status") == "running":
                results["running"] += 1
            elif result.get("status") == "pending":
                results["pending"] += 1
            elif result.get("status") == "cancelled":
                results["cancelled"] += 1
        
        logger.info(f"Batch check complete: {results}")
        return results