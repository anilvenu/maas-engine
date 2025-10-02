"""Celery tasks for batch management."""

import logging
from datetime import datetime, UTC
from typing import List, Dict, Any

from src.tasks.celery_app import celery
from src.db.session import get_db_session
from src.db.models import Batch, Job, BatchStatus, Configuration
import src.core.constants as constants

logger = logging.getLogger(__name__)


@celery.task(name='src.tasks.batch_tasks.check_batch_completion')
def check_batch_completion(batch_id: int) -> Dict[str, Any]:
    """
    Check if all jobs in a batch are complete.
    A batch is considered complete when:
    1. All jobs are in terminal state (completed, failed, or cancelled)
    2. Every non-skipped configuration has at least one completed job
    
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

        previous_status = batch.status
        
        # Get all jobs and configurations for this batch
        jobs = db.query(Job).filter(Job.batch_id == batch_id).all()
        configurations = db.query(Configuration).filter(
            Configuration.batch_id == batch_id
        ).all()
        
        # Build job status counts
        status_counts = {
            "total": len(jobs),
            "pending": sum(1 for job in jobs if job.status == constants.JobStatus.PENDING.value),
            "submitted": sum(1 for job in jobs if job.status == constants.JobStatus.SUBMITTED.value),
            "queued": sum(1 for job in jobs if job.status == constants.JobStatus.QUEUED.value),
            "running": sum(1 for job in jobs if job.status == constants.JobStatus.RUNNING.value),
            "completed": sum(1 for job in jobs if job.status == constants.JobStatus.COMPLETED.value),
            "failed": sum(1 for job in jobs if job.status == constants.JobStatus.FAILED.value),
            "cancelled": sum(1 for job in jobs if job.status == constants.JobStatus.CANCELLED.value),
            "active": sum(1 for job in jobs if job.status in [
                constants.JobStatus.PENDING.value, 
                constants.JobStatus.SUBMITTED.value, 
                constants.JobStatus.QUEUED.value, 
                constants.JobStatus.RUNNING.value
            ]),
            "terminal": sum(1 for job in jobs if job.status in [
                constants.JobStatus.COMPLETED.value, 
                constants.JobStatus.CANCELLED.value
            ])
        }
        
        # Check configuration completion
        config_status = {}
        configs_requiring_completion = []
        configs_completed = []
        configs_incomplete = []
        
        for config in configurations:
            config_key = f"{config.config_name}_v{config.version}"
            
            if config.skip:
                config_status[config_key] = "skipped"
                logger.info(f"Configuration {config_key} is skipped")
            else:
                # Check if this configuration has a completed job
                completed_jobs_for_config = [
                    job for job in jobs 
                    if job.configuration_id == config.id 
                    and job.status == constants.JobStatus.COMPLETED.value
                ]
                
                if completed_jobs_for_config:
                    config_status[config_key] = "completed"
                    configs_completed.append(config_key)
                    logger.info(f"Configuration {config_key} has {len(completed_jobs_for_config)} completed job(s)")
                else:
                    # Check if there are any active jobs for this config
                    active_jobs_for_config = [
                        job for job in jobs 
                        if job.configuration_id == config.id 
                        and job.status in constants.JobStatus.ACTIVE_STATUSES
                    ]
                    
                    if active_jobs_for_config:
                        config_status[config_key] = "in_progress"
                        configs_incomplete.append(config_key)
                        logger.info(f"Configuration {config_key} has {len(active_jobs_for_config)} active job(s)")
                    else:
                        # Check for failed/cancelled jobs
                        failed_jobs_for_config = [
                            job for job in jobs 
                            if job.configuration_id == config.id 
                            and job.status in [constants.JobStatus.FAILED.value, constants.JobStatus.CANCELLED.value]
                        ]
                        
                        if failed_jobs_for_config:
                            config_status[config_key] = "failed"
                            configs_incomplete.append(config_key)
                            logger.info(f"Configuration {config_key} has only failed/cancelled jobs")
                        else:
                            config_status[config_key] = "not_started"
                            configs_incomplete.append(config_key)
                            logger.info(f"Configuration {config_key} has no jobs or only pending jobs")
                
                if not config.skip:
                    configs_requiring_completion.append(config_key)
        
        logger.info(f"Batch {batch_id} job counts: {status_counts}")
        logger.info(f"Batch {batch_id} configuration status: {config_status}")
        logger.info(f"Configs requiring completion: {configs_requiring_completion}")
        logger.info(f"Configs completed: {configs_completed}")
        logger.info(f"Configs incomplete: {configs_incomplete}")
        
        # Determine new batch status
        new_status = None
        status_reason = ""
        
        # If batch is already in terminal state, don't change it
        if constants.BatchStatus.is_terminal(batch.status):
            new_status = batch.status
            status_reason = f"Batch already in terminal state: {batch.status}"
            logger.info(f"Batch {batch_id} already in terminal state: {batch.status}")
        
        # If there are no jobs at all
        elif not jobs:
            new_status = constants.BatchStatus.FAILED.value
            status_reason = "No jobs found in batch"
            logger.warning(f"Batch {batch_id} has no jobs")
        
        # If there are still active jobs, batch is running
        elif status_counts["active"] > 0:
            new_status = constants.BatchStatus.RUNNING.value
            status_reason = f"{status_counts['active']} jobs still active"
            logger.info(f"Batch {batch_id} still has {status_counts['active']} active jobs")
        
        # All jobs are terminal, check if all required configs are completed
        else:
            # Check if every non-skipped configuration has a completed job
            all_configs_satisfied = all(
                config_status.get(f"{config.config_name}_v{config.version}") in ["completed", "skipped"]
                for config in configurations
                if config.is_active  # Only check active configurations
            )
            
            if all_configs_satisfied:
                new_status = constants.BatchStatus.COMPLETED.value
                status_reason = f"All {len(configs_requiring_completion)} required configurations completed"
                logger.info(f"All required configurations for batch {batch_id} are completed")
            else:
                # Some configurations don't have completed jobs
                incomplete_count = len(configs_incomplete)
                new_status = constants.BatchStatus.FAILED.value
                status_reason = f"{incomplete_count} configuration(s) without completed jobs: {', '.join(configs_incomplete[:5])}"
                logger.info(f"Batch {batch_id} failed: {incomplete_count} configs incomplete")
        
        # Update batch status if changed
        status_changed = False
        if new_status != batch.status:
            batch.status = new_status
            if new_status in [constants.BatchStatus.COMPLETED.value, 
                             constants.BatchStatus.FAILED.value, 
                             constants.BatchStatus.CANCELLED.value]:
                batch.completed_ts = datetime.now(UTC)
            status_changed = True
            logger.info(f"Batch {batch_id} status changed from {previous_status} to {new_status}")
        
        # ALWAYS create a BatchStatus record for tracking
        batch_status_record = BatchStatus(
            batch_id=batch.id,
            status=new_status,
            batch_summary={
                "job_counts": status_counts,
                "configuration_status": config_status,
                "configs_completed": len(configs_completed),
                "configs_incomplete": len(configs_incomplete),
                "configs_skipped": sum(1 for v in config_status.values() if v == "skipped"),
                "previous_status": previous_status,
                "status_changed": status_changed,
                "reason": status_reason
            },
            comments=status_reason
        )
        
        db.add(batch_status_record)
        db.commit()
        
        logger.info(f"Logged BatchStatus record for batch {batch_id}: {new_status} (changed: {status_changed})")
        
        return {
            "batch_id": batch_id,
            "status": new_status,
            "previous_status": previous_status,
            "status_changed": status_changed,
            "reason": status_reason,
            "job_counts": status_counts,
            "configuration_status": config_status
        }


@celery.task(name='src.tasks.batch_tasks.check_all_batch')
def check_all_batch() -> Dict[str, Any]:
    """
    Periodic task to check all running batch.
    
    Returns:
        Dict with summary of checked batch
    """
    logger.info("=" * 80)
    logger.info("Starting check_all_batch task")
    logger.info("=" * 80)
    
    with get_db_session() as db:
        # Get all non-terminal batch
        running_batch = db.query(Batch).filter(
            Batch.status.in_([constants.BatchStatus.PENDING.value, constants.BatchStatus.RUNNING.value])
        ).all()
        
        logger.info(f"Found {len(running_batch)} active batch to check")
        
        results = {
            "checked": 0,
            "completed": 0,
            "failed": 0,
            "running": 0,
            "pending": 0,
            "cancelled": 0,
            "errors": 0,
            "batch_details": []
        }
        
        for batch in running_batch:
            try:
                logger.info(f"Checking batch {batch.id}: {batch.name}")
                result = check_batch_completion(batch.id)
                results["checked"] += 1
                
                status = result.get("status")
                results["batch_details"].append({
                    "batch_id": batch.id,
                    "batch_name": batch.name,
                    "status": status,
                    "status_changed": result.get("status_changed", False)
                })
                
                if status == constants.BatchStatus.COMPLETED.value:
                    results["completed"] += 1
                elif status == constants.BatchStatus.FAILED.value:
                    results["failed"] += 1
                elif status == constants.BatchStatus.RUNNING.value:
                    results["running"] += 1
                elif status == constants.BatchStatus.PENDING.value:
                    results["pending"] += 1
                elif status == constants.BatchStatus.CANCELLED.value:
                    results["cancelled"] += 1
                    
            except Exception as e:
                logger.error(f"Error checking batch {batch.id}: {e}")
                results["errors"] += 1
                results["batch_details"].append({
                    "batch_id": batch.id,
                    "batch_name": batch.name,
                    "error": str(e)
                })
        
        logger.info("=" * 80)
        logger.info(f"Batch check complete: {results}")
        logger.info(f"Checked: {results['checked']}, Completed: {results['completed']}, "
                   f"Failed: {results['failed']}, Running: {results['running']}, "
                   f"Errors: {results['errors']}")
        logger.info("=" * 80)
        
        return results


@celery.task(name='src.tasks.batch_tasks.startup_batch_check')
def startup_batch_check() -> Dict[str, Any]:
    """
    Special task to run on startup to check all batch.
    
    Returns:
        Dict with summary of checked batch
    """
    logger.info("*" * 80)
    logger.info("STARTUP: Running initial batch check")
    logger.info("*" * 80)
    
    # Small delay to ensure system is ready
    import time
    time.sleep(5)
    
    result = check_all_batch()
    
    logger.info("*" * 80)
    logger.info(f"STARTUP: Initial batch check completed: {result}")
    logger.info("*" * 80)
    
    return result