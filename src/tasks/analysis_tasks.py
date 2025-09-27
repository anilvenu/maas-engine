"""Celery tasks for analysis management."""

import logging
from datetime import datetime, UTC
from typing import List, Dict, Any

from src.tasks.celery_app import celery
from src.db.session import get_db_session
from src.db.models import Analysis, Job
from src.core.constants import AnalysisStatus, JobStatus

logger = logging.getLogger(__name__)


@celery.task(name='src.tasks.analysis_tasks.check_analysis_completion')
def check_analysis_completion(analysis_id: int) -> Dict[str, Any]:
    """
    Check if all jobs in an analysis are complete.
    
    Args:
        analysis_id: Database analysis ID
        
    Returns:
        Dict with analysis status
    """
    logger.info(f"Checking completion for analysis {analysis_id}")
    
    with get_db_session() as db:
        analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
        if not analysis:
            logger.error(f"Analysis {analysis_id} not found")
            return {"error": "Analysis not found"}
        
        # Skip if already terminal
        if AnalysisStatus.is_terminal(analysis.status):
            return {"status": analysis.status, "terminal": True}
        
        # Get all jobs for this analysis
        jobs = db.query(Job).filter(Job.analysis_id == analysis_id).all()
        
        if not jobs:
            logger.warning(f"Analysis {analysis_id} has no jobs")
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
        
        logger.info(f"Analysis {analysis_id} job counts: {status_counts}")
        
        # Determine analysis status
        if status_counts["active"] > 0:
            # Still have active jobs
            if analysis.status != AnalysisStatus.RUNNING.value:
                analysis.status = AnalysisStatus.RUNNING.value
                db.commit()
            return {"status": "running", "counts": status_counts}
        
        # All jobs are terminal
        if status_counts["completed"] == status_counts["total"]:
            # All completed successfully
            analysis.status = AnalysisStatus.COMPLETED.value
            analysis.completed_ts = datetime.now(UTC)
            db.commit()
            logger.info(f"Analysis {analysis_id} completed successfully")
            return {"status": "completed", "counts": status_counts}
        
        elif status_counts["failed"] > 0:
            # Some jobs failed
            analysis.status = AnalysisStatus.FAILED.value
            analysis.completed_ts = datetime.now(UTC)
            db.commit()
            logger.info(f"Analysis {analysis_id} failed with {status_counts['failed']} failed jobs")
            return {"status": "failed", "counts": status_counts}
        
        elif status_counts["cancelled"] == status_counts["total"]:
            # All jobs cancelled
            analysis.status = AnalysisStatus.CANCELLED.value
            analysis.completed_ts = datetime.now(UTC)
            db.commit()
            logger.info(f"Analysis {analysis_id} cancelled")
            return {"status": "cancelled", "counts": status_counts}
        
        else:
            # Mixed terminal states (some completed, some cancelled)
            analysis.status = AnalysisStatus.COMPLETED.value
            analysis.completed_ts = datetime.now(UTC)
            db.commit()
            logger.info(f"Analysis {analysis_id} completed with mixed results")
            return {"status": "completed_with_cancellations", "counts": status_counts}


@celery.task(name='src.tasks.analysis_tasks.check_all_analyses')
def check_all_analyses() -> Dict[str, Any]:
    """
    Periodic task to check all running analyses.
    
    Returns:
        Dict with summary of checked analyses
    """
    logger.info("Checking all running analyses")
    
    with get_db_session() as db:
        # Get all non-terminal analyses
        running_analyses = db.query(Analysis).filter(
            Analysis.status.in_([AnalysisStatus.PENDING.value, AnalysisStatus.RUNNING.value])
        ).all()
        
        results = {
            "checked": 0,
            "completed": 0,
            "failed": 0,
            "still_running": 0
        }
        
        for analysis in running_analyses:
            result = check_analysis_completion(analysis.id)
            results["checked"] += 1
            
            if result.get("status") == "completed":
                results["completed"] += 1
            elif result.get("status") == "failed":
                results["failed"] += 1
            elif result.get("status") == "running":
                results["still_running"] += 1
        
        logger.info(f"Analysis check complete: {results}")
        return results