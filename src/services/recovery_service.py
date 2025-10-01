"""Recovery service for system resilience."""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from src.db.session import get_db_session
from src.db.models import Job, SystemRecovery, Analysis
from src.core.constants import JobStatus, AnalysisStatus, RecoveryType
from src.tasks.recovery_tasks import (
    perform_recovery_check,
    recover_single_job,
    startup_recovery
)

logger = logging.getLogger(__name__)


class RecoveryService:
    """Service for managing system recovery operations."""
    
    def __init__(self):
        """Initialize recovery service."""
        self.logger = logger
    
    def trigger_startup_recovery(self) -> str:
        """
        Trigger recovery on system startup.
        Returns task ID.
        """
        self.logger.info("Triggering startup recovery")
        task = startup_recovery.apply_async(queue='recovery')
        return task.id
    
    def trigger_manual_recovery(self) -> Dict[str, Any]:
        """
        Manually trigger recovery process.
        
        Returns:
            Dict with task information
        """
        self.logger.info("Manual recovery triggered")
        task = perform_recovery_check.apply_async(
            args=[RecoveryType.MANUAL.value],
            queue='recovery'
        )
        
        return {
            "task_id": task.id,
            "status": "recovery_started",
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def get_recovery_status(self, recovery_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Get status of recovery operations.
        
        Args:
            recovery_id: Specific recovery ID or None for latest
            
        Returns:
            Dict with recovery status
        """
        with get_db_session() as db:
            if recovery_id:
                recovery = db.query(SystemRecovery).filter(
                    SystemRecovery.id == recovery_id
                ).first()
            else:
                # Get latest recovery
                recovery = db.query(SystemRecovery).order_by(
                    SystemRecovery.started_at.desc()
                ).first()
            
            if not recovery:
                return {"status": "no_recovery_found"}
            
            status = "in_progress" if not recovery.completed_at else "completed"
            
            duration = None
            if recovery.completed_at:
                duration = (recovery.completed_at - recovery.started_at).total_seconds()
            
            return {
                "recovery_id": recovery.id,
                "type": recovery.recovery_type,
                "status": status,
                "started_at": recovery.started_at.isoformat(),
                "completed_at": recovery.completed_at.isoformat() if recovery.completed_at else None,
                "duration_seconds": duration,
                "jobs_recovered": recovery.jobs_recovered,
                "jobs_resubmitted": recovery.jobs_resubmitted,
                "jobs_resumed_polling": recovery.jobs_resumed_polling,
                "metadata": recovery.recovery_metadata
            }
    
    def get_recovery_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recovery operation history.
        
        Args:
            limit: Number of records to return
            
        Returns:
            List of recovery operations
        """
        with get_db_session() as db:
            recoveries = db.query(SystemRecovery).order_by(
                SystemRecovery.started_at.desc()
            ).limit(limit).all()
            
            history = []
            for recovery in recoveries:
                duration = None
                if recovery.completed_at:
                    duration = (recovery.completed_at - recovery.started_at).total_seconds()
                
                history.append({
                    "recovery_id": recovery.id,
                    "type": recovery.recovery_type,
                    "started_at": recovery.started_at.isoformat(),
                    "completed_at": recovery.completed_at.isoformat() if recovery.completed_at else None,
                    "duration_seconds": duration,
                    "jobs_recovered": recovery.jobs_recovered,
                    "jobs_resubmitted": recovery.jobs_resubmitted,
                    "jobs_resumed_polling": recovery.jobs_resumed_polling
                })
            
            return history
    
    def check_system_health(self) -> Dict[str, Any]:
        """
        Check overall system health and identify issues.
        
        Returns:
            Dict with health status and recommendations
        """
        with get_db_session() as db:
            # Check for stale jobs
            stale_threshold = datetime.utcnow() - timedelta(hours=1)
            stale_jobs = db.query(Job).filter(
                Job.status.in_(['submitted', 'queued', 'running']),
                Job.updated_ts < stale_threshold
            ).count()
            
            # Check for stuck analysis
            stuck_analysis = db.query(Analysis).filter(
                Analysis.status == AnalysisStatus.RUNNING.value,
                Analysis.updated_ts < stale_threshold
            ).count()
            
            # Check recent failures
            recent_failures = db.query(Job).filter(
                Job.status == JobStatus.FAILED.value,
                Job.completed_ts > datetime.utcnow() - timedelta(hours=1)
            ).count()
            
            # Get last recovery
            last_recovery = db.query(SystemRecovery).order_by(
                SystemRecovery.started_at.desc()
            ).first()
            
            health_status = "healthy"
            issues = []
            recommendations = []
            
            if stale_jobs > 0:
                health_status = "degraded"
                issues.append(f"{stale_jobs} stale jobs detected")
                recommendations.append("Run recovery to resume stale jobs")
            
            if stuck_analysis > 0:
                health_status = "degraded"
                issues.append(f"{stuck_analysis} stuck analysis detected")
                recommendations.append("Check analysis completion logic")
            
            if recent_failures > 10:
                health_status = "degraded"
                issues.append(f"{recent_failures} recent job failures")
                recommendations.append("Check Moody's API connectivity")
            
            return {
                "status": health_status,
                "stale_jobs": stale_jobs,
                "stuck_analysis": stuck_analysis,
                "recent_failures": recent_failures,
                "last_recovery": {
                    "timestamp": last_recovery.started_at.isoformat() if last_recovery else None,
                    "type": last_recovery.recovery_type if last_recovery else None
                },
                "issues": issues,
                "recommendations": recommendations
            }