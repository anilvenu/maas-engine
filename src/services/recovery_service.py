"""Recovery service for system resilience."""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, UTC

from src.db.session import get_db_session
from src.db.models import Job, SystemRecovery, Batch
import src.core.constants as constants
from src.tasks.recovery_tasks import (
    perform_recovery_check,
    startup_recovery,
    health_check_recovery,
    detect_orphan_jobs
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
            args=[constants.RecoveryType.MANUAL.value],
            queue='recovery'
        )
        
        return {
            "task_id": task.id,
            "status": "recovery_started",
            "timestamp": datetime.now(UTC).isoformat()
        }
    
    def trigger_health_check_recovery(self) -> Dict[str, Any]:
        """
        Trigger health check with automatic recovery if needed.
        
        Returns:
            Dict with task information
        """
        self.logger.info("Health check recovery triggered")
        task = health_check_recovery.apply_async(queue='recovery')
        
        return {
            "task_id": task.id,
            "status": "health_check_started",
            "timestamp": datetime.now(UTC).isoformat()
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
            
            # Extract detailed metrics from metadata
            metadata = recovery.recovery_metadata or {}
            
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
                "detailed_metrics": {
                    "pending_submitted": metadata.get("pending_submitted", 0),
                    "polling_resumed": metadata.get("polling_resumed", 0),
                    "resubmitted": metadata.get("resubmitted", 0),
                    "status_updated": metadata.get("status_updated", 0),
                    "batch_checked": metadata.get("batch_checked", 0),
                    "errors": metadata.get("errors", 0)
                },
                "job_details": {
                    "pending_jobs": metadata.get("details", {}).get("pending_jobs", []),
                    "active_jobs": metadata.get("details", {}).get("active_jobs", []),
                    "resubmitted_jobs": metadata.get("details", {}).get("resubmitted_jobs", [])
                } if metadata.get("details") else None
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
                
                metadata = recovery.recovery_metadata or {}
                
                history.append({
                    "recovery_id": recovery.id,
                    "type": recovery.recovery_type,
                    "started_at": recovery.started_at.isoformat(),
                    "completed_at": recovery.completed_at.isoformat() if recovery.completed_at else None,
                    "duration_seconds": duration,
                    "jobs_recovered": recovery.jobs_recovered,
                    "jobs_resubmitted": recovery.jobs_resubmitted,
                    "jobs_resumed_polling": recovery.jobs_resumed_polling,
                    "errors": metadata.get("errors", 0),
                    "success": metadata.get("errors", 0) == 0 if recovery.completed_at else None
                })
            
            return history
    
    def check_system_health(self) -> Dict[str, Any]:
        """
        Check overall system health and identify issues.
        
        Returns:
            Dict with health status and recommendations
        """
        with get_db_session() as db:
            now = datetime.now(UTC)
            health = {
                "status": "healthy",
                "timestamp": now.isoformat(),
                "metrics": {},
                "issues": [],
                "recommendations": []
            }
            
            # Check for stale jobs (not updated recently)
            stale_threshold = now - timedelta(minutes=30)
            stale_jobs = db.query(Job).filter(
                Job.status.in_(['submitted', 'queued', 'running']),
                Job.updated_ts < stale_threshold
            ).all()
            
            health["metrics"]["stale_jobs"] = len(stale_jobs)
            if len(stale_jobs) > 0:
                health["status"] = "degraded"
                health["issues"].append(f"{len(stale_jobs)} stale jobs detected")
                health["recommendations"].append("Run manual recovery to resume stale jobs")
                
                # Include job IDs for debugging
                health["stale_job_ids"] = [j.id for j in stale_jobs[:10]]  # First 10
            
            # Check for stuck batches
            stuck_threshold = now - timedelta(hours=1)
            stuck_batch = db.query(Batch).filter(
                Batch.status == constants.BatchStatus.RUNNING.value,
                Batch.updated_ts < stuck_threshold
            ).all()
            
            health["metrics"]["stuck_batches"] = len(stuck_batch)
            if len(stuck_batch) > 0:
                health["status"] = "degraded"
                health["issues"].append(f"{len(stuck_batch)} stuck batches detected")
                health["recommendations"].append("Check batch completion logic")
                health["stuck_batch_ids"] = [b.id for b in stuck_batch[:5]]
            
            # Check for high failure rate
            failure_window = now - timedelta(hours=1)
            recent_failures = db.query(Job).filter(
                Job.status == constants.JobStatus.FAILED.value,
                Job.completed_ts > failure_window
            ).count()
            
            total_recent = db.query(Job).filter(
                Job.completed_ts > failure_window
            ).count()
            
            failure_rate = (recent_failures / total_recent * 100) if total_recent > 0 else 0
            health["metrics"]["recent_failures"] = recent_failures
            health["metrics"]["failure_rate"] = round(failure_rate, 2)
            
            if failure_rate > 20 and recent_failures > 5:
                health["status"] = "degraded"
                health["issues"].append(f"High failure rate: {failure_rate:.1f}%")
                health["recommendations"].append("Check Moody's API connectivity")
            
            # Check for pending jobs that are old
            old_pending_threshold = now - timedelta(hours=2)
            old_pending = db.query(Job).filter(
                Job.status == constants.JobStatus.PENDING.value,
                Job.created_ts < old_pending_threshold
            ).count()
            
            health["metrics"]["old_pending_jobs"] = old_pending
            if old_pending > 0:
                health["status"] = "degraded"
                health["issues"].append(f"{old_pending} old pending jobs")
                health["recommendations"].append("Submit pending jobs or check submission logic")
            
            # Check last recovery
            last_recovery = db.query(SystemRecovery).order_by(
                SystemRecovery.started_at.desc()
            ).first()
            
            if last_recovery:
                time_since_recovery = (now - last_recovery.started_at).total_seconds() / 3600
                health["metrics"]["hours_since_recovery"] = round(time_since_recovery, 2)
                health["last_recovery"] = {
                    "timestamp": last_recovery.started_at.isoformat(),
                    "type": last_recovery.recovery_type,
                    "success": last_recovery.recovery_metadata.get("errors", 0) == 0 if last_recovery.recovery_metadata else None
                }
                
                if time_since_recovery > 24:
                    health["recommendations"].append("Consider running manual recovery (>24h since last)")
            else:
                health["last_recovery"] = None
                health["recommendations"].append("No recovery history found")
            
            # Check for jobs without workflow IDs
            jobs_no_workflow = db.query(Job).filter(
                Job.status.in_(['submitted', 'queued', 'running']),
                Job.workflow_id == None
            ).count()
            
            health["metrics"]["jobs_without_workflow"] = jobs_no_workflow
            if jobs_no_workflow > 0:
                health["status"] = "degraded"
                health["issues"].append(f"{jobs_no_workflow} active jobs without workflow IDs")
                health["recommendations"].append("Resubmit jobs without workflow IDs")
            
            # Overall status determination
            if not health["issues"]:
                health["summary"] = "System is healthy"
            elif len(health["issues"]) == 1:
                health["summary"] = f"System degraded: {health['issues'][0]}"
            else:
                health["summary"] = f"System degraded: {len(health['issues'])} issues detected"
            
            return health
    
    def get_recovery_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """
        Get recovery statistics for the specified time period.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            Dict with recovery statistics
        """
        with get_db_session() as db:
            cutoff = datetime.now(UTC) - timedelta(hours=hours)
            
            recoveries = db.query(SystemRecovery).filter(
                SystemRecovery.started_at > cutoff
            ).all()
            
            if not recoveries:
                return {
                    "period_hours": hours,
                    "recovery_count": 0,
                    "message": "No recoveries in this period"
                }
            
            stats = {
                "period_hours": hours,
                "recovery_count": len(recoveries),
                "total_jobs_recovered": sum(r.jobs_recovered or 0 for r in recoveries),
                "total_jobs_resubmitted": sum(r.jobs_resubmitted or 0 for r in recoveries),
                "total_jobs_resumed_polling": sum(r.jobs_resumed_polling or 0 for r in recoveries),
                "recovery_types": {},
                "average_duration_seconds": None,
                "success_rate": None
            }
            
            # Count by type
            for recovery in recoveries:
                recovery_type = recovery.recovery_type
                if recovery_type not in stats["recovery_types"]:
                    stats["recovery_types"][recovery_type] = 0
                stats["recovery_types"][recovery_type] += 1
            
            # Calculate average duration and success rate
            durations = []
            successful = 0
            for recovery in recoveries:
                if recovery.completed_at:
                    duration = (recovery.completed_at - recovery.started_at).total_seconds()
                    durations.append(duration)
                    
                    # Consider recovery successful if no errors
                    metadata = recovery.recovery_metadata or {}
                    if metadata.get("errors", 0) == 0:
                        successful += 1
            
            if durations:
                stats["average_duration_seconds"] = round(sum(durations) / len(durations), 2)
                stats["success_rate"] = round((successful / len(recoveries)) * 100, 2)
            
            return stats