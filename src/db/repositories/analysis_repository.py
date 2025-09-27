"""Repository for Analysis operations."""

from typing import List, Optional, Dict, Any
from datetime import datetime, UTC
from sqlalchemy.orm import Session
from sqlalchemy import and_

from src.db.models import Analysis, Configuration, Job
from src.db.repositories.base_repository import BaseRepository
from src.core.constants import AnalysisStatus, JobStatus


class AnalysisRepository(BaseRepository[Analysis]):
    """Repository for analysis-related database operations."""
    
    def __init__(self, db: Session):
        super().__init__(Analysis, db)
    
    def get_with_jobs(self, analysis_id: int) -> Optional[Analysis]:
        """Get analysis with all related jobs."""
        return self.db.query(Analysis)\
            .filter(Analysis.id == analysis_id)\
            .first()
    
    def get_active_analyses(self) -> List[Analysis]:
        """Get all non-terminal analyses."""
        return self.db.query(Analysis)\
            .filter(Analysis.status.in_(['pending', 'running']))\
            .all()
    
    def get_analysis_summary(self, analysis_id: int) -> Dict[str, Any]:
        """Get analysis summary with job statistics."""
        analysis = self.get(analysis_id)
        if not analysis:
            return None
        
        jobs = self.db.query(Job).filter(Job.analysis_id == analysis_id).all()
        
        status_counts = {}
        for job in jobs:
            status_counts[job.status] = status_counts.get(job.status, 0) + 1
        
        progress_percentage = 0.0
        if jobs:
            completed_jobs = status_counts.get('completed', 0)
            progress_percentage = (completed_jobs / len(jobs)) * 100

        return {
            "id": analysis.id,
            "name": analysis.name,
            "status": analysis.status,
            "total_jobs": len(jobs),
            "job_status_counts": status_counts,
            "created_ts": analysis.created_ts,
            "updated_ts": analysis.updated_ts,
            "completed_ts": analysis.completed_ts,
            "progress_percentage": progress_percentage, 
        }
    
    def create_from_yaml(self, name: str, yaml_config: Dict[str, Any], 
                        description: str = None) -> Analysis:
        """Create analysis from YAML configuration."""
        analysis = self.create(
            name=name,
            description=description,
            yaml_config=yaml_config,
            status=AnalysisStatus.PENDING.value
        )
        return analysis
    
    def update_status(self, analysis_id: int, status: str) -> Optional[Analysis]:
        """Update analysis status."""
        updates = {"status": status}
        if status in [AnalysisStatus.COMPLETED.value, AnalysisStatus.FAILED.value, 
                     AnalysisStatus.CANCELLED.value]:
            updates["completed_ts"] = datetime.now(UTC)
        
        return self.update(analysis_id, **updates)
    
    def cancel_analysis(self, analysis_id: int) -> bool:
        """Cancel an analysis and all its jobs."""
        analysis = self.get(analysis_id)
        if not analysis:
            return False
        
        # Update all non-terminal jobs to cancelled
        self.db.query(Job)\
            .filter(
                and_(
                    Job.analysis_id == analysis_id,
                    Job.status.notin_(['completed', 'failed', 'cancelled'])
                )
            )\
            .update(
                {"status": JobStatus.CANCELLED.value, "completed_ts": datetime.now(UTC)},
                synchronize_session=False
            )
        
        # Update analysis status
        analysis.status = AnalysisStatus.CANCELLED.value
        analysis.completed_ts = datetime.now(UTC)
        self.db.commit()
        
        return True