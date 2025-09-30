"""Analysis management endpoints."""

from fastapi import APIRouter, HTTPException, Depends, status
from typing import List, Optional
from sqlalchemy.orm import Session

from src.api.dependencies.database import get_db
from src.api.dependencies.auth import verify_api_key

from src.api.models.schemas import (
    AnalysisCreate, AnalysisUpdate, AnalysisResponse, 
    AnalysisSummaryResponse, SubmissionResponse, YAMLUpload
)
from src.db.repositories.analysis_repository import AnalysisRepository
from src.services.orchestrator import Orchestrator
from src.initiator import YAMLProcessor
from src.core.exceptions import AnalysisNotFoundException

# This router handles all analysis-related endpoints
router = APIRouter(prefix="/api/analysis", tags=["Analysis"])

# List all analysis with optional filtering by status
@router.get("/", response_model=List[AnalysisResponse])
def list_analysis(
    skip: int = 0,
    limit: int = 100,
    status: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """List all analysis with optional filtering."""
    repo = AnalysisRepository(db)
    
    if status:
        # Get analysis by status, ordered by ID ascending so that skip/limit works as expected
        analysis = db.query(repo.model).filter(repo.model.status == status).order_by(repo.model.id.asc()).offset(skip).limit(limit).all()

    else:
        # Get all analysis with pagination
         analysis = db.query(repo.model).order_by(repo.model.id.asc()).offset(skip).limit(limit).all()
            
    return analysis

# Create a new analysis
@router.post("/from-yaml", 
             response_model=AnalysisSummaryResponse,
             dependencies=[Depends(verify_api_key)])
def create_from_yaml(
    yaml_data: YAMLUpload,
    db: Session = Depends(get_db)
):
    """Create analysis from YAML configuration."""
    processor = YAMLProcessor()
    # Print the YAML content for debugging
    try:
        result = processor.process_yaml_data(
            yaml_data.yaml_content,
            auto_submit=yaml_data.auto_submit
        )

        # Get the created analysis
        repo = AnalysisRepository(db)
        print(f"Fetching analysis with ID: {result['analysis_id']}")  # Debugging line
        print(f"Analysis summary: {repo.get_analysis_summary(result['analysis_id'])}")  # Debugging line
        return repo.get_analysis_summary(result["analysis_id"])
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/{analysis_id}", response_model=AnalysisSummaryResponse)
def get_analysis(
    analysis_id: int,
    db: Session = Depends(get_db)
):
    """Get analysis details with summary."""
    repo = AnalysisRepository(db)
    summary = repo.get_analysis_summary(analysis_id)
    
    if not summary:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Analysis {analysis_id} not found"
        )
    
    return summary


@router.put("/{analysis_id}", 
            response_model=AnalysisResponse,
            dependencies=[Depends(verify_api_key)])
def update_analysis(
    analysis_id: int,
    analysis_update: AnalysisUpdate,
    db: Session = Depends(get_db)
):
    """Update analysis metadata."""
    repo = AnalysisRepository(db)
    
    updated = repo.update(
        analysis_id,
        **analysis_update.dict(exclude_unset=True)
    )
    
    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Analysis {analysis_id} not found"
        )
    
    return updated


@router.delete("/{analysis_id}", 
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(verify_api_key)])
def delete_analysis(
    analysis_id: int,
    db: Session = Depends(get_db)
):
    """Delete an analysis and all related data."""
    repo = AnalysisRepository(db)
    
    if not repo.delete(analysis_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Analysis {analysis_id} not found"
        )


@router.post("/{analysis_id}/submit", 
             response_model=SubmissionResponse,
             dependencies=[Depends(verify_api_key)])
def submit_analysis_jobs(
    analysis_id: int,
    db: Session = Depends(get_db)
):
    """Submit all jobs for an analysis."""
    orchestrator = Orchestrator()
    
    try:
        result = orchestrator.submit_analysis_jobs(analysis_id)
        return result
    except AnalysisNotFoundException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.post("/{analysis_id}/cancel",
             dependencies=[Depends(verify_api_key)])
def cancel_analysis(
    analysis_id: int,
    db: Session = Depends(get_db)
):
    """Cancel an analysis and all its jobs."""
    orchestrator = Orchestrator()
    
    try:
        result = orchestrator.cancel_analysis(analysis_id)
        return result
    except AnalysisNotFoundException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.get("/{analysis_id}/progress")
def get_analysis_progress(
    analysis_id: int,
    db: Session = Depends(get_db)
):
    """Get detailed progress of an analysis."""
    orchestrator = Orchestrator()
    
    try:
        progress = orchestrator.get_analysis_progress(analysis_id)
        return progress
    except AnalysisNotFoundException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )