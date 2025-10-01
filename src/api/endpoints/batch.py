"""Batch management endpoints."""

import logging
from fastapi import APIRouter, HTTPException, Depends, status
from typing import List, Optional
from sqlalchemy.orm import Session

from src.api.dependencies.database import get_db
from src.api.dependencies.auth import verify_api_key

from src.api.models.schemas import (
    BatchResponse, BatchSummaryResponse, SubmissionResponse, YAMLUpload
)
from src.db.repositories.batch_repository import BatchRepository
from src.services.orchestrator import Orchestrator
from src.initiator import YAMLProcessor
from src.core.exceptions import BatchNotFoundException

logger = logging.getLogger(__name__)

# This router handles all batch-related endpoints
router = APIRouter(prefix="/api/batch", tags=["Batch"])

# List all batch with optional filtering by status
@router.get("/", 
            response_model=List[BatchResponse])
def list_batch(
    skip: int = 0,
    limit: int = 100,
    status: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """List all batch with optional filtering."""
    repo = BatchRepository(db)
    
    if status:
        # Get batch by status, ordered by ID ascending so that skip/limit works as expected
        batch = db.query(repo.model).filter(repo.model.status == status).order_by(repo.model.id.asc()).offset(skip).limit(limit).all()

    else:
        # Get all batch with pagination
         batch = db.query(repo.model).order_by(repo.model.id.asc()).offset(skip).limit(limit).all()
            
    return batch

# Create a new batch
@router.post("/", 
             response_model=BatchSummaryResponse,
             dependencies=[Depends(verify_api_key)])
def create_batch(
    yaml_data: YAMLUpload,
    db: Session = Depends(get_db)
):
    """Create batch from YAML configuration."""
    processor = YAMLProcessor()

    # Print the YAML content for debugging
    logger.debug(f"Received YAML content: {yaml_data.yaml_content}")
    
    try:
        result = processor.process_yaml_data(
            yaml_data.yaml_content,
            auto_submit=yaml_data.auto_submit
        )

        # Get the created batch
        repo = BatchRepository(db)
        print(f"Fetching batch with ID: {result['batch_id']}")  # Debugging line
        print(f"Batch summary: {repo.get_batch_summary(result['batch_id'])}")  # Debugging line
        return repo.get_batch_summary(result["batch_id"])
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/{batch_id}", response_model=BatchSummaryResponse)
def get_batch(
    batch_id: int,
    db: Session = Depends(get_db)
):
    """Get batch details with summary."""
    repo = BatchRepository(db)
    summary = repo.get_batch_summary(batch_id)
    
    if not summary:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Batch {batch_id} not found"
        )
    
    return summary


@router.delete("/{batch_id}", 
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(verify_api_key)])
def delete_batch(
    batch_id: int,
    db: Session = Depends(get_db)
):
    """Delete an batch and all related data."""
    repo = BatchRepository(db)
    
    if not repo.delete(batch_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Batch {batch_id} not found"
        )


@router.post("/{batch_id}/submit", 
             response_model=SubmissionResponse,
             dependencies=[Depends(verify_api_key)])
def submit_batch_jobs(
    batch_id: int,
    db: Session = Depends(get_db)
):
    """Submit all jobs for an batch."""
    orchestrator = Orchestrator()
    
    try:
        result = orchestrator.submit_batch_jobs(batch_id)
        return result
    except BatchNotFoundException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.post("/{batch_id}/cancel",
             dependencies=[Depends(verify_api_key)])
def cancel_batch(
    batch_id: int,
    db: Session = Depends(get_db)
):
    """Cancel an batch and all its jobs."""
    orchestrator = Orchestrator()
    
    try:
        result = orchestrator.cancel_batch(batch_id)
        return result
    except BatchNotFoundException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.get("/{batch_id}/progress")
def get_batch_progress(
    batch_id: int,
    db: Session = Depends(get_db)
):
    """Get detailed progress of an batch."""
    orchestrator = Orchestrator()
    
    try:
        progress = orchestrator.get_batch_progress(batch_id)
        return progress
    except BatchNotFoundException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )