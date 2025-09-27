"""Job management endpoints."""

from fastapi import APIRouter, HTTPException, Depends, Query, status
from typing import List, Optional
from sqlalchemy.orm import Session
from datetime import datetime, UTC

from src.api.dependencies.database import get_db
from src.api.models.schemas import (
    JobCreate, JobResponse, JobDetailResponse, JobRetry
)
from src.db.repositories.job_repository import JobRepository
from src.services.orchestrator import Orchestrator
from src.tasks.job_tasks import submit_job, poll_workflow_status, cancel_job
from src.core.exceptions import JobNotFoundException

router = APIRouter(prefix="/api/jobs", tags=["Jobs"])


@router.get("/", response_model=List[JobResponse])
def list_jobs(
    analysis_id: Optional[int] = Query(None, description="Filter by analysis ID"),
    status: Optional[str] = Query(None, description="Filter by status"),
    skip: int = 0,
    limit: int = 1000,
    db: Session = Depends(get_db)
):
    """List jobs with optional filtering."""
    repo = JobRepository(db)
    
    query = db.query(repo.model)
    
    print(analysis_id, status)
    print(f"   Query before filters: {query}")

    if analysis_id:
        query = query.filter(repo.model.analysis_id == analysis_id)
    if status:
        query = query.filter(repo.model.status == status)

    print(f"   Query after filters: {query}")

    jobs = query.offset(skip).limit(limit).all()
       
    return jobs


@router.post("/", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
def create_job(
    analysis_id: int,
    job: JobCreate,
    db: Session = Depends(get_db)
):
    """Create a new job for an analysis."""
    repo = JobRepository(db)
    
    db_job = repo.create_job(
        analysis_id=analysis_id,
        configuration_id=job.configuration_id
    )
    
    return db_job


@router.get("/{job_id}", response_model=JobDetailResponse)
def get_job(
    job_id: int,
    db: Session = Depends(get_db)
):
    """Get detailed job information."""
    repo = JobRepository(db)
    
    job = repo.get_with_details(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    # Build detailed response
    metrics = repo.get_job_metrics(job_id)
    
    response = JobDetailResponse(
        id=job.id,
        analysis_id=job.analysis_id,
        configuration_id=job.configuration_id,
        workflow_id=job.workflow_id,
        status=job.status,
        retry_count=job.retry_count,
        last_error=job.last_error,
        created_ts=job.created_ts,
        initiation_ts=job.initiation_ts,
        updated_ts=job.updated_ts,
        completed_ts=job.completed_ts,
        configuration_name=job.configuration.config_name if job.configuration else None,
        metrics=metrics,
        poll_count=len(job.workflow_statuses) if job.workflow_statuses else 0
    )
    
    return response


@router.post("/{job_id}/initiate")
def initiate_job(
    job_id: int,
    db: Session = Depends(get_db)
):
    """Submit a job to Moody's API."""
    repo = JobRepository(db)
    
    job = repo.get(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    if job.status != "planned":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job is already {job.status}"
        )
    
    # Submit via Celery
    task = submit_job.delay(job_id)
    
    return {
        "job_id": job_id,
        "task_id": task.id,
        "status": "submission_queued"
    }


@router.post("/{job_id}/cancel")
def cancel_job_endpoint(
    job_id: int,
    db: Session = Depends(get_db)
):
    """Cancel a running job."""
    repo = JobRepository(db)
    
    job = repo.get(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    if job.status in ["completed", "failed", "cancelled"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job is already {job.status}"
        )
    
    # Cancel via Celery
    task = cancel_job.delay(job_id)
    
    return {
        "job_id": job_id,
        "task_id": task.id,
        "status": "cancellation_requested"
    }


@router.post("/{job_id}/retry", response_model=JobResponse)
def retry_job(
    job_id: int,
    retry_config: JobRetry,
    db: Session = Depends(get_db)
):
    """Retry a failed job."""
    orchestrator = Orchestrator()
    
    try:
        result = orchestrator.retry_failed_job(
            job_id,
            retry_config.config_override
        )
        
        # Get the new job
        repo = JobRepository(db)
        new_job = repo.get(result["new_job_id"])
        
        return new_job
        
    except (JobNotFoundException, Exception) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/{job_id}/poll")
def force_poll_job(
    job_id: int,
    db: Session = Depends(get_db)
):
    """Force an immediate poll of job status."""
    repo = JobRepository(db)
    
    job = repo.get(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    if not job.workflow_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job has no workflow ID"
        )
    
    # Poll via Celery
    task = poll_workflow_status.delay(job_id, job.workflow_id)
    
    return {
        "job_id": job_id,
        "workflow_id": job.workflow_id,
        "task_id": task.id,
        "status": "poll_queued"
    }