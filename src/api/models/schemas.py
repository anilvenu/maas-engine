"""Pydantic models for API request/response schemas."""

from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum


# Enums
class JobStatusEnum(str, Enum):
    pending = "pending"
    submitted = "submitted"
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


class BatchStatusEnum(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


# Request Models
class ConfigurationCreate(BaseModel):
    """Request model for creating configuration."""
    config_name: str = Field(..., min_length=1, max_length=255)
    config_data: Dict[str, Any]


class ConfigurationUpdate(BaseModel):
    """Request model for updating configuration."""
    config_data: Dict[str, Any]


class JobCreate(BaseModel):
    """Request model for creating job."""
    configuration_id: int


class JobResubmit(BaseModel):
    """Request model for resubmiting job."""
    config_override: Optional[Dict[str, Any]] = None


class YAMLUpload(BaseModel):
    """Request model for YAML upload."""
    yaml_content: Dict[str, Any]
    auto_submit: bool = True


# Response Models <TODO>
class JobStatusInfo(BaseModel):
    """Job status information."""
    job_id: str
    status: str
    progress_percentage: Optional[int] = None
    last_polled: Optional[datetime] = None


class JobResponse(BaseModel):
    """Response model for job."""
    id: int
    batch_id: int
    configuration_id: int
    workflow_id: Optional[str] = None
    status: JobStatusEnum
    resubmit_count: int
    last_error: Optional[str] = None
    created_ts: datetime
    initiation_ts: Optional[datetime] = None
    updated_ts: datetime
    completed_ts: Optional[datetime] = None
    age_minutes: Optional[int] = None
    time_in_current_status_minutes: Optional[int] = None
    
    class Config:
        orm_mode = True


class JobDetailResponse(JobResponse):
    """Detailed job response with metrics."""
    configuration_name: Optional[str] = None
    job_status: Optional[JobStatusInfo] = None
    metrics: Optional[Dict[str, Any]] = None
    poll_count: int = 0
    resubmit_history: Optional[List[Dict[str, Any]]] = None


class ConfigurationResponse(BaseModel):
    """Response model for configuration."""
    id: int
    batch_id: int
    config_name: str
    config_data: Dict[str, Any]
    is_active: bool
    version: int
    created_ts: datetime
    updated_ts: datetime
    
    class Config:
        orm_mode = True


class BatchResponse(BaseModel):
    """Response model for batch."""
    id: int
    name: str
    description: Optional[str] = None
    status: BatchStatusEnum
    yaml_config: Optional[Dict[str, Any]] = None
    created_ts: datetime
    updated_ts: datetime
    completed_ts: Optional[datetime] = None
    
    class Config:
        orm_mode = True


class BatchSummaryResponse(BatchResponse):
    """Batch summary with statistics."""
    total_jobs: int
    job_status_counts: Dict[str, int]
    progress_percentage: float
    configurations: Optional[List[ConfigurationResponse]] = None
    jobs: Optional[List[JobResponse]] = None


class SubmissionResponse(BaseModel):
    """Response for job submission."""
    submitted: int
    skipped: int
    errors: int
    job_ids: List[int]


class SystemHealthResponse(BaseModel):
    """System health status."""
    status: str
    database: bool
    redis: bool
    celery_worker: bool
    moodys_api: bool
    timestamp: datetime


class RecoveryStatusResponse(BaseModel):
    """Recovery status response."""
    stale_jobs: int
    recovered: int
    resubmitted: int
    resumed_polling: int
    errors: int
    recovery_id: Optional[int] = None