"""Application constants and enums."""

from enum import Enum


class JobStatus(str, Enum):
    """Job status enumeration."""
    PLANNED = "planned"
    INITIATED = "initiated"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    
    @classmethod
    def is_terminal(cls, status: str) -> bool:
        """Check if status is terminal (no more processing needed)."""
        return status in [cls.COMPLETED.value, cls.FAILED.value, cls.CANCELLED.value]

    @classmethod
    def is_active(cls, status: str) -> bool:
        """Check if status indicates active processing."""
        return status in [cls.INITIATED.value, cls.QUEUED.value, cls.RUNNING.value]


class AnalysisStatus(str, Enum):
    """Analysis status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    
    @classmethod
    def is_terminal(cls, status: str) -> bool:
        """Check if status is terminal."""
        return status in [cls.COMPLETED.value, cls.FAILED.value, cls.CANCELLED.value]


class RecoveryType(str, Enum):
    """Recovery type enumeration."""
    STARTUP = "startup"
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    CRASH = "crash"


class TaskQueue(str, Enum):
    """Celery task queue names."""
    DEFAULT = "default"
    JOBS = "jobs"
    POLLING = "polling"
    RECOVERY = "recovery"


class HTTPStatusCode:
    """HTTP status codes for retry logic."""
    REQUEST_TIMEOUT = 408
    TOO_MANY_REQUESTS = 429
    INTERNAL_SERVER_ERROR = 500
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504
    
    # Retryable status codes
    RETRYABLE = [408, 429, 500, 503, 504]


class MoodyAPIEndpoints:
    """Mock Moody's API endpoints."""
    SUBMIT_WORKFLOW = "/workflows"
    WORKFLOW_STATUS = "/workflows/{workflow_id}/status"
    CANCEL_WORKFLOW = "/workflows/{workflow_id}/cancel"


# Error messages
class ErrorMessages:
    """Standard error messages."""
    JOB_NOT_FOUND = "Job with ID {job_id} not found"
    ANALYSIS_NOT_FOUND = "Analysis with ID {analysis_id} not found"
    WORKFLOW_SUBMISSION_FAILED = "Failed to submit workflow: {error}"
    POLL_FAILED = "Failed to poll workflow status: {error}"
    RECOVERY_FAILED = "Recovery process failed: {error}"
    INVALID_STATUS_TRANSITION = "Invalid status transition from {from_status} to {to_status}"
    MAX_RETRIES_EXCEEDED = "Maximum retry attempts ({max_retries}) exceeded"