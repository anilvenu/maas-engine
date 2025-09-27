"""SQLAlchemy database models."""

from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime, 
    ForeignKey, JSON, Enum, UniqueConstraint, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime, UTC

from src.core.constants import JobStatus, AnalysisStatus, RecoveryType

Base = declarative_base()


class Analysis(Base):
    """Analysis model."""
    __tablename__ = "irp_analysis"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    status = Column(Enum(AnalysisStatus, values_callable=lambda x: [e.value for e in x]), default=AnalysisStatus.PENDING.value)
    yaml_config = Column(JSON)
    created_ts = Column(DateTime(timezone=True), server_default=func.now())
    updated_ts = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    completed_ts = Column(DateTime(timezone=True))
    
    # Relationships
    configurations = relationship("Configuration", back_populates="analysis", cascade="all, delete-orphan")
    jobs = relationship("Job", back_populates="analysis", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Analysis(id={self.id}, name={self.name}, status={self.status})>"


class Configuration(Base):
    """Configuration model."""
    __tablename__ = "irp_configuration"
    
    id = Column(Integer, primary_key=True)
    analysis_id = Column(Integer, ForeignKey("irp_analysis.id", ondelete="CASCADE"), nullable=False)
    config_name = Column(String(255), nullable=False)
    config_data = Column(JSON, nullable=False)
    is_active = Column(Boolean, default=True)
    created_ts = Column(DateTime(timezone=True), server_default=func.now())
    updated_ts = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    version = Column(Integer, default=1)
    
    # Relationships
    analysis = relationship("Analysis", back_populates="configurations")
    jobs = relationship("Job", back_populates="configuration")
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('analysis_id', 'config_name', 'version', name='unique_active_config_per_analysis'),
    )
    
    def __repr__(self):
        return f"<Configuration(id={self.id}, name={self.config_name}, version={self.version})>"


class Job(Base):
    """Job model."""
    __tablename__ = "irp_job"
    
    id = Column(Integer, primary_key=True)
    analysis_id = Column(Integer, ForeignKey("irp_analysis.id", ondelete="CASCADE"), nullable=False)
    configuration_id = Column(Integer, ForeignKey("irp_configuration.id"), nullable=False)
    workflow_id = Column(String(255), unique=True)
    status = Column(Enum(JobStatus, values_callable=lambda x: [e.value for e in x]), default=JobStatus.PLANNED.value)
    retry_count = Column(Integer, default=0)
    last_error = Column(Text)
    created_ts = Column(DateTime(timezone=True), server_default=func.now())
    initiation_ts = Column(DateTime(timezone=True))
    updated_ts = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    completed_ts = Column(DateTime(timezone=True))
    parent_job_id = Column(Integer, ForeignKey("irp_job.id"))
    celery_task_id = Column(String(255))
    last_poll_ts = Column(DateTime(timezone=True))
    
    # Relationships
    analysis = relationship("Analysis", back_populates="jobs")
    configuration = relationship("Configuration", back_populates="jobs")
    parent_job = relationship("Job", remote_side=[id])
    workflow_statuses = relationship("WorkflowStatus", back_populates="job", cascade="all, delete-orphan")
    retry_history = relationship("RetryHistory", back_populates="job", cascade="all, delete-orphan")
    
    # Indexes
    __table_args__ = (
        Index('idx_job_status', 'status'),
        Index('idx_job_analysis_id', 'analysis_id'),
        Index('idx_job_workflow_id', 'workflow_id'),
        Index('idx_job_updated_ts', 'updated_ts'),
    )
    
    def __repr__(self):
        return f"<Job(id={self.id}, workflow_id={self.workflow_id}, status={self.status})>"
    
    @property
    def age_minutes(self) -> int:
        """Get age of job in minutes since creation."""
        if self.created_ts:
            delta = datetime.now(UTC) - self.created_ts
            return int(delta.total_seconds() / 60)
        return 0
    
    @property
    def time_in_current_status_minutes(self) -> int:
        """Get time in current status in minutes."""
        if self.updated_ts:
            delta = datetime.now(UTC) - self.updated_ts
            return int(delta.total_seconds() / 60)
        return 0


class WorkflowStatus(Base):
    """Workflow status tracking model."""
    __tablename__ = "irp_workflow_status"
    
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("irp_job.id", ondelete="CASCADE"), nullable=False)
    status = Column(String(50), nullable=False)
    response_data = Column(JSON)
    http_status_code = Column(Integer)
    polled_at = Column(DateTime(timezone=True), server_default=func.now())
    poll_duration_ms = Column(Integer)
    
    # Relationships
    job = relationship("Job", back_populates="workflow_statuses")
    
    # Indexes
    __table_args__ = (
        Index('idx_workflow_status_job_id', 'job_id'),
        Index('idx_workflow_status_polled_at', 'polled_at'),
    )
    
    def __repr__(self):
        return f"<WorkflowStatus(id={self.id}, job_id={self.job_id}, status={self.status})>"


class RetryHistory(Base):
    """Retry history model."""
    __tablename__ = "irp_retry_history"
    
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("irp_job.id", ondelete="CASCADE"), nullable=False)
    retry_attempt = Column(Integer, nullable=False)
    error_code = Column(String(20))
    error_message = Column(Text)
    retry_after_seconds = Column(Integer)
    attempted_at = Column(DateTime(timezone=True), server_default=func.now())
    succeeded = Column(Boolean, default=False)
    
    # Relationships
    job = relationship("Job", back_populates="retry_history")
    
    # Indexes
    __table_args__ = (
        Index('idx_retry_history_job_id', 'job_id'),
    )
    
    def __repr__(self):
        return f"<RetryHistory(id={self.id}, job_id={self.job_id}, attempt={self.retry_attempt})>"


class SystemRecovery(Base):
    """System recovery tracking model."""
    __tablename__ = "irp_system_recovery"
    
    id = Column(Integer, primary_key=True)
    recovery_type = Column(Enum(RecoveryType), nullable=False)
    jobs_recovered = Column(Integer, default=0)
    jobs_resubmitted = Column(Integer, default=0)
    jobs_resumed_polling = Column(Integer, default=0)
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True))
    recovery_metadata = Column(JSON)
    
    def __repr__(self):
        return f"<SystemRecovery(id={self.id}, type={self.recovery_type}, recovered={self.jobs_recovered})>"


class CeleryTaskState(Base):
    """Celery task state for recovery."""
    __tablename__ = "irp_celery_task_state"
    
    id = Column(Integer, primary_key=True)
    task_id = Column(String(255), unique=True, nullable=False)
    task_name = Column(String(255), nullable=False)
    job_id = Column(Integer, ForeignKey("irp_job.id"))
    task_args = Column(JSON)
    task_kwargs = Column(JSON)
    eta = Column(DateTime(timezone=True))
    expires = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Indexes
    __table_args__ = (
        Index('idx_celery_task_job_id', 'job_id'),
    )
    
    def __repr__(self):
        return f"<CeleryTaskState(id={self.id}, task_id={self.task_id}, job_id={self.job_id})>"