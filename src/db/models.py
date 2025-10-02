"""SQLAlchemy database models."""

from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime, 
    ForeignKey, JSON, Enum, UniqueConstraint, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime, UTC
import enum
import src.core.constants as constants

Base = declarative_base()


class Batch(Base):
    """Batch model."""
    __tablename__ = "irp_batch"

    ALL_STATUSES = ["pending", "running", "completed", "failed", "cancelled"]
    ACTIVE_STATUSES = ["pending", "running"]
    TERMINAL_STATUSES = ["completed", "cancelled"]
    FAILED_STATUSES = ["failed"]

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)

    status = Column(Enum(*ALL_STATUSES),default="pending", nullable=False)

    yaml_config = Column(JSON)
    created_ts = Column(DateTime(timezone=True), server_default=func.now())
    updated_ts = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    completed_ts = Column(DateTime(timezone=True))
    
    # Relationships
    configurations = relationship("Configuration", back_populates="batch", cascade="all, delete-orphan")
    jobs = relationship("Job", back_populates="batch", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Batch(id={self.id}, name={self.name}, status={self.status})>"


class Configuration(Base):
    """Configuration model."""
    __tablename__ = "irp_configuration"
    
    id = Column(Integer, primary_key=True)
    batch_id = Column(Integer, ForeignKey("irp_batch.id", ondelete="CASCADE"), nullable=False)
    config_name = Column(String(255), nullable=False)
    config_data = Column(JSON, nullable=False)
    is_active = Column(Boolean, default=True)
    skip = Column(Boolean, default=False, nullable=False)
    created_ts = Column(DateTime(timezone=True), server_default=func.now())
    updated_ts = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    version = Column(Integer, default=1, nullable=False)
    
    # Relationships
    batch = relationship("Batch", back_populates="configurations")
    jobs = relationship("Job", back_populates="configuration")
    
    # Constraints - unique batch_id, config_name, version
    __table_args__ = (
        UniqueConstraint('batch_id', 'config_name', 'version', 
                        name='uq_batch_config_version'),
        Index('idx_configuration_active', 'batch_id', 'config_name', 'is_active'),
    )
    
    def __repr__(self):
        return f"<Configuration(id={self.id}, name={self.config_name}, version={self.version}, skip={self.skip})>"


class Job(Base):
    """Job model."""
    __tablename__ = "irp_job"

    ALL_STATUSES = ["pending", "submitted", "queued", "running", "completed", "failed", "cancelled"]
    INITIAL_STATUSES = ["pending"]
    ACTIVE_STATUSES = ["pending", "submitted", "queued", "running"]
    FAILED_STATUSES = ["failed"]
    TERMINAL_STATUSES = ["completed", "cancelled"]
    RESUBMITTABLE_STATUSES = ["failed", "cancelled"]

    id = Column(Integer, primary_key=True)
    batch_id = Column(Integer, 
                      ForeignKey("irp_batch.id", ondelete="CASCADE"), 
                      nullable=False)
    configuration_id = Column(Integer, 
                              ForeignKey("irp_configuration.id"), 
                              nullable=False)
    workflow_id = Column(String(255), 
                         unique=True)
    status = Column(Enum(*ALL_STATUSES, name="job_status"), 
                         default="pending", 
                         nullable=False)
    retry_count = Column(Integer, default=0)
    last_error = Column(Text)
    created_ts = Column(DateTime(timezone=True), 
                        server_default=func.now())
    initiation_ts = Column(DateTime(timezone=True))
    updated_ts = Column(DateTime(timezone=True), 
                        server_default=func.now(), 
                        onupdate=func.now())
    completed_ts = Column(DateTime(timezone=True))
    parent_job_id = Column(Integer, ForeignKey("irp_job.id"))
    celery_task_id = Column(String(255))
    last_poll_ts = Column(DateTime(timezone=True))
    
    # Relationships
    batch = relationship("Batch", back_populates="jobs")
    configuration = relationship("Configuration", back_populates="jobs")
    parent_job = relationship("Job", remote_side=[id])
    job_statuses = relationship("JobStatus", back_populates="job", cascade="all, delete-orphan")
    retry_history = relationship("RetryHistory", back_populates="job", cascade="all, delete-orphan")
    
    # Indexes
    __table_args__ = (
        Index('idx_job_status', 'status'),
        Index('idx_job_batch_id', 'batch_id'),
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


class BatchStatus(Base):
    """Batch status tracking model."""
    __tablename__ = "irp_batch_status"
    
    id = Column(Integer, primary_key=True)
    batch_id = Column(Integer, ForeignKey("irp_batch.id", ondelete="CASCADE"), nullable=False)
    status = Column(String(50), nullable=False)
    batch_summary = Column(JSON)
    updated_at = Column(DateTime(timezone=True), server_default=func.now())
    comments = Column(String(255))
    
    # Relationships
    batch = relationship("Batch")
    
    # Indexes
    __table_args__ = (
        Index('idx_batch_status_batch_id', 'batch_id'),
        Index('idx_batch_status_updated_at', 'updated_at'),
    )
    
    def __repr__(self):
        return f"<BatchStatus(id={self.id}, batch_id={self.batch_id}, status={self.status})>"


class JobStatus(Base):
    """Job status tracking model."""
    __tablename__ = "irp_job_status"
    
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("irp_job.id", ondelete="CASCADE"), nullable=False)
    status = Column(String(50), nullable=False)
    response_data = Column(JSON)
    http_status_code = Column(Integer)
    polled_at = Column(DateTime(timezone=True), server_default=func.now())
    poll_duration_ms = Column(Integer)
    
    # Relationships
    job = relationship("Job", back_populates="job_statuses")
    
    # Indexes
    __table_args__ = (
        Index('idx_job_status_job_id', 'job_id'),
        Index('idx_job_status_polled_at', 'polled_at'),
    )
    
    def __repr__(self):
        return f"<JobStatus(id={self.id}, job_id={self.job_id}, status={self.status})>"





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
    recovery_type = Column(Enum(constants.RecoveryType), nullable=False)
    jobs_recovered = Column(Integer, default=0)
    jobs_resubmitted = Column(Integer, default=0)
    jobs_resumed_polling = Column(Integer, default=0)
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True))
    recovery_metadata = Column(JSON)
    
    def __repr__(self):
        return f"<SystemRecovery(id={self.id}, type={self.recovery_type}, recovered={self.jobs_recovered})>"