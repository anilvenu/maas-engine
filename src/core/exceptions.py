"""Custom exceptions for the application."""


class IRPException(Exception):
    """Base exception for IRP application."""
    pass


class JobNotFoundException(IRPException):
    """Raised when a job is not found."""
    pass


class BatchNotFoundException(IRPException):
    """Raised when an batch is not found."""
    pass


class JobSubmissionException(IRPException):
    """Raised when workflow submission fails."""
    pass


class PollException(IRPException):
    """Raised when polling fails."""
    pass


class RecoveryException(IRPException):
    """Raised when recovery process fails."""
    pass


class InvalidStatusTransitionException(IRPException):
    """Raised when an invalid status transition is attempted."""
    pass


class MaxRetriesExceededException(IRPException):
    """Raised when maximum retry attempts are exceeded."""
    pass


class ConfigurationException(IRPException):
    """Raised when configuration is invalid."""
    pass


class DatabaseException(IRPException):
    """Raised when database operations fail."""
    pass