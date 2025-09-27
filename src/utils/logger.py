"""Logging configuration for the application."""

import logging
import logging.config
import json
import sys
from pathlib import Path
from datetime import datetime, UTC
from typing import Any, Dict

from src.core.config import settings


class JSONFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add extra fields if present
        if hasattr(record, "job_id"):
            log_data["job_id"] = record.job_id
        if hasattr(record, "analysis_id"):
            log_data["analysis_id"] = record.analysis_id
        if hasattr(record, "workflow_id"):
            log_data["workflow_id"] = record.workflow_id
        if hasattr(record, "task_id"):
            log_data["task_id"] = record.task_id
            
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
            
        return json.dumps(log_data)


def setup_logging(
    log_level: str = None,
    log_file: str = None,
    json_format: bool = True
) -> None:
    """
    Setup logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        json_format: Use JSON formatting for logs
    """
    log_level = log_level or settings.LOG_LEVEL
    
    # Create logs directory if it doesn't exist
    if log_file:
        log_path = Path(log_file).parent
        log_path.mkdir(parents=True, exist_ok=True)
    
    # Logging configuration
    config: Dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },
            "json": {
                "()": JSONFormatter
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "json" if json_format else "default",
                "stream": "ext://sys.stdout"
            }
        },
        "root": {
            "level": log_level,
            "handlers": ["console"]
        },
        "loggers": {
            "src": {
                "level": log_level,
                "handlers": ["console"],
                "propagate": False
            },
            "sqlalchemy.engine": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False
            },
            "celery": {
                "level": log_level,
                "handlers": ["console"],
                "propagate": False
            }
        }
    }
    
    # Add file handler if log file is specified
    if log_file:
        config["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": log_level,
            "formatter": "json" if json_format else "default",
            "filename": log_file,
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5
        }
        config["root"]["handlers"].append("file")
        config["loggers"]["src"]["handlers"].append("file")
    
    logging.config.dictConfig(config)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the given name.
    
    Args:
        name: Logger name (typically __name__)
    
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class LogContext:
    """Context manager for adding context to logs."""
    
    def __init__(self, logger: logging.Logger, **kwargs):
        """
        Initialize log context.
        
        Args:
            logger: Logger instance
            **kwargs: Context fields to add to logs
        """
        self.logger = logger
        self.context = kwargs
        self.old_factory = None
    
    def __enter__(self):
        """Enter context - add context fields to logger."""
        old_factory = logging.getLogRecordFactory()
        
        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            for key, value in self.context.items():
                setattr(record, key, value)
            return record
        
        self.old_factory = old_factory
        logging.setLogRecordFactory(record_factory)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context - restore original logger."""
        if self.old_factory:
            logging.setLogRecordFactory(self.old_factory)


# Initialize logging when module is imported
setup_logging()

# Create module logger
logger = get_logger(__name__)