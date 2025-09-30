"""Database session management.

Handles connection pooling, session creation, and transaction management.
Provides utilities for both FastAPI dependency injection and standalone scripts.

Key Features:
- Connection pooling with configurable parameters.
- Session management with context managers.
- Transaction management with automatic commit/rollback.
- Health check utility for database connectivity.
- Logging for database operations and errors.
- Initialization function to create tables if they don't exist.
- Configurable via environment variables or settings module.

Usage:
- Use `get_db` as a FastAPI dependency to get a session per request.
- Use `get_db_session` context manager in Celery tasks or scripts.
- Use `DatabaseManager` for executing functions within transactions.
- Call `init_db()` at application startup to ensure tables are created.
- Call `DatabaseManager.health_check()` to verify database connectivity.
"""

from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
import logging

from src.core.config import settings
from src.db.models import Base

logger = logging.getLogger(__name__)

# Create engine with connection pooling
engine = create_engine(
    settings.DATABASE_URL,
    poolclass=QueuePool,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_POOL_MAX_OVERFLOW,
    pool_pre_ping=settings.DB_POOL_PRE_PING,  # Verify connections before using
    echo=settings.DEBUG,  # Log SQL in debug mode
)

# Create session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


def init_db() -> None:
    """Initialize database - create tables if they don't exist."""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


def get_db() -> Generator[Session, None, None]:
    """
    Get database session.
    To be used as dependency injection in FastAPI.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """
    Context manager for database sessions.
    Use this in Celery tasks and standalone scripts.
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


class DatabaseManager:
    """Database manager for handling transactions."""
    
    @staticmethod
    def execute_in_transaction(func, *args, **kwargs):
        """
        Execute a function within a database transaction.
        Automatically commits on success, rolls back on failure.
        """
        with get_db_session() as session:
            try:
                result = func(session, *args, **kwargs)
                session.commit()
                return result
            except Exception as e:
                session.rollback()
                logger.error(f"Transaction failed: {e}")
                raise
    
    @staticmethod
    def health_check() -> bool:
        """Check if database is accessible."""
        try:
            with get_db_session() as session:
                session.execute(text("SELECT 1"))
                logger.info("Database health check passed")
                return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False