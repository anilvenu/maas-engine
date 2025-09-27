"""Database dependency for FastAPI."""

from typing import Generator
from sqlalchemy.orm import Session
from src.db.session import SessionLocal


def get_db() -> Generator[Session, None, None]:
    """
    Get database session dependency.
    
    Yields:
        Database session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()