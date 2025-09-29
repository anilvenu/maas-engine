from fastapi import Header, HTTPException, status
from typing import Optional
from src.core.config import settings

def verify_api_key(x_api_key: Optional[str] = Header(None)):
    """Verify API key from header."""
    if not x_api_key or x_api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key"
        )
    return x_api_key