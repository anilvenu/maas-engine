"""FastAPI main application."""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from datetime import datetime, UTC

from src.api.endpoints import analysis, jobs, system
from src.core.config import settings
from src.utils.logger import setup_logging

# Setup logging
setup_logging(log_level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    yield
    # Shutdown
    logger.info("Shutting down application")


# Create FastAPI application
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="IRP Batch Processing System API",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Exception handlers
@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    """Handle 404 errors."""
    return JSONResponse(
        status_code=404,
        content={
            "error": "Not found",
            "path": request.url.path,
            "timestamp": datetime.now(UTC).isoformat()
        }
    )


@app.exception_handler(500)
async def internal_error_handler(request: Request, exc):
    """Handle 500 errors."""
    logger.error(f"Internal error: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "timestamp": datetime.now(UTC).isoformat()
        }
    )


# Include routers
app.include_router(analysis.router)
app.include_router(jobs.router)
app.include_router(system.router)


# Root endpoint
@app.get("/")
def root():
    """Root endpoint."""
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "running",
        "timestamp": datetime.now(UTC),
        "endpoints": {
            "analysis": "/api/analysis",
            "jobs": "/api/jobs",
            "system": "/api/system",
            "health": "/api/system/health",
            "docs": "/docs",
            "redoc": "/redoc"
        }
    }


@app.get("/api")
def api_info():
    """API information."""
    return {
        "version": "1.0.0",
        "endpoints": [
            {
                "path": "/api/analysis",
                "description": "Analysis management"
            },
            {
                "path": "/api/jobs", 
                "description": "Job management"
            },
            {
                "path": "/api/system",
                "description": "System monitoring and management"
            }
        ]
    }