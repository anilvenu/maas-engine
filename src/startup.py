"""Application startup tasks."""

import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.services.recovery_service import RecoveryService
from src.utils.logger import setup_logging
from src.db.session import DatabaseManager
from src.tasks.batch_tasks import startup_batch_check
import time

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


def run_startup_tasks():
    """
    Run startup tasks including recovery and batch checks.
    This should be called when the application starts.
    """
    logger.info("=" * 80)
    logger.info("Starting application startup tasks")
    logger.info("=" * 80)
    
    # Wait for services to be ready
    max_retries = 10
    retry_count = 0
    
    # Check database connection
    while retry_count < max_retries:
        try:
            if DatabaseManager.health_check():
                logger.info("✓ Database connection established")
                break
        except Exception as e:
            logger.warning(f"Waiting for database... ({retry_count}/{max_retries})")
            time.sleep(2)
            retry_count += 1
    
    if retry_count >= max_retries:
        logger.error("⚠ Failed to connect to database")
        return False
    
    # Check Redis connection
    retry_count = 0
    while retry_count < max_retries:
        try:
            import redis
            from src.core.config import settings
            r = redis.Redis.from_url(settings.REDIS_URL)
            r.ping()
            logger.info("✓ Redis connection established")
            break
        except Exception as e:
            logger.warning(f"Waiting for Redis... ({retry_count}/{max_retries})")
            time.sleep(2)
            retry_count += 1
    
    if retry_count >= max_retries:
        logger.error("✗ Failed to connect to Redis")
        return False
    
    # Check Celery workers are ready
    retry_count = 0
    while retry_count < max_retries:
        try:
            from src.tasks.celery_app import celery
            insp = celery.control.inspect(timeout=2)
            stats = insp.stats()
            if stats:
                logger.info(f"✓ Celery workers ready: {len(stats)} worker(s) active")
                break
            else:
                logger.warning(f"Waiting for Celery workers... ({retry_count}/{max_retries})")
        except Exception as e:
            logger.warning(f"Waiting for Celery workers... ({retry_count}/{max_retries})")
        time.sleep(2)
        retry_count += 1
    
    if retry_count >= max_retries:
        logger.warning("⚠ Celery workers may not be ready, continuing anyway...")
    
    # Small additional delay to ensure everything is stable
    logger.info("Waiting for system stabilization...")
    time.sleep(3)
    
    startup_results = {
        "recovery_task_id": None,
        "batch_check_task_id": None,
        "success": True
    }
    
    # Trigger startup recovery
    logger.info("-" * 80)
    logger.info("Phase 1: Triggering startup recovery")
    logger.info("-" * 80)
    
    try:
        recovery_service = RecoveryService()
        task_id = recovery_service.trigger_startup_recovery()
        startup_results["recovery_task_id"] = task_id
        logger.info(f"✓ Startup recovery submitted: task_id={task_id}")
    except Exception as e:
        logger.error(f"✗ Failed to trigger startup recovery: {e}")
        startup_results["success"] = False
    
    # Trigger initial batch check
    logger.info("-" * 80)
    logger.info("Phase 2: Triggering initial batch check")
    logger.info("-" * 80)
    
    try:
        # Use the startup-specific batch check task
        task = startup_batch_check.apply_async(queue='default')
        startup_results["batch_check_task_id"] = task.id
        logger.info(f"✓ Initial batch check submitted: task_id={task.id}")
    except Exception as e:
        logger.error(f"✗ Failed to trigger initial batch check: {e}")
        startup_results["success"] = False
    
    # Log summary
    logger.info("=" * 80)
    if startup_results["success"]:
        logger.info("✓ STARTUP TASKS COMPLETED SUCCESSFULLY")
    else:
        logger.error("✗ STARTUP TASKS COMPLETED WITH ERRORS")
    logger.info(f"Recovery Task ID: {startup_results['recovery_task_id']}")
    logger.info(f"Batch Check Task ID: {startup_results['batch_check_task_id']}")
    logger.info("=" * 80)
    
    return startup_results["success"]


def main():
    """Main entry point for startup script."""
    success = run_startup_tasks()
    if success:
        logger.info("Startup process completed successfully")
        sys.exit(0)
    else:
        logger.error("Startup process failed")
        sys.exit(1)


if __name__ == "__main__":
    main()