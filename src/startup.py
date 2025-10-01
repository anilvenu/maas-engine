"""Application startup tasks."""

import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.services.recovery_service import RecoveryService
from src.utils.logger import setup_logging
from src.db.session import DatabaseManager
import time

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


def run_startup_tasks():
    """
    Run startup tasks including recovery.
    This should be called when the application starts.
    """
    logger.info("Running startup tasks")
    
    # Wait for services to be ready
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Check database connection
            if DatabaseManager.health_check():
                logger.info("Database connection established")
                break
        except Exception as e:
            logger.warning(f"Waiting for database... ({retry_count}/{max_retries})")
            time.sleep(2)
            retry_count += 1
    
    if retry_count >= max_retries:
        logger.error("Failed to connect to database")
        return False
    
    # Trigger startup recovery
    try:
        recovery_service = RecoveryService()
        task_id = recovery_service.trigger_startup_recovery()
        logger.info(f"Startup recovery submitted: task_id={task_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to trigger startup recovery: {e}")
        return False


def main():
    """Main entry point for startup script."""
    success = run_startup_tasks()
    if success:
        logger.info("Startup tasks completed successfully")
    else:
        logger.error("Startup tasks failed")
        sys.exit(1)


if __name__ == "__main__":
    main()