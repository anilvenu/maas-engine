"""
Migration script to update database schema for Phase 1 changes.
Run this script to add the skip field and update constraints.

Usage:
    docker exec -it irp-app python src/db/migrate_phase1.py
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from sqlalchemy import text, inspect
from src.db.session import engine, SessionLocal
from src.db.models import Base
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_column_exists(inspector, table_name, column_name):
    """Check if a column exists in a table."""
    columns = [col['name'] for col in inspector.get_columns(table_name)]
    return column_name in columns


def check_constraint_exists(inspector, table_name, constraint_name):
    """Check if a constraint exists in a table."""
    constraints = inspector.get_unique_constraints(table_name)
    constraint_names = [c['name'] for c in constraints]
    return constraint_name in constraint_names


def migrate():
    """Run migration to add skip field and update constraints."""
    
    try:
        # Create inspector to check existing schema
        inspector = inspect(engine)
        
        with engine.begin() as conn:
            logger.info("Starting Phase 1 migration...")
            
            # 1. Add skip column to irp_configuration if it doesn't exist
            if not check_column_exists(inspector, 'irp_configuration', 'skip'):
                logger.info("Adding 'skip' column to irp_configuration table...")
                conn.execute(text("""
                    ALTER TABLE irp_configuration 
                    ADD COLUMN skip BOOLEAN NOT NULL DEFAULT FALSE
                """))
                logger.info("✓ Added 'skip' column")
            else:
                logger.info("'skip' column already exists")
            
            # 2. Make version column NOT NULL with default 1 if needed
            logger.info("Updating version column to be NOT NULL...")
            # First update any NULL values to 1
            conn.execute(text("""
                UPDATE irp_configuration 
                SET version = 1 
                WHERE version IS NULL
            """))
            
            # Then alter the column
            conn.execute(text("""
                ALTER TABLE irp_configuration 
                ALTER COLUMN version SET NOT NULL,
                ALTER COLUMN version SET DEFAULT 1
            """))
            logger.info("✓ Updated version column")
            
            # 3. Drop old constraint if it exists
            try:
                conn.execute(text("""
                    ALTER TABLE irp_configuration 
                    DROP CONSTRAINT IF EXISTS unique_active_config_per_batch
                """))
                logger.info("✓ Dropped old constraint")
            except Exception as e:
                logger.warning(f"Could not drop old constraint: {e}")
            
            # 4. Create new unique constraint on (batch_id, config_name, version)
            logger.info("Creating new unique constraint...")
            conn.execute(text("""
                ALTER TABLE irp_configuration 
                DROP CONSTRAINT IF EXISTS uq_batch_config_version
            """))
            
            conn.execute(text("""
                ALTER TABLE irp_configuration 
                ADD CONSTRAINT uq_batch_config_version 
                UNIQUE (batch_id, config_name, version)
            """))
            logger.info("✓ Created unique constraint on (batch_id, config_name, version)")
            
            # 5. Create index for better performance on active configurations
            logger.info("Creating index for active configurations...")
            conn.execute(text("""
                DROP INDEX IF EXISTS idx_configuration_active
            """))
            
            conn.execute(text("""
                CREATE INDEX idx_configuration_active 
                ON irp_configuration (batch_id, config_name, is_active)
            """))
            logger.info("✓ Created index for active configurations")
            
            # 6. Update existing configurations to ensure unique versions
            logger.info("Checking and fixing version numbers...")
            conn.execute(text("""
                WITH version_fix AS (
                    SELECT id, 
                           batch_id, 
                           config_name,
                           ROW_NUMBER() OVER (
                               PARTITION BY batch_id, config_name 
                               ORDER BY created_ts
                           ) as new_version
                    FROM irp_configuration
                )
                UPDATE irp_configuration c
                SET version = vf.new_version
                FROM version_fix vf
                WHERE c.id = vf.id
                  AND c.version != vf.new_version
            """))
            logger.info("✓ Fixed version numbers")
            
            logger.info("Phase 1 migration completed successfully!")
            
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


def rollback():
    """Rollback migration changes."""
    try:
        with engine.begin() as conn:
            logger.info("Rolling back Phase 1 migration...")
            
            # Drop the new constraint
            conn.execute(text("""
                ALTER TABLE irp_configuration 
                DROP CONSTRAINT IF EXISTS uq_batch_config_version
            """))
            
            # Drop the skip column
            conn.execute(text("""
                ALTER TABLE irp_configuration 
                DROP COLUMN IF EXISTS skip
            """))
            
            # Restore old constraint
            conn.execute(text("""
                ALTER TABLE irp_configuration 
                ADD CONSTRAINT unique_active_config_per_batch 
                UNIQUE (batch_id, config_name, version)
            """))
            
            logger.info("Rollback completed successfully!")
            
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        raise


def verify():
    """Verify migration was successful."""
    try:
        inspector = inspect(engine)
        
        # Check if skip column exists
        columns = [col['name'] for col in inspector.get_columns('irp_configuration')]
        assert 'skip' in columns, "Skip column not found"
        
        # Check if constraint exists
        constraints = inspector.get_unique_constraints('irp_configuration')
        constraint_names = [c['name'] for c in constraints]
        assert 'uq_batch_config_version' in constraint_names, "New constraint not found"
        
        # Test creating configurations with same name but different versions
        with SessionLocal() as db:
            from src.db.repositories.configuration_repository import ConfigurationRepository
            from src.db.repositories.batch_repository import BatchRepository
            
            # Create a test batch
            batch_repo = BatchRepository(db)
            test_batch = batch_repo.create_batch(
                name="Migration Test Batch",
                yaml_config={},
                description="Test batch for migration verification"
            )
            
            # Create configurations with same name
            config_repo = ConfigurationRepository(db)
            config1 = config_repo.create_configuration(
                batch_id=test_batch.id,
                name="test_config",
                config_data={"test": "v1"}
            )
            assert config1.version == 1, "First config should be version 1"
            
            config2 = config_repo.create_configuration(
                batch_id=test_batch.id,
                name="test_config",
                config_data={"test": "v2"}
            )
            assert config2.version == 2, "Second config should be version 2"
            
            # Clean up
            batch_repo.delete(test_batch.id)
        
        logger.info("✓ Migration verification passed!")
        return True
        
    except Exception as e:
        logger.error(f"Migration verification failed: {e}")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Database migration for Phase 1")
    parser.add_argument("--rollback", action="store_true", help="Rollback the migration")
    parser.add_argument("--verify", action="store_true", help="Verify the migration")
    
    args = parser.parse_args()
    
    if args.rollback:
        rollback()
    elif args.verify:
        verify()
    else:
        migrate()
        verify()