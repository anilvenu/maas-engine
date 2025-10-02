"""Repository for Configuration operations."""

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func

from src.db.models import Configuration
from src.db.repositories.base_repository import BaseRepository


class ConfigurationRepository(BaseRepository[Configuration]):
    """Repository for configuration-related database operations."""
    
    def __init__(self, db: Session):
        super().__init__(Configuration, db)
    
    def get_by_batch(self, batch_id: int, active_only: bool = True) -> List[Configuration]:
        """Get configurations for a batch."""
        query = self.db.query(Configuration).filter(Configuration.batch_id == batch_id)
        if active_only:
            query = query.filter(Configuration.is_active == True)
        return query.all()
    
    def get_latest_version(self, batch_id: int, config_name: str) -> Optional[int]:
        """Get the latest version number for a configuration name in a batch."""
        max_version = self.db.query(func.max(Configuration.version))\
            .filter(
                Configuration.batch_id == batch_id,
                Configuration.config_name == config_name
            ).scalar()
        return max_version if max_version else 0
    
    def create_configuration(self, batch_id: int, name: str, 
                           config_data: Dict[str, Any], skip: bool = False) -> Configuration:
        """Create a new configuration."""
        # Get the latest version for this config name
        latest_version = self.get_latest_version(batch_id, name)
        new_version = latest_version + 1
        
        # Create new configuration
        return self.create(
            batch_id=batch_id,
            config_name=name,
            config_data=config_data,
            is_active=True,
            skip=skip,
            version=new_version
        )
    
    def create_new_version(self, original_config_id: int, 
                          config_data: Dict[str, Any]) -> Configuration:
        """
        Create a new version of an existing configuration.
        Used for resubmits with config overrides.
        """
        # Get the original configuration
        original = self.get(original_config_id)
        if not original:
            raise ValueError(f"Configuration {original_config_id} not found")
        
        # Get the latest version for this config name
        latest_version = self.get_latest_version(original.batch_id, original.config_name)
        new_version = latest_version + 1
        
        # Deactivate all previous versions
        self.db.query(Configuration).filter(
                Configuration.batch_id == original.batch_id,
                Configuration.config_name == original.config_name
            ).update({"is_active": False})

        # Create new version
        new_config = Configuration(
            batch_id=original.batch_id,
            config_name=original.config_name,
            config_data=config_data,
            is_active=True,
            skip=original.skip,  # Inherit skip status
            version=new_version
        )
        
        self.db.add(new_config)
        self.db.commit()
        self.db.refresh(new_config)
        
        return new_config
    
    def update_configuration(self, config_id: int, 
                           config_data: Dict[str, Any]) -> Configuration:
        """Update configuration (creates new version)."""
        return self.create_new_version(config_id, config_data)
    
    def set_skip_status(self, config_id: int, skip: bool) -> Configuration:
        """Set the skip status of a configuration."""
        config = self.get(config_id)
        if config:
            config.skip = skip
            self.db.commit()
            self.db.refresh(config)
        return config
    
    def get_active_configuration(self, batch_id: int, config_name: str) -> Optional[Configuration]:
        """Get the active configuration for a given name in a batch."""
        return self.db.query(Configuration)\
            .filter(
                Configuration.batch_id == batch_id,
                Configuration.config_name == config_name,
                Configuration.is_active == True
            )\
            .order_by(Configuration.version.desc())\
            .first()
    
    def get_all_versions(self, batch_id: int, config_name: str) -> List[Configuration]:
        """Get all versions of a configuration in a batch."""
        return self.db.query(Configuration)\
            .filter(
                Configuration.batch_id == batch_id,
                Configuration.config_name == config_name
            )\
            .order_by(Configuration.version.asc())\
            .all()