"""Repository for Configuration operations."""

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session

from src.db.models import Configuration
from src.db.repositories.base_repository import BaseRepository


class ConfigurationRepository(BaseRepository[Configuration]):
    """Repository for configuration-related database operations."""
    
    def __init__(self, db: Session):
        super().__init__(Configuration, db)
    
    def get_by_batch(self, batch_id: int, active_only: bool = True) -> List[Configuration]:
        """Get configurations for an batch."""
        query = self.db.query(Configuration).filter(Configuration.batch_id == batch_id)
        if active_only:
            query = query.filter(Configuration.is_active == True)
        return query.all()
    
    def create_configuration(self, batch_id: int, name: str, 
                           config_data: Dict[str, Any]) -> Configuration:
        """Create a new configuration."""
        # Check if configuration with same name exists
        existing = self.db.query(Configuration)\
            .filter(
                Configuration.batch_id == batch_id,
                Configuration.config_name == name
            ).first()
        
        version = 1
        if existing:
            # Get max version
            max_version = self.db.query(Configuration.version)\
                .filter(
                    Configuration.batch_id == batch_id,
                    Configuration.config_name == name
                ).scalar()
            version = max_version + 1 if max_version else 1
        
        return self.create(
            batch_id=batch_id,
            config_name=name,
            config_data=config_data,
            is_active=True,
            version=version
        )
    
    def update_configuration(self, config_id: int, 
                           config_data: Dict[str, Any]) -> Configuration:
        """Update configuration (creates new version)."""
        existing = self.get(config_id)
        if not existing:
            return None
        
        # Deactivate current version
        existing.is_active = False
        
        # Create new version
        new_config = self.create_configuration(
            existing.batch_id,
            existing.config_name,
            config_data
        )
        
        self.db.commit()
        return new_config