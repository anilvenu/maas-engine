"""YAML processor and job initiator."""

import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))


from src.db.session import get_db_session
from src.db.repositories.analysis_repository import AnalysisRepository
from src.db.repositories.configuration_repository import ConfigurationRepository
from src.db.repositories.job_repository import JobRepository
from src.services.orchestrator import Orchestrator
from src.core.exceptions import ConfigurationException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class YAMLProcessor:
    """Process YAML configuration and create analysis with jobs."""
    
    def __init__(self):
        """Initialize YAML processor."""
        self.orchestrator = Orchestrator()
        self.logger = logger
    
    def _validate_yaml(self, yaml_data: Dict[str, Any]) -> bool:
        """
        Validate YAML structure.
        
        Args:
            yaml_data: Parsed YAML data
            
        Returns:
            True if valid
            
        Raises:
            ConfigurationException: If validation fails
        """
        required_fields = ['analysis', 'analysis.name', 'analysis.configurations']
        
        # Check required fields
        if 'analysis' not in yaml_data:
            raise ConfigurationException("Missing 'analysis' section in YAML")
        
        analysis = yaml_data['analysis']
        
        if 'name' not in analysis:
            raise ConfigurationException("Missing 'name' in analysis section")
        
        if 'configurations' not in analysis:
            raise ConfigurationException("Missing 'configurations' in analysis section")
        
        if not isinstance(analysis['configurations'], list):
            raise ConfigurationException("'configurations' must be a list")
        
        if len(analysis['configurations']) == 0:
            raise ConfigurationException("At least one configuration is required")
        
        # Validate each configuration
        for i, config in enumerate(analysis['configurations']):
            if 'name' not in config:
                raise ConfigurationException(f"Configuration {i} missing 'name'")
            
            if 'model' not in config:
                raise ConfigurationException(f"Configuration {i} missing 'model'")
        
        return True

    
    def process_yaml_data(self, yaml_data: Dict[str, Any], 
                         auto_submit: bool = True) -> Dict[str, Any]:
        """
        Process YAML data and create analysis with jobs.
        
        Args:
            yaml_data: Parsed YAML data
            auto_submit: Automatically submit jobs after creation
            
        Returns:
            Dict with processing results
        """
        analysis_config = yaml_data['analysis']
        
        with get_db_session() as db:
            analysis_repo = AnalysisRepository(db)
            config_repo = ConfigurationRepository(db)
            job_repo = JobRepository(db)
            
            # Create analysis
            analysis = analysis_repo.create_from_yaml(
                name=analysis_config['name'],
                description=analysis_config.get('description', ''),
                yaml_config=yaml_data
            )
            
            self.logger.info(f"Created analysis {analysis.id}: {analysis.name}")
            
            # Create configurations and jobs
            job_ids = []
            for config_data in analysis_config['configurations']:
                # Create configuration
                config = config_repo.create_configuration(
                    analysis_id=analysis.id,
                    name=config_data['name'],
                    config_data=config_data
                )
                
                self.logger.info(f"Created configuration {config.id}: {config.config_name}")
                
                # Create job for this configuration
                job = job_repo.create_job(
                    analysis_id=analysis.id,
                    configuration_id=config.id
                )
                
                job_ids.append(job.id)
                self.logger.info(f"Created job {job.id} for configuration {config.config_name}")
            
            db.commit()
            
            results = {
                "analysis_id": analysis.id,
                "analysis_name": analysis.name,
                "configurations_created": len(analysis_config['configurations']),
                "jobs_created": len(job_ids),
                "job_ids": job_ids,
                "status": "created"
            }
            
            # Auto-submit jobs if requested
            if auto_submit:
                self.logger.info(f"Auto-submitting jobs for analysis {analysis.id}")
                submission_results = self.orchestrator.submit_analysis_jobs(analysis.id)
                results["submission"] = submission_results
                results["status"] = "submitted"
            
            return results

    def process_yaml_file(self, yaml_path: str, 
                         auto_submit: bool = True) -> Dict[str, Any]:
        """
        Process YAML file and create analysis with jobs.
        
        Args:
            yaml_path: Path to YAML file
            auto_submit: Automatically submit jobs after creation
            
        Returns:
            Dict with processing results
        """
        self.logger.info(f"Processing YAML file: {yaml_path}")
        
        # Read and parse YAML
        try:
            with open(yaml_path, 'r') as f:
                yaml_data = yaml.safe_load(f)
        except Exception as e:
            raise ConfigurationException(f"Failed to read YAML file: {e}")
        
        # Validate YAML
        self._validate_yaml(yaml_data)
        
        # Process YAML data
        return self.process_yaml_data(yaml_data, auto_submit)


def main():
    """Main entry point for initiator."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Process YAML and create analysis with jobs")
    parser.add_argument("yaml_path", help="Path to YAML file or directory")
    parser.add_argument("--submit", action="store_true", help="Submit jobs after creation")
    
    args = parser.parse_args()
    
    processor = YAMLProcessor()
    
    try:
        results = processor.process_yaml_file(
            args.yaml_path,
            auto_submit = args.submit
        )
        
        print(f"\nProcessing complete:")
        print(f"Results: {results}")
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()