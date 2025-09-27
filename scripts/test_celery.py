#!/usr/bin/env python3
"""Test Celery tasks."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import time
from src.tasks.job_tasks import submit_job, poll_workflow_status
from src.tasks.analysis_tasks import check_analysis_completion
from src.db.session import get_db_session
from src.db.models import Analysis, Configuration, Job
from src.core.constants import AnalysisStatus, JobStatus
import json


def test_celery_tasks():
    """Test basic Celery task execution."""
    
    print("\n=== Testing Celery Tasks ===\n")
    
    with get_db_session() as db:
        # 1. Create test analysis
        print("1. Creating test analysis...")
        analysis = Analysis(
            name="Celery Test Analysis",
            description="Testing Celery tasks",
            status=AnalysisStatus.PENDING.value,
            yaml_config={"test": True}
        )
        db.add(analysis)
        db.commit()
        print(f"   Created analysis ID: {analysis.id}")
        
        # 2. Create test configuration  
        print("\n2. Creating test configuration...")
        config = Configuration(
            analysis_id=analysis.id,
            config_name="Test Config",
            config_data={
                "model": "test_model",
                "parameters": {"param1": "value1"}
            }
        )
        db.add(config)
        db.commit()
        print(f"   Created configuration ID: {config.id}")
        
        # 3. Create test job
        print("\n3. Creating test job...")
        job = Job(
            analysis_id=analysis.id,
            configuration_id=config.id,
            status=JobStatus.PLANNED.value
        )
        db.add(job)
        db.commit()
        print(f"   Created job ID: {job.id}")
        
        # 4. Submit job using Celery task
        print("\n4. Submitting job via Celery...")
        result = submit_job.delay(job.id)
        print(f"   Task ID: {result.id}")
        print("   Waiting for result...")
        
        try:
            task_result = result.get(timeout=10)
            print(f"   Result: {json.dumps(task_result, indent=2)}")
        except Exception as e:
            print(f"   Error: {e}")
        
        # 5. Check analysis completion
        print("\n5. Checking analysis completion...")
        result = check_analysis_completion.delay(analysis.id)
        try:
            task_result = result.get(timeout=5)
            print(f"   Result: {json.dumps(task_result, indent=2)}")
        except Exception as e:
            print(f"   Error: {e}")
    
    print("\n=== Test Complete ===\n")


if __name__ == "__main__":
    test_celery_tasks()