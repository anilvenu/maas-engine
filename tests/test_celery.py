#!/usr/bin/env python3
"""Test Celery tasks."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import time
from src.tasks.job_tasks import submit_job, poll_job_status
from src.tasks.batch_tasks import check_batch_completion
from src.db.session import get_db_session
from src.db.models import Batch, Configuration, Job
from src.core.constants import BatchStatus, JobStatus
import json


def test_celery_tasks():
    """Test basic Celery task execution."""
    
    print("\n=== Testing Celery Tasks ===\n")
    
    with get_db_session() as db:
        # 1. Create test batch
        print("1. Creating test batch...")
        batch = Batch(
            name="Celery Test Batch",
            description="Testing Celery tasks",
            status=BatchStatus.PENDING.value,
            yaml_config={"test": True}
        )
        db.add(batch)
        db.commit()
        print(f"   Created batch ID: {batch.id}")
        
        # 2. Create test configuration  
        print("\n2. Creating test configuration...")
        config = Configuration(
            batch_id=batch.id,
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
            batch_id=batch.id,
            configuration_id=config.id,
            status=JobStatus.PENDING.value
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
        
        # 5. Check batch completion
        print("\n5. Checking batch completion...")
        result = check_batch_completion.delay(batch.id)
        try:
            task_result = result.get(timeout=5)
            print(f"   Result: {json.dumps(task_result, indent=2)}")
        except Exception as e:
            print(f"   Error: {e}")
    
    print("\n=== Test Complete ===\n")


if __name__ == "__main__":
    test_celery_tasks()