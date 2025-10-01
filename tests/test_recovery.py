#!/usr/bin/env python3
"""Test recovery functionality."""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import time
from src.services.recovery_service import RecoveryService
from src.db.session import get_db_session
from src.db.models import Job
from src.tasks.job_tasks import submit_job


def test_recovery():
    """Test recovery by simulating a crash and recovery."""
    
    print("\n=== Testing Recovery System ===\n")
    
    # 1. Create and submit a test job
    print("1. Creating test job...")
    with get_db_session() as db:
        from src.db.repositories.batch_repository import BatchRepository
        from src.db.repositories.configuration_repository import ConfigurationRepository
        from src.db.repositories.job_repository import JobRepository
        
        # Create batch
        batch_repo = BatchRepository(db)
        batch = batch_repo.create_batch(
            "Recovery Test",
            {"test": True},
            "Testing recovery"
        )
        
        # Create configuration
        config_repo = ConfigurationRepository(db)
        config = config_repo.create_configuration(
            batch.id,
            "Test Config",
            {"model": "test", "params": {}}
        )
        
        # Create job
        job_repo = JobRepository(db)
        job = job_repo.create_job(batch.id, config.id)
        job_id = job.id
        
        print(f"   Created job ID: {job_id}")
    
    # 2. Submit the job
    print("\n2. Submitting job...")
    task = submit_job.delay(job_id)
    print(f"   Submitted with task ID: {task.id}")
    
    # 3. Wait for it to start polling
    print("\n3. Waiting for job to start...")
    time.sleep(5)
    
    # 4. Check job status
    with get_db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()
        print(f"   Job status: {job.status}")
        print(f"   Workflow ID: {job.workflow_id}")
    
    # 5. Simulate crash (in real scenario, restart services here)
    print("\n4. Simulating system crash...")
    print("   (In production, polling tasks would be lost from Redis)")
    
    # 6. Run recovery
    print("\n5. Running recovery...")
    recovery_service = RecoveryService()
    result = recovery_service.trigger_manual_recovery()
    print(f"   Recovery started: {result['task_id']}")
    
    # 7. Wait for recovery to complete
    print("\n6. Waiting for recovery to complete...")
    time.sleep(10)
    
    # 8. Check recovery status
    status = recovery_service.get_recovery_status()
    print(f"   Recovery status: {status['status']}")
    print(f"   Jobs recovered: {status.get('jobs_recovered', 0)}")
    print(f"   Jobs resumed polling: {status.get('jobs_resumed_polling', 0)}")
    print(f"   Jobs resubmitted: {status.get('jobs_resubmitted', 0)}")
    
    # 9. Verify job is being polled again
    print("\n7. Checking if job is active again...")
    with get_db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()
        print(f"   Job status: {job.status}")
        print(f"   Last poll: {job.last_poll_ts}")
    
    print("\n=== Recovery Test Complete ===\n")


if __name__ == "__main__":
    test_recovery()