#!/usr/bin/env python3
"""Test the complete workflow from YAML to job execution."""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import time
from src.initiator import YAMLProcessor
from src.services.orchestrator import Orchestrator

def test_full_flow():
    """Test complete flow from YAML processing to job execution."""
    
    print("\n=== Testing Complete Workflow ===\n")
    
    # 1. Process YAML file
    print("1. Processing YAML file...")
    processor = YAMLProcessor()
    
    # Create test YAML
    test_yaml = {
        "analysis": {
            "name": "Integration Test Analysis",
            "description": "Testing complete workflow",
            "configurations": [
                {
                    "name": "Test Config 1",
                    "model": "test_model_1",
                    "parameters": {"param1": "value1"}
                },
                {
                    "name": "Test Config 2",
                    "model": "test_model_2",
                    "parameters": {"param2": "value2"}
                }
            ]
        }
    }
    
    # Process with auto-submit
    result = processor.process_yaml_data(test_yaml, auto_submit=True)
    print(f"   Analysis created: ID={result['analysis_id']}")
    print(f"   Jobs created: {result['jobs_created']}")
    print(f"   Submission: {result.get('submission', {})}")
    
    # 2. Check progress
    print("\n2. Checking analysis progress...")
    orchestrator = Orchestrator()
    
    # Wait a bit for jobs to start
    time.sleep(5)
    
    progress = orchestrator.get_analysis_progress(result['analysis_id'])
    print(f"   Progress: {progress['progress_percentage']:.0f}%")
    print(f"   Job statuses: {progress['analysis']['job_status_counts']}")
    
    print("\n=== Test Complete ===\n")


if __name__ == "__main__":
    test_full_flow()