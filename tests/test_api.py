#!/usr/bin/env python3
"""Test the Management API."""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import httpx
import json
import asyncio


async def test_api():
    """Test the API endpoints."""
    
    base_url = "http://localhost:8000"
    
    async with httpx.AsyncClient() as client:
        print("\n=== Testing Management API ===\n")
        
        # 1. Test health check
        print("1. Testing health check...")
        response = await client.get(f"{base_url}/api/system/health")
        print(f"   Health: {response.json()}")
        
        # 2. Create batch from YAML
        print("\n2. Creating batch from YAML...")
        yaml_data = {
            "yaml_content": {
                "batch": {
                    "name": "API Test Batch",
                    "description": "Testing via API",
                    "configurations": [
                        {
                            "name": "Config 1",
                            "model": "model_1",
                            "parameters": {"param": "value"}
                        }
                    ]
                }
            },
            "auto_submit": False
        }
        
        response = await client.post(
            f"{base_url}/api/batch",
            json=yaml_data
        )
        if response.status_code == 200:
            batch = response.json()
            batch_id = batch["id"]
            print(f"   Created batch: {batch_id}")
        else:
            print(f"   Error: {response.status_code} - {response.text}")
            return
        
        # 3. Get batch details
        print("\n3. Getting batch details...")
        response = await client.get(f"{base_url}/api/batch/{batch_id}")
        print(f"   Status: {response.json()['status']}")
        print(f"   Jobs: {response.json()['total_jobs']}")
        
        # 4. Submit jobs
        print("\n4. Submitting jobs...")
        response = await client.post(f"{base_url}/api/batch/{batch_id}/submit")
        print(f"   Submission result: {response.json()}")
        
        # 5. Get progress
        print("\n5. Getting progress...")
        await asyncio.sleep(2)  # Wait for processing
        response = await client.get(f"{base_url}/api/batch/{batch_id}/progress")
        progress = response.json()
        print(f"   Progress: {progress}")
        
        # 6. List jobs
        print("\n6. Listing jobs...")
        response = await client.get(f"{base_url}/api/jobs/?batch_id={batch_id}")
        jobs = response.json()
        for job in jobs[:3]:  # Show first 3
            print(f"     - Job {job['id']}: {job['status']}")
        
        # 7. System stats
        print("\n7. Getting system stats...")
        response = await client.get(f"{base_url}/api/system/stats")
        stats = response.json()
        print(f"   Batch: {stats['batch']}")
        print(f"   Jobs: {stats['jobs']}")
        
        print("\n=== API Test Complete ===\n")


if __name__ == "__main__":
    asyncio.run(test_api())