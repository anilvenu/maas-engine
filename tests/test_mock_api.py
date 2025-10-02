#!/usr/bin/env python3
"""Test the Mock Moody's API."""

import httpx
import asyncio
import json
from typing import Dict, Any
import time


async def test_workflow_lifecycle():
    """Test complete workflow lifecycle."""
    
    base_url = "http://localhost:8001/mock"
    
    async with httpx.AsyncClient() as client:
        print("\n=== Testing Mock Moody's API ===\n")
        
        # 1. Submit workflow
        print("1. Submitting workflow...")
        submission_data = {
            "batch_id": 1,
            "configuration_id": 1,
            "model_name": "test_model",
            "parameters": {"param1": "value1"}
        }
        
        try:
            response = await client.post(
                f"{base_url}/workflows",
                json=submission_data
            )
            response.raise_for_status()
            result = response.json()
            workflow_id = result["workflow_id"]
            print(f"   ✓ Workflow submitted: {workflow_id}")
            print(f"   Status: {result['status']}")
        except httpx.HTTPStatusError as e:
            print(f"   ✗ Submission failed: {e.response.status_code} - {e.response.text}")
            return
        
        # 2. Poll status multiple times
        print("\n2. Polling workflow status...")
        for i in range(8):
            await asyncio.sleep(2)  # Wait between polls
            
            try:
                response = await client.get(f"{base_url}/workflows/{workflow_id}/status")
                response.raise_for_status()
                status_result = response.json()
                
                print(f"   Poll #{i+1}: {status_result['status']}", end="")
                if status_result.get('progress_percentage'):
                    print(f" ({status_result['progress_percentage']}%)")
                else:
                    print()
                
                # Stop if terminal state
                if status_result['status'] in ['completed', 'failed', 'cancelled']:
                    print(f"\n   ✓ Workflow reached terminal state: {status_result['status']}")
                    if status_result.get('result_data'):
                        print(f"   Result: {json.dumps(status_result['result_data'], indent=2)}")
                    break
                    
            except httpx.HTTPStatusError as e:
                print(f"   ✗ Status check failed: {e.response.status_code}")
        
        # 3. Test cancellation (submit another workflow)
        print("\n3. Testing cancellation...")
        response = await client.post(f"{base_url}/workflows", json=submission_data)
        cancel_workflow_id = response.json()["workflow_id"]
        print(f"   Submitted workflow for cancellation: {cancel_workflow_id}")
        
        # Poll once to move it to queued
        await asyncio.sleep(1)
        await client.get(f"{base_url}/workflows/{cancel_workflow_id}/status")
        
        # Cancel it
        try:
            response = await client.post(f"{base_url}/workflows/{cancel_workflow_id}/cancel")
            response.raise_for_status()
            print(f"   ✓ Workflow cancelled successfully")
        except httpx.HTTPStatusError as e:
            print(f"   ✗ Cancellation failed: {e.response.status_code}")
        
        # 4. Check statistics
        print("\n4. Getting statistics...")
        response = await client.get(f"{base_url}/stats")
        stats = response.json()
        print(f"   Total workflows: {stats['total']}")
        print(f"   By status: {json.dumps(stats['by_status'], indent=2)}")
        
        print("\n=== Test Complete ===\n")


if __name__ == "__main__":
    asyncio.run(test_workflow_lifecycle())