"""Mock Moody's API for testing."""

from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, UTC
import random
import uuid
import time
import logging

# Create separate FastAPI app for mock service
app = FastAPI(title="Mock Moody's API", version="1.0.0")

# In-memory storage for workflows
workflows_db = {}

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Request/Response Models
class WorkflowSubmissionRequest(BaseModel):
    """Request model for workflow submission."""
    analysis_id: int
    configuration_id: int
    model_name: str
    parameters: Dict[str, Any]


class WorkflowSubmissionResponse(BaseModel):
    """Response model for workflow submission."""
    workflow_id: str
    status: str
    message: str
    submitted_at: datetime


class WorkflowStatusResponse(BaseModel):
    """Response model for workflow status."""
    workflow_id: str
    status: str
    progress_percentage: Optional[int] = None
    message: Optional[str] = None
    result_data: Optional[Dict[str, Any]] = None
    updated_at: datetime
    estimated_completion: Optional[datetime] = None


class WorkflowCancelResponse(BaseModel):
    """Response model for workflow cancellation."""
    workflow_id: str
    status: str
    message: str


# Workflow state machine
class WorkflowState:
    """Manages workflow state transitions."""
    
    TRANSITIONS = {
        "submitted": ["queued", "failed"],
        "queued": ["running", "cancelled", "failed"],
        "running": ["completed", "failed", "cancelled"],
        "completed": [],
        "failed": [],
        "cancelled": []
    }
    
    def __init__(self, workflow_id: str):
        self.workflow_id = workflow_id
        self.status = "submitted"
        self.created_at = datetime.now(UTC)
        self.updated_at = datetime.now(UTC)
        self.poll_count = 0
        self.progress = 0
        self.result_data = None
        
    def advance_state(self):
        """Advance workflow to next state based on poll count."""
        self.poll_count += 1
        self.updated_at = datetime.now(UTC)
        
        # State progression logic
        if self.status == "submitted" and self.poll_count >= 1:
            self.status = "queued"
            self.progress = 10
            
        elif self.status == "queued" and self.poll_count >= 2:
            # 90% chance to start running, 10% chance to fail
            if random.random() > 0.1:
                self.status = "running"
                self.progress = 30
            else:
                self.status = "failed"
                self.progress = 0
                
        elif self.status == "running":
            # Progress increases with each poll
            self.progress = min(90, self.progress + random.randint(10, 25))
            
            # Complete after reaching 90% or after enough polls
            if self.progress >= 90 or self.poll_count >= 6:
                # 80% success rate
                if random.random() > 0.2:
                    self.status = "completed"
                    self.progress = 100
                    self.result_data = {
                        "execution_time_seconds": random.randint(30, 300),
                        "records_processed": random.randint(1000, 10000),
                        "output_location": f"s3://results/{self.workflow_id}/output.json"
                    }
                else:
                    self.status = "failed"
                    self.progress = self.progress
                    
    def cancel(self):
        """Cancel the workflow if it's in a cancellable state."""
        if self.status in ["queued", "running"]:
            self.status = "cancelled"
            self.updated_at = datetime.now(UTC)
            return True
        return False


# API Endpoints
@app.get("/")
def root():
    """Health check endpoint."""
    return {"status": "healthy", "service": "Mock Moody's API", "time": datetime.now(UTC)}

@app.get("/mock")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "Mock Moody's API", "time": datetime.now(UTC)}

@app.post("/mock/workflows", response_model=WorkflowSubmissionResponse)
async def submit_workflow(request: WorkflowSubmissionRequest):
    """
    Submit a new workflow.
    Simulates API throttling and errors occasionally.
    """
    # Simulate network latency
    await asyncio.sleep(random.uniform(0.1, 0.5))
    
    # Simulate occasional errors
    error_chance = random.random()
    
    # 5% chance of rate limiting (429)
    if error_chance < 0.05:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": "10"}
        )
    
    # 2% chance of service unavailable (503)
    elif error_chance < 0.07:
        raise HTTPException(
            status_code=503,
            detail="Service temporarily unavailable"
        )
    
    # 1% chance of internal error (500)
    elif error_chance < 0.08:
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )
    
    # Successful submission
    workflow_id = f"WF-{uuid.uuid4()}"
    workflow = WorkflowState(workflow_id)
    workflows_db[workflow_id] = workflow
    
    logger.info(f"Workflow submitted: {workflow_id}")
    
    return WorkflowSubmissionResponse(
        workflow_id=workflow_id,
        status="accepted",
        message="Workflow submitted successfully",
        submitted_at=workflow.created_at
    )


@app.get("/mock/workflows/{workflow_id}/status", response_model=WorkflowStatusResponse)
async def get_workflow_status(workflow_id: str):
    """
    Get workflow status.
    Simulates state progression and occasional errors.
    """
    # Simulate network latency
    await asyncio.sleep(random.uniform(0.1, 0.3))
    
    # Check if workflow exists
    if workflow_id not in workflows_db:
        # Simulate "not found" for recovery testing
        raise HTTPException(
            status_code=404,
            detail=f"Workflow {workflow_id} not found"
        )
    
    workflow = workflows_db[workflow_id]
    
    # Simulate occasional polling errors
    if random.random() < 0.02:  # 2% chance
        raise HTTPException(
            status_code=503,
            detail="Unable to retrieve status"
        )
    
    # Advance workflow state
    workflow.advance_state()
    
    # Calculate estimated completion
    estimated_completion = None
    if workflow.status == "running":
        remaining_time = random.randint(60, 300)
        estimated_completion = datetime.now(UTC) + timedelta(seconds=remaining_time)
    
    logger.info(f"Status check for {workflow_id}: {workflow.status} (poll #{workflow.poll_count})")
    
    return WorkflowStatusResponse(
        workflow_id=workflow_id,
        status=workflow.status,
        progress_percentage=workflow.progress if workflow.status == "running" else None,
        message=f"Workflow is {workflow.status}",
        result_data=workflow.result_data,
        updated_at=workflow.updated_at,
        estimated_completion=estimated_completion
    )


@app.post("/mock/workflows/{workflow_id}/cancel", response_model=WorkflowCancelResponse)
async def cancel_workflow(workflow_id: str):
    """Cancel a running workflow."""
    # Check if workflow exists
    if workflow_id not in workflows_db:
        raise HTTPException(
            status_code=404,
            detail=f"Workflow {workflow_id} not found"
        )
    
    workflow = workflows_db[workflow_id]
    
    if workflow.cancel():
        logger.info(f"Workflow cancelled: {workflow_id}")
        return WorkflowCancelResponse(
            workflow_id=workflow_id,
            status="cancelled",
            message="Workflow cancellation requested"
        )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Workflow {workflow_id} cannot be cancelled in {workflow.status} state"
        )


@app.get("/mock/workflows")
def list_workflows(limit: int = 10):
    """List all workflows (for debugging)."""
    workflows = []
    for wf_id, wf in list(workflows_db.items())[:limit]:
        workflows.append({
            "workflow_id": wf_id,
            "status": wf.status,
            "created_at": wf.created_at,
            "updated_at": wf.updated_at,
            "poll_count": wf.poll_count,
            "progress": wf.progress
        })
    
    return {
        "total": len(workflows_db),
        "returned": len(workflows),
        "workflows": workflows
    }


@app.delete("/mock/workflows")
def clear_workflows():
    """Clear all workflows (for testing)."""
    count = len(workflows_db)
    workflows_db.clear()
    logger.info(f"Cleared {count} workflows")
    return {"message": f"Cleared {count} workflows"}


@app.get("/mock/stats")
def get_statistics():
    """Get statistics about workflows."""
    stats = {
        "total": len(workflows_db),
        "by_status": {},
        "avg_poll_count": 0
    }
    
    if workflows_db:
        for wf in workflows_db.values():
            stats["by_status"][wf.status] = stats["by_status"].get(wf.status, 0) + 1
        
        stats["avg_poll_count"] = sum(wf.poll_count for wf in workflows_db.values()) / len(workflows_db)
    
    return stats


# Add missing import
import asyncio

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)