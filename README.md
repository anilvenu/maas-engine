
# IRP Model as a Service Engine - POC

## Architecture

### Solution Components
```
┌─────────────────────────────────────────────┐
│               Docker Container              │
│                                             │
│  ┌─────────────────┐  ┌──────────────────┐  │
│  │                 │  │                  │  │
│  │  FastAPI App    │  │  Mock Moody's    │  │
│  │  (Management)   │  │  API Service     │  │
│  │  Port: 8000     │  │  Port: 8001      │  │
│  └────────┬────────┘  └──────────────────┘  │
│           │                                 │
│  ┌────────▼────────────────────────────┐    │
│  │   Orchestration Layer               │    │
│  │   - Job Submission Proxy            │    │
│  │   - Status Tracker                  │    │
│  │   - Retry Manager                   │    │
│  └────────┬────────────────────────────┘    │
│           │                                 │
│  ┌────────▼────────┐  ┌──────────────────┐  │
│  │                 │  │                  │  │
│  │  Celery Workers │  │  Celery Beat     │  │
│  │  (Scalable)     │  │  (Scheduler)     │  │
│  └────────┬────────┘  └──────────────────┘  │
│           │                                 │
└───────────┼─────────────────────────────────┘
            │
    ┌───────▼────────┐  ┌────────────────┐
    │                │  │                │
    │  PostgreSQL    │  │     Redis      │
    │  (State Store) │  │  (Job Queue)   │
    └────────────────┘  └────────────────┘

```

- **FastAPI Management Service**: CRUD operations, job control, status viewing
- **Orchestration Layer**: Job submission proxy, implements retry logic, tracks all operations
- **Celery Workers**: Execute async tasks (submission, polling, analysis checking)
- **Celery Beat**: Scheduled tasks (recovery polls, analysis completion checks)
- **PostgreSQL**: Persistent state storage with full audit trail
- Redis: Job queue, result backend, distributed locks

**Testing Component**
- **Mock Moody's API**: Simulates workflow submission and status endpoints

### Job Submission Flow

**Submission Flow**

```
YAML Upload → Parse → Create Analysis → Create Configurations 
→ Create Jobs (status: planned) → Queue Submission Tasks
→ Submit to Moody's (via Orchestration Layer) → Update to initiated
→ Store Celery Task ID → Schedule Polling Tasks
```

```
               deploy
        ┌──────────┐
        │ planned  │
        └────┬─────┘
             │ initiate
        ┌────▼─────┐
        │initiated │
        └────┬─────┘
             │ submit & response
        ┌────▼─────┐
     ┌──┤ queued   ├──┐
     │  └────┬─────┘  │
     │       │        │ cancel
     │  ┌────▼─────┐  │
     ├──┤ running  ├──┤
     │  └────┬─────┘  │
     │       │        │
     │  ┌────┴────────┴───┐
     │  │                 │
┌────▼─────┐        ┌─────▼─────┐      ┌───────────┐
│cancelled │        │ completed │      │  failed   │
└──────────┘        └───────────┘      └─────┬─────┘
                                             │ retry
                                       ┌─────▼─────┐
                                       │ planned   │
                                       │(new job)  │
                                       └───────────┘

```

**Polling Strategy**

Initial Poll: 30 seconds after submission
Subsequent Polls: Based on job status
Max poll duration: configurable

**Analysis Completion Check**

Runs every N minutes via Celery Beat
Checks all "running" analyses
Updates to "completed" when all non-cancelled jobs are finished

Edge cases:
- All cancelled
- One or more Failed
- Mixed states


## Docker Container Setup

**Overview of Containers**

| Service        | Container Name | Image / Build                  | Ports Exposed                                               | Depends On                  | Purpose                                   |
| -------------- | -------------- | ------------------------------ | ----------------------------------------------------------- | --------------------------- | ----------------------------------------- |
| **PostgreSQL** | `irp-postgres` | `postgres:15-alpine`           | `${POSTGRES_PORT:-5432}:5432`                               | –                           | Relational database (persistent storage). |
| **Redis**      | `irp-redis`    | `redis:7-alpine`               | `${REDIS_PORT:-6379}:6379`                                  | –                           | Cache, message broker, Celery backend.    |
| **App**        | `irp-app`      | Built from `Dockerfile.app`    | `${API_PORT:-8000}:8000` <br> `${MOCK_API_PORT:-8001}:8001` | PostgreSQL, Redis (healthy) | Main application API and mock Moody’s API |
| **Worker**     | `irp-worker`   | Built from `Dockerfile.worker` | –                                                           | PostgreSQL, Redis, App      | Celery worker for background jobs.        |
| **Beat**       | `irp-beat`     | Built from `Dockerfile.beat`   | –                                                           | PostgreSQL, Redis, Worker   | Celery scheduler for periodic tasks.      |
| **Flower**     | `irp-flower`   | `mher/flower:2.0`              | `${FLOWER_PORT:-5555}:5555`                                 | Redis, Worker               | Web UI to monitor Celery tasks/workers.   |


**URLs and Ports**


| Service         | URL / Host Mapping                       | Default Port | Purpose                     |
| --------------- | ---------------------------------------- | ------------ | --------------------------- |
| PostgreSQL      | `localhost:${POSTGRES_PORT}`             | 5432         | Database connection         |
| Redis           | `localhost:${REDIS_PORT}`                | 6379         | Cache / Celery broker       |
| Application API | `http://localhost:${API_PORT}`           | 8000         | Main app API                |
| Mock API        | `http://localhost:${MOCK_API_PORT}/mock` | 8001         | Moody’s mock API            |
| Flower          | `http://localhost:${FLOWER_PORT}`        | 5555         | Celery monitoring dashboard |


**Mounts**

| Service        | Mounts                                                                                                | Purpose / Consequences                                                                                                                                                                             |
| -------------- | ----------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **PostgreSQL** | `../../data/postgres:/var/lib/postgresql/data` <br> `../postgres/init:/docker-entrypoint-initdb.d:ro` | - Persists DB data on host (prevents data loss on restart). <br> - Initialization SQL scripts run automatically on first start.                                                                    |
| **Redis**      | `../../data/redis:/data` <br> `../redis/redis.conf:/usr/local/etc/redis/redis.conf:ro`                | - Persists Redis state (optional depending on config). <br> - Custom Redis config applied at startup.                                                                                              |
| **App**        | `../../src:/app/src` <br> `../../config:/app/config` <br> `../../logs:/app/logs`                      | - Live sync of application source code (changes on host immediately affect container). <br> - Configuration injected from host. <br> - Logs written to host filesystem (persist between restarts). |
| **Worker**     | `../../src:/app/src` <br> `../../logs:/app/logs`                                                      | - Shares same source and logs as app container. <br> - Keeps worker and app code in sync.                                                                                                          |
| **Beat**       | `../../src:/app/src` <br> `../../logs:/app/logs`                                                      | - Same consequences as worker: synced code and persistent logs.                                                                                                                                    |
| **Flower**     | *(none)*                                                                                              | Stateless; no volumes needed.                                                                                                                                                                  |
### Building, Running and Validating the Containers

**Bringing up just PostgreSQL and Redis (for check)**
```
docker-compose up -d postgres redis
```

**Bringing up Everything**
```
docker compose up -d
```

**Checking PostgreSQL Container and Database**
``` bash
docker exec -it irp-postgres psql -U irp_user -d irp_db -c "SELECT 1"
```
this should produce the following output...
```
 ?column? 
----------
        1
(1 row)
```

**Checking Redis**
```
docker exec -it irp-redis redis-cli ping
```
this should produce the following output...
```
PONG
```

**Checking Celery with a Test App**
```
uv run celery -A src.tasks.celery_test_app worker --loglevel=info
```
this should produce the following output...
```
 -------------- celery@premiumiq v5.3.4 (emerald-rush)
--- ***** ----- 
-- ******* ---- Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39 2025-09-27 15:43:40
- *** --- * --- 
- ** ---------- [config]
- ** ---------- .> app:         irp_tasks:0x73273582c2d0
- ** ---------- .> transport:   redis://localhost:6379/0
- ** ---------- .> results:     redis://localhost:6379/1
- *** --- * --- .> concurrency: 8 (prefork)
-- ******* ---- .> task events: OFF (enable -E to monitor tasks in this worker)
--- ***** ----- 
 -------------- [queues]
                .> celery           exchange=celery(direct) key=celery                
```
hit [Ctrl] + C to exit.


### Troubleshooting

``` 
ERROR  - PermissionError: [Errno 13] Permission denied: '......./data/postgres'
```
This could happen if you are running this on WSL Ubuntu 
```
sudo chmod -R 755 data/
```


## Data Model

to do


## Foundation

Core Configuration - Settings, constants, enums (job statuses, etc.)

Database Models - SQLAlchemy models matching our PostgreSQL schema

Database Session Management - Connection pooling, session handling

Basic Logging Setup - Structured logging for debugging



### Configuration Management 
```src/core/config.py```

Environment-based settings with validation
All configuration centralized
Cached settings for performance


### Constants & Enums 
```src/core/constants.py```

Job and Analysis status enums with helper methods
HTTP status codes for retry logic
Standard error messages

### Custom Exceptions 
```src/core/exceptions.py```

Hierarchical exception structure
Specific exceptions for different scenarios

### Database Models 
```src/db/models.py```

Complete SQLAlchemy models matching your PostgreSQL schema
Relationships properly defined
Helper properties for age calculations

### Session Management 
```src/db/session.py```

Connection pooling configuration
Context managers for safe transactions
Database health check

### Structured Logging 
```src/utils/logger.py```

JSON formatted logs for production
Context-aware logging (job_id, workflow_id, etc.)
Rotating file handlers




## Celery Setup and Tasks

Celery application, queue routing, retry strategy, and periodic tasks.
All tasks live under src.tasks.*.

Key Settings

Serialization: JSON-only (task_serializer='json', accept_content=['json'])

Time limits:

Soft: 240s (raises exception, task can handle cleanup)

Hard: 300s (task forcibly killed)

Retries:

Auto-retry for all exceptions

Up to 3 retries with exponential backoff (task_retry_backoff=True)

Max backoff = 600s

Workers:

worker_prefetch_multiplier=1 → fair task distribution

worker_max_tasks_per_child=100 → prevents memory leaks

Result backend:

Persistent

Expiration: 24h

Queues

| Queue      | Purpose                         | Priority |
| ---------- | ------------------------------- | -------- |
| `default`  | General tasks (analysis checks) | —        |
| `jobs`     | Job submission                  | 5        |
| `polling`  | Workflow status polling         | 3        |
| `recovery` | Recovery tasks                  | 10       |


Task Routing

submit_job → jobs queue

poll_workflow_status → polling queue

check_analysis_completion → default queue

recovery_tasks.* → recovery queue

Periodic Tasks (Celery Beat)

| Task                     | Schedule         |
| ------------------------ | ---------------- |
| `check_all_analyses`     | Every 30 minutes |
| `perform_recovery_check` | Every 60 minutes |


Job Tasks (job_tasks.py)

Job tasks handle the lifecycle of Moody’s workflows: submission, polling, and cancellation.

2.1. submit_job(job_id)

| Aspect           | Description                                                                                                                                                   |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Purpose          | Submit a job to Moody’s API and record its workflow ID.                                                                                                       |
| Input            | `job_id` (database ID of the job).                                                                                                                            |
| Output           | Dict with `workflow_id`, `status`, `message`.                                                                                                                 |
| Key Actions      | - Fetch job from DB. <br> - Submit job to Moody’s API. <br> - Save `workflow_id` and update job status to `INITIATED`. <br> - Schedule first poll with delay. |
| Failure Handling | - If job not found → error. <br> - If API returns retryable HTTP error → retried with exponential backoff. <br> - Errors logged in `RetryHistory`.            |

This task initiates the workflow and ensures polling starts automatically after submission.

2.2. poll_workflow_status(job_id, workflow_id)

| Aspect              | Description                                                                                                                                                                 |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Purpose             | Query Moody’s API for current workflow progress.                                                                                                                            |
| Input               | `job_id`, `workflow_id`.                                                                                                                                                    |
| Output              | Dict with `status`, `progress`, `terminal`.                                                                                                                                 |
| Key Actions         | - Poll Moody’s API for workflow status. <br> - Save poll results in `WorkflowStatus`. <br> - Update `Job.status` if changed. <br> - Schedule next poll if job not terminal. |
| Terminal Conditions | `COMPLETED`, `FAILED`, `CANCELLED`.                                                                                                                                         |
| Failure Handling    | - Retry transient errors. <br> - Mark job `FAILED` if workflow not found. <br> - Fail if exceeded maximum polling duration.                                                 |

This task ensures that the system tracks job status until completion, failure, cancellation or timeout (failure).

2.3. cancel_job(job_id)

| Aspect           | Description                                                                                                         |
| ---------------- | ------------------------------------------------------------------------------------------------------------------- |
| Purpose          | Cancel an active workflow in Moody’s API.                                                                           |
| Input            | `job_id`.                                                                                                           |
| Output           | Dict with `status` and `message`.                                                                                   |
| Key Actions      | - Verify job exists and is cancellable. <br> - Call cancel endpoint in Moody’s API. <br> - Mark job as `CANCELLED`. |
| Failure Handling | - Return error if job not found or already terminal. <br> - Handle HTTP errors gracefully.                          |

Cancellation provides operator control to stop unnecessary processing.


Analysis Tasks (analysis_tasks.py)

Analysis tasks roll up job statuses to determine the overall state of an analysis.

| Aspect           | Description                                                                                                                                                                                                   |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Purpose          | Evaluate whether all jobs under an analysis are complete.                                                                                                                                                     |
| Input            | `analysis_id`.                                                                                                                                                                                                |
| Output           | Dict with `status` and job `counts`.                                                                                                                                                                          |
| Key Actions      | - Count job statuses (completed, failed, cancelled, active). <br> - If jobs still active → analysis `RUNNING`. <br> - If all terminal → set analysis to final state (completed, failed, cancelled, or mixed). |
| Failure Handling | - Return error if analysis not found.                                                                                                                                                                         |

This ensures analysis status always reflects the current state of its jobs.

3.2. check_all_analyses()

| Aspect      | Description                                                                                                                                  |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Purpose     | Periodic sweep of all non-terminal analyses.                                                                                                 |
| Input       | None.                                                                                                                                        |
| Output      | Summary dict (`checked`, `completed`, `failed`, `still_running`).                                                                            |
| Key Actions | - Find analyses with status `pending` or `running`. <br> - Call `check_analysis_completion` for each. <br> - Aggregate results into summary. |

Runs periodically to ensure system-wide consistency of analysis states.

Failure Handling & Observability

Failures are expected in distributed systems. This app handles them through retry logic, logging, and database tracking.

| Mechanism         | Description                                                                                |
| ----------------- | ------------------------------------------------------------------------------------------ |
| **Retries**       | Tasks auto-retry up to 3 times with exponential backoff + jitter.                          |
| **Retry History** | `RetryHistory` table stores error codes, messages, retry delays.                           |
| **Timeouts**      | - Task-level: 240s soft, 300s hard. <br> - Polling: capped by `POLL_MAX_DURATION_SECONDS`. |
| **Logging**       | Every task logs key state transitions, errors, and retry attempts.                         |


. End-to-End Workflow

Job Submission
submit_job sends job to Moody’s API and records workflow ID.

Polling
poll_workflow_status queries status until terminal or timeout.

Analysis Evaluation
check_analysis_completion rolls up job statuses.

Automation
check_all_analyses runs periodically to catch missed analyses.

Cancellation (Optional)
cancel_job allows operators to manually stop a workflow.

Key Features:

Task Routing: Different queues for different task types (jobs, polling, recovery)
Retry Logic: Automatic retries with exponential backoff for transient errors
Status Tracking: Records all status changes and poll results
Error Handling: Handles specific HTTP errors (429, 503, etc.) appropriately
Polling Management: Automatic scheduling of next poll based on status
Analysis Completion: Checks if all jobs in analysis are done

Moody's API Mock
```
uv run uvicorn src.api.endpoints.mock_moodys:app --host 0.0.0.0 --port 8001
```
Celery Worker Run
```
uv run celery -A src.tasks.celery_app worker --loglevel=info -Q default,jobs,polling,recovery
```

Run a test for Celery jobs
```
uv run python scripts/test_celery.py
```

## Core Application

wires together repositories, services, and processors into a job orchestration system. The goal is to transform YAML-defined analyses into persisted database entities, spawn jobs, orchestrate execution, and monitor progress through completion.

- Repository layer
- Orchestrator service
- YAML processor

### Repository Layer 
Repositories encapsulate all database access logic and provide a clean, testable API for the service layer.

**BaseRepository** 

- Provides common CRUD operations (get, get_all, create, update, delete, exists).
- Keeps SQLAlchemy session handling encapsulated.

**AnalysisRepository**

Focuses on Analysis lifecycle:

- Retrieve analyses with jobs.
- Get active analyses (pending, running).
- Produce analysis summaries including job counts per status.
- Create new analyses from YAML.
- Update status and mark completion timestamps.
- Cancel an analysis and cascade cancel jobs.

**JobRepository**

Manages Job lifecycle:

- Fetch jobs with details (config, workflow, retry history).
- Track active/stale jobs.
- Update statuses with timestamps.
- Record workflow polls and retry attempts.
- Produce metrics (age, time in status, execution duration).

**ConfigurationRepository**

Handles job configurations:

- Fetch active configurations for an analysis.
- Create configurations with versioning (increment version on updates).
- Update configs by creating a new version and deactivating old ones.


Moody's API Mock
```
uv run uvicorn src.api.endpoints.mock_moodys:app --host 0.0.0.0 --port 8001
```
Celery Worker Run
```
uv run celery -A src.tasks.celery_app worker --loglevel=info -Q default,jobs,polling,recovery
```
Celery Beat
```
celery -A src.tasks.celery_app beat --loglevel=info
```
Core APIs
```
uv run uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
```


```
uv run python src/initiator.py config/sample_analysis.yaml
```



Repository Pattern: Clean database access with CRUD operations
Orchestration: Manages entire job lifecycle (submit, cancel, retry, track)
YAML Processing: Validates and processes configuration files
Auto-submission: Creates and optionally submits jobs automatically
Progress Tracking: Real-time analysis and job metrics

The system can now:

Load a YAML file
Create analysis and configurations
Generate jobs
Submit them to Mock Moody's
Track progress automatically




Testing the APIs


``` bash
# Submit a workflow
curl -X POST http://localhost:8001/mock/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "analysis_id": 1,
    "configuration_id": 1,
    "model_name": "test_model",
    "parameters": {"test": "value"}
  }'

# Check status (replace WF-xxx with actual ID from above)
curl http://localhost:8001/mock/workflows/WF-xxx/status

# Get statistics
curl http://localhost:8001/mock/stats
```

API Endpoints:

Analysis: /api/analyses - Full CRUD, submit jobs, track progress
Jobs: /api/jobs - Create, initiate, cancel, retry, force poll
System: /api/system - Health checks, recovery, statistics


Key Features:

Complete REST API for all operations
Auto-generated documentation (Swagger UI)
Real-time monitoring of jobs and analyses
Health checks for all system components
YAML upload directly via API
Progress tracking with percentages and metrics

Interactive docs: http://localhost:8000/docs
API root: http://localhost:8000
Health check: http://localhost:8000/api/system/health