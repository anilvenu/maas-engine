
# IRP Model as a Service Engine - POC

## Architecture

### Solution Components
```
┌─────────────────────────────────────────────┐
│               Docker Container              │
│                                             │
│  ┌─────────────────┐  ┌──────────────────┐  │
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
│  │  Celery Workers │  │  Celery Beat     │  │
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
→ Create Jobs (status: pending) → Queue Submission Tasks
→ Submit to Moody's (via Orchestration Layer) → Update to submitted
→ Store Celery Task ID → Schedule Polling Tasks
```

```
               deploy
        ┌──────────┐
        │ pending  │
        └────┬─────┘
             │ initiate
        ┌────▼─────┐
        │submitted │
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
                                       │ pending   │
                                       │(new job)  │
                                       └───────────┘

```

**Polling Strategy**

Initial Poll: 30 seconds after submission
Subsequent Polls: Based on job status
Max poll duration: configurable

**Analysis Completion Check**

Runs every N minutes via Celery Beat
Checks all "running" analysis
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

## Tasks

docs/TASKS.md


## Moody's API Mock
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

wires together repositories, services, and processors into a job orchestration system. The goal is to transform YAML-defined analysis into persisted database entities, spawn jobs, orchestrate execution, and monitor progress through completion.

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

- Retrieve analysis with jobs.
- Get active analysis (pending, running).
- Produce analysis summaries including job counts per status.
- Create new analysis from YAML.
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

Analysis: /api/analysis - Full CRUD, submit jobs, track progress
Jobs: /api/jobs - Create, initiate, cancel, retry, force poll
System: /api/system - Health checks, recovery, statistics


Key Features:

Complete REST API for all operations
Auto-generated documentation (Swagger UI)
Real-time monitoring of jobs and analysis
Health checks for all system components
YAML upload directly via API
Progress tracking with percentages and metrics

Interactive docs: http://localhost:8000/docs
API root: http://localhost:8000
Health check: http://localhost:8000/api/system/health