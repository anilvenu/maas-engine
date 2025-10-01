# Celery Setup and Tasks

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
| `default`  | General tasks (batch checks) | —        |
| `jobs`     | Job submission                  | 5        |
| `polling`  | Workflow status polling         | 3        |
| `recovery` | Recovery tasks                  | 10       |


Task Routing

submit_job → jobs queue

poll_job_status → polling queue

check_batch_completion → default queue

recovery_tasks.* → recovery queue

Periodic Tasks (Celery Beat)

| Task                     | Schedule         |
| ------------------------ | ---------------- |
| `check_all_batch`     | Every 30 minutes |
| `perform_recovery_check` | Every 60 minutes |


Job Tasks (job_tasks.py)

Job tasks handle the lifecycle of Moody’s workflows: submission, polling, and cancellation.

2.1. submit_job(job_id)

| Aspect           | Description                                                                                                                                                   |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Purpose          | Submit a job to Moody’s API and record its workflow ID.                                                                                                       |
| Input            | `job_id` (database ID of the job).                                                                                                                            |
| Output           | Dict with `workflow_id`, `status`, `message`.                                                                                                                 |
| Key Actions      | - Fetch job from DB. <br> - Submit job to Moody’s API. <br> - Save `workflow_id` and update job status to `SUBMITTED`. <br> - Schedule first poll with delay. |
| Failure Handling | - If job not found → error. <br> - If API returns retryable HTTP error → retried with exponential backoff. <br> - Errors logged in `RetryHistory`.            |

This task initiates the workflow and ensures polling starts automatically after submission.

2.2. poll_job_status(job_id, workflow_id)

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


Batch Tasks (batch_tasks.py)

Batch tasks roll up job statuses to determine the overall state of an batch.

| Aspect           | Description                                                                                                                                                                                                   |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Purpose          | Evaluate whether all jobs under an batch are complete.                                                                                                                                                     |
| Input            | `batch_id`.                                                                                                                                                                                                |
| Output           | Dict with `status` and job `counts`.                                                                                                                                                                          |
| Key Actions      | - Count job statuses (completed, failed, cancelled, active). <br> - If jobs still active → batch `RUNNING`. <br> - If all terminal → set batch to final state (completed, failed, cancelled, or mixed). |
| Failure Handling | - Return error if batch not found.                                                                                                                                                                         |

This ensures batch status always reflects the current state of its jobs.

3.2. check_all_batch()

| Aspect      | Description                                                                                                                                  |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Purpose     | Periodic sweep of all non-terminal batch.                                                                                                 |
| Input       | None.                                                                                                                                        |
| Output      | Summary dict (`checked`, `completed`, `failed`, `still_running`).                                                                            |
| Key Actions | - Find batch with status `pending` or `running`. <br> - Call `check_batch_completion` for each. <br> - Aggregate results into summary. |

Runs periodically to ensure system-wide consistency of batch states.

Failure Handling & Observability

Failures are expected in distributed systems. This app handles them through resubmit logic, logging, and database tracking.

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
poll_job_status queries status until terminal or timeout.

Batch Evaluation
check_batch_completion rolls up job statuses.

Automation
check_all_batch runs periodically to catch missed batch.

Cancellation (Optional)
cancel_job allows operators to manually stop a workflow.

Key Features:

Task Routing: Different queues for different task types (jobs, polling, recovery)
Retry Logic: Automatic retries with exponential backoff for transient errors
Status Tracking: Records all status changes and poll results
Error Handling: Handles specific HTTP errors (429, 503, etc.) appropriately
Polling Management: Automatic scheduling of next poll based on status
Batch Completion: Checks if all jobs in batch are done
