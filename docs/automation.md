# Automation Overview

This document outlines how the post-webhook automation pipeline operates after the
refactors in this change-set.

## Components

- **Task helpers** (`src/tasks/pipeline.py`)
  - Provide callable functions for rehydrate flows, LLM backfill, Monday sync, and
    project-by-ID refreshes.
- **Queue worker** (`src/services/queue_worker.py`)
  - In-process async worker that runs rehydrate → analyse → Monday push jobs.
  - Persists job status to the new `job_queue` table for observability.
- **Webhook integration** (`src/webhooks/webhook_server.py`)
  - Subitem/hidden item updates enqueue rehydrate jobs for affected parent projects.
  - Parent item updates still perform immediate analysis and enqueue a Monday push.
- **Scheduler** (`src/api/app.py`)
  - APScheduler runs three periodic jobs:
    - Hourly delta rehydrate (`rehydrate_delta`).
    - Nightly LLM backfill (`backfill_llm`).
    - 10-minute Monday sync (`sync_projects_to_monday`).
  - Queue worker is started alongside the FastAPI app.

## Operations

### Monitoring Jobs

The `job_queue` table records every queue task with status transitions (`queued`,
`running`, `completed`, `failed`). Use Supabase SQL or dashboards to monitor queue
health.

### Running Manually

All previous scripts still exist as thin wrappers around the task helpers. They can
be invoked manually if required for smoke tests or emergency replays.

### Adding New Tasks

1. Implement the core logic inside `src/tasks/pipeline.py`.
2. Add a queue handler in `queue_worker.py` and expose enqueue helpers.
3. Wire the new tasks via webhooks or scheduler as appropriate.


