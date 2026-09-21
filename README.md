# Background Job Tracker Python SDK

[![PyPI Version](https://img.shields.io/pypi/v/background-job-tracker?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/background-job-tracker/)
[![Python Versions](https://img.shields.io/pypi/pyversions/background-job-tracker?logo=python&logoColor=white)](https://pypi.org/project/background-job-tracker/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Celery Support](https://img.shields.io/badge/Celery-5.0+-brightgreen.svg)](https://docs.celeryq.dev/)
[![RQ Support](https://img.shields.io/badge/RQ-1.10+-red.svg)](https://python-rq.org/)
[![Typing: Typed](https://img.shields.io/badge/Typing-PEP%20561-informational.svg)](https://peps.python.org/pep-0561/)

The official Python SDK for **Background Job Tracker (BJT)** — an end-to-end reliability, telemetry, and automated incident detection platform for background task workers.

Turn opaque background tasks into fully observable, auditable workflows. Automatically capture task durations, retries, failures, tracebacks, worker hostnames, and queue latencies without impacting application performance.

---

## Key Highlights

- ⚡ **Zero Performance Impact**: Telemetry is non-blocking. Events are placed in a memory-bounded local queue and flushed asynchronously by a background daemon thread.
- 🛡️ **Zero Failure Impact**: The SDK shields your tasks. Network partitions, timeouts, or 5xx backend errors never cause your customer-facing background tasks to fail.
- 🍴 **Process-Fork Safe**: Built-in PID change detection automatically resets isolation barriers when Celery forks worker processes (e.g. `prefork` pool), preventing shared lock or dead-thread issues.
- 🔄 **Intelligent Resilience**: Outgoing batches automatically retry with exponential backoff and jitter upon receiving `429 Too Many Requests` or transient `5xx` errors.
- 🔍 **Task Auto-Discovery**: Automatically syncs discovered task signatures and queues with your dashboard on worker startup.
- 📦 **First-Class Integrations**: Drop-in support for **Celery** and **Python RQ** with 3 lines of code.

---

## Installation

Install the base package with `pip`:

```bash
pip install background-job-tracker
```

### With Framework Extras

Install with pre-configured dependencies for your task queue:

```bash
# For Celery
pip install "background-job-tracker[celery]"

# For Python RQ (Redis Queue)
pip install "background-job-tracker[rq]"

# For all supported frameworks
pip install "background-job-tracker[all]"
```

Using `uv`:
```bash
uv add background-job-tracker
```

Using `poetry`:
```bash
poetry add background-job-tracker
```

---

## Quickstart

### 1. Celery (Django or Standalone)

Integrate Background Job Tracker into your Celery application in 3 lines of code.

#### Step 1: Set Your Environment Variables
Export your project credentials (found in your Background Job Tracker dashboard):

```bash
export BACKGROUND_JOB_TRACKER_API_KEY="bjt_live_xxxxxxxxxxxxxxxx"
# Point to your BJT backend API ingestion URL (NOT the frontend dashboard URL)
# For local dev: http://localhost:8000 | For self-hosted/production: https://your-bjt-api.example.com
export BACKGROUND_JOB_TRACKER_BASE_URL="http://localhost:8000"
```

#### Step 2: Initialize in `celery.py`

In your Django `celery.py` (or wherever your `Celery` app instance is configured):

```python
import os
from celery import Celery
from background_job_tracker import Tracker
from background_job_tracker.integrations.celery import CeleryIntegration

# Standard Celery application setup
app = Celery("my_project")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

# Initialize the Background Job Tracker telemetry
tracker = Tracker()  # Automatically reads BACKGROUND_JOB_TRACKER_API_KEY from environment
CeleryIntegration(app=app, tracker=tracker)
```

That's it! When your Celery worker starts up:
1. `worker_ready` signal auto-registers all defined Celery tasks with the platform.
2. `task_prerun`, `task_postrun`, `task_success`, `task_failure`, `task_retry`, and `task_revoked` signals automatically report lifecycle executions.

---

### 2. Python RQ (Redis Queue)

To monitor Python RQ workers, attach the `RQIntegration` to your RQ worker script:

```python
import os
from redis import Redis
from rq import Worker, Queue
from background_job_tracker import Tracker
from background_job_tracker.integrations.rq import RQIntegration

# 1. Initialize Tracker
tracker = Tracker(
    api_key=os.environ["BACKGROUND_JOB_TRACKER_API_KEY"],
    base_url=os.getenv("BACKGROUND_JOB_TRACKER_BASE_URL", "http://localhost:8000"),
)

# 2. Configure RQ Integration
#    Pass modules or functions to automatically discover task names
rq_integration = RQIntegration(
    tracker=tracker,
    modules=["my_app.tasks"],
)

# 3. Attach integration to your worker
if __name__ == "__main__":
    redis_conn = Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    queue = Queue("default", connection=redis_conn)

    worker = Worker([queue], connection=redis_conn)
    rq_integration.attach(worker)

    # Start processing jobs
    worker.work(with_scheduler=True)
```

Alternatively, use the convenient one-liner helper `setup_rq`:

```python
from background_job_tracker.integrations.rq import setup_rq

setup_rq(worker, tracker=tracker, modules=["my_app.tasks"])
worker.work()
```

---

### 3. Manual / Custom Task Tracking

If you run custom background threads, asyncio workers, or non-standard queue systems, you can directly enqueue execution events:

```python
from background_job_tracker import Tracker

tracker = Tracker(api_key="bjt_live_xxxxxxxxxxxxxxxx")

# Track an execution event
tracker.enqueue_event(
    {
        "task_identifier": "reports.generate_monthly_pdf",
        "external_id": "job_984572049",
        "status": "success",  # "running" | "success" | "failure" | "retry" | "revoked"
        "duration_seconds": 2.45,
        "started_at": "2026-09-10T12:00:00.000000Z",
        "completed_at": "2026-09-10T12:00:02.450000Z",
        "worker": "worker-node-1",
        "queue": "reports",
        "retry_count": 0,
        "metadata": {
            "report_id": 402,
            "format": "pdf",
        },
    }
)

# In CLI scripts or shutdown hooks, ensure all events are flushed:
tracker.shutdown(timeout=5.0)
```

---

## Configuration Reference

The `Tracker` client can be configured programmatically or via environment variables:

| Setting | Environment Variable | Default | Description |
|---|---|---|---|
| **API Key** | `BACKGROUND_JOB_TRACKER_API_KEY` (or `BJT_SDK_API_KEY`) | *Required* | Project API key generated in the BJT Dashboard. |
| **Base URL** | `BACKGROUND_JOB_TRACKER_BASE_URL` | `http://localhost:8000` | Backend API ingestion URL (e.g. `http://localhost:8000` for local dev or `https://your-bjt-api.example.com` for production). Must point to the backend API, not the frontend. |
| **Batch Size** | `batch_size` | `100` | Maximum number of events bundled into a single HTTP POST request. |
| **Flush Interval** | `flush_interval` | `5.0` | Maximum seconds to wait before flushing an incomplete batch. |
| **Max Queue Size** | `max_queue_size` | `10000` | In-memory queue limit. If reached, new events are safely dropped. |
| **Discovery Interval**| `task_discovery_interval` | `300.0` | Seconds between periodic background task discovery syncs. |

### Example with Explicit Options

```python
tracker = Tracker(
    api_key="bjt_live_xxxxxxxxxxxxxxxx",
    base_url="http://localhost:8000",  # or your production backend URL: https://your-bjt-api.example.com
    batch_size=50,  # Flush after 50 events
    flush_interval=2.0,  # Or flush every 2 seconds
    max_queue_size=20000,  # Buffer up to 20,000 events in memory
    task_discovery_interval=600.0,  # Sync task list every 10 minutes
)
```

---

## Architecture & Zero-Impact Guarantee

```
┌─────────────────────────────────────────────────────────────────┐
│                     Customer Worker Process                     │
│                                                                 │
│   [Celery / RQ Task]                                            │
│          │                                                      │
│          ▼ (Signals / Hooks)                                    │
│   [Tracker.enqueue_event]                                       │
│          │  (Non-blocking put_nowait < 0.01ms)                  │
│          ▼                                                      │
│   ┌───────────────┐                                             │
│   │ Bounded Queue │  (If queue is full: safely drops event,     │
│   └───────┬───────┘   never blocks execution or starves memory) │
│           │                                                     │
│           │ (Internal thread consumption)                       │
│           ▼                                                     │
│   ┌─────────────────────────────┐                               │
│   │   BackgroundSender Thread   │                               │
│   │  - Batches events           │                               │
│   │  - Handles HTTP retry logic │                               │
│   │  - Exponential backoff      │                               │
│   └─────────────┬───────────────┘                               │
└─────────────────┼───────────────────────────────────────────────┘
                  │
                  ▼ (Asynchronous HTTPS POST)
       ┌────────────────────────┐
       │ Background Job Tracker │
       │     SaaS Backend       │
       └────────────────────────┘
```

1. **Non-blocking queue enqueue**: When a task completes, the signal handler performs a `put_nowait()` on an in-memory `queue.Queue`. This completes in microseconds (< 0.01ms).
2. **Dedicated daemon sender thread**: A separate daemon thread extracts batches and transmits them via HTTP.
3. **Queue overflow drop protection**: If the network connection goes down and the queue reaches `max_queue_size` (10,000 items), subsequent events are dropped with a warning. Your workers will **never** run out of memory or pause task processing.
4. **Fork safety in Celery**: Celery prefork workers use `fork()` without `exec()`. Background threads do not survive forks. The SDK detects the PID change upon the first signal inside a child worker and automatically spawns a dedicated queue and sender thread for that process.

---

## Debugging & Logging

The SDK logs diagnostic messages through the standard Python `logging` module under the logger hierarchy `background_job_tracker`:

```python
import logging

# Enable verbose logging for the SDK
logging.getLogger("background_job_tracker").setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"))
logging.getLogger("background_job_tracker").addHandler(handler)
```

---

## Testing & Verification

To run unit tests locally:

```bash
# Clone the repository
git clone https://github.com/Vishwajit-Herma/background-job-tracker-sdk.git
cd background-job-tracker-sdk

# Install dependencies and test runner
pip install -e ".[all,dev]"

# Run tests
pytest
```

---

## License

This project is licensed under the MIT License — see the [LICENSE](https://github.com/Vishwajit-Herma/background-job-tracker-sdk/blob/main/LICENSE) file for details.
