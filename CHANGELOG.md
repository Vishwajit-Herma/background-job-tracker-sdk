# Changelog

All notable changes to the `background-job-tracker` SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-09-10

### Changed
- Updated PyPI and Python version badges with official logos and fresh endpoint URLs to bypass PyPI Camo proxy cache.

## [0.1.1] - 2026-09-10

### Fixed
- Fixed relative `LICENSE` link in `README.md` to point directly to the GitHub repository file so PyPI renders it correctly.
- Updated project metadata links in `pyproject.toml`.

## [0.1.0] - 2026-09-10

### Added
- **Core Telemetry Client (`Tracker`)**:
  - Thread-safe, bounded in-memory event queue (`max_queue_size=10000`) with zero-overhead overflow drop protection to ensure monitored applications never block or experience latency degradation.
  - Asynchronous HTTP transmission via daemon worker thread (`BackgroundSender`).
  - Automatic time-based and batch-size-based flushing (`batch_size=100`, `flush_interval=5.0s`).
  - Transparent HTTP retries with exponential backoff and jitter for transient network failures (408, 429, 500, 502, 503, 504).
  - Graceful shutdown (`tracker.shutdown(timeout=5.0)`) guaranteeing in-flight queue drain on process termination.
- **Process Fork Safety**:
  - Automatic PID change detection protecting Celery prefork worker pools (`billiard` / `multiprocessing`) by isolating queue and sender thread instances per child worker process.
- **Celery Integration (`CeleryIntegration`)**:
  - Auto-instrumentation of task lifecycles via Celery signals: `task_prerun`, `task_postrun`, `task_success`, `task_failure`, `task_retry`, and `task_revoked`.
  - Captures execution duration, exceptions, stack traces, retry counts, worker hostname, and routing queues.
  - Automatic task discovery and periodic registry synchronization triggered on `worker_ready`.
- **Python RQ Integration (`RQIntegration`, `setup_rq`)**:
  - Worker execution hooks and job status tracking (`running`, `success`, `failure`, `retry`).
  - Dynamic task discovery for registered functions and Python modules.
- **Type Annotations & Packaging**:
  - PEP 561 compliance with bundled `py.typed` marker file.
  - Support for optional dependency extras: `pip install "background-job-tracker[celery]"`, `pip install "background-job-tracker[rq]"`, and `pip install "background-job-tracker[all]"`.
