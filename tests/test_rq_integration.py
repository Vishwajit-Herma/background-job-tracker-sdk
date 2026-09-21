import time
from unittest import mock

import pytest

rq = pytest.importorskip("rq")
import redis  # noqa: E402
from rq import Queue, Retry, Worker  # noqa: E402
from rq.registry import ScheduledJobRegistry  # noqa: E402

from background_job_tracker.integrations.rq import RQIntegration, setup_rq  # noqa: E402


class DummyRQJob:
    def __init__(self, id, func_name="tasks.process_data", origin="default"):
        self.id = id
        self.func_name = func_name
        self.origin = origin
        self.meta = {}
        self.retry_intervals = [10, 30]
        self.retries_left = 1
        self.worker_name = "rq-worker-node-1"


def sample_app_task():
    return "ok"


def failing_app_task():
    raise ValueError("Sample task error")


def _internal_private_helper():
    pass


def test_rq_integration_perform_start():
    tracker = mock.Mock()
    integration = RQIntegration(tracker, auto_discover=False)
    job = DummyRQJob("rq_job_123")

    started_info = integration.on_job_perform_start(job)

    assert tracker.enqueue_event.call_count == 1
    event = tracker.enqueue_event.call_args[0][0]

    assert event["status"] == "running"
    assert event["external_id"] == "rq_job_123"
    assert event["task_identifier"] == "tasks.process_data"
    assert event["framework"] == "rq"
    assert event["worker"] == "rq-worker-node-1"
    assert event["queue"] == "default"
    assert event["retry_count"] == 1  # 2 total intervals - 1 left = 1 retried
    assert "_bjt_started_at" in job.meta
    assert started_info["retry_count"] == 1


def test_rq_integration_success():
    tracker = mock.Mock()
    integration = RQIntegration(tracker, auto_discover=False)
    job = DummyRQJob("rq_job_456")

    started_info = integration.on_job_perform_start(job)
    time.sleep(0.01)
    integration.on_job_success(job, started_info=started_info)

    assert tracker.enqueue_event.call_count == 2
    event = tracker.enqueue_event.call_args[0][0]

    assert event["status"] == "success"
    assert event["external_id"] == "rq_job_456"
    assert event["framework"] == "rq"
    assert "duration_ms" in event
    assert event["duration_ms"] >= 0
    assert "started_at" in event
    assert "finished_at" in event


def test_rq_integration_failure():
    tracker = mock.Mock()
    integration = RQIntegration(tracker, auto_discover=False)
    job = DummyRQJob("rq_job_789")

    started_info = integration.on_job_perform_start(job)

    last_exc = {
        "error_type": "KeyError",
        "error_message": "Invalid key lookup in RQ job",
        "traceback": "Traceback (most recent call last):\n  File 'worker.py', line 5\nKeyError",
    }
    integration.on_job_failure(job, started_info=started_info, last_exc=last_exc)

    assert tracker.enqueue_event.call_count == 2
    event = tracker.enqueue_event.call_args[0][0]

    assert event["status"] == "failed"
    assert event["external_id"] == "rq_job_789"
    assert event["framework"] == "rq"
    assert event["error_type"] == "KeyError"
    assert "Invalid key lookup" in event["error_message"]
    assert "Traceback" in event["traceback"]


def test_rq_telemetry_isolation_on_exception():
    tracker = mock.Mock()
    tracker.enqueue_event.side_effect = RuntimeError("Internal telemetry error")
    integration = RQIntegration(tracker, auto_discover=False)
    job = DummyRQJob("rq_job_err")

    try:
        started_info = integration.on_job_perform_start(job)
        integration.on_job_success(job, started_info=started_info)
        integration.on_job_failure(job, started_info=started_info, value=ValueError("test"))
    except Exception as e:
        pytest.fail(f"RQ telemetry handler raised exception: {e}")


def test_rq_dict_job_handling():
    tracker = mock.Mock()
    integration = RQIntegration(tracker, auto_discover=False)

    dict_job = {
        "id": "dict_rq_001",
        "func_name": "dict_tasks.send_webhook",
        "origin": "webhooks",
        "worker_name": "worker-dict-1",
        "retry_count": 2,
    }

    started_info = integration.on_job_perform_start(dict_job)
    integration.on_job_success(dict_job, started_info=started_info)

    assert tracker.enqueue_event.call_count == 2
    start_event = tracker.enqueue_event.call_args_list[0][0][0]
    success_event = tracker.enqueue_event.call_args_list[1][0][0]

    assert start_event["external_id"] == "dict_rq_001"
    assert start_event["task_identifier"] == "dict_tasks.send_webhook"
    assert start_event["framework"] == "rq"
    assert start_event["queue"] == "webhooks"
    assert start_event["worker"] == "worker-dict-1"
    assert success_event["status"] == "success"


def test_rq_task_discovery_explicit_functions_and_modules():
    tracker = mock.Mock()
    integration = RQIntegration(tracker, auto_discover=False)

    discovered = integration.discover_tasks(
        functions=[
            sample_app_task,
            _internal_private_helper,
            "custom.external_task",
            "rq.queue.enqueue",
        ]
    )

    assert "custom.external_task" in discovered
    assert any("sample_app_task" in t for t in discovered)
    assert not any("_internal_private_helper" in t for t in discovered)
    assert not any(t.startswith("rq.") for t in discovered)


def test_rq_internal_task_exclusion():
    tracker = mock.Mock()
    integration = RQIntegration(tracker, auto_discover=False)

    assert integration._is_internal_task("rq.worker.perform_job") is True
    assert integration._is_internal_task("celery.ping") is True
    assert integration._is_internal_task("django.contrib.admin") is True
    assert integration._is_internal_task("background_job_tracker.client") is True
    assert integration._is_internal_task("app.tasks._private_func") is True
    assert integration._is_internal_task("app.tasks.process_order") is False


def test_rq_sync_tasks_calls_tracker():
    tracker = mock.Mock()
    integration = RQIntegration(tracker, auto_discover=False)

    integration.sync_tasks(tasks=["app.tasks.process_payment", "rq.internal.task", "app.tasks.send_email"])

    assert tracker.sync_tasks.call_count == 1
    synced_tasks = tracker.sync_tasks.call_args[0][0]

    assert "app.tasks.process_payment" in synced_tasks
    assert "app.tasks.send_email" in synced_tasks
    assert "rq.internal.task" not in synced_tasks


def test_rq_worker_attach_idempotent():
    tracker = mock.Mock()
    worker = mock.Mock()
    worker.connection = mock.Mock()
    setup_rq(worker, tracker=tracker, auto_discover=False)

    assert getattr(worker, "_bjt_attached", False) is True
    assert hasattr(worker, "handle_exception")
    assert hasattr(worker, "execute_job")

    # Second call must be idempotent
    integration2 = setup_rq(worker, tracker=tracker, auto_discover=False)
    assert integration2.worker is worker


def test_rq_worker_real_execution_flow():
    """
    Integration test using a Worker (forked process execution)
    to verify complete parent/child process execution flow telemetry.
    """
    tracker = mock.Mock()
    try:
        r = redis.Redis(host="localhost", port=6379, db=0)
        r.ping()
    except Exception:
        pytest.skip("Redis not reachable for RQ integration test")

    queue = Queue("bjt-rq-test-queue", connection=r)
    queue.empty()

    worker = Worker([queue], connection=r)
    setup_rq(worker, tracker=tracker, auto_discover=False)

    # 1. Successful job
    job1 = queue.enqueue(sample_app_task)
    worker.work(burst=True)

    # Check telemetry events emitted to tracker
    events = [call[0][0] for call in tracker.enqueue_event.call_args_list]

    job1_events = [e for e in events if e.get("external_id") == job1.id]
    assert len(job1_events) == 2
    assert job1_events[0]["status"] == "running"
    assert job1_events[1]["status"] == "success"
    assert job1_events[1]["duration_ms"] >= 0

    # Reset tracker mock
    tracker.reset_mock()

    # 2. Failing job
    job2 = queue.enqueue(failing_app_task)
    worker.work(burst=True)

    events2 = [call[0][0] for call in tracker.enqueue_event.call_args_list]
    job2_events = [e for e in events2 if e.get("external_id") == job2.id]
    assert len(job2_events) == 2
    assert job2_events[0]["status"] == "running"
    assert job2_events[1]["status"] == "failed"
    assert job2_events[1]["error_type"] == "ValueError"
    assert "Sample task error" in job2_events[1]["error_message"]
    assert "Traceback" in job2_events[1]["traceback"]


def test_rq_worker_retry_flow():
    """
    Test job retry flow using real Worker to ensure events are recorded for each attempt.
    """
    tracker = mock.Mock()
    try:
        r = redis.Redis(host="localhost", port=6379, db=0)
        r.ping()
    except Exception:
        pytest.skip("Redis not reachable for RQ integration test")

    queue = Queue("bjt-rq-test-retry-queue", connection=r)
    queue.empty()

    worker = Worker([queue], connection=r)
    setup_rq(worker, tracker=tracker, auto_discover=False)

    job = queue.enqueue(failing_app_task, retry=Retry(max=1, interval=0))

    # First attempt
    worker.work(burst=True)

    # Re-enqueue scheduled retries manually
    registry = ScheduledJobRegistry(queue=queue)
    for job_id in registry.get_job_ids():
        job_to_retry = queue.fetch_job(job_id)
        if job_to_retry:
            registry.remove(job_to_retry)
            queue.enqueue_job(job_to_retry)

    # Second attempt
    worker.work(burst=True)

    # Check telemetry events emitted to tracker
    events = [call[0][0] for call in tracker.enqueue_event.call_args_list]
    job_events = [e for e in events if e.get("external_id") == job.id]

    assert len(job_events) == 4

    # First attempt events
    assert job_events[0]["status"] == "running"
    assert job_events[0]["retry_count"] == 0
    assert job_events[1]["status"] == "failed"
    assert job_events[1]["retry_count"] == 0

    # Second attempt events
    assert job_events[2]["status"] == "running"
    assert job_events[2]["retry_count"] == 1
    assert job_events[3]["status"] == "failed"
    assert job_events[3]["retry_count"] == 1
