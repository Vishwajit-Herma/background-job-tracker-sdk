import os
import time
from unittest import mock

import pytest

from background_job_tracker.client import Tracker
from background_job_tracker.integrations.celery import CeleryIntegration


class DummyRequest:
    def __init__(self, id, hostname="worker-1", retries=0, routing_key="celery"):
        self.id = id
        self.hostname = hostname
        self.retries = retries
        self.delivery_info = {"routing_key": routing_key} if routing_key else {}


class DummyTask:
    def __init__(self, name, req_id="ext_id_1"):
        self.name = name
        self.request = DummyRequest(req_id)


def test_celery_integration_prerun():
    tracker = mock.Mock()
    app = mock.Mock()

    integration = CeleryIntegration(app, tracker)
    task = DummyTask("demo_task", "ext_prerun_1")

    integration.on_task_prerun(sender=task, task_id="ext_prerun_1", task=task)
    assert hasattr(task.request, "bjt_started_at")
    assert hasattr(task.request, "bjt_started_at_iso")
    assert tracker.enqueue_event.call_count == 1

    event = tracker.enqueue_event.call_args[0][0]
    assert event["status"] == "running"
    assert event["external_id"] == "ext_prerun_1"
    assert event["task_identifier"] == "demo_task"
    assert event["framework"] == "celery"
    assert event["worker"] == "worker-1"
    assert event["queue"] == "celery"
    assert event["retry_count"] == 0
    assert "started_at" in event


def test_celery_integration_success_and_duration():
    tracker = mock.Mock()
    app = mock.Mock()

    integration = CeleryIntegration(app, tracker)
    task = DummyTask("demo_task", "ext_success_1")

    # Prerun to establish start time
    integration.on_task_prerun(sender=task, task_id="ext_success_1", task=task)
    time.sleep(0.01)

    # Success
    integration.on_task_success(sender=task, task_id="ext_success_1", result="ok")
    assert tracker.enqueue_event.call_count == 2
    event = tracker.enqueue_event.call_args[0][0]

    assert event["status"] == "success"
    assert event["external_id"] == "ext_success_1"
    assert event["framework"] == "celery"
    assert "duration_ms" in event
    assert event["duration_ms"] >= 10
    assert "started_at" in event
    assert "finished_at" in event

    # Postrun cleanup
    integration.on_task_postrun(sender=task, task_id="ext_success_1", task=task)
    assert not hasattr(task.request, "bjt_started_at")


def test_celery_integration_failure_with_traceback():
    tracker = mock.Mock()
    integration = CeleryIntegration(mock.Mock(), tracker)
    task = DummyTask("demo_task", "ext_fail_1")

    class DummyEinfo:
        exception = ValueError("Simulated database connection error")
        traceback = "Traceback (most recent call last):\n  File 'app.py', line 10, in task\nValueError"

    integration.on_task_failure(sender=task, task_id="ext_fail_1", einfo=DummyEinfo())

    assert tracker.enqueue_event.call_count == 1
    event = tracker.enqueue_event.call_args[0][0]
    assert event["status"] == "failed"
    assert event["external_id"] == "ext_fail_1"
    assert event["framework"] == "celery"
    assert event["error_type"] == "ValueError"
    assert event["error_message"] == "Simulated database connection error"
    assert "Traceback" in event["traceback"]


def test_celery_integration_retry_and_retry_count():
    tracker = mock.Mock()
    integration = CeleryIntegration(mock.Mock(), tracker)
    task = DummyTask("retry_task", "ext_retry_1")

    class DummyEinfo:
        exception = RuntimeError("Service unavailable")
        traceback = "Traceback line 1..."

    # Prerun 1
    integration.on_task_prerun(sender=task, task_id="ext_retry_1", task=task)
    event_run1 = tracker.enqueue_event.call_args[0][0]

    # Retry signal
    integration.on_task_retry(sender=task, task_id="ext_retry_1", einfo=DummyEinfo())
    event_retry = tracker.enqueue_event.call_args[0][0]

    # Prerun 2 (Celery increments retry count)
    task.request.retries = 1
    integration.on_task_prerun(sender=task, task_id="ext_retry_1", task=task)
    event_run2 = tracker.enqueue_event.call_args[0][0]

    assert event_run1["status"] == "running"
    assert event_run1["retry_count"] == 0

    assert event_retry["status"] == "retry"
    assert event_retry["error_type"] == "RuntimeError"

    assert event_run2["status"] == "running"
    assert event_run2["retry_count"] == 1


def test_celery_integration_revoked():
    tracker = mock.Mock()
    integration = CeleryIntegration(mock.Mock(), tracker)
    task = DummyTask("revoked_task", "ext_revoked_1")

    # Prerun
    integration.on_task_prerun(sender=task, task_id="ext_revoked_1", task=task)

    # Revoked signal
    integration.on_task_revoked(sender=task, request=task.request, terminated=True, signum=9)

    assert tracker.enqueue_event.call_count == 2
    event = tracker.enqueue_event.call_args[0][0]
    assert event["status"] == "cancelled"
    assert event["external_id"] == "ext_revoked_1"
    assert event["task_identifier"] == "revoked_task"


def test_worker_and_queue_extraction():
    tracker = mock.Mock()
    integration = CeleryIntegration(mock.Mock(), tracker)

    # Custom request object with dict delivery info containing 'queue'
    req_dict = {
        "id": "dict_req_123",
        "task": "dict_task",
        "hostname": "custom-node-99",
        "delivery_info": {"queue": "high-priority"},
    }

    integration.on_task_prerun(sender="dict_task", request=req_dict)
    event = tracker.enqueue_event.call_args[0][0]

    assert event["external_id"] == "dict_req_123"
    assert event["task_identifier"] == "dict_task"
    assert event["worker"] == "custom-node-99"
    assert event["queue"] == "high-priority"


def test_duplicate_signal_registration():
    tracker = mock.Mock()
    app = mock.Mock()

    integration1 = CeleryIntegration(app, tracker)
    integration2 = CeleryIntegration(app, tracker)

    # Ensure connecting twice is idempotent and does not raise
    integration1.connect_signals()
    integration2.connect_signals()

    task = DummyTask("demo_task", "ext_dup_1")
    integration1.on_task_prerun(sender=task, task_id="ext_dup_1", task=task)
    assert tracker.enqueue_event.call_count == 1


def test_task_discovery():
    tracker = mock.Mock()
    app = mock.Mock()
    app.tasks = {
        "app.tasks.process_order": mock.Mock(),
        "app.tasks.send_email": mock.Mock(),
        "celery.chord": mock.Mock(),
        "celery.backend_cleanup": mock.Mock(),
    }

    integration = CeleryIntegration(app, tracker)
    integration.on_worker_ready()

    # Built-in celery.* tasks should be excluded
    tracker.sync_tasks.assert_called_once_with(["app.tasks.process_order", "app.tasks.send_email"])
    tracker.set_task_provider.assert_called_once()


def test_telemetry_isolation_on_exception():
    tracker = mock.Mock()
    tracker.enqueue_event.side_effect = RuntimeError("SDK internal failure")

    integration = CeleryIntegration(mock.Mock(), tracker)
    task = DummyTask("error_task", "ext_err_1")

    # Signal handlers must log but never raise exceptions to customer code
    try:
        integration.on_task_prerun(sender=task, task_id="ext_err_1", task=task)
        integration.on_task_success(sender=task, task_id="ext_err_1")
        integration.on_task_failure(sender=task, task_id="ext_err_1")
    except Exception as e:
        pytest.fail(f"Telemetry signal raised exception: {e}")


def test_fork_safety_reinitialization():
    client = Tracker(api_key="test_api_key", base_url="http://localhost:8000")
    original_pid = client._pid
    original_sender = client.sender
    original_queue = client.event_queue

    # Simulate process fork by altering _pid
    client._pid = original_pid - 100

    client.enqueue_event({"external_id": "fork_test", "status": "running"})

    assert client._pid == os.getpid()
    assert client.sender is not original_sender
    assert client.event_queue is not original_queue

    client.shutdown(timeout=1.0)
