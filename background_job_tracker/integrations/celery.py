"""
Celery Integration for Background Job Tracker SDK.

This module provides signal handlers that automatically capture the lifecycle
of Celery tasks (prerun, success, failure, retry, revoked) and transmit telemetry
to the Background Job Tracker SaaS backend.
"""

import contextlib
import logging
import socket
import time
import uuid
from datetime import UTC, datetime

try:
    from celery.signals import (
        task_failure,
        task_postrun,
        task_prerun,
        task_retry,
        task_revoked,
        task_success,
        worker_ready,
    )

    CELERY_AVAILABLE = True
except ImportError:  # pragma: no cover
    task_failure = task_postrun = task_prerun = task_retry = task_revoked = task_success = worker_ready = None
    CELERY_AVAILABLE = False

logger = logging.getLogger("background_job_tracker.celery")


class CeleryIntegration:
    """
    Integrates with Celery using signal connections to capture job executions.

    This class automatically discovers tasks when a Celery worker starts up
    and registers signal handlers for the task execution lifecycle.
    """

    def __init__(self, app, tracker=None):
        """
        Initialize the Celery integration.

        Args:
            app (celery.Celery): The Celery application instance.
            tracker (Tracker, optional): The Background Job Tracker client instance.
                If None, uses `Tracker.get_instance()`.
        """
        if not CELERY_AVAILABLE:
            raise ImportError(
                "Celery is required to use CeleryIntegration. "
                "Install it with: pip install 'background-job-tracker[celery]'"
            )

        if tracker is None:
            from ..client import Tracker

            tracker = Tracker.get_instance()
            if tracker is None:
                raise ValueError(
                    "No Tracker instance provided and none could be inferred from environment. "
                    "Initialize a Tracker instance or set BACKGROUND_JOB_TRACKER_API_KEY."
                )

        self.app = app
        self.tracker = tracker
        self._connected = False
        self._synced_tasks = None
        self.connect_signals()
        self._sync_tasks_if_needed()

    def _sync_tasks_if_needed(self):
        """Sync tasks with tracker if tasks are available and have changed."""
        try:
            tasks = self._get_filtered_tasks()
            if tasks and set(tasks) != self._synced_tasks:
                self.tracker.sync_tasks(tasks)
                self._synced_tasks = set(tasks)
                if hasattr(self.tracker, "set_task_provider"):
                    self.tracker.set_task_provider(self._get_filtered_tasks)
        except Exception as e:
            logger.debug(f"Task sync skipped: {e}")

    def connect_signals(self):
        """
        Connect to Celery signals idempotently.

        The dispatch_uid prevents duplicate signal connections if this method
        is called multiple times.
        """
        if self._connected:
            return

        # Ensure idempotent registration using framework-specific dispatch_uid
        task_prerun.connect(self.on_task_prerun, weak=False, dispatch_uid="bjt_celery_task_prerun")
        task_postrun.connect(self.on_task_postrun, weak=False, dispatch_uid="bjt_celery_task_postrun")
        task_success.connect(self.on_task_success, weak=False, dispatch_uid="bjt_celery_task_success")
        task_failure.connect(self.on_task_failure, weak=False, dispatch_uid="bjt_celery_task_failure")
        task_retry.connect(self.on_task_retry, weak=False, dispatch_uid="bjt_celery_task_retry")
        task_revoked.connect(self.on_task_revoked, weak=False, dispatch_uid="bjt_celery_task_revoked")
        worker_ready.connect(self.on_worker_ready, weak=False, dispatch_uid="bjt_celery_worker_ready")

        self._connected = True

    def _set_request_attr(self, request, attr_name, value):
        if isinstance(request, dict):
            request[attr_name] = value
        elif request is not None:
            with contextlib.suppress(AttributeError, TypeError):
                setattr(request, attr_name, value)

    def _get_request_attr(self, request, attr_name, default=None):
        if isinstance(request, dict):
            return request.get(attr_name, default)
        if request is not None:
            return getattr(request, attr_name, default)
        return default

    def _del_request_attr(self, request, attr_name):
        if isinstance(request, dict):
            request.pop(attr_name, None)
        elif request is not None:
            with contextlib.suppress(AttributeError, TypeError):
                delattr(request, attr_name)

    def _extract_request(self, sender, kwargs):
        """
        Extract the Celery request context from signal kwargs or sender.
        """
        request = kwargs.get("request")
        if request and (getattr(request, "id", None) or isinstance(request, dict)):
            return request

        task = kwargs.get("task") or sender
        if hasattr(task, "request") and getattr(task.request, "id", None):
            return task.request

        if sender and getattr(sender, "id", None):
            return sender

        return None

    def _extract_external_id(self, request, kwargs):
        """
        Extract the task external ID (UUID) from request or kwargs.
        """
        if request:
            if getattr(request, "id", None):
                return str(request.id)
            if isinstance(request, dict):
                req_id = request.get("id") or request.get("task_id")
                if req_id:
                    return str(req_id)
        if "task_id" in kwargs and kwargs["task_id"]:
            return str(kwargs["task_id"])
        return None

    def _extract_task_name(self, sender, kwargs):
        """
        Extract the task name (identifier) from signal kwargs or sender.
        """
        task = kwargs.get("task")
        if task and hasattr(task, "name") and task.name:
            return str(task.name)

        if hasattr(sender, "name") and sender.name:
            return str(sender.name)

        request = kwargs.get("request")
        if request:
            if getattr(request, "task", None):
                return str(request.task)
            if isinstance(request, dict) and request.get("task"):
                return str(request.get("task"))

        if "task_name" in kwargs and kwargs["task_name"]:
            return str(kwargs["task_name"])

        if isinstance(sender, str):
            return sender

        return "unknown"

    def _get_worker_name(self, request=None):
        """
        Determine the worker node executing the task.
        """
        if request:
            if getattr(request, "hostname", None):
                return str(request.hostname)
            if isinstance(request, dict) and request.get("hostname"):
                return str(request.get("hostname"))
        return socket.gethostname()

    def _get_queue_name(self, request):
        """
        Determine the queue routing key or queue name from the task request.
        """
        if request:
            delivery_info = getattr(request, "delivery_info", None)
            if isinstance(delivery_info, dict):
                return str(delivery_info.get("routing_key") or delivery_info.get("queue") or "")
            if isinstance(request, dict):
                d_info = request.get("delivery_info")
                if isinstance(d_info, dict):
                    return str(d_info.get("routing_key") or d_info.get("queue") or "")
        return ""

    def _build_base_event(self, request, task_name, kwargs=None):
        """
        Build the foundational telemetry payload shared by all task events.
        """
        kwargs = kwargs or {}
        now_utc = datetime.now(UTC)
        ext_id = self._extract_external_id(request, kwargs)

        retries = 0
        if request:
            if hasattr(request, "retries"):
                retries = getattr(request, "retries", 0)
            elif isinstance(request, dict):
                retries = request.get("retries", 0)
        if "retries" in kwargs:
            retries = kwargs["retries"]

        return {
            "event_id": uuid.uuid4().hex,
            "external_id": ext_id,
            "task_identifier": task_name,
            "framework": "celery",
            "event_timestamp": now_utc.isoformat(),
            "worker": self._get_worker_name(request),
            "queue": self._get_queue_name(request),
            "retry_count": retries,
        }

    def _add_duration(self, event, request):
        """
        Calculate the local execution duration since task_prerun.
        """
        if request:
            started_at_val = self._get_request_attr(request, "bjt_started_at")
            if started_at_val is not None:
                duration_ms = int((time.monotonic() - started_at_val) * 1000)
                event["duration_ms"] = max(0, duration_ms)

            started_at_iso = self._get_request_attr(request, "bjt_started_at_iso")
            if started_at_iso is not None:
                event["started_at"] = started_at_iso

        event["finished_at"] = event["event_timestamp"]

    def on_task_prerun(self, sender=None, **kwargs):
        """
        Celery signal handler for when a task is about to begin execution.
        """
        try:
            request = self._extract_request(sender, kwargs)
            task_name = self._extract_task_name(sender, kwargs)
            event = self._build_base_event(request, task_name, kwargs)

            if not event.get("external_id"):
                return

            now_monotonic = time.monotonic()
            if request:
                self._set_request_attr(request, "bjt_started_at", now_monotonic)
                self._set_request_attr(request, "bjt_started_at_iso", event["event_timestamp"])

            event["status"] = "running"
            event["started_at"] = event["event_timestamp"]

            self.tracker.enqueue_event(event)
        except Exception as e:
            logger.error(f"Error in Celery on_task_prerun telemetry: {e}")

    def on_task_success(self, sender=None, **kwargs):
        """
        Celery signal handler for when a task successfully completes.
        """
        try:
            request = self._extract_request(sender, kwargs)
            task_name = self._extract_task_name(sender, kwargs)
            event = self._build_base_event(request, task_name, kwargs)

            if not event.get("external_id"):
                return

            event["status"] = "success"
            self._add_duration(event, request)

            self.tracker.enqueue_event(event)
        except Exception as e:
            logger.error(f"Error in Celery on_task_success telemetry: {e}")

    def on_task_failure(self, sender=None, **kwargs):
        """
        Celery signal handler for when a task raises an unhandled exception.
        """
        try:
            request = self._extract_request(sender, kwargs)
            task_name = self._extract_task_name(sender, kwargs)
            event = self._build_base_event(request, task_name, kwargs)

            if not event.get("external_id"):
                return

            event["status"] = "failed"

            einfo = kwargs.get("einfo")
            exception = kwargs.get("exception")
            traceback_str = kwargs.get("traceback")

            if einfo:
                exc = einfo.exception if hasattr(einfo, "exception") else exception
                event["error_type"] = type(exc).__name__ if exc else ""
                event["error_message"] = str(exc) if exc else ""
                event["traceback"] = (
                    str(einfo.traceback)
                    if getattr(einfo, "traceback", None)
                    else (str(traceback_str) if traceback_str else "")
                )
            elif exception:
                event["error_type"] = type(exception).__name__
                event["error_message"] = str(exception)
                event["traceback"] = str(traceback_str) if traceback_str else ""

            self._add_duration(event, request)

            self.tracker.enqueue_event(event)
        except Exception as e:
            logger.error(f"Error in Celery on_task_failure telemetry: {e}")

    def on_task_retry(self, sender=None, **kwargs):
        """
        Celery signal handler for when a task explicitly schedules a retry.
        """
        try:
            request = self._extract_request(sender, kwargs)
            task_name = self._extract_task_name(sender, kwargs)
            event = self._build_base_event(request, task_name, kwargs)

            if not event.get("external_id"):
                return

            event["status"] = "retry"

            einfo = kwargs.get("einfo")
            reason = kwargs.get("reason")

            if einfo:
                exc = einfo.exception if hasattr(einfo, "exception") else reason
                event["error_type"] = type(exc).__name__ if exc else ""
                event["error_message"] = str(exc) if exc else ""
                event["traceback"] = str(einfo.traceback) if getattr(einfo, "traceback", None) else ""
            elif reason:
                if isinstance(reason, Exception):
                    event["error_type"] = type(reason).__name__
                    event["error_message"] = str(reason)
                else:
                    event["error_type"] = "TaskRetry"
                    event["error_message"] = str(reason)

            self._add_duration(event, request)

            self.tracker.enqueue_event(event)
        except Exception as e:
            logger.error(f"Error in Celery on_task_retry telemetry: {e}")

    def on_task_revoked(self, sender=None, **kwargs):
        """
        Celery signal handler for when a task is revoked (cancelled) before or during execution.
        """
        try:
            request = self._extract_request(sender, kwargs)
            task_name = self._extract_task_name(sender, kwargs)
            event = self._build_base_event(request, task_name, kwargs)

            if not event.get("external_id"):
                return

            event["status"] = "cancelled"
            self._add_duration(event, request)

            self.tracker.enqueue_event(event)
        except Exception as e:
            logger.error(f"Error in Celery on_task_revoked telemetry: {e}")

    def on_task_postrun(self, sender=None, **kwargs):
        """
        Celery signal handler that fires after a task completes (regardless of success/failure).
        Used here to clean up injected timing variables.
        """
        try:
            request = self._extract_request(sender, kwargs)
            if request:
                self._del_request_attr(request, "bjt_started_at")
                self._del_request_attr(request, "bjt_started_at_iso")
        except Exception:
            pass

    def _get_filtered_tasks(self):
        """
        Return all application tasks, filtering out built-in Celery framework tasks.
        """
        if not self.app or not hasattr(self.app, "tasks"):
            return []

        tasks = []
        for task_name in self.app.tasks:
            if not task_name.startswith("celery."):
                tasks.append(task_name)
        return tasks

    def on_worker_ready(self, sender=None, **kwargs):
        """
        Celery signal handler that fires once the worker is fully initialized.
        Initiates task discovery sync if tasks have changed.
        """
        try:
            if self.app:
                self._sync_tasks_if_needed()
        except Exception as e:
            logger.error(f"Error in Celery on_worker_ready telemetry: {e}")
