"""
RQ (Redis Queue) Integration for Background Job Tracker SDK.

Provides automated tracking for Python RQ jobs using RQ worker execution boundary wrapping,
normalizing telemetry into the standard BJT execution payload format, as well as
task discovery and registry synchronization.
"""

import contextlib
import importlib
import inspect
import logging
import socket
import sys
import time
import traceback as tb_module
import types
import uuid
from datetime import UTC, datetime

from background_job_tracker.client import Tracker

logger = logging.getLogger("background_job_tracker.rq")


class RQIntegration:
    """
    Integrates with Python RQ (Redis Queue) to capture job execution events
    and synchronize task definitions with the BJT platform.

    Provides standard callbacks and worker execution wrappers for RQ jobs
    that enqueue standardized telemetry payloads to the BJT Tracker client, as well
    as automatic task discovery.
    """

    def __init__(
        self,
        tracker=None,
        worker=None,
        auto_discover=True,
        modules=None,
        functions=None,
        queues=None,
    ):
        """
        Initialize RQ integration.

        Args:
            tracker (Tracker, optional): The BJT client instance. If None, uses Tracker.get_instance().
            worker (rq.Worker, optional): The RQ Worker instance to hook into.
            auto_discover (bool, optional): Whether to run initial task discovery and sync. Defaults to True.
            modules (list, optional): List of module names or module objects to inspect for tasks.
            functions (list, optional): List of task functions or identifier strings to register.
            queues (list, optional): List of RQ Queue instances or names to inspect.
        """
        if tracker is None:
            tracker = Tracker.get_instance()
        self.tracker = tracker
        self.worker = None

        if worker is not None:
            self.attach(worker)

        if auto_discover:
            try:
                self.sync_tasks(modules=modules, functions=functions, queues=queues)
            except Exception as e:
                logger.error(f"Error during RQ initial task synchronization: {e}")

    def attach(self, worker):
        """
        Attach integration hooks to an RQ Worker instance.

        Idempotently hooks into worker.execute_job (parent process execution boundary)
        and worker.handle_exception (child process exception detail persistence).
        Works with standard production rq.Worker (forked workhorse processes).
        """
        self.worker = worker

        if getattr(worker, "_bjt_attached", False) is True:
            return self

        # 1. Patch handle_exception (runs in child process when an exception occurs)
        # ONLY captures and persists exception info into job.meta['_bjt_last_exc']
        original_handle_exception = getattr(worker, "handle_exception", None)

        def patched_handle_exception(worker_self, job, *exc_info):
            try:
                if exc_info and exc_info[0]:
                    exc_type = exc_info[0]
                    exc_value = exc_info[1] if len(exc_info) > 1 else None
                    tb = exc_info[2] if len(exc_info) > 2 else None

                    tb_str = ""
                    if tb:
                        if isinstance(tb, str):
                            tb_str = tb
                        else:
                            try:
                                tb_str = "".join(tb_module.format_exception(exc_type, exc_value, tb))
                            except Exception:
                                tb_str = str(tb)
                    elif hasattr(job, "exc_info") and job.exc_info:
                        tb_str = str(job.exc_info)

                    error_type = exc_type.__name__ if hasattr(exc_type, "__name__") else str(exc_type)
                    error_message = str(exc_value) if exc_value is not None else ""

                    if hasattr(job, "meta"):
                        if job.meta is None:
                            job.meta = {}
                        job.meta["_bjt_last_exc"] = {
                            "error_type": error_type,
                            "error_message": error_message,
                            "traceback": tb_str,
                        }

                    if hasattr(job, "save_meta"):
                        try:
                            job.save_meta()
                        except Exception as e:
                            logger.error(f"Failed to save_meta in handle_exception: {e}")
            except Exception as e:
                logger.error(f"Error in RQ handle_exception patch: {e}")

            if original_handle_exception:
                return original_handle_exception(job, *exc_info)
            return True

        worker.handle_exception = types.MethodType(patched_handle_exception, worker)

        # 2. Patch execute_job (runs in parent process!)
        original_execute_job = getattr(worker, "execute_job", None)
        if original_execute_job:

            def patched_execute_job(worker_self, job, queue):
                task_name = self._extract_task_name(job)
                if self._is_internal_task(task_name):
                    return original_execute_job(job, queue)

                # Emit RUNNING from parent process before execution
                started_info = None
                try:
                    started_info = self.on_job_perform_start(job)
                except Exception as e:
                    logger.error(f"Error in RQ on_job_perform_start telemetry: {e}")

                exec_exception = None
                try:
                    res = original_execute_job(job, queue)
                    return res
                except Exception as exc:
                    exec_exception = exc
                    raise exc
                finally:
                    # Parent process after execute_job returns
                    try:
                        # Refresh job state from Redis if available
                        if hasattr(job, "refresh"):
                            try:
                                job.refresh()
                            except Exception as e:
                                logger.debug(f"Could not refresh RQ job: {e}")

                        last_exc = None
                        if hasattr(job, "meta") and isinstance(job.meta, dict):
                            last_exc = job.meta.pop("_bjt_last_exc", None)
                            if hasattr(job, "save_meta"):
                                with contextlib.suppress(Exception):
                                    job.save_meta()

                        if exec_exception is not None and not last_exc:
                            last_exc = {
                                "error_type": type(exec_exception).__name__,
                                "error_message": str(exec_exception),
                                "traceback": tb_module.format_exc(),
                            }

                        if last_exc or getattr(job, "is_failed", False):
                            self.on_job_failure(
                                job,
                                started_info=started_info,
                                last_exc=last_exc,
                            )
                        else:
                            self.on_job_success(
                                job,
                                started_info=started_info,
                            )
                    except Exception as e:
                        logger.error(f"Error in RQ post-execution telemetry: {e}")

            worker.execute_job = types.MethodType(patched_execute_job, worker)

        worker._bjt_attached = True

        # Perform task discovery from worker's queues
        try:
            self.sync_tasks()
        except Exception as e:
            logger.error(f"Error syncing tasks on worker attach: {e}")

        return self

    def _is_internal_task(self, task_identifier):
        """
        Check whether a task identifier is an internal/framework task.
        """
        if not task_identifier or not isinstance(task_identifier, str):
            return True

        internal_prefixes = (
            "rq.",
            "celery.",
            "django.",
            "background_job_tracker.",
            "sys.",
            "os.",
            "builtins.",
            "unittest.",
            "pytest.",
            "pip.",
            "setuptools.",
            "pkg_resources.",
            "waffle.",
            "rest_framework.",
            "allauth.",
        )

        if task_identifier.startswith(internal_prefixes):
            return True

        func_name = task_identifier.split(".")[-1]
        return func_name.startswith("_")

    def discover_tasks(self, modules=None, functions=None, queues=None):
        """
        Discover application RQ task identifiers.

        Args:
            modules (list, optional): Module names or module objects to inspect.
            functions (list, optional): Functions or task identifier strings to register.
            queues (list, optional): RQ Queue instances or queue names to inspect.

        Returns:
            list: Sorted list of discovered non-internal task identifier strings.
        """
        discovered = set()

        # 1. Functions / task identifier strings
        if functions:
            for fn in functions:
                if callable(fn):
                    mod = getattr(fn, "__module__", "")
                    qual = getattr(fn, "__qualname__", getattr(fn, "__name__", ""))
                    if mod and qual:
                        discovered.add(f"{mod}.{qual}")
                elif isinstance(fn, str):
                    discovered.add(fn)

        # 2. Modules
        if modules:
            for mod in modules:
                if isinstance(mod, str):
                    try:
                        mod = importlib.import_module(mod)
                    except ImportError:
                        continue
                if hasattr(mod, "__name__") and hasattr(mod, "__dict__"):
                    for name, obj in inspect.getmembers(mod, inspect.isfunction):
                        if getattr(obj, "__module__", None) == mod.__name__ and not name.startswith("_"):
                            discovered.add(f"{mod.__name__}.{name}")

        # 3. Queues (explicit or worker queues)
        queues_to_inspect = queues or (self.worker.queues if self.worker and hasattr(self.worker, "queues") else None)
        if queues_to_inspect:
            for q in queues_to_inspect:
                if hasattr(q, "get_job_ids"):
                    try:
                        job_ids = q.get_job_ids()
                        for jid in job_ids[:100]:
                            job = q.job_class.fetch(jid, connection=q.connection)
                            if job:
                                task_name = self._extract_task_name(job)
                                if task_name and task_name != "unknown":
                                    discovered.add(task_name)
                    except Exception as e:
                        logger.debug(f"Error inspecting RQ queue for tasks: {e}")

        # 4. Fallback: inspect loaded application modules in sys.modules
        if not functions and not modules and not queues_to_inspect and not discovered:
            for mod_name, mod in list(sys.modules.items()):
                if not mod or not hasattr(mod, "__file__") or not mod.__file__:
                    continue
                if "site-packages" in mod.__file__ or "lib/python" in mod.__file__:
                    continue
                if self._is_internal_task(mod_name):
                    continue
                for name, obj in inspect.getmembers(mod, inspect.isfunction):
                    if getattr(obj, "__module__", None) == mod_name and not name.startswith("_"):
                        discovered.add(f"{mod_name}.{name}")

        filtered = [t for t in discovered if not self._is_internal_task(t)]
        return sorted(filtered)

    def sync_tasks(self, tasks=None, modules=None, functions=None, queues=None):
        """
        Discover and synchronize task identifiers with BJT platform.

        Calls tracker.sync_tasks() with discovered or provided task identifiers.
        """
        if tasks:
            task_list = [t for t in tasks if not self._is_internal_task(t)]
        else:
            task_list = self.discover_tasks(modules=modules, functions=functions, queues=queues)

        if task_list and self.tracker:
            try:
                self.tracker.sync_tasks(sorted(task_list))
            except Exception as e:
                logger.error(f"Error sending sync_tasks command to tracker: {e}")
        return task_list

    def _extract_external_id(self, job):
        if hasattr(job, "id") and job.id:
            return str(job.id)
        if isinstance(job, dict):
            req_id = job.get("id") or job.get("job_id")
            if req_id:
                return str(req_id)
        return None

    def _extract_task_name(self, job):
        if hasattr(job, "func_name") and job.func_name:
            return str(job.func_name)
        if hasattr(job, "func") and job.func:
            if isinstance(job.func, str):
                return job.func
            if hasattr(job.func, "__module__") and hasattr(job.func, "__qualname__"):
                return f"{job.func.__module__}.{job.func.__qualname__}"
        if hasattr(job, "description") and job.description:
            desc = str(job.description)
            if "(" in desc:
                return desc.split("(")[0].strip()
            return desc
        if isinstance(job, dict):
            name = job.get("func_name") or job.get("task_identifier") or job.get("description")
            if name:
                return str(name)
        return "unknown"

    def _get_worker_name(self, job=None):
        if self.worker and hasattr(self.worker, "name") and self.worker.name:
            return str(self.worker.name)
        if job:
            if hasattr(job, "worker_name") and job.worker_name:
                return str(job.worker_name)
            if isinstance(job, dict) and job.get("worker_name"):
                return str(job.get("worker_name"))
        return socket.gethostname()

    def _get_queue_name(self, job):
        if hasattr(job, "origin") and job.origin:
            return str(job.origin)
        if isinstance(job, dict):
            queue_name = job.get("origin") or job.get("queue")
            if queue_name:
                return str(queue_name)
        return ""

    def _build_base_event(self, job, retry_count_override=None):
        now_utc = datetime.now(UTC)
        ext_id = self._extract_external_id(job)
        task_name = self._extract_task_name(job)

        if retry_count_override is not None:
            retry_count = retry_count_override
        else:
            retry_count = 0
            if hasattr(job, "retry_intervals") and job.retry_intervals:
                retries_left = getattr(job, "retries_left", None)
                if retries_left is not None:
                    retry_count = max(0, len(job.retry_intervals) - retries_left)
            elif hasattr(job, "retries_left") and getattr(job, "retries_left", None) is not None:
                retry_count = getattr(job, "retry_count", 0)
            elif isinstance(job, dict):
                retry_count = job.get("retry_count", 0)

        return {
            "event_id": uuid.uuid4().hex,
            "external_id": ext_id,
            "task_identifier": task_name,
            "framework": "rq",
            "event_timestamp": now_utc.isoformat(),
            "worker": self._get_worker_name(job),
            "queue": self._get_queue_name(job),
            "retry_count": retry_count,
        }

    def _add_duration(self, event, job, started_info=None):
        started_at_monotonic = None
        started_at_iso = None

        if started_info:
            started_at_monotonic = started_info.get("started_at_monotonic")
            started_at_iso = started_info.get("started_at_iso")

        if started_at_monotonic is None and hasattr(job, "meta") and isinstance(job.meta, dict):
            started_at_monotonic = job.meta.get("_bjt_started_at")
            started_at_iso = job.meta.get("_bjt_started_at_iso")
        elif started_at_monotonic is None and isinstance(job, dict):
            started_at_monotonic = job.get("_bjt_started_at")
            started_at_iso = job.get("_bjt_started_at_iso")

        if started_at_monotonic is not None:
            duration_ms = int((time.monotonic() - started_at_monotonic) * 1000)
            event["duration_ms"] = max(0, duration_ms)

        if started_at_iso is not None:
            event["started_at"] = started_at_iso

        event["finished_at"] = event["event_timestamp"]

    def on_job_perform_start(self, job):
        """
        Hook called immediately before an RQ job starts execution.
        Emitted from parent worker process.
        """
        try:
            event = self._build_base_event(job)
            if not event.get("external_id"):
                return None

            now_monotonic = time.monotonic()
            if hasattr(job, "meta"):
                if job.meta is None:
                    job.meta = {}
                job.meta["_bjt_started_at"] = now_monotonic
                job.meta["_bjt_started_at_iso"] = event["event_timestamp"]
            elif isinstance(job, dict):
                job["_bjt_started_at"] = now_monotonic
                job["_bjt_started_at_iso"] = event["event_timestamp"]

            event["status"] = "running"
            event["started_at"] = event["event_timestamp"]

            if self.tracker:
                self.tracker.enqueue_event(event)

            return {
                "started_at_monotonic": now_monotonic,
                "started_at_iso": event["event_timestamp"],
                "retry_count": event["retry_count"],
            }
        except Exception as e:
            logger.error(f"Error in RQ on_job_perform_start telemetry: {e}")
            return None

    def on_job_success(self, job, started_info=None, *args, **kwargs):
        """
        Hook/callback called when an RQ job finishes successfully.
        Emitted from parent worker process.
        """
        try:
            retry_count_override = started_info.get("retry_count") if started_info else None
            event = self._build_base_event(job, retry_count_override=retry_count_override)
            if not event.get("external_id"):
                return

            event["status"] = "success"
            self._add_duration(event, job, started_info=started_info)

            if self.tracker:
                self.tracker.enqueue_event(event)
        except Exception as e:
            logger.error(f"Error in RQ on_job_success telemetry: {e}")

    def on_job_failure(
        self,
        job,
        started_info=None,
        last_exc=None,
        type=None,
        value=None,
        traceback=None,
        *args,
        **kwargs,
    ):
        """
        Hook/callback called when an RQ job fails with an exception.
        Emitted from parent worker process.
        """
        try:
            retry_count_override = started_info.get("retry_count") if started_info else None
            event = self._build_base_event(job, retry_count_override=retry_count_override)
            if not event.get("external_id"):
                return

            event["status"] = "failed"

            if last_exc and isinstance(last_exc, dict):
                event["error_type"] = last_exc.get("error_type", "Exception")
                event["error_message"] = last_exc.get("error_message", "")
                event["traceback"] = last_exc.get("traceback", "")
            else:
                if type and issubclass(type, Exception):
                    event["error_type"] = type.__name__
                elif hasattr(value, "__class__"):
                    event["error_type"] = value.__class__.__name__
                else:
                    event["error_type"] = "Exception"

                event["error_message"] = str(value) if value is not None else ""

                if traceback:
                    if isinstance(traceback, str):
                        event["traceback"] = traceback
                    else:
                        try:
                            event["traceback"] = "".join(tb_module.format_tb(traceback))
                        except Exception:
                            event["traceback"] = str(traceback)
                elif hasattr(job, "exc_info") and job.exc_info:
                    event["traceback"] = str(job.exc_info)

            self._add_duration(event, job, started_info=started_info)

            if self.tracker:
                self.tracker.enqueue_event(event)
        except Exception as e:
            logger.error(f"Error in RQ on_job_failure telemetry: {e}")


def setup_rq(
    worker,
    tracker=None,
    auto_discover=True,
    modules=None,
    functions=None,
    queues=None,
):
    """
    Convenience function to set up RQ integration on an RQ worker.

    Args:
        worker (rq.Worker): The RQ Worker instance to hook into.
        tracker (Tracker, optional): The BJT Tracker instance. If None, uses Tracker.get_instance().
        auto_discover (bool, optional): Whether to run initial task discovery.
        modules (list, optional): Modules to inspect for tasks.
        functions (list, optional): Task functions or names to register.
        queues (list, optional): Queues to inspect.

    Returns:
        RQIntegration: The initialized RQ integration instance.
    """
    if tracker is None:
        tracker = Tracker.get_instance()

    return RQIntegration(
        tracker=tracker,
        worker=worker,
        auto_discover=auto_discover,
        modules=modules,
        functions=functions,
        queues=queues,
    )
