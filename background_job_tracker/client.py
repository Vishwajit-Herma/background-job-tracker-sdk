"""
Client module for the Background Job Tracker SDK.

Provides the main `Tracker` class used to initialize the telemetry client,
enqueue events, and manage background transmission safely across process forks.
"""

import logging
import os
import queue
import threading

from .sender import BackgroundSender

logger = logging.getLogger("background_job_tracker")


class Tracker:
    """
    Main SDK client for tracking background job telemetry.

    The Tracker manages a thread-safe queue of telemetry events and delegates
    network transmission to a background daemon thread (`BackgroundSender`).
    It includes fork-safety mechanisms to ensure Celery prefork workers don't
    share the same background thread or queue instance.
    """

    _instance = None

    def __init__(
        self,
        api_key=None,
        base_url="http://localhost:8000",
        batch_size=100,
        flush_interval=5.0,
        max_queue_size=10000,
        task_discovery_interval=300.0,
    ):
        """
        Initialize the Tracker.

        Args:
            api_key (str, optional): Project API Key for authentication.
                Defaults to env var `BACKGROUND_JOB_TRACKER_API_KEY` or `BJT_SDK_API_KEY`.
            base_url (str, optional): Base URL of the SaaS platform.
                Defaults to env var `BACKGROUND_JOB_TRACKER_BASE_URL` or localhost.
            batch_size (int, optional): Maximum number of events to send in a single HTTP batch.
            flush_interval (float, optional): Maximum time in seconds to wait before flushing an incomplete batch.
            max_queue_size (int, optional): Maximum local queue size. Once full, new events are safely dropped.
            task_discovery_interval (float, optional): Interval in seconds for periodic task discovery sync.
        """
        self._lock = threading.Lock()

        self._api_key = api_key or os.environ.get("BACKGROUND_JOB_TRACKER_API_KEY") or os.environ.get("BJT_SDK_API_KEY")
        if not self._api_key:
            raise ValueError(
                "Background Job Tracker SDK requires a valid API key. "
                "Provide it via Tracker(api_key='...') or set the BACKGROUND_JOB_TRACKER_API_KEY environment variable."
            )

        self._base_url = (os.environ.get("BACKGROUND_JOB_TRACKER_BASE_URL") or base_url).rstrip("/")
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_queue_size = max_queue_size
        self.task_discovery_interval = task_discovery_interval
        self._task_provider = None
        self._initialized = True
        self._pid = None

        Tracker._instance = self

        # Ensure the queue and sender thread are initialized
        self._ensure_fork_safe()

    @classmethod
    def get_instance(cls):
        """
        Return the global default Tracker instance, or create one from environment variables if available.
        """
        if cls._instance is not None:
            return cls._instance
        api_key = os.environ.get("BACKGROUND_JOB_TRACKER_API_KEY") or os.environ.get("BJT_SDK_API_KEY")
        if api_key:
            cls._instance = cls(api_key=api_key)
            return cls._instance
        return None

    def set_task_provider(self, provider_func):
        """
        Register a callback function to provide a list of discovered task names.

        Args:
            provider_func (callable): A function that returns a list of task names (strings).
        """
        self._task_provider = provider_func
        if hasattr(self, "sender"):
            self.sender.set_task_provider(provider_func, self.task_discovery_interval)

    def _ensure_fork_safe(self):
        """
        Verify if the process ID has changed since the last initialization.
        If it has changed (e.g., in a Celery prefork worker), re-initialize
        the queue and spawn a new BackgroundSender thread for the new process.
        """
        current_pid = os.getpid()
        if self._pid != current_pid:
            # Re-initialize for the new process
            self._pid = current_pid
            self.event_queue = queue.Queue(maxsize=self.max_queue_size)

            self.sender = BackgroundSender(
                api_key=self._api_key,
                base_url=self._base_url,
                event_queue=self.event_queue,
                batch_size=self.batch_size,
                flush_interval=self.flush_interval,
                task_discovery_interval=self.task_discovery_interval,
                task_provider=self._task_provider,
            )
            self.sender.start()

    def enqueue_event(self, event_dict):
        """
        Non-blocking enqueue of a telemetry event.

        Args:
            event_dict (dict): The telemetry event data to send.
        """
        if not getattr(self, "_initialized", False):
            return

        # Ensure fork safety before appending to the queue
        self._ensure_fork_safe()

        try:
            self.event_queue.put_nowait(event_dict)
        except queue.Full:
            # Safely drop the event if the queue is full to prevent blocking customer tasks
            logger.warning("Background Job Tracker telemetry queue is full. Dropping event.")

    def sync_tasks(self, tasks):
        """
        Queue a task registry sync command to be processed by the background sender.

        Args:
            tasks (list): A list of task identifiers (strings).
        """
        if not getattr(self, "_initialized", False):
            return

        self._ensure_fork_safe()

        try:
            self.event_queue.put_nowait({"_type": "sync_tasks", "tasks": list(tasks)})
        except queue.Full:
            logger.warning("Background Job Tracker telemetry queue is full. Dropping sync command.")

    def shutdown(self, timeout=5.0):
        """
        Gracefully shut down the background sender thread, flushing pending events.

        Args:
            timeout (float): Maximum time to wait for the sender thread to join.
        """
        if hasattr(self, "sender") and self.sender.is_alive():
            self.sender.stop(timeout=timeout)
