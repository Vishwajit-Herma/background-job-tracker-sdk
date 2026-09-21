"""
Sender module for the Background Job Tracker SDK.

Provides the `BackgroundSender` daemon thread which processes the in-memory queue,
batches telemetry events, and reliably transmits them to the SaaS backend using
exponential backoff and retry policies.
"""

import contextlib
import logging
import queue
import random
import threading
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("background_job_tracker.sender")


class BackgroundSender(threading.Thread):
    """
    A daemon thread that reads from a local telemetry queue, batches events,
    and transmits them via HTTP.

    It supports:
      - Time and size-based batching.
      - Periodic polling of a task registry (discovery).
      - Transparent HTTP retries for 5xx/timeout errors.
      - Fallback exponential backoff for persistent transient errors, which blocks
        the sender thread but leaves the application's task execution unaffected.
    """

    def __init__(
        self,
        api_key,
        base_url,
        event_queue,
        batch_size=100,
        flush_interval=5.0,
        task_discovery_interval=300.0,
        task_provider=None,
    ):
        """
        Initialize the BackgroundSender.

        Args:
            api_key (str): Project API Key for authentication.
            base_url (str): Base URL of the SaaS platform.
            event_queue (queue.Queue): The thread-safe queue containing telemetry events.
            batch_size (int, optional): Max events per batch.
            flush_interval (float, optional): Max wait time in seconds before flushing a batch.
            task_discovery_interval (float, optional): Wait time between automatic task registry syncs.
            task_provider (callable, optional): A function returning list of discovered tasks.
        """
        super().__init__(name="BackgroundJobTrackerSender")
        self.daemon = True
        self.api_key = api_key
        self.base_url = base_url
        self.event_queue = event_queue
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self.task_discovery_interval = task_discovery_interval
        self.task_provider = task_provider

        self._stop_event = threading.Event()

        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({"X-API-Key": self.api_key})
        self.session.headers.update({"Content-Type": "application/json"})

        # Exponential backoff for 5xx and connection errors inside requests library
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=["POST"],
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def set_task_provider(self, provider_func, interval):
        """
        Update the task discovery provider and interval dynamically.
        """
        self.task_provider = provider_func
        self.task_discovery_interval = interval

    def run(self):
        """
        Main run loop for the background sender thread.
        Reads from the queue, batches events, syncs tasks periodically,
        and flushes the batch via HTTP.
        """
        batch = []
        last_flush_time = time.monotonic()
        last_discovery_time = time.monotonic()

        fallback_retries = 0
        MAX_FALLBACK_RETRIES = 5

        while not self._stop_event.is_set():
            try:
                now = time.monotonic()

                # Periodic Task Discovery
                # Note: This is a best-effort synchronization mechanism.
                # `last_discovery_time` is updated regardless of network success,
                # meaning it will try again at the next periodic interval rather than aggressively retrying in a loop.
                if (
                    self.task_provider
                    and self.task_discovery_interval
                    and now - last_discovery_time >= self.task_discovery_interval
                ):
                    try:
                        tasks = self.task_provider()
                        if tasks:
                            self._sync_tasks(tasks)
                    except Exception as e:
                        logger.error(f"Error during periodic task discovery: {e}")
                    last_discovery_time = time.monotonic()
                    now = time.monotonic()

                try:
                    # Calculate remaining wait time based on the flush interval
                    timeout = max(0.1, self.flush_interval - (now - last_flush_time))
                    item = self.event_queue.get(timeout=timeout)

                    if item.get("_type") == "_stop":
                        pass  # Ignore, just to wake up
                    elif item.get("_type") == "sync_tasks":
                        # If we receive an explicit sync command, flush existing batch first
                        if batch:
                            if self._flush_batch(batch):
                                batch = []
                                fallback_retries = 0
                            last_flush_time = time.monotonic()
                        self._sync_tasks(item["tasks"])
                    else:
                        batch.append(item)

                except queue.Empty:
                    pass

                now = time.monotonic()
                # Flush batch if batch size is reached, or flush interval has elapsed with items in batch
                if batch and (len(batch) >= self.batch_size or now - last_flush_time >= self.flush_interval):
                    if self._flush_batch(batch):
                        batch = []
                        fallback_retries = 0
                    else:
                        # Transient error, apply fallback retry with exponential backoff + jitter
                        # Trade-off note: Using time.sleep() here blocks this telemetry sender thread.
                        # While this sender is backing off, new events will accumulate in the queue.
                        # If the queue fills up, new events will be safely dropped, preserving customer
                        # task execution performance without throwing blocking exceptions.
                        fallback_retries += 1
                        if fallback_retries > MAX_FALLBACK_RETRIES:
                            logger.error("Max fallback retries exceeded. Dropping batch.")
                            batch = []
                            fallback_retries = 0
                        else:
                            base_delay = 2 ** (fallback_retries - 1)
                            jitter = random.uniform(0, 0.5)
                            # TWEAK: Using _stop_event.wait() instead of time.sleep() ensures that
                            # if the application is shutting down, we immediately abort the backoff
                            # delay and allow the daemon to gracefully exit, rather than blocking.
                            self._stop_event.wait(base_delay + jitter)

                    last_flush_time = time.monotonic()

            except Exception as e:
                logger.error(f"Unexpected error in background sender loop: {e}")
                # Sleep briefly to avoid tight loops on persistent internal errors
                self._stop_event.wait(1.0)

        # Loop exited due to stop_event, drain remaining queue and flush
        try:
            while True:
                item = self.event_queue.get_nowait()
                if item.get("_type") not in ("sync_tasks", "_stop"):
                    batch.append(item)
                    if len(batch) >= self.batch_size:
                        self._flush_batch(batch)
                        batch = []
        except queue.Empty:
            pass

        if batch:
            self._flush_batch(batch)

    def _flush_batch(self, batch):
        """
        Synchronously transmit a batch of telemetry events via HTTP POST.

        Returns:
            bool: True if transmission succeeded or if error is fatal (so batch is dropped).
                  False if the error is transient and requires a fallback retry.
        """
        if not batch:
            return True

        url = f"{self.base_url}/api/ingestion/executions/batch/"
        payload = {"executions": batch}

        try:
            response = self.session.post(url, json=payload, timeout=10.0)
            if response.status_code in (400, 401, 403):
                # Expected rejection (e.g. project inactive/deleted). Drop batch cleanly without error.
                logger.info(f"Telemetry batch dropped ({response.status_code}): {response.text}")
                return True
            elif response.status_code >= 500 or response.status_code in (408, 429):
                # Transient error
                logger.warning(f"Transient error ingesting telemetry: {response.status_code}. Will fallback retry.")
                return False

            response.raise_for_status()
            return True
        except Exception as e:
            logger.warning(f"Network error flushing telemetry: {e}. Will fallback retry.")
            return False

    def _sync_tasks(self, tasks):
        """
        Synchronously transmit a task registry sync payload.
        """
        url = f"{self.base_url}/api/jobs/sync/"
        payload = {"tasks": tasks}

        try:
            response = self.session.post(url, json=payload, timeout=10.0)
            if response.status_code in (400, 401, 403):
                logger.info(f"Task registry sync skipped ({response.status_code}): {response.text}")
            elif response.status_code >= 500 or response.status_code in (408, 429):
                logger.warning(f"Transient error syncing task registry: {response.status_code}")
            else:
                response.raise_for_status()
        except Exception as e:
            logger.warning(f"Error syncing task registry: {e}")

    def stop(self, timeout=5.0):
        """
        Signal the daemon thread to stop and wait for it to join.
        """
        self._stop_event.set()

        with contextlib.suppress(queue.Full):
            # Send a stop event to wake the queue if it's blocking
            self.event_queue.put_nowait({"_type": "_stop"})

        if self.is_alive():
            self.join(timeout=timeout)
