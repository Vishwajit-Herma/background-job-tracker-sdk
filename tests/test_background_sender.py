import queue
import time
from unittest import mock

from background_job_tracker.client import Tracker
from background_job_tracker.sender import BackgroundSender


def test_sender_batching():
    event_queue = queue.Queue()
    sender = BackgroundSender("key", "http://test", event_queue, batch_size=2, flush_interval=10.0)

    with mock.patch.object(sender.session, "post") as mock_post:
        mock_post.return_value.status_code = 202

        sender.start()

        # Add 3 events
        event_queue.put({"id": 1})
        event_queue.put({"id": 2})
        event_queue.put({"id": 3})

        # Wait a bit
        time.sleep(0.1)

        # Since batch_size=2, the first two should have triggered a flush
        assert mock_post.call_count == 1

        # Stop will flush remaining
        sender.stop(timeout=1.0)
        assert mock_post.call_count == 2

        # Check payloads
        calls = mock_post.call_args_list
        assert len(calls[0].kwargs["json"]["executions"]) == 2
        assert len(calls[1].kwargs["json"]["executions"]) == 1


def test_client_overflow():
    Tracker._instance = None
    client = Tracker(api_key="key", base_url="http://test", max_queue_size=2)
    # Patch sender so it doesn't drain
    client.sender.stop()

    # Fill queue
    client.enqueue_event({"id": 1})
    client.enqueue_event({"id": 2})

    # Overflow
    client.enqueue_event({"id": 3})  # Should not block or raise error

    assert client.event_queue.qsize() == 2


def test_sender_sync_tasks():
    event_queue = queue.Queue()
    sender = BackgroundSender("key", "http://test", event_queue, batch_size=10, flush_interval=10.0)

    with mock.patch.object(sender.session, "post") as mock_post:
        mock_post.return_value.status_code = 200
        sender.start()

        event_queue.put({"_type": "sync_tasks", "tasks": ["task1"]})
        time.sleep(0.1)
        sender.stop()

        mock_post.assert_called_once()
        assert mock_post.call_args.kwargs["json"]["tasks"] == ["task1"]
        assert "/api/jobs/sync/" in mock_post.call_args.args[0]


def test_periodic_discovery():
    event_queue = queue.Queue()
    provider_mock = mock.Mock(return_value=["periodic_task"])

    sender = BackgroundSender(
        "key",
        "http://test",
        event_queue,
        flush_interval=0.1,
        task_discovery_interval=0.1,
        task_provider=provider_mock,
    )

    with mock.patch.object(sender.session, "post") as mock_post:
        mock_post.return_value.status_code = 200
        sender.start()

        # Wait for periodic discovery to trigger
        time.sleep(0.3)
        sender.stop()

        assert provider_mock.called
        assert mock_post.call_count >= 1
        assert mock_post.call_args.kwargs["json"]["tasks"] == ["periodic_task"]


def test_transient_batch_recovery():
    event_queue = queue.Queue()
    sender = BackgroundSender("key", "http://test", event_queue, batch_size=1, flush_interval=0.01)

    # Mock _stop_event.wait to bypass exponential backoff delay in test
    with (
        mock.patch.object(sender.session, "post") as mock_post,
        mock.patch.object(sender._stop_event, "wait") as mock_wait,
    ):
        mock_resp_500 = mock.Mock()
        mock_resp_500.status_code = 500
        mock_resp_200 = mock.Mock()
        mock_resp_200.status_code = 200

        mock_post.side_effect = [mock_resp_500, mock_resp_200]

        def side_effect_wait(*args, **kwargs):
            if mock_post.call_count >= 2:
                sender._stop_event.set()

        mock_wait.side_effect = side_effect_wait

        sender.start()
        event_queue.put({"id": 1, "_type": "event"})

        sender.join(timeout=1.0)
        sender.stop(timeout=1.0)  # Ensure it stops if it didn't

        assert mock_post.call_count == 2
        # Backoff wait was called once
        assert mock_wait.call_count >= 1


def test_sender_exception_shielding():
    event_queue = queue.Queue()
    sender = BackgroundSender("key", "http://test", event_queue, batch_size=1)

    with (
        mock.patch.object(sender.session, "post") as mock_post,
        mock.patch.object(sender._stop_event, "wait", wraps=sender._stop_event.wait),
        mock.patch(
            "time.monotonic",
            side_effect=[
                time.monotonic(),
                time.monotonic(),
                Exception("Bizarre internal error"),
                time.monotonic(),
                time.monotonic(),
                time.monotonic(),
                time.monotonic(),
            ],
        ),
    ):
        mock_resp = mock.Mock()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp

        sender.start()
        event_queue.put({"id": 1, "_type": "event"})

        time.sleep(0.1)
        sender.stop()

        # Sender should have survived the initial exception and successfully posted the event
        assert mock_post.call_count == 1
        assert sender.is_alive() is False


def test_sender_shutdown_drain_timeout():
    event_queue = queue.Queue()
    sender = BackgroundSender("key", "http://test", event_queue, batch_size=2)

    with mock.patch.object(sender.session, "post") as mock_post:
        # Simulate a slow network request
        def slow_post(*args, **kwargs):
            time.sleep(0.5)
            resp = mock.Mock()
            resp.status_code = 200
            return resp

        mock_post.side_effect = slow_post

        sender.start()
        event_queue.put({"id": 1, "_type": "event"})
        event_queue.put({"id": 2, "_type": "event"})

        # Stop with a short timeout
        start_time = time.time()
        sender.stop(timeout=0.2)
        end_time = time.time()

        # The stop method should join within the timeout (wait ~0.2s max)
        # Actually join(0.2) will return after 0.2s even if thread is alive.
        # But wait, we don't want to assert thread is dead if it's blocking, we just want to ensure stop() unblocks.
        assert end_time - start_time < 0.3

        # Cleanup
        sender.join()


def test_network_failures_trigger_fallback():
    event_queue = queue.Queue()
    sender = BackgroundSender("key", "http://test", event_queue, batch_size=1, flush_interval=0.01)

    with (
        mock.patch.object(sender.session, "post") as mock_post,
        mock.patch.object(sender._stop_event, "wait") as mock_wait,
        mock.patch("background_job_tracker.sender.logger") as mock_logger,
    ):
        mock_resp_429 = mock.Mock()
        mock_resp_429.status_code = 429

        # Side effect: Two 429s, then Success
        mock_resp_200 = mock.Mock()
        mock_resp_200.status_code = 200
        mock_post.side_effect = [mock_resp_429, mock_resp_429, mock_resp_200]

        # We need the mock_wait to simulate time passing so the loop continues instantly,
        # but we also need to stop the sender after the 3rd call.
        def side_effect_wait(*args, **kwargs):
            if mock_post.call_count >= 3:
                sender._stop_event.set()

        mock_wait.side_effect = side_effect_wait

        sender.start()
        event_queue.put({"id": 1, "_type": "event"})

        sender.join(timeout=1.0)
        sender.stop(timeout=1.0)

        assert mock_post.call_count == 3
        # Ensure backoff wait was called twice
        assert mock_wait.call_count >= 2
        mock_logger.warning.assert_called_with("Transient error ingesting telemetry: 429. Will fallback retry.")


def test_connection_error_triggers_fallback():
    import requests

    event_queue = queue.Queue()
    sender = BackgroundSender("key", "http://test", event_queue, batch_size=1, flush_interval=0.01)

    with (
        mock.patch.object(sender.session, "post") as mock_post,
        mock.patch.object(sender._stop_event, "wait") as mock_wait,
        mock.patch("background_job_tracker.sender.logger") as mock_logger,
    ):
        mock_resp_200 = mock.Mock()
        mock_resp_200.status_code = 200

        # First call raises ConnectionError, second succeeds
        mock_post.side_effect = [requests.exceptions.ConnectionError("Network Down"), mock_resp_200]

        def side_effect_wait(*args, **kwargs):
            if mock_post.call_count >= 2:
                sender._stop_event.set()

        mock_wait.side_effect = side_effect_wait

        sender.start()
        event_queue.put({"id": 1, "_type": "event"})

        sender.join(timeout=1.0)
        sender.stop(timeout=1.0)

        assert mock_post.call_count == 2
        assert mock_wait.call_count >= 1
        assert "Network error flushing telemetry" in mock_logger.warning.call_args[0][0]


def test_max_retries_exceeded_drops_batch():
    event_queue = queue.Queue()
    sender = BackgroundSender("key", "http://test", event_queue, batch_size=1, flush_interval=0.01)

    with (
        mock.patch.object(sender.session, "post") as mock_post,
        mock.patch.object(sender._stop_event, "wait") as mock_wait,
        mock.patch("background_job_tracker.sender.logger") as mock_logger,
    ):
        mock_resp_500 = mock.Mock()
        mock_resp_500.status_code = 500

        # Always fail
        mock_post.side_effect = [mock_resp_500] * 10

        # Max retries = 5 fallback retries + 1 initial call = 6 calls.
        def side_effect_wait(*args, **kwargs):
            if mock_post.call_count >= 6:
                sender._stop_event.set()

        mock_wait.side_effect = side_effect_wait

        sender.start()
        event_queue.put({"id": 1, "_type": "event"})

        sender.join(timeout=1.0)
        sender.stop(timeout=1.0)

        # It should try 1 initial time + 5 fallback retries = 6 times
        assert mock_post.call_count >= 6
        mock_logger.error.assert_any_call("Max fallback retries exceeded. Dropping batch.")


def test_http_adapter_retry_after_and_5xx_real_server():
    """Verify that requests.Session's HTTPAdapter respects 429 Retry-After and 5xx retries over real HTTP."""
    import http.server
    import threading

    request_count = 0
    request_times = []

    class MockHTTPHandler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            nonlocal request_count
            request_count += 1
            request_times.append(time.time())
            content_length = int(self.headers.get("Content-Length", 0))
            if content_length > 0:
                self.rfile.read(content_length)

            if request_count == 1:
                # 1st attempt: 429 Rate Limit with Retry-After: 1
                self.send_response(429)
                self.send_header("Retry-After", "1")
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"detail": "Rate limit exceeded"}')
            elif request_count == 2:
                # 2nd attempt: 503 Service Unavailable
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"detail": "Service Unavailable"}')
            else:
                # 3rd attempt: 202 Accepted
                self.send_response(202)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status": "accepted"}')

        def log_message(self, format, *args):
            pass  # Suppress server logs during test

    server = http.server.HTTPServer(("127.0.0.1", 0), MockHTTPHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    port = server.server_port
    server_url = f"http://127.0.0.1:{port}"

    try:
        event_queue = queue.Queue()
        sender = BackgroundSender(
            api_key="test_real_key",
            base_url=server_url,
            event_queue=event_queue,
            batch_size=1,
            flush_interval=0.1,
        )

        # Direct post via session should trigger the underlying HTTPAdapter retries
        url = f"{server_url}/api/ingestion/executions/batch/"
        start_time = time.time()
        response = sender.session.post(url, json={"executions": [{"id": 1}]}, timeout=5.0)
        elapsed = time.time() - start_time

        assert response.status_code == 202
        assert request_count == 3
        # Ensure Retry-After: 1 introduced at least ~0.9s delay
        assert elapsed >= 0.9
        assert len(request_times) == 3
        assert request_times[1] - request_times[0] >= 0.8
    finally:
        server.shutdown()
        server.server_close()
