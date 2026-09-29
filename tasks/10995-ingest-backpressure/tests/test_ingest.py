import http.client
import json
import os
import socket
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ingest
from fake_upstream import FakeUpstream


def wait_for(pred, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.02)
    return False


class IngestTestCase(unittest.TestCase):
    upstream_delay = 0.0
    queue_size = 16
    workers = 2

    def setUp(self):
        self.upstream = FakeUpstream(delay=self.upstream_delay)
        threading.Thread(target=self.upstream.serve_forever, daemon=True).start()
        up_url = "http://127.0.0.1:%d/ingest" % self.upstream.server_address[1]
        self.service = ingest.IngestService(
            up_url, queue_size=self.queue_size,
            workers=self.workers, timeout=1.0).start()
        self.server = ingest.serve("127.0.0.1", 0, self.service)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.service.stop()
        self.upstream.shutdown()
        self.upstream.server_close()

    def post(self, body=b"x" * 64):
        deadline = time.monotonic() + 10
        while True:
            try:
                conn = http.client.HTTPConnection("127.0.0.1", self.port,
                                                  timeout=5)
                conn.request("POST", "/ingest", body=body)
                resp = conn.getresponse()
                resp.read()
                status = resp.status
                conn.close()
                return status
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)

    def metrics(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("GET", "/metrics")
        resp = conn.getresponse()
        data = json.loads(resp.read())
        conn.close()
        return data

    def test_accept_and_forward(self):
        self.assertEqual(self.post(), 202)
        self.assertTrue(wait_for(lambda: self.metrics()["forwarded"] == 1))
        self.assertEqual(self.upstream.received, 1)

    def test_metrics_endpoint_fields(self):
        data = self.metrics()
        for key in ("queue_len", "queue_high_water", "accepted", "rejected",
                    "dropped", "forwarded", "p99_seconds"):
            self.assertIn(key, data)

    def test_oversize_body_rejected(self):
        self.assertEqual(self.post(b"x" * (ingest.MAX_BODY + 1)), 413)
        self.assertEqual(self.metrics()["accepted"], 0)


class TestBurstTraffic(IngestTestCase):
    """Burst beyond capacity: fast-fail 429, queue stays bounded, no hang."""
    upstream_delay = 0.1  # 1 worker -> ~10 req/s capacity
    queue_size = 8
    workers = 1

    def test_burst_fast_fail(self):
        total = 60
        statuses = []
        lock = threading.Lock()

        def producer():
            status = self.post()
            with lock:
                statuses.append(status)

        threads = [threading.Thread(target=producer) for _ in range(total)]
        started = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        elapsed = time.monotonic() - started

        self.assertEqual(len(statuses), total)  # nobody hung
        self.assertLess(elapsed, 10)
        self.assertTrue(set(statuses) <= {202, 429})
        self.assertIn(429, statuses)  # overload really happened
        data = self.metrics()
        self.assertEqual(data["accepted"] + data["rejected"], total)
        self.assertGreater(data["rejected"], 0)
        self.assertLessEqual(data["queue_high_water"], self.queue_size)


class TestSlowConsumer(IngestTestCase):
    """Consumer (worker) slower than producers: backlog bounded, then drains."""
    upstream_delay = 0.05
    queue_size = 4
    workers = 1

    def test_bounded_backlog_and_drain(self):
        for _ in range(40):
            self.post()
        data = self.metrics()
        self.assertLessEqual(data["queue_high_water"], self.queue_size)
        self.assertGreater(data["rejected"], 0)
        self.assertTrue(wait_for(lambda: self.metrics()["queue_len"] == 0,
                                 timeout=10))
        self.assertTrue(wait_for(
            lambda: (self.metrics()["forwarded"] + self.metrics()["dropped"]
                     == self.metrics()["accepted"])))


class TestSlowUpstreamRecovery(IngestTestCase):
    """Upstream slows down -> rejections; upstream heals -> service recovers."""
    upstream_delay = 0.2
    queue_size = 8
    workers = 2

    def test_slow_upstream_then_recovery(self):
        for _ in range(30):
            self.post()
        data = self.metrics()
        self.assertGreater(data["rejected"], 0)

        self.upstream.delay = 0.0  # upstream recovers
        self.assertTrue(wait_for(lambda: self.metrics()["queue_len"] == 0,
                                 timeout=10))
        before = self.metrics()["forwarded"]
        self.assertEqual(self.post(), 202)  # accepted again, not 429
        self.assertTrue(
            wait_for(lambda: self.metrics()["forwarded"] == before + 1))
        self.assertEqual(self.metrics()["queue_len"], 0)


class TestUpstreamDown(unittest.TestCase):
    """Forwarding failures are counted as dropped, never retried forever."""

    def test_dropped_on_upstream_error(self):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        dead_port = sock.getsockname()[1]
        sock.close()
        service = ingest.IngestService(
            "http://127.0.0.1:%d/" % dead_port,
            queue_size=8, workers=1, timeout=0.5).start()
        try:
            for _ in range(3):
                self.assertTrue(service.submit(b"x"))
            self.assertTrue(
                wait_for(lambda: service.metrics.snapshot(0)["dropped"] == 3))
            self.assertEqual(service.metrics.snapshot(0)["forwarded"], 0)
        finally:
            service.stop()


if __name__ == "__main__":
    unittest.main()
