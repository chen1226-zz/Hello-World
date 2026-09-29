"""Ingest gateway: receive requests, queue them (bounded), forward upstream.

Overload policy: fast-fail. When the bounded queue is full, POST /ingest
responds 429 immediately instead of buffering unboundedly in memory.
"""
import json
import os
import queue
import threading
import time
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MAX_BODY = 1 << 20  # 1 MiB per request


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self.accepted = 0
        self.rejected = 0  # queue full -> client got 429
        self.dropped = 0   # dequeued but forwarding to upstream failed
        self.forwarded = 0
        self.high_water = 0
        self._lat = deque(maxlen=4096)

    def inc(self, name):
        with self._lock:
            setattr(self, name, getattr(self, name) + 1)

    def observe(self, seconds, qlen):
        with self._lock:
            self._lat.append(seconds)
            self.high_water = max(self.high_water, qlen)

    def snapshot(self, qlen):
        with self._lock:
            lat = sorted(self._lat)
            p99 = lat[min(len(lat) - 1, int(len(lat) * 0.99))] if lat else 0.0
            return {
                "queue_len": qlen,
                "queue_high_water": self.high_water,
                "accepted": self.accepted,
                "rejected": self.rejected,
                "dropped": self.dropped,
                "forwarded": self.forwarded,
                "p99_seconds": round(p99, 6),
            }


class IngestService:
    """Bounded queue + worker pool that forwards bodies to the upstream."""

    def __init__(self, upstream, queue_size=256, workers=4, timeout=2.0):
        self.upstream = upstream
        self.timeout = timeout
        self.queue = queue.Queue(maxsize=queue_size)
        self.metrics = Metrics()
        self._stop = threading.Event()
        self._workers = [
            threading.Thread(target=self._drain, daemon=True, name=f"fwd-{i}")
            for i in range(workers)
        ]

    def start(self):
        for worker in self._workers:
            worker.start()
        return self

    def stop(self):
        self._stop.set()
        for worker in self._workers:
            worker.join(timeout=2)

    def submit(self, body):
        """Enqueue without blocking; return False when the queue is full."""
        try:
            self.queue.put_nowait(body)
            return True
        except queue.Full:
            return False

    def _drain(self):
        while not self._stop.is_set():
            try:
                body = self.queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                req = urllib.request.Request(
                    self.upstream, data=body,
                    headers={"Content-Type": "application/octet-stream"})
                with urllib.request.urlopen(req, timeout=self.timeout):
                    pass
                self.metrics.inc("forwarded")
            except Exception:
                # No retry: retries under overload amplify traffic (retry storm).
                self.metrics.inc("dropped")
            finally:
                self.queue.task_done()


def make_handler(service):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _reply(self, code, obj=b""):
            if isinstance(obj, (dict, list)):
                obj = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Length", str(len(obj)))
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            if obj:
                self.wfile.write(obj)

        def do_GET(self):
            if self.path == "/health":
                self._reply(200, {"status": "ok"})
            elif self.path == "/metrics":
                self._reply(200, service.metrics.snapshot(service.queue.qsize()))
            else:
                self._reply(404, {"error": "not found"})

        def do_POST(self):
            started = time.monotonic()
            if self.path != "/ingest":
                return self._reply(404, {"error": "not found"})
            length = int(self.headers.get("Content-Length") or 0)
            if length > MAX_BODY:
                while length > 0:  # drain to keep the connection consistent
                    chunk = self.rfile.read(min(length, 65536))
                    if not chunk:
                        break
                    length -= len(chunk)
                return self._reply(413, {"error": "body too large"})
            body = self.rfile.read(length)
            if service.submit(body):
                service.metrics.inc("accepted")
                self._reply(202, {"status": "queued"})
            else:
                service.metrics.inc("rejected")
                self._reply(429, {"error": "overloaded, retry later"})
            service.metrics.observe(time.monotonic() - started,
                                    service.queue.qsize())

        def log_message(self, *args):
            pass

    return Handler


def serve(host, port, service):
    class Server(ThreadingHTTPServer):
        daemon_threads = True
        request_queue_size = 128

    return Server((host, port), make_handler(service))


def main():
    upstream = os.environ.get("UPSTREAM_URL", "http://127.0.0.1:9000/ingest")
    port = int(os.environ.get("PORT", "8080"))
    service = IngestService(
        upstream,
        queue_size=int(os.environ.get("QUEUE_SIZE", "256")),
        workers=int(os.environ.get("WORKERS", "4")),
    ).start()
    print(f"ingest listening on :{port}, upstream={upstream}")
    try:
        serve("0.0.0.0", port, service).serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
