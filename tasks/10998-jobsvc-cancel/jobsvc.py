"""jobsvc — request-scoped background jobs with cooperative cancellation.

Cancellation model (see README.md for the full rationale):

  * One ``CancelToken`` per request, shared by every layer of the job.
  * I/O layer: the HTTP handler polls for client disconnect and for the
    request deadline while waiting, and cancels the token.
      -> checkpoint: the handler wait loop / response write.
  * Compute layer: ``heavy_compute`` calls ``token.throw_if_cancelled()``
    at the top of every chunk, so a busy loop never runs longer than one
    chunk (~sub-ms) after cancellation.
      -> checkpoint: head of every compute chunk.
  * Service layer: tracks live worker threads and tokens, links child
    tokens to a parent token, and ``cancel_all()`` drains everything on
    shutdown so no "ghost" thread can outlive its request.  A bounded
    compute-slot semaphore caps how many threads burn CPU at once; the
    rest queue on a cancellable wait instead of all fighting for the GIL.
"""

import json
import os
import select
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

POLL_INTERVAL = 0.01  # s; how often the I/O layer re-checks cancellation


class Cancelled(Exception):
    """Raised inside a worker when its CancelToken fires."""


class CancelToken:
    """Cooperative cancellation token shared by every layer of a job."""

    def __init__(self, parent=None, timeout=None):
        self._event = threading.Event()
        self._parent = parent
        self._deadline = None if timeout is None else time.monotonic() + timeout

    def cancel(self):
        self._event.set()

    @property
    def timed_out(self):
        return self._deadline is not None and time.monotonic() >= self._deadline

    @property
    def cancelled(self):
        if self._event.is_set() or self.timed_out:
            return True
        return self._parent is not None and self._parent.cancelled

    def throw_if_cancelled(self):
        if self.cancelled:
            raise Cancelled("timed out" if self.timed_out else "cancelled")


def heavy_compute(token, chunks=5000, chunk_size=3000):
    """CPU-bound work sliced into chunks, one cancellation checkpoint each."""
    total = 0
    for _ in range(chunks):
        token.throw_if_cancelled()  # CHECKPOINT: head of the compute loop
        time.sleep(0)  # yield the GIL so I/O threads stay responsive
        total += sum(i * i for i in range(chunk_size))
    return total


class Job:
    def __init__(self, token, chunks=5000, chunk_size=3000):
        self.token = token
        self.chunks = chunks
        self.chunk_size = chunk_size
        self.status = "pending"  # pending -> done | cancelled | timeout
        self.result = None
        self.done = threading.Event()


class JobService:
    """Owns worker threads; guarantees no worker outlives its request."""

    def __init__(self, default_timeout=30.0, max_compute=None):
        self.default_timeout = default_timeout
        if max_compute is None:
            max_compute = max(4, os.cpu_count() or 4)
        self._slots = threading.Semaphore(max_compute)
        self._lock = threading.Lock()
        self._workers = set()
        self._tokens = set()

    def new_token(self, parent=None, timeout=None):
        if timeout is None:
            timeout = self.default_timeout
        token = CancelToken(parent=parent, timeout=timeout)
        with self._lock:
            self._tokens.add(token)
        return token

    def submit(self, job):
        worker = threading.Thread(target=self._run, args=(job,),
                                  name="jobsvc-worker", daemon=True)
        with self._lock:
            self._workers.add(worker)
        worker.start()
        return worker

    def _run(self, job):
        try:
            # CHECKPOINT (queue wait): blocked workers must still notice
            # cancellation, so wait for a compute slot in timed slices.
            while not self._slots.acquire(timeout=POLL_INTERVAL):
                job.token.throw_if_cancelled()
            try:
                job.result = heavy_compute(job.token, job.chunks,
                                           job.chunk_size)
                job.status = "done"
            finally:
                self._slots.release()
        except Cancelled:
            job.status = "timeout" if job.token.timed_out else "cancelled"
        finally:
            job.done.set()
            with self._lock:
                self._workers.discard(threading.current_thread())
                self._tokens.discard(job.token)

    def active_workers(self):
        with self._lock:
            return len(self._workers)

    def cancel_all(self):
        with self._lock:
            tokens = list(self._tokens)
        for token in tokens:
            token.cancel()


def _client_gone(conn):
    """True if the peer closed the connection (readable + EOF, or error)."""
    try:
        readable, _, _ = select.select([conn], [], [], 0)
        if not readable:
            return False
        return conn.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT) == b""
    except BlockingIOError:
        return False
    except OSError:
        return True


class JobHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        service = self.server.service
        token = service.new_token()
        job = Job(token)
        worker = service.submit(job)
        try:
            # CHECKPOINT (I/O layer): while blocked waiting, keep polling
            # for client disconnect and for the request deadline.
            while not job.done.wait(POLL_INTERVAL):
                if _client_gone(self.connection) or token.cancelled:
                    token.cancel()
                    break
            if job.status == "done":
                body = json.dumps({"result": job.result}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)  # CHECKPOINT: write fails if peer left
        except (BrokenPipeError, ConnectionResetError):
            token.cancel()
        finally:
            token.cancel()  # request is over: the worker must not outlive it
            worker.join(timeout=2.0)

    def log_message(self, *args):
        pass


class JobServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 1024  # deep listen backlog for burst load

    def handle_error(self, request, client_address):
        # Clients disconnecting mid-request is normal for a cancellable
        # service; only unexpected errors are worth a traceback.
        if not isinstance(sys.exc_info()[1], OSError):
            super().handle_error(request, client_address)


def make_server(host, port, service):
    server = JobServer((host, port), JobHandler)
    server.service = service
    return server
