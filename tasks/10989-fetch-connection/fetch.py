"""Thread-safe retry HTTP client with a shared, per-host connection pool.

Each request fully drains its response body (or discards the socket on
error) while it is the sole user of that connection. Response bytes can
never spill into another request, and sockets are always returned to the
pool or closed, so no file descriptors leak even on truncation, timeout
or cancellation.
"""

import http.client
import socket
import threading
import time
from collections import deque
from urllib.parse import urljoin, urlsplit

_IDEMPOTENT = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE"})
_REDIRECTS = frozenset({301, 302, 303, 307, 308})


class FetchError(Exception):
    """Network/protocol failure after retries, or truncated response."""


class TimeoutError(FetchError):
    """The server did not answer within the configured timeout."""


class TooManyRedirects(FetchError):
    """The redirect limit was exceeded."""


class Response:
    __slots__ = ("status", "headers", "body")

    def __init__(self, status, headers, body):
        self.status = status
        self.headers = headers
        self.body = body

    @property
    def ok(self):
        return 200 <= self.status < 300


class _Pool:
    __slots__ = ("idle", "lock", "size")

    def __init__(self):
        self.idle = deque()
        self.lock = threading.Lock()
        self.size = 0


class Client:
    def __init__(self, timeout=10.0, retries=3, max_redirects=5,
                 max_pool_per_host=16, backoff=0.02):
        self.timeout = timeout
        self.retries = max(0, retries)
        self.max_redirects = max_redirects
        self.max_pool_per_host = max_pool_per_host
        self.backoff = backoff
        self._pools = {}
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def close(self):
        self._closed = True
        pools, self._pools = self._pools, {}
        for pool in pools.values():
            with pool.lock:
                while pool.idle:
                    self._silent_close(pool.idle.popleft())

    def get(self, url, headers=None, **kw):
        return self.fetch(url, method="GET", headers=headers, **kw)

    def post(self, url, body=None, headers=None, **kw):
        return self.fetch(url, method="POST", body=body, headers=headers, **kw)

    def fetch(self, url, method="GET", headers=None, body=None):
        if self._closed:
            raise FetchError("client is closed")
        method = method.upper()
        parsed = urlsplit(url)
        if parsed.scheme not in ("http", "https"):
            raise FetchError("unsupported url: %r" % (parsed.scheme,))
        headers = dict(headers or {})
        if body is not None and isinstance(body, str):
            body = body.encode("utf-8")
        hops = 0
        while True:
            host, port = parsed.hostname, parsed.port
            target = parsed.path or "/"
            if parsed.query:
                target += "?" + parsed.query
            key = (parsed.scheme, host, port)
            pool = self._pools.setdefault(key, _Pool())
            status, resp_headers, location, data = None, None, None, None
            for attempt in range(self.retries + 1):
                conn = None
                try:
                    with pool.lock:
                        if pool.idle:
                            conn = pool.idle.popleft()
                    if conn is None:
                        conn = self._connect(parsed.scheme, host, port)
                        conn.connect()
                    req_headers = dict(headers)
                    req_headers.setdefault("Host", conn.host)
                    req_headers.setdefault("Accept-Encoding", "identity")
                    if body is not None and "Content-Length" not in req_headers:
                        req_headers["Content-Length"] = str(len(body))
                    req_headers.setdefault("Connection", "keep-alive")
                    conn.request(method, target, body=body, headers=req_headers)
                    r = conn.getresponse()
                    data = r.read()
                    status, resp_headers = r.status, r.getheaders()
                    reusable = not r.will_close
                    if not reusable:
                        self._silent_close(conn)
                        conn = None
                    break
                except (socket.timeout, TimeoutError) as exc:
                    self._silent_close(conn)
                    if attempt >= self.retries:
                        raise TimeoutError(str(exc)) from exc
                except (http.client.HTTPException, OSError) as exc:
                    self._silent_close(conn)
                    if attempt >= self.retries or method not in _IDEMPOTENT:
                        raise FetchError(str(exc)) from exc
                time.sleep(self.backoff * (2 ** attempt))
            else:
                raise FetchError("retries exhausted")
            if conn is not None:
                self._give(pool, conn)
            location = dict(resp_headers).get("Location") if status in _REDIRECTS else None
            if location:
                hops += 1
                if hops > self.max_redirects:
                    break
                parsed = urlsplit(urljoin(parsed.geturl(), location))
                if status == 303 or (status in (301, 302) and method != "HEAD"):
                    method, body = "GET", None
                    headers = {k: v for k, v in headers.items()
                               if k.lower() not in ("content-length", "content-type")}
                continue
            return Response(status, resp_headers, data)
        raise TooManyRedirects("more than %d redirects" % self.max_redirects)

    def _connect(self, scheme, host, port):
        if scheme == "https":
            conn = http.client.HTTPSConnection(host, port, timeout=self.timeout)
        else:
            conn = http.client.HTTPConnection(host, port, timeout=self.timeout)
        return conn

    def _give(self, pool, conn):
        with pool.lock:
            if len(pool.idle) >= self.max_pool_per_host:
                self._silent_close(conn)
                return
            pool.idle.append(conn)

    @staticmethod
    def _silent_close(conn):
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


_default = Client()
fetch = _default.fetch


def get(url, headers=None, **kw):
    return _default.get(url, headers=headers, **kw)


def post(url, body=None, headers=None, **kw):
    return _default.post(url, body=body, headers=headers, **kw)
