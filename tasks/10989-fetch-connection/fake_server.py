"""Local-only HTTP/1.1 fake server for the fetch client tests.

Endpoints:
  /health                 -> 200 ok
  /body?n=N               -> Content-Length body of N deterministic bytes
  /chunked?n=N            -> chunked body of N deterministic bytes
  /drop?n=N               -> Content-Length N, connection closed after N//2
  /chunkdrop?n=N          -> chunked N bytes, closed after N//2 chunks
  /status?code=C          -> given status code with a short body
  /redirect?hops=H        -> 302 chain ending at /body?n=100
  /slow?ms=M              -> sleep M ms before answering (keep-alive)
  POST /echo              -> echo request body
"""

import socketserver
import threading
import time
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlsplit


def expected_body(n):
    n = int(n)
    block = b"0123456789abcdefghijklmnopqrstuvwxyz"
    return (block * (n // len(block) + 1))[:n]


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    timeout = 5

    def log_message(self, *args):
        pass

    def setup(self):
        super().setup()
        self.connection.settimeout(self.timeout)

    def _url(self):
        return urlsplit(self.path)

    def _qs(self, name, default):
        qs = parse_qs(self._url().query)
        try:
            return int(qs.get(name, [default])[0])
        except ValueError:
            return default

    def _send(self, status, body=b"", chunked=False, extra=None, keepalive=True):
        self.send_response(status)
        self.send_header("Server", "fake")
        if extra:
            for key, value in extra.items():
                self.send_header(key, value)
        if chunked:
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Connection", "keep-alive" if keepalive else "close")
            self.end_headers()
            self._write_chunks(body, terminate=keepalive)
        else:
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "keep-alive" if keepalive else "close")
            self.end_headers()
            if body:
                self.wfile.write(body)
        self.wfile.flush()

    def _write_chunks(self, body, terminate):
        sent, sizes = 0, (3, 17, 4096)
        i = 0
        while sent < len(body):
            chunk = body[sent:sent + sizes[i % len(sizes)]]
            self.wfile.write(("%x\r\n" % len(chunk)).encode())
            self.wfile.write(chunk + b"\r\n")
            sent += len(chunk)
            i += 1
        if terminate:
            self.wfile.write(b"0\r\n\r\n")

    def _send_drop(self, n, chunked):
        body = expected_body(n)
        half = max(1, len(body) // 2)
        self.send_response(200)
        self.send_header("Server", "fake")
        if chunked:
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Connection", "close")
            self.end_headers()
            self._write_chunks(body[:half], terminate=False)
        else:
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body[:half])
        self.wfile.flush()
        self.close_connection = True

    def do_GET(self):
        parts = self._url()
        path, qs = parts.path, parse_qs(parts.query)
        if path == "/health":
            self._send(200, b"ok")
        elif path == "/body":
            self._send(200, expected_body(self._qs("n", 0)))
        elif path == "/chunked":
            self._send(200, expected_body(self._qs("n", 0)), chunked=True)
        elif path == "/drop":
            self._send_drop(self._qs("n", 100), chunked=False)
        elif path == "/chunkdrop":
            self._send_drop(self._qs("n", 100), chunked=True)
        elif path == "/status":
            code = self._qs("code", 200)
            self._send(code, ("status %d" % code).encode())
        elif path == "/redirect":
            hops = self._qs("hops", 1)
            if hops <= 1:
                self._send(200, expected_body(100))
            else:
                self._send(302, b"", extra={"Location": "/redirect?hops=%d" % (hops - 1)})
        elif path == "/slow":
            ms = max(0, self._qs("ms", 100))
            time.sleep(ms / 1000.0)
            self._send(200, ("slow %d" % ms).encode())
        else:
            self._send(404, b"not found", keepalive=False)

    def do_POST(self):
        if self._url().path != "/echo":
            self._send(404, b"not found", keepalive=False)
            return
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        self._send(200, body)


class _Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 128


class FakeServer(threading.Thread):
    def __init__(self, host="127.0.0.1"):
        super().__init__(daemon=True)
        self._server = _Server((host, 0), _Handler)

    @property
    def port(self):
        return self._server.server_address[1]

    def url(self, path="/"):
        return "http://%s:%d%s" % (self._server.server_address[0], self.port, path)

    def run(self):
        self._server.serve_forever(poll_interval=0.05)

    def close(self):
        self._server.shutdown()
        self._server.server_close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.close()
        return False
