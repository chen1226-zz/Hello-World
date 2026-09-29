"""Local fake upstream for tests: configurable latency and status code."""
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class FakeUpstream(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, host="127.0.0.1", port=0, delay=0.0, status=200):
        self.delay = delay      # seconds of simulated work per request
        self.status = status    # status code returned to the ingest gateway
        self.received = 0
        self._lock = threading.Lock()
        super().__init__((host, port), self._make_handler())

    def _make_handler(self):
        upstream = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    self.rfile.read(length)
                if upstream.delay:
                    time.sleep(upstream.delay)
                with upstream._lock:
                    upstream.received += 1
                body = b'{"ok": true}'
                self.send_response(upstream.status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        return Handler


def main():
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
    delay = float(sys.argv[2]) if len(sys.argv) > 2 else 0.05
    server = FakeUpstream(port=port, delay=delay)
    print(f"fake upstream on :{port} delay={delay}s")
    server.serve_forever()


if __name__ == "__main__":
    main()
