"""Burst test: inject >=10x baseline traffic for 5s against the ingest gateway.

Checks: burst memory peak <= 2x baseline RSS, burst p99 latency < 200ms,
and the queue drains after the burst. Prints the OK line on success.
"""
import http.client
import json
import statistics
import threading
import time

import ingest
from fake_upstream import FakeUpstream

BASELINE_SECONDS = 2.0
BURST_SECONDS = 5.0
P99_BUDGET_MS = 200.0
MEM_BUDGET_RATIO = 2.0


def rss_bytes():
    with open("/proc/self/status") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError("VmRSS not found")


class RssSampler(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.samples = []  # (monotonic_ts, rss_bytes)
        self._stopped = threading.Event()

    def run(self):
        while not self._stopped.is_set():
            self.samples.append((time.monotonic(), rss_bytes()))
            time.sleep(0.02)

    def stop(self):
        self._stopped.set()


class Client(threading.Thread):
    """Closed-loop load generator: send, wait for response, think, repeat."""

    def __init__(self, port, deadline, think_time, results):
        super().__init__(daemon=True)
        self.port = port
        self.deadline = deadline
        self.think_time = think_time
        self.results = results  # shared list of (latency_seconds, status)

    def run(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        while time.monotonic() < self.deadline:
            started = time.monotonic()
            try:
                conn.request("POST", "/ingest", body=b"x" * 256)
                resp = conn.getresponse()
                resp.read()
                self.results.append((time.monotonic() - started, resp.status))
            except Exception:
                time.sleep(0.05)
                conn.close()
                conn = http.client.HTTPConnection("127.0.0.1", self.port,
                                                  timeout=5)
            if self.think_time:
                time.sleep(self.think_time)
        conn.close()


def run_phase(port, seconds, threads, think_time):
    results = []
    deadline = time.monotonic() + seconds
    clients = [Client(port, deadline, think_time, results)
               for _ in range(threads)]
    for client in clients:
        client.start()
    for client in clients:
        client.join()
    return results


def percentile(values, pct):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    return ordered[min(len(ordered) - 1, int(len(ordered) * pct / 100.0))]


def get_metrics(port, conn):
    conn.request("GET", "/metrics")
    resp = conn.getresponse()
    return json.loads(resp.read())


def main():
    upstream = FakeUpstream(delay=0.05)  # 4 workers -> ~80 req/s capacity
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    up_url = "http://127.0.0.1:%d/ingest" % upstream.server_address[1]
    service = ingest.IngestService(up_url, queue_size=256, workers=4,
                                   timeout=2.0).start()
    server = ingest.serve("127.0.0.1", 0, service)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    sampler = RssSampler()
    sampler.start()

    run_phase(port, BASELINE_SECONDS, threads=2, think_time=0.05)  # ~40 req/s
    t_base_end = time.monotonic()

    burst = run_phase(port, BURST_SECONDS, threads=16, think_time=0.005)  # 10x+
    t_burst_end = time.monotonic()

    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    metrics = get_metrics(port, conn)
    deadline = time.monotonic() + 10
    while metrics["queue_len"] > 0 and time.monotonic() < deadline:
        time.sleep(0.1)
        metrics = get_metrics(port, conn)
    drained = metrics["queue_len"] == 0
    conn.close()
    sampler.stop()
    sampler.join()

    base_samples = [rss for ts, rss in sampler.samples if ts <= t_base_end]
    burst_samples = [rss for ts, rss in sampler.samples
                     if t_base_end < ts <= t_burst_end]
    base = statistics.median(base_samples) if base_samples else 1
    peak = max(burst_samples) if burst_samples else base
    p99 = percentile([lat * 1000 for lat, _ in burst], 99)
    statuses = {}
    for _, status in burst:
        statuses[status] = statuses.get(status, 0) + 1

    print("baseline rss: %.1f MB, burst peak rss: %.1f MB (limit %.1f MB)"
          % (base / 1e6, peak / 1e6, MEM_BUDGET_RATIO * base / 1e6))
    print("burst p99: %.1f ms (limit %.0f ms), burst requests: %d, statuses: %s"
          % (p99, P99_BUDGET_MS, len(burst), statuses))
    print("queue drained after burst: %s, final metrics: %s" % (drained, metrics))

    if peak <= MEM_BUDGET_RATIO * base and p99 < P99_BUDGET_MS and drained:
        print("OK: mem peak <= 2x baseline, p99 < 200ms")
        return 0
    print("FAIL: mem peak > 2x baseline or p99 >= 200ms or queue not draining")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
