"""Stress check: 200 concurrent requests, all cancelled after 100ms.

Expected output:
    OK: all workers exited in <200ms, threads back to baseline
"""

import http.client
import sys
import threading
import time

import jobsvc

REQUESTS = 200
CANCEL_AFTER = 0.1      # s; clients disconnect this long after connecting
WORKER_BUDGET = 0.2     # s; workers must all exit within 200ms of cancel
THREAD_BUDGET = 5.0     # s; grace for handler threads to drain afterwards


def main():
    service = jobsvc.JobService(default_timeout=30.0)
    server = jobsvc.make_server("127.0.0.1", 0, service)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    baseline_threads = threading.active_count()

    conns = []
    for _ in range(REQUESTS):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        conn.request("GET", "/work")
        conns.append(conn)

    time.sleep(CANCEL_AFTER)
    assert service.active_workers() > 0, "no workers started; server broken?"

    for conn in conns:  # client cancels: close every connection
        conn.close()
    cancel_at = time.monotonic()  # last FIN is on the wire now

    workers_gone_at = None
    while time.monotonic() - cancel_at < THREAD_BUDGET:
        if service.active_workers() == 0:
            workers_gone_at = time.monotonic()
            break
        time.sleep(0.002)

    threads_ok = False
    while time.monotonic() - cancel_at < THREAD_BUDGET:
        if threading.active_count() <= baseline_threads:
            threads_ok = True
            break
        time.sleep(0.01)

    server.shutdown()
    server.server_close()

    if workers_gone_at is None:
        print("FAIL: ghost workers still running after cancel")
        return 1
    elapsed_ms = (workers_gone_at - cancel_at) * 1000
    if elapsed_ms >= WORKER_BUDGET * 1000:
        print("FAIL: workers took %.0fms to exit (budget 200ms)" % elapsed_ms)
        return 1
    if not threads_ok:
        print("FAIL: thread count did not return to baseline")
        return 1
    print("OK: all workers exited in <200ms, threads back to baseline")
    return 0


if __name__ == "__main__":
    sys.exit(main())
