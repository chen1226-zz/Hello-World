"""High-concurrency regression harness for the shared-connection client.

Run: python3 interop_stress.py
Prints: OK: 2000 requests, 0 mismatch, fds back to baseline
"""

import os
import sys
import threading
import traceback

from fake_server import FakeServer, expected_body
from fetch import Client

REQUESTS = 2000
THREADS = 16
FD_SLACK = 2


def open_fds():
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return 0


def build_plan(server):
    plan = []
    for i in range(REQUESTS):
        kind = i % 10
        if kind in (0, 1):
            n = 1 + (i * 37) % 40000
            plan.append((server.url("/body?n=%d" % n), expected_body(n), 200))
        elif kind in (2, 3):
            n = 1 + (i * 53) % 40000
            plan.append((server.url("/chunked?n=%d" % n), expected_body(n), 200))
        elif kind == 4:
            n = 20 + (i * 7) % 5000
            plan.append((server.url("/drop?n=%d" % n), None, None))
        elif kind == 5:
            n = 20 + (i * 11) % 5000
            plan.append((server.url("/chunkdrop?n=%d" % n), None, None))
        elif kind == 6:
            code = (404, 500, 418)[i % 3]
            plan.append((server.url("/status?code=%d" % code),
                         ("status %d" % code).encode(), code))
        elif kind == 7:
            hops = 1 + i % 4
            plan.append((server.url("/redirect?hops=%d" % hops),
                         expected_body(100), 200))
        elif kind == 8:
            plan.append((server.url("/slow?ms=20"), b"slow 20", 200))
        else:
            n = 1 + (i * 13) % 20000
            plan.append((server.url("/body?n=%d" % n), expected_body(n), 200))
    return plan


def main():
    baseline = open_fds()
    with FakeServer() as server:
        plan = build_plan(server)
        with Client(timeout=10.0, retries=2, max_pool_per_host=32) as client:
            mismatch = [0]
            lock = threading.Lock()

            def worker(seq):
                local_mismatch = 0
                for index in range(seq, len(plan), THREADS):
                    url, want_body, want_status = plan[index]
                    try:
                        resp = client.get(url)
                    except Exception as exc:  # truncation endpoints only
                        if want_body is not None:
                            local_mismatch += 1
                            print("unexpected error: %r on %s" % (exc, url))
                        continue
                    if want_status is not None and resp.status != want_status:
                        local_mismatch += 1
                        print("status mismatch %d != %d on %s"
                              % (resp.status, want_status, url))
                    elif want_body is not None and resp.body != want_body:
                        local_mismatch += 1
                        print("body mismatch (%d != %d) on %s"
                              % (len(resp.body), len(want_body), url))
                if local_mismatch:
                    with lock:
                        mismatch[0] += local_mismatch

            threads = [threading.Thread(target=worker, args=(i,))
                       for i in range(THREADS)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            total_mismatch = mismatch[0]
        if total_mismatch:
            print("FAIL: %d mismatches" % total_mismatch)
            return 1

    after = open_fds()
    if after > baseline + FD_SLACK:
        print("FAIL: fd leak, baseline=%d after=%d" % (baseline, after))
        return 1
    print("OK: %d requests, %d mismatch, fds back to baseline"
          % (REQUESTS, total_mismatch))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
