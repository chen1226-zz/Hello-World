"""Concurrency stress test: total allowed must never exceed
burst + rate * elapsed, no matter how many threads race on one key.

Runs 100 rounds; prints OK only if every round stays within the cap.
"""

import sys
import threading
import time

from ratebucket import RateBucket

ROUNDS = 100
THREADS = 8
RATE = 1000.0      # tokens per second
BURST = 100.0
DURATION = 0.05    # seconds of hammering per round


def run_once() -> tuple[int, float]:
    limiter = RateBucket(rate=RATE, burst=BURST)
    allowed = 0
    count_lock = threading.Lock()
    start = time.monotonic()
    deadline = start + DURATION

    def worker() -> None:
        local = 0
        while time.monotonic() < deadline:
            if limiter.allow("shared"):
                local += 1
        with count_lock:
            nonlocal allowed
            allowed += local

    threads = [threading.Thread(target=worker) for _ in range(THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start
    return allowed, elapsed


def main() -> int:
    worst_ratio = 0.0
    for round_no in range(1, ROUNDS + 1):
        allowed, elapsed = run_once()
        cap = BURST + RATE * elapsed
        worst_ratio = max(worst_ratio, allowed / cap)
        if allowed > cap + 1e-9:
            print(
                f"FAIL round {round_no}: allowed={allowed} "
                f"> burst + rate*elapsed={cap:.3f}"
            )
            return 1
    print("OK: allowed <= burst + rate*elapsed")
    print(f"rounds={ROUNDS} worst allowed/cap ratio={worst_ratio:.3f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
