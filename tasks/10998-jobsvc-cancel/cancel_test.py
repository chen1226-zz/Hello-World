"""压测: 发起 200 个请求, 100ms 后全部取消,
要求所有工作线程在 200ms 内退出且线程数回落到基线."""
import threading
import time

from jobsvc import JobService

N = 200
CANCEL_AFTER = 0.1
EXIT_BUDGET = 0.2


def main():
    baseline = threading.active_count()
    svc = JobService()
    jobs = [svc.submit() for _ in range(N)]
    time.sleep(CANCEL_AFTER)

    for job in jobs:
        job.cancel()

    start = time.monotonic()
    for job in jobs:
        remaining = EXIT_BUDGET - (time.monotonic() - start)
        if remaining <= 0 or not job.done.wait(remaining):
            raise SystemExit("FAIL: workers did not exit within 200ms")
    elapsed = time.monotonic() - start
    if svc.active_count() != 0:
        raise SystemExit("FAIL: service still tracks active jobs")

    deadline = time.monotonic() + 2.0
    while threading.active_count() > baseline and time.monotonic() < deadline:
        time.sleep(0.005)
    if threading.active_count() != baseline:
        raise SystemExit(
            f"FAIL: threads {threading.active_count()} != baseline {baseline}")

    print(f"OK: all workers exited in <200ms (actual {elapsed * 1000:.0f}ms), "
          "threads back to baseline")


if __name__ == "__main__":
    main()
