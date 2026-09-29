"""Chaos harness for lockd.

Injects 500ms of business latency against a 200ms lease while freezing
"processes" (SIGSTOP-style) mid-critical-section, then verifies:
  * no two workers were inside the critical section holding a valid
    lease at the same time (0 concurrent entries), and
  * every task was committed exactly once (no duplicate / lost writes).

Usage: python3 chaos.py [--runs N]     (default 60, or CHAOS_RUNS env)
"""

import argparse
import os
import threading
import time

from lockd import LockServer, LeaseLock, LeaseLost

BIZ_DELAY = 0.5   # injected business latency
LEASE_TTL = 0.2   # lease duration
FREEZE = 0.35     # process freeze, deliberately longer than the lease


class Resource:
    """Downstream store; only applies writes carrying the current fencing token."""

    def __init__(self, server, name):
        self.server = server
        self.name = name
        self.commits = 0
        self.rejected = 0
        self._mu = threading.Lock()

    def commit(self, token):
        with self._mu:
            if not self.server.valid(self.name, token):
                self.rejected += 1
                return False
            self.commits += 1
            return True


class CriticalSection:
    """Flags entries that overlap another *currently valid* lease holder."""

    def __init__(self, server, name):
        self.server = server
        self.name = name
        self.violations = 0
        self._active = set()
        self._mu = threading.Lock()

    def enter(self, token):
        with self._mu:
            self._active = {t for t in self._active
                            if self.server.valid(self.name, t)}
            if self._active or not self.server.valid(self.name, token):
                self.violations += 1
                return False
            self._active.add(token)
            return True

    def exit(self, token):
        with self._mu:
            self._active.discard(token)


def run_task(server, resource, section, task_id, freeze, biz_delay, ttl, freeze_dur):
    """Two workers race to process one task; exactly one may commit."""
    done = threading.Event()

    def worker(wid):
        lock = LeaseLock(server, resource.name,
                         owner="t%d-w%d" % (task_id, wid), ttl=ttl)
        try:
            if not lock.acquire(timeout=30.0) or done.is_set():
                return
            token = lock.token
            if not section.enter(token):
                return
            try:
                if freeze and wid == 0:
                    # SIGSTOP-style freeze: the renewer stalls with the process.
                    froze = threading.Event()

                    def hook():
                        if not froze.is_set():
                            froze.set()
                            time.sleep(freeze_dur)

                    lock.before_renew = hook
                    time.sleep(biz_delay + freeze_dur)
                    # Wakes up clueless: the stale write must be fenced out.
                    resource.commit(token)
                else:
                    time.sleep(biz_delay)
                    if done.is_set():
                        return
                    lock.check()  # abort if the lease was lost mid-work
                    if resource.commit(token):
                        done.set()
            except LeaseLost:
                pass
            finally:
                section.exit(token)
        finally:
            lock.release()

    threads = [threading.Thread(target=worker, args=(wid,)) for wid in (0, 1)]
    threads[0].start()
    time.sleep(min(ttl / 4, 0.05))  # stagger: worker 0 normally wins first
    threads[1].start()
    for thread in threads:
        thread.join()


def run_chaos(runs, lanes, biz_delay=BIZ_DELAY, ttl=LEASE_TTL, freeze_dur=FREEZE):
    """Run `runs` contention groups across `lanes` independent resources."""
    server = LockServer()
    lanes = max(1, min(lanes, runs))
    bundles = [(Resource(server, "res-%d" % i), CriticalSection(server, "res-%d" % i))
               for i in range(lanes)]

    def lane(lane_id, count):
        resource, section = bundles[lane_id]
        for i in range(count):
            run_task(server, resource, section, task_id=lane_id * 100000 + i,
                     freeze=(i % 2 == 0), biz_delay=biz_delay,
                     ttl=ttl, freeze_dur=freeze_dur)

    counts = [runs // lanes + (1 if i < runs % lanes else 0) for i in range(lanes)]
    threads = [threading.Thread(target=lane, args=(i, counts[i]))
               for i in range(lanes)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    commits = sum(r.commits for r, _ in bundles)
    rejected = sum(r.rejected for r, _ in bundles)
    violations = sum(s.violations for _, s in bundles)
    return commits, rejected, violations


def main():
    parser = argparse.ArgumentParser(description="lockd chaos harness")
    parser.add_argument("--runs", type=int,
                        default=int(os.environ.get("CHAOS_RUNS", "60")))
    args = parser.parse_args()
    commits, rejected, violations = run_chaos(args.runs, lanes=8)
    if violations == 0 and commits == args.runs:
        print("OK: %d runs, 0 concurrent entries" % args.runs)
        print("fencing rejected %d stale writes" % rejected)
        return 0
    print("FAIL: %d runs, %d concurrent entries, %d commits"
          % (args.runs, violations, commits))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
