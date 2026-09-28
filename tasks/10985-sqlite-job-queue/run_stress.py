"""Stress test: N worker processes race to claim and finish 200 tasks."""

import multiprocessing as mp
import os
import shutil
import sys
import tempfile

from jobstore import JobStore

TASKS = 200
WORKERS = 8


def worker(path, wid):
    name = f"w{wid}"
    with JobStore(path) as store:
        while True:
            job = store.claim(name)
            if job is None:
                return
            assert store.complete(job["id"], name)


def main():
    workdir = tempfile.mkdtemp(prefix="jobstore-stress-")
    try:
        path = os.path.join(workdir, "jobs.db")
        with JobStore(path) as store:
            for i in range(TASKS):
                store.add_job(f"task-{i}")

        procs = [mp.Process(target=worker, args=(path, i)) for i in range(WORKERS)]
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        bad = [p.exitcode for p in procs if p.exitcode != 0]
        assert not bad, f"workers crashed: {bad}"

        with JobStore(path) as store:
            counts = store.counts()
            claims = store.claim_counts()
        assert counts == {"done": TASKS}, f"bad final state: {counts}"
        assert len(claims) == TASKS, f"{TASKS - len(claims)} tasks never claimed"
        assert all(n == 1 for _, n in claims), "a task was claimed more than once"
        print(f"OK: {TASKS} tasks, each claimed once")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
