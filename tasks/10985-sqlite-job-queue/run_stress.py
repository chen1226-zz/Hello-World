"""复现脚本：4 个进程并发抢 200 个任务，每个任务只能被领到一次。"""

import json
import os
import subprocess
import sys
import tempfile

from jobstore import JobStore

TASKS = 200
WORKERS = 4

CHILD = r"""
import json, sys
from jobstore import JobStore

db, worker, out = sys.argv[1], sys.argv[2], sys.argv[3]
store = JobStore(db)
claimed = []
for _ in range(500):
    job_id = store.claim(worker)
    if job_id is None:
        break
    claimed.append(job_id)
json.dump(claimed, open(out, "w"))
"""


def main():
    root = tempfile.mkdtemp()
    db = os.path.join(root, "jobs.db")
    store = JobStore(db)
    store.add([f"job-{i:03d}" for i in range(TASKS)])

    procs = []
    outs = []
    for w in range(WORKERS):
        out = os.path.join(root, f"w{w}.json")
        outs.append(out)
        procs.append(
            subprocess.Popen(
                [sys.executable, "-c", CHILD, db, f"w{w}", out],
                cwd=os.path.dirname(os.path.abspath(__file__)),
            )
        )
    for p in procs:
        p.wait(timeout=180)

    claimed = []
    for out in outs:
        with open(out, encoding="utf-8") as fh:
            claimed.extend(json.load(fh))

    duplicates = len(claimed) - len(set(claimed))
    missing = TASKS - len(set(claimed))
    store.close()
    final = JobStore(db)
    stats = final.stats()
    final.close()

    problems = []
    if duplicates:
        problems.append(f"{duplicates} 个任务被重复领取")
    if missing:
        problems.append(f"{missing} 个任务没有被任何 worker 领到")
    if stats["claimed"] != TASKS:
        problems.append(f"最终 claimed={stats['claimed']}，应为 {TASKS}")

    if problems:
        print("FAIL: " + "; ".join(problems))
        print(f"  领取总数={len(claimed)} 去重后={len(set(claimed))} stats={stats}")
        raise SystemExit(1)
    print(f"OK: {TASKS} tasks, each claimed once")


if __name__ == "__main__":
    main()
