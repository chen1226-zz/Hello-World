import json
import os
import subprocess
import sys
import tempfile
import unittest

from jobstore import JobStore

TASK_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CHILD_CLAIM = r"""
import json, sys
from jobstore import JobStore

db, worker, out = sys.argv[1], sys.argv[2], sys.argv[3]
store = JobStore(db)
claimed = []
while True:
    job_id = store.claim(worker)
    if job_id is None:
        break
    claimed.append(job_id)
store.close()
json.dump(claimed, open(out, "w"))
"""


def fresh():
    return JobStore(os.path.join(tempfile.mkdtemp(), "jobs.db"))


class TestJobStore(unittest.TestCase):
    def test_add_and_claim_single_process(self):
        """既有断言：单进程下按 id 顺序领取。"""
        store = fresh()
        store.add(["a", "b", "c"])
        self.assertEqual(store.claim("w0"), 1)
        self.assertEqual(store.claim("w0"), 2)

    def test_claim_returns_none_when_empty(self):
        """既有断言：没有待办时返回 None。"""
        store = fresh()
        self.assertIsNone(store.claim("w0"))

    def test_stats_counts(self):
        """既有断言：统计正确。"""
        store = fresh()
        store.add(["a", "b", "c"])
        store.claim("w0")
        stats = store.stats()
        self.assertEqual(stats["total"], 3)
        self.assertEqual(stats["claimed"], 1)
        self.assertEqual(stats["pending"], 2)


class TestConcurrency(unittest.TestCase):
    def test_multiprocess_claims_unique(self):
        """4 个进程抢同一批任务：每个任务只被一个 worker 领到，且不丢。"""
        root = tempfile.mkdtemp()
        db = os.path.join(root, "jobs.db")
        total = 60
        store = JobStore(db)
        store.add([f"job-{i}" for i in range(total)])
        store.close()

        procs, outs = [], []
        for w in range(4):
            out = os.path.join(root, f"w{w}.json")
            outs.append(out)
            procs.append(
                subprocess.Popen(
                    [sys.executable, "-c", CHILD_CLAIM, db, f"w{w}", out],
                    cwd=TASK_DIR,
                )
            )
        for p in procs:
            self.assertEqual(p.wait(timeout=60), 0)

        claimed = []
        for out in outs:
            with open(out, encoding="utf-8") as fh:
                claimed.extend(json.load(fh))

        self.assertEqual(len(claimed), total, "任务总数对不上（有丢失）")
        self.assertEqual(len(set(claimed)), total, "存在重复领取")
        final = JobStore(db)
        stats = final.stats()
        final.close()
        self.assertEqual(stats["claimed"], total)
        self.assertEqual(stats["pending"], 0)

    def test_crash_recovery(self):
        """worker 在事务未提交时崩溃：任务保持 pending，可被后续 worker 领取。"""
        root = tempfile.mkdtemp()
        db = os.path.join(root, "jobs.db")
        store = JobStore(db)
        store.add(["a", "b", "c"])
        store.close()

        # 子进程开启事务、改了状态，但不提交就直接退出（模拟崩溃）。
        crash = (
            "import sqlite3, os;"
            "c = sqlite3.connect(%r, timeout=5);"
            "c.execute('BEGIN IMMEDIATE');"
            "c.execute(\"UPDATE jobs SET status='claimed', worker='x' WHERE id=1\");"
            "os._exit(0)"
        ) % db
        subprocess.check_call([sys.executable, "-c", crash])

        store = JobStore(db)
        self.assertEqual(store.stats()["pending"], 3, "崩溃未回滚，状态被污染")
        self.assertEqual(store.claim("w0"), 1, "崩溃后数据库被锁或任务丢失")
        store.close()

    def test_no_handle_leak(self):
        """长时间反复打开/关闭连接，文件描述符不泄漏。"""
        if not os.path.isdir("/proc/self/fd"):
            self.skipTest("需要 /proc 文件系统")
        path = os.path.join(tempfile.mkdtemp(), "jobs.db")
        JobStore(path).close()  # 预热，排除一次性初始化开销
        before = len(os.listdir("/proc/self/fd"))
        for _ in range(300):
            JobStore(path).close()
        after = len(os.listdir("/proc/self/fd"))
        self.assertLessEqual(after, before, "连接句柄泄漏")


if __name__ == "__main__":
    unittest.main()
