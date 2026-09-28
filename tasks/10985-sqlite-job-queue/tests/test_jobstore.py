import multiprocessing as mp
import os
import shutil
import tempfile
import unittest

from jobstore import JobStore


def _drain_worker(path, wid):
    name = f"w{wid}"
    with JobStore(path) as store:
        while True:
            job = store.claim(name)
            if job is None:
                return
            assert store.complete(job["id"], name)


class JobStoreCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="jobstore-test-")
        self.path = os.path.join(self.dir, "test.db")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)


class TestBasicFlow(JobStoreCase):
    def test_add_claim_complete(self):
        with JobStore(self.path) as store:
            jid = store.add_job("payload-1")
            job = store.claim("w0")
            self.assertEqual(job, {"id": jid, "payload": "payload-1"})
            self.assertTrue(store.complete(jid, "w0"))
            self.assertEqual(store.counts(), {"done": 1})

    def test_claim_empty_returns_none(self):
        with JobStore(self.path) as store:
            self.assertIsNone(store.claim("w0"))

    def test_fifo_order(self):
        with JobStore(self.path) as store:
            ids = [store.add_job(f"p{i}") for i in range(5)]
            claimed = [store.claim("w0")["id"] for _ in range(5)]
            self.assertEqual(claimed, ids)

    def test_complete_rejects_wrong_owner(self):
        with JobStore(self.path) as store:
            jid = store.add_job("p")
            store.claim("w0")
            self.assertFalse(store.complete(jid, "intruder"))
            self.assertEqual(store.counts(), {"running": 1})


class TestMultiProcessContention(JobStoreCase):
    def test_each_job_claimed_exactly_once(self):
        tasks, workers = 60, 4
        with JobStore(self.path) as store:
            for i in range(tasks):
                store.add_job(f"task-{i}")
        procs = [
            mp.Process(target=_drain_worker, args=(self.path, i))
            for i in range(workers)
        ]
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        self.assertTrue(all(p.exitcode == 0 for p in procs))
        with JobStore(self.path) as store:
            self.assertEqual(store.counts(), {"done": tasks})
            claims = store.claim_counts()
            self.assertEqual(len(claims), tasks)
            self.assertTrue(all(n == 1 for _, n in claims))


class TestCrashRecovery(JobStoreCase):
    def test_stale_running_job_is_requeued(self):
        with JobStore(self.path) as store:
            jid = store.add_job("p")
            store.claim("dead-worker")  # worker "crashes": never completes
            self.assertEqual(store.counts(), {"running": 1})
            self.assertEqual(store.requeue_expired(0), 1)
            self.assertEqual(store.counts(), {"pending": 1})
            job = store.claim("new-worker")
            self.assertEqual(job["id"], jid)
            self.assertTrue(store.complete(jid, "new-worker"))
            self.assertEqual(store.counts(), {"done": 1})

    def test_fresh_running_job_is_not_requeued(self):
        with JobStore(self.path) as store:
            store.add_job("p")
            store.claim("slow-worker")
            self.assertEqual(store.requeue_expired(3600), 0)
            self.assertEqual(store.counts(), {"running": 1})


class TestResourceUsage(JobStoreCase):
    def _fd_count(self):
        return len(os.listdir("/proc/self/fd"))

    @unittest.skipUnless(os.path.isdir("/proc/self/fd"), "needs /proc")
    def test_open_close_does_not_leak_fds(self):
        with JobStore(self.path) as store:
            store.add_job("warmup")
        baseline = self._fd_count()
        for _ in range(100):
            with JobStore(self.path) as store:
                store.add_job("x")
                store.claim("w")
        self.assertLessEqual(self._fd_count(), baseline)

    def test_many_operations_on_one_connection(self):
        with JobStore(self.path) as store:
            for i in range(500):
                store.add_job(f"p{i}")
            for _ in range(500):
                job = store.claim("w0")
                self.assertTrue(store.complete(job["id"], "w0"))
            self.assertEqual(store.counts(), {"done": 500})


if __name__ == "__main__":
    unittest.main()
