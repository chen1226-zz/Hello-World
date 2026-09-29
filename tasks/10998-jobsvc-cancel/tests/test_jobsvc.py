import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import jobsvc


class JobsvcCancelTests(unittest.TestCase):
    def setUp(self):
        self.service = jobsvc.JobService(default_timeout=30.0)

    def tearDown(self):
        self.service.cancel_all()

    def _submit(self, token, chunks=5000, chunk_size=3000):
        job = jobsvc.Job(token, chunks=chunks, chunk_size=chunk_size)
        worker = self.service.submit(job)
        return job, worker

    def test_request_cancel(self):
        token = self.service.new_token()
        job, worker = self._submit(token)
        time.sleep(0.05)
        start = time.monotonic()
        token.cancel()
        worker.join(timeout=2.0)
        elapsed = time.monotonic() - start
        self.assertFalse(worker.is_alive())
        self.assertEqual(job.status, "cancelled")
        self.assertLess(elapsed, 0.2)
        self.assertEqual(self.service.active_workers(), 0)

    def test_request_timeout(self):
        token = self.service.new_token(timeout=0.05)
        job, worker = self._submit(token)
        start = time.monotonic()
        worker.join(timeout=2.0)
        elapsed = time.monotonic() - start
        self.assertFalse(worker.is_alive())
        self.assertEqual(job.status, "timeout")
        self.assertLess(elapsed, 0.5)
        self.assertEqual(self.service.active_workers(), 0)

    def test_parent_cancel(self):
        parent = self.service.new_token()
        children = [self.service.new_token(parent=parent) for _ in range(3)]
        jobs, workers = zip(*(self._submit(t) for t in children))
        time.sleep(0.05)
        start = time.monotonic()
        parent.cancel()
        for worker in workers:
            worker.join(timeout=2.0)
        elapsed = time.monotonic() - start
        for worker in workers:
            self.assertFalse(worker.is_alive())
        for job in jobs:
            self.assertEqual(job.status, "cancelled")
        self.assertLess(elapsed, 0.2)
        self.assertEqual(self.service.active_workers(), 0)

    def test_cancel_complete_race(self):
        for _ in range(50):
            token = self.service.new_token()
            job, worker = self._submit(token, chunks=1, chunk_size=3000)
            token.cancel()  # may fire before, during, or after completion
            worker.join(timeout=2.0)
            self.assertFalse(worker.is_alive())
            self.assertTrue(job.done.is_set())
            self.assertIn(job.status, ("done", "cancelled"))
        self.assertEqual(self.service.active_workers(), 0)

    def test_normal_completion(self):
        token = self.service.new_token()
        job, worker = self._submit(token, chunks=5, chunk_size=1000)
        worker.join(timeout=5.0)
        self.assertEqual(job.status, "done")
        self.assertIsNotNone(job.result)
        self.assertEqual(self.service.active_workers(), 0)


if __name__ == "__main__":
    unittest.main()
