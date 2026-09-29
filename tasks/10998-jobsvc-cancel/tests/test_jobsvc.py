import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobsvc import CancelToken, JobService  # noqa: E402


class JobSvcCancelTest(unittest.TestCase):
    def setUp(self):
        self.svc = JobService()

    def wait_done(self, job, timeout=5.0):
        self.assertTrue(job.done.wait(timeout), "job did not finish in time")

    def test_request_cancel(self):
        job = self.svc.submit()
        job.cancel()
        self.wait_done(job)
        self.assertTrue(job.cancelled)
        self.assertIsNone(job.result)
        self.assertEqual(self.svc.active_count(), 0)

    def test_request_timeout(self):
        job = self.svc.submit(timeout=0.05)
        self.wait_done(job)
        self.assertTrue(job.cancelled)
        self.assertIsNone(job.result)

    def test_parent_cancel_cascades(self):
        parent = CancelToken()
        jobs = [self.svc.submit(parent=parent) for _ in range(3)]
        parent.cancel()
        for job in jobs:
            self.wait_done(job)
            self.assertTrue(job.cancelled)
        self.assertEqual(self.svc.active_count(), 0)

    def test_cancel_races_normal_completion(self):
        for _ in range(50):
            job = self.svc.submit(units=1)
            job.cancel()
            self.wait_done(job)
            self.assertTrue(job.result is not None or job.cancelled)
            self.assertFalse(job.cancelled and job.result is not None)
        self.assertEqual(self.svc.active_count(), 0)

    def test_cancel_during_blocking_io(self):
        job = self.svc.submit(units=1, io_time=30.0)
        time.sleep(0.05)
        job.cancel()
        start = time.monotonic()
        self.wait_done(job)
        self.assertLess(time.monotonic() - start, 0.2)
        self.assertTrue(job.cancelled)

    def test_normal_completion_unaffected(self):
        job = self.svc.submit(units=5)
        self.wait_done(job)
        self.assertIsNone(job.error)
        self.assertIsNotNone(job.result)
        self.assertEqual(self.svc.active_count(), 0)

    def test_threads_return_to_baseline(self):
        baseline = threading.active_count()
        jobs = [self.svc.submit() for _ in range(20)]
        for job in jobs:
            job.cancel()
        for job in jobs:
            self.wait_done(job)
        deadline = time.monotonic() + 2.0
        while threading.active_count() > baseline and time.monotonic() < deadline:
            time.sleep(0.005)
        self.assertEqual(threading.active_count(), baseline)


if __name__ == "__main__":
    unittest.main()
