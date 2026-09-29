import gc
import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fake_server
from fake_server import FakeServer, expected_body
from fetch import Client, FetchError, TimeoutError, TooManyRedirects


def open_fds():
    return len(os.listdir("/proc/self/fd"))


class FetchTest(unittest.TestCase):
    def setUp(self):
        self.server = FakeServer()
        self.server.start()

    def tearDown(self):
        self.server.close()

    def url(self, path):
        return self.server.url(path)

    def assert_fds_released(self, baseline):
        gc.collect()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if open_fds() <= baseline + 1:
                return
            time.sleep(0.02)
        self.assertLessEqual(open_fds(), baseline + 1,
                             "fd leak: baseline=%d now=%d" % (baseline, open_fds()))

    def test_bodies_varying_length(self):
        with Client() as client:
            for n in (0, 1, 17, 4095, 4096, 4097, 30000):
                resp = client.get(self.url("/body?n=%d" % n))
                self.assertEqual(resp.status, 200)
                self.assertEqual(resp.body, expected_body(n))

    def test_chunked_varying_length(self):
        with Client() as client:
            for n in (0, 1, 5, 4096, 25000):
                resp = client.get(self.url("/chunked?n=%d" % n))
                self.assertEqual(resp.status, 200)
                self.assertEqual(resp.body, expected_body(n))

    def test_concurrent_no_crosstalk_no_leak(self):
        # Regression: chunked bodies of different lengths plus a mid-stream
        # disconnect. Responses must never cross wires and all sockets must
        # come back. Completes well under 60 seconds.
        baseline = open_fds()
        threads_n, per_thread = 12, 40
        mismatches, errors = [], []

        def worker(seq):
            with Client(timeout=5.0, retries=2, max_pool_per_host=8) as client:
                for i in range(seq, threads_n * per_thread, threads_n):
                    mode = i % 3
                    n = 1 + (i * 997) % 30000
                    if mode == 0:
                        url = self.url("/chunked?n=%d" % n)
                    elif mode == 1:
                        url = self.url("/body?n=%d" % n)
                    else:
                        url = self.url("/chunkdrop?n=%d" % n)
                    try:
                        resp = client.get(url)
                    except FetchError:
                        if mode != 2:
                            errors.append("unexpected error on %s" % url)
                        continue
                    if mode == 2:
                        errors.append("expected truncation on %s" % url)
                    elif resp.status != 200 or resp.body != expected_body(n):
                        mismatches.append(url)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(threads_n)]
        start = time.monotonic()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertLess(time.monotonic() - start, 60.0)
        self.assertEqual(errors, [])
        self.assertEqual(mismatches, [])
        self.assert_fds_released(baseline)

    def test_content_length_truncation(self):
        with Client(retries=1) as client:
            with self.assertRaises(FetchError):
                client.get(self.url("/drop?n=200"))
            resp = client.get(self.url("/body?n=123"))
            self.assertEqual(resp.body, expected_body(123))

    def test_chunked_truncation(self):
        with Client(retries=1) as client:
            with self.assertRaises(FetchError):
                client.get(self.url("/chunkdrop?n=400"))
            resp = client.get(self.url("/chunked?n=321"))
            self.assertEqual(resp.body, expected_body(321))

    def test_timeout(self):
        with Client(timeout=0.05, retries=1, backoff=0) as client:
            with self.assertRaises(TimeoutError):
                client.get(self.url("/slow?ms=1000"))
        with Client(timeout=2.0) as client:
            self.assertEqual(client.get(self.url("/body?n=10")).body,
                             expected_body(10))

    def test_error_status(self):
        with Client() as client:
            for code in (404, 500, 418):
                resp = client.get(self.url("/status?code=%d" % code))
                self.assertEqual(resp.status, code)
                self.assertFalse(resp.ok)

    def test_redirect_chain(self):
        with Client() as client:
            resp = client.get(self.url("/redirect?hops=4"))
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.body, expected_body(100))

    def test_redirect_limit(self):
        with Client(max_redirects=2) as client:
            with self.assertRaises(TooManyRedirects):
                client.get(self.url("/redirect?hops=5"))

    def test_post_echo(self):
        with Client() as client:
            payload = expected_body(5000)
            resp = client.post(self.url("/echo"), body=payload)
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.body, payload)

    def test_pool_sockets_closed(self):
        baseline = open_fds()
        client = Client()
        for n in (1, 100, 5000):
            self.assertEqual(client.get(self.url("/body?n=%d" % n)).body,
                             expected_body(n))
        client.close()
        self.assert_fds_released(baseline)


if __name__ == "__main__":
    unittest.main()
