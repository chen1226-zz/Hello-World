"""Pooled-handler tests: reset contract, error/cancel recycling, concurrency.

Run from the task directory:  python3 -m unittest discover -s tests
"""
import os
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from handler import (  # noqa: E402
    CancelledError, Handler, HandlerError, ObjectPool, Request, Response,
)


class CancelFlag:
    """Duck-typed stand-in for threading.Event (only is_set/set are used)."""
    def __init__(self, set_now=False):
        self._set = set_now
    def is_set(self):
        return self._set
    def set(self):
        self._set = True


class ResetContractTests(unittest.TestCase):
    def test_request_reset_clears_every_field(self):
        request = Request()
        request.username, request.amount = "bob", 42
        request.headers["k"] = "v"
        request.body.extend(b"payload")
        request.reset()
        self.assertEqual((request.username, request.amount, request.headers,
                          request.body), ("", 0, {}, bytearray()))
    def test_response_reset_clears_every_field(self):
        response = Response()
        response.status, response.error = 500, "boom"
        response.headers["h"] = "1"
        response.fields.update(username="bob", amount=99)
        response.body.extend(b"payload")
        response.reset()
        self.assertEqual((response.status, response.error, response.headers,
                          response.fields, response.body),
                         (200, "", {}, {}, bytearray()))
    def test_reset_keeps_body_capacity(self):
        request = Request()
        request.body.extend(b"X" * 4096)
        request.reset()
        self.assertEqual(len(request.body), 0)
        request.body.extend(b"Y" * 4096)
        self.assertEqual(len(request.body), 4096)


class SequentialReuseTests(unittest.TestCase):
    def setUp(self):
        self.handler = Handler()
    def assert_clean_ping(self, result):
        self.assertEqual(result.status, 200)
        self.assertEqual((result.username, result.amount, result.body),
                         ("", 0, b""))
        self.assertEqual(result.headers, {"X-Request-User": ""})
        self.assertEqual(result.fields, {"pong": True})
    def test_echo_then_ping_has_no_carryover(self):
        big = b"Z" * 100_000
        first = self.handler.handle("carol", 777, {"X-T": "1"}, big, path="/echo")
        self.assertEqual((first.username, first.amount, first.body),
                         ("carol", 777, big))
        self.assertEqual(first.fields["echo_len"], len(big))
        self.assertEqual(first.headers["X-Echo"], "1")
        self.assert_clean_ping(self.handler.handle("", 0, {}, b"", path="/ping"))
    def test_error_and_unknown_paths_recycle_clean(self):
        with self.assertRaises(HandlerError):
            self.handler.handle("dave", 5, {}, b"errbody", path="/boom")
        self.assert_clean_ping(self.handler.handle("", 0, {}, b"", path="/ping"))
        with self.assertRaises(HandlerError):
            self.handler.handle("eve", 8, {"X-H": "v"}, b"nope", path="/other")
        self.assert_clean_ping(self.handler.handle("", 0, {}, b"", path="/ping"))
    def test_snapshot_detached_from_pooled_objects(self):
        first = self.handler.handle("frank", 1, {"A": "b"}, b"abc", path="/echo")
        self.handler.handle("", 0, {}, b"", path="/ping")
        first.fields["username"] = "tampered"
        first.headers["X-Echo"] = "tampered"
        third = self.handler.handle("grace", 2, {"A": "ok"}, b"x", path="/echo")
        self.assertEqual(third.fields["username"], "grace")
        self.assertEqual(third.headers["X-Echo"], "1")
        self.assertEqual(third.headers["X-Request-User"], "grace")
    def test_allocation_count_stays_bounded_sequentially(self):
        self.handler.handle("", 0, {}, b"", path="/ping")
        self.handler.handle("warm", 1, {}, b"b", path="/echo")
        allocated = self.handler.pool.allocations
        for i in range(200):
            self.handler.handle(f"u{i}", i, {"X-Big": "y"}, b"data", path="/echo")
            self.handler.handle("", 0, {}, b"", path="/ping")
        self.assertEqual(self.handler.pool.allocations, allocated)
        self.assertEqual(allocated, 2)


class CancelAndPoolTests(unittest.TestCase):
    def test_pre_cancelled_request_still_recycles_clean(self):
        handler = Handler()
        with self.assertRaises(CancelledError):
            handler.handle("hank", 3, {"X-C": "z"}, b"partial", path="/echo",
                           cancel_event=CancelFlag(set_now=True))
        result = handler.handle("", 0, {}, b"", path="/ping")
        self.assertEqual((result.username, result.amount, result.body),
                         ("", 0, b""))
        self.assertNotIn("X-C", result.headers)
    def test_cancel_mid_flight_from_other_thread(self):
        handler = Handler()
        event = threading.Event()
        threading.Thread(target=event.set).start()
        with self.assertRaises(CancelledError):
            handler.handle("ivy", 9, {}, b"x" * 8, path="/echo",
                           cancel_event=event)
        result = handler.handle("", 0, {}, b"", path="/ping")
        self.assertEqual(result.username, "")
        self.assertEqual(result.body, b"")
    def test_pool_reset_happens_on_put_back(self):
        pool = ObjectPool()
        request = pool._take(Request)
        request.username = "stale"
        pool._put_back(Request, request)
        recycled = pool._take(Request)
        self.assertIs(recycled, request)
        self.assertEqual(recycled.username, "")


class ConcurrentReuseTests(unittest.TestCase):
    def test_concurrent_borrowers_never_share_or_leak(self):
        handler = Handler()
        threads, per_thread, errors = 8, 100, []
        def worker(tid):
            try:
                for i in range(per_thread):
                    route = i % 3
                    if route == 0:
                        with self.assertRaises(HandlerError):
                            handler.handle(f"t{tid}", i, {f"H{tid}": "1"},
                                           b"boom", path="/boom")
                    elif route == 1:
                        flag = CancelFlag(set_now=(tid == 0 and i == 1))
                        try:
                            result = handler.handle(
                                f"user-{tid}-{i}", i, {f"H{tid}": str(i)},
                                bytes([tid]) * 256,
                                path="/echo", cancel_event=flag)
                        except CancelledError:
                            continue
                        self.assertEqual(result.username, f"user-{tid}-{i}")
                        self.assertEqual(result.amount, i)
                        self.assertEqual(result.body, bytes([tid]) * 256)
                        self.assertEqual(result.headers["X-Request-User"],
                                         f"user-{tid}-{i}")
                        self.assertNotIn(f"H{tid}", result.headers)
                    else:
                        result = handler.handle("", 0, {}, b"", path="/ping")
                        self.assertEqual((result.username, result.amount,
                                          result.body), ("", 0, b""))
                        self.assertNotIn(f"H{tid}", result.headers)
            except Exception as exc:  # pragma: no cover - failure reporter
                errors.append(exc)
        with ThreadPoolExecutor(max_workers=threads) as executor:
            list(executor.map(worker, range(threads)))
        self.assertEqual(errors, [])
        self.assertLessEqual(handler.pool.allocations, threads * 2)
    def test_large_payloads_do_not_leak_under_contention(self):
        handler = Handler()
        secret = f"secret-user-{os.getpid()}"
        big = secret.encode() * 4096
        stop = threading.Event()
        def big_writer():
            i = 0
            while not stop.is_set():
                result = handler.handle(secret, 10 ** 9 + i,
                                        {"X-Big": secret}, big, path="/echo")
                if result.username != secret or result.body != big:
                    raise AssertionError("big response corrupted")
                i += 1
        def small_reader():
            while not stop.is_set():
                result = handler.handle("", 0, {}, b"", path="/ping")
                if (result.username or result.amount or result.body
                        or "X-Big" in result.headers):
                    raise AssertionError("stale data leaked into small response")
        workers = [threading.Thread(target=big_writer),
                   threading.Thread(target=small_reader)]
        for worker in workers:
            worker.start()
        threading.Event().wait(0.2)
        stop.set()
        for worker in workers:
            worker.join(timeout=10)
            self.assertFalse(worker.is_alive())


if __name__ == "__main__":
    unittest.main()
