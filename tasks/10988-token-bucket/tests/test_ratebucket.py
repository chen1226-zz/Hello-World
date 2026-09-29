import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ratebucket import RateBucket


class FakeClock:
    """Controllable clock; can be stepped backwards to simulate
    the wall-clock regressions that monotonic clocks prevent."""

    def __init__(self, now: float = 1000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, dt: float) -> None:
        self.now += dt


class TokenBucketTest(unittest.TestCase):
    def test_burst_capacity(self):
        clock = FakeClock()
        rb = RateBucket(rate=10, burst=5, clock=clock)
        self.assertEqual(sum(rb.allow("k") for _ in range(5)), 5)
        self.assertFalse(rb.allow("k"))

    def test_refill_capped_at_burst(self):
        clock = FakeClock()
        rb = RateBucket(rate=10, burst=5, clock=clock)
        for _ in range(5):
            rb.allow("k")
        clock.advance(100.0)  # huge forward jump
        self.assertEqual(sum(rb.allow("k") for _ in range(6)), 5)

    def test_retry_after_precision(self):
        clock = FakeClock()
        rb = RateBucket(rate=10, burst=5, clock=clock)
        self.assertEqual(rb.retry_after("k"), 0.0)
        for _ in range(5):
            rb.allow("k")
        self.assertAlmostEqual(rb.retry_after("k"), 0.1)
        self.assertAlmostEqual(rb.retry_after("k", cost=2.5), 0.25)
        clock.advance(0.05)
        self.assertAlmostEqual(rb.retry_after("k"), 0.05)

    def test_per_key_isolation(self):
        clock = FakeClock()
        rb = RateBucket(rate=1, burst=2, clock=clock)
        self.assertTrue(rb.allow("a"))
        self.assertTrue(rb.allow("a"))
        self.assertFalse(rb.allow("a"))
        self.assertTrue(rb.allow("b"))

    def test_clock_backward_jump_no_lockout_no_overissue(self):
        clock = FakeClock()
        rb = RateBucket(rate=10, burst=10, clock=clock)
        for _ in range(10):
            self.assertTrue(rb.allow("k"))  # drain
        clock.advance(1.0)  # 1s of real time passes
        rb.retry_after("k")  # materialize the refill: 10 tokens
        clock.advance(-5.0)  # clock jumps backwards 5s
        # Not falsely locked out: the 10 refilled tokens are still there.
        granted = sum(rb.allow("k") for _ in range(20))
        self.assertEqual(granted, 10)
        # retry_after must stay bounded by a full refill window.
        self.assertLessEqual(rb.retry_after("k"), 10 / 10 + 1e-9)
        # When the clock recovers past its old value, no extra tokens
        # may appear beyond burst + rate * real elapsed.
        clock.advance(5.0)  # back to the pre-jump reading
        self.assertEqual(sum(rb.allow("k") for _ in range(20)), 0)
        clock.advance(0.5)  # 0.5s of real new time -> 5 tokens
        self.assertEqual(sum(rb.allow("k") for _ in range(20)), 5)

    def test_clock_forward_jump_no_overissue(self):
        clock = FakeClock()
        rb = RateBucket(rate=100, burst=8, clock=clock)
        for _ in range(8):
            rb.allow("k")
        clock.advance(3600.0)  # one hour forward
        granted = sum(rb.allow("k") for _ in range(100))
        self.assertEqual(granted, 8)  # capped at burst, not rate*3600

    def test_idle_ttl_reset(self):
        clock = FakeClock()
        rb = RateBucket(rate=1, burst=100, idle_ttl=10, clock=clock)
        for _ in range(100):
            rb.allow("k")
        clock.advance(9.9)  # within TTL: only the refilled 9.9 tokens
        self.assertFalse(rb.allow("k", cost=50))
        self.assertAlmostEqual(rb.retry_after("k", cost=50), 40.1)

    def test_idle_ttl_reset_boundary(self):
        clock = FakeClock()
        rb = RateBucket(rate=1, burst=100, idle_ttl=10, clock=clock)
        for _ in range(100):
            rb.allow("k")
        clock.advance(9.999)  # idle just below TTL: no reset
        self.assertFalse(rb.allow("k", cost=100))
        clock2 = FakeClock()
        rb2 = RateBucket(rate=1, burst=100, idle_ttl=10, clock=clock2)
        for _ in range(100):
            rb2.allow("k")
        clock2.advance(10.0)  # idle for exactly TTL: reset to full
        self.assertTrue(rb2.allow("k", cost=100))

    def test_reclaim_idle_boundary(self):
        clock = FakeClock()
        rb = RateBucket(rate=1, burst=2, idle_ttl=10, clock=clock)
        rb.allow("fresh")
        clock.advance(10.0)
        rb.allow("boundary")  # last seen exactly TTL before the end
        clock.advance(9.999)
        rb.allow("fresh")  # keep "fresh" hot
        clock.advance(0.001)
        # "boundary" has now been idle exactly 10s -> reclaimed;
        # "fresh" idle 0.001s -> kept.
        self.assertEqual(rb.reclaim_idle(), 1)
        self.assertEqual(len(rb), 1)
        # Reclaimed key restarts with a full bucket.
        self.assertEqual(sum(rb.allow("boundary") for _ in range(3)), 2)

    def test_reclaim_disabled_without_ttl(self):
        rb = RateBucket(rate=1, burst=1)
        rb.allow("k")
        self.assertEqual(rb.reclaim_idle(), 0)
        self.assertEqual(len(rb), 1)

    def test_concurrent_no_overissue(self):
        rb = RateBucket(rate=500.0, burst=50.0)
        allowed = 0
        lock = threading.Lock()
        start = time.monotonic()
        deadline = start + 0.2

        def worker():
            local = 0
            while time.monotonic() < deadline:
                if rb.allow("hot"):
                    local += 1
            with lock:
                nonlocal allowed
                allowed += local

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.monotonic() - start
        self.assertLessEqual(allowed, 50.0 + 500.0 * elapsed + 1e-9)

    def test_concurrent_many_keys(self):
        rb = RateBucket(rate=1000.0, burst=10.0)
        keys = [f"key-{i}" for i in range(16)]
        start = time.monotonic()
        deadline = start + 0.1
        counts = [0] * len(keys)
        lock = threading.Lock()

        def worker(idx):
            local = 0
            while time.monotonic() < deadline:
                if rb.allow(keys[idx]):
                    local += 1
            with lock:
                counts[idx] += local

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(len(keys))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.monotonic() - start
        for c in counts:
            self.assertLessEqual(c, 10.0 + 1000.0 * elapsed + 1e-9)


if __name__ == "__main__":
    unittest.main()
