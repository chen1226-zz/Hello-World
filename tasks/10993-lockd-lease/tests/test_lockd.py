import threading
import time
import unittest

from lockd import LockServer, LeaseLock, LeaseLost


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, dt):
        self.now += dt


def wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


class PartitionProxy:
    """Server proxy that drops every call while partitioned."""

    def __init__(self, inner):
        self._inner = inner
        self.partitioned = False

    def _call(self, fn, *args):
        if self.partitioned:
            raise ConnectionError("network partition")
        return fn(*args)

    def acquire(self, *a):
        return self._call(self._inner.acquire, *a)

    def renew(self, *a):
        return self._call(self._inner.renew, *a)

    def release(self, *a):
        return self._call(self._inner.release, *a)

    def valid(self, *a):
        return self._call(self._inner.valid, *a)


class LeaseBoundaryTest(unittest.TestCase):
    def test_expiry_boundary(self):
        clock = FakeClock()
        srv = LockServer(clock=clock)
        t1 = srv.acquire("r", "a", 0.2)
        self.assertGreater(t1, 0)
        clock.advance(0.199)
        self.assertTrue(srv.valid("r", t1))
        self.assertEqual(srv.acquire("r", "b", 0.2), 0)  # still held
        clock.advance(0.002)  # just past the expiry boundary
        self.assertFalse(srv.valid("r", t1))
        t2 = srv.acquire("r", "b", 0.2)
        self.assertGreater(t2, t1)  # fencing token strictly increases

    def test_exact_boundary_is_expired(self):
        clock = FakeClock()
        srv = LockServer(clock=clock)
        t1 = srv.acquire("r", "a", 0.2)
        clock.advance(0.2)  # exactly at expiry: the lease is dead
        self.assertFalse(srv.valid("r", t1))
        self.assertFalse(srv.renew("r", t1, 0.2))

    def test_renew_boundary(self):
        clock = FakeClock()
        srv = LockServer(clock=clock)
        t1 = srv.acquire("r", "a", 0.2)
        clock.advance(0.15)
        self.assertTrue(srv.renew("r", t1, 0.2))
        clock.advance(0.199)
        self.assertTrue(srv.valid("r", t1))  # would be expired without renew
        clock.advance(0.002)
        self.assertFalse(srv.valid("r", t1))


class RenewFailureTest(unittest.TestCase):
    def test_renew_rejects_wrong_token_and_expired_lease(self):
        clock = FakeClock()
        srv = LockServer(clock=clock)
        t1 = srv.acquire("r", "a", 0.2)
        self.assertFalse(srv.renew("r", t1 + 999, 0.2))  # wrong token
        clock.advance(0.2)
        self.assertFalse(srv.renew("r", t1, 0.2))  # cannot renew expired lease

    def test_client_marks_lost_when_renew_fails(self):
        srv = LockServer()
        lock = LeaseLock(srv, "r", "a", ttl=0.05)
        self.assertTrue(lock.acquire(timeout=1))
        srv.release("r", lock.token)  # lease yanked behind the client's back
        self.assertTrue(wait_for(lambda: lock.lost))
        with self.assertRaises(LeaseLost):
            lock.check()
        lock.release()

    def test_renewal_keeps_short_lease_alive(self):
        srv = LockServer()
        lock = LeaseLock(srv, "r", "a", ttl=0.1)
        self.assertTrue(lock.acquire(timeout=1))
        time.sleep(0.35)  # 3.5x ttl: only renewal keeps us alive
        self.assertFalse(lock.lost)
        lock.check()  # must not raise
        self.assertTrue(srv.valid("r", lock.token))
        lock.release()
        self.assertEqual(srv.acquire("r", "b", 0.1) > 0, True)


class KillRecoveryTest(unittest.TestCase):
    def test_killed_holder_recovers_with_new_token(self):
        srv = LockServer()
        victim = LeaseLock(srv, "r", "victim", ttl=0.05)
        self.assertTrue(victim.acquire(timeout=1))
        old_token = victim.token
        victim._stop.set()  # simulate SIGKILL: renewer stops, no release
        victim._thread.join(timeout=2)
        time.sleep(0.08)  # lease expires server-side
        survivor = LeaseLock(srv, "r", "survivor", ttl=0.05)
        self.assertTrue(survivor.acquire(timeout=2))
        self.assertGreater(survivor.token, old_token)
        # killed worker "wakes" (SIGCONT) and tries to use its old token
        self.assertFalse(srv.valid("r", old_token))
        with self.assertRaises(LeaseLost):
            victim.check()
        # restarted process must re-acquire and gets a fresh fencing token
        survivor.release()
        restarted = LeaseLock(srv, "r", "victim", ttl=0.05)
        self.assertTrue(restarted.acquire(timeout=2))
        self.assertGreater(restarted.token, survivor.token)
        restarted.release()
        victim.release()


class PartitionRecoveryTest(unittest.TestCase):
    def test_partition_then_heal(self):
        srv = LockServer()
        proxy = PartitionProxy(srv)
        lock = LeaseLock(proxy, "r", "a", ttl=0.05)
        self.assertTrue(lock.acquire(timeout=1))
        old_token = lock.token
        proxy.partitioned = True
        # renewals fail during the partition: client must give up the lock
        self.assertTrue(wait_for(lambda: lock.lost))
        time.sleep(0.08)  # lease expires server-side while partitioned
        proxy.partitioned = False  # heal
        with self.assertRaises(LeaseLost):
            lock.check()
        self.assertFalse(srv.renew("r", old_token, 0.05))  # cannot resurrect
        other = LeaseLock(proxy, "r", "b", ttl=0.05)
        self.assertTrue(other.acquire(timeout=2))
        self.assertGreater(other.token, old_token)
        other.release()
        lock.release()

    def test_acquire_retries_through_partition(self):
        srv = LockServer()
        proxy = PartitionProxy(srv)
        proxy.partitioned = True
        result = []

        def try_acquire():
            result.append(LeaseLock(proxy, "r", "a", ttl=0.05).acquire(timeout=2))

        thread = threading.Thread(target=try_acquire)
        thread.start()
        time.sleep(0.1)
        proxy.partitioned = False  # heal while the client is still retrying
        thread.join(timeout=3)
        self.assertEqual(result, [True])


class ChaosStressTest(unittest.TestCase):
    def test_200_groups_zero_concurrent_entries(self):
        import chaos
        commits, rejected, violations = chaos.run_chaos(
            runs=200, lanes=10, biz_delay=0.05, ttl=0.02, freeze_dur=0.04)
        self.assertEqual(violations, 0)
        self.assertEqual(commits, 200)  # every task committed exactly once
        self.assertGreater(rejected, 0)  # fencing actually fired


if __name__ == "__main__":
    unittest.main()
