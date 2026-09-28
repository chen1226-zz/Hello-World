import multiprocessing as mp
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from hotcache import HotCache

_ORIGIN_DB = None


def _counting_origin(key):
    conn = sqlite3.connect(_ORIGIN_DB, timeout=30)
    conn.execute("CREATE TABLE IF NOT EXISTS calls(key TEXT PRIMARY KEY, n INTEGER)")
    conn.execute("INSERT INTO calls VALUES(?, 1)"
                 " ON CONFLICT(key) DO UPDATE SET n = n + 1", (key,))
    conn.commit()
    conn.close()
    time.sleep(0.2)
    return "v-" + key


def _mp_worker(db, keys, start_at, queue):
    while time.time() < start_at:
        time.sleep(0.005)
    cache = HotCache(_counting_origin, ttl=30.0, db_path=db)
    queue.put([cache.get(k) for k in keys])


class MemModeTest(unittest.TestCase):
    def test_hit_and_stats_shape(self):
        calls = []
        cache = HotCache(lambda k: calls.append(k) or "v-" + k, ttl=10.0)
        self.assertEqual(cache.get("a"), "v-a")
        self.assertEqual(cache.get("a"), "v-a")
        self.assertEqual(len(calls), 1)
        stats = cache.stats()
        for name in ("refreshes", "hits", "stale_served", "keys"):
            self.assertIn(name, stats)
        self.assertEqual(stats["refreshes"], 1)
        self.assertEqual(stats["hits"], 1)
        self.assertEqual(stats["keys"], 1)

    def test_failure_fallback_serves_last_good(self):
        state = {"fail": False}

        def origin(key):
            if state["fail"]:
                raise RuntimeError("origin down")
            return "good"

        cache = HotCache(origin, ttl=0.1, stale_ttl=0.0)
        self.assertEqual(cache.get("a"), "good")
        state["fail"] = True
        time.sleep(0.15)
        self.assertEqual(cache.get("a"), "good")
        self.assertEqual(cache.stats()["stale_served"], 1)

    def test_failure_without_known_value_raises(self):
        def origin(key):
            raise RuntimeError("origin down")

        cache = HotCache(origin, ttl=0.1)
        with self.assertRaises(RuntimeError):
            cache.get("nope")
        self.assertEqual(cache.stats()["stale_served"], 0)

    def test_swr_returns_stale_immediately_and_refreshes_once(self):
        calls = []

        def origin(key):
            calls.append(key)
            time.sleep(0.3)
            return "v%d" % len(calls)

        cache = HotCache(origin, ttl=0.1, stale_ttl=60.0)
        self.assertEqual(cache.get("a"), "v1")
        time.sleep(0.15)
        t0 = time.monotonic()
        for _ in range(5):
            self.assertEqual(cache.get("a"), "v1")
        self.assertLess(time.monotonic() - t0, 0.2)
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if cache.get("a") == "v2":
                break
            time.sleep(0.02)
        else:
            self.fail("background refresh never landed")
        self.assertEqual(len(calls), 2)


class CrossProcessTest(unittest.TestCase):
    def test_single_flight_across_processes(self):
        global _ORIGIN_DB
        with tempfile.TemporaryDirectory() as tmp:
            _ORIGIN_DB = os.path.join(tmp, "cache.db")
            keys = ["k%d" % i for i in range(5)]
            ctx = mp.get_context("fork")
            queue = ctx.Queue()
            start_at = time.time() + 0.8
            procs = [ctx.Process(target=_mp_worker, args=(_ORIGIN_DB, keys, start_at, queue))
                     for _ in range(4)]
            for p in procs:
                p.start()
            results = [queue.get() for _ in procs]
            for p in procs:
                p.join(15)
            for r in results:
                self.assertEqual(r, ["v-" + k for k in keys])
            conn = sqlite3.connect(_ORIGIN_DB)
            total = conn.execute("SELECT COALESCE(SUM(n), 0) FROM calls").fetchone()[0]
            versions = [r[0] for r in conn.execute("SELECT version FROM hotcache")]
            conn.close()
            self.assertEqual(total, len(keys))
            self.assertEqual(versions, [1] * len(keys))
            stats = HotCache(_counting_origin, ttl=30.0, db_path=_ORIGIN_DB).stats()
            self.assertEqual(stats["refreshes"], len(keys))
            self.assertEqual(stats["keys"], len(keys))


class CasTest(unittest.TestCase):
    def test_stale_instance_cannot_overwrite_newer_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "cache.db")
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE hotcache(key TEXT PRIMARY KEY, value TEXT,"
                         " version INTEGER, expires_at REAL, refreshing_until REAL)")
            conn.execute("INSERT INTO hotcache VALUES('k', 'old', 5, 0.0, NULL)")
            conn.commit()
            conn.close()
            entered = threading.Event()
            release = threading.Event()

            def slow_origin(key):
                entered.set()
                release.wait(5)
                return "slow"

            cache = HotCache(slow_origin, ttl=10.0, db_path=db)
            out = {}
            t = threading.Thread(target=lambda: out.setdefault("v", cache.get("k")))
            t.start()
            self.assertTrue(entered.wait(5))
            conn = sqlite3.connect(db)
            conn.execute("UPDATE hotcache SET value='fresh', version=6, expires_at=?,"
                         " refreshing_until=NULL WHERE key='k'", (time.time() + 100,))
            conn.commit()
            conn.close()
            release.set()
            t.join(5)
            self.assertEqual(out["v"], "slow")
            conn = sqlite3.connect(db)
            row = conn.execute("SELECT value, version FROM hotcache WHERE key='k'").fetchone()
            conn.close()
            self.assertEqual(row, ("fresh", 6))


class JitterTest(unittest.TestCase):
    def test_jitter_spreads_expiry(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "cache.db")
            t0 = 1000.0
            cache = HotCache(lambda k: "v", ttl=10.0, jitter=2.0, db_path=db,
                             clock=lambda: t0)
            for i in range(200):
                cache.get("k%d" % i)
            conn = sqlite3.connect(db)
            expiries = [r[0] for r in conn.execute("SELECT expires_at FROM hotcache")]
            conn.close()
            self.assertTrue(all(t0 + 10.0 <= e <= t0 + 12.0 for e in expiries))
            self.assertGreater(max(expiries) - min(expiries), 1.0)
            self.assertGreater(len(set(expiries)), 100)


if __name__ == "__main__":
    unittest.main()
