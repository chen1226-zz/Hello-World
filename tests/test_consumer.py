"""Tests for exactly-once consumption: duplicate delivery, concurrent
consumers, and crash (kill -9) + restart."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumer import apply_event, connect, run
from replay import expected_summary, make_events

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GIVE_SEED = 1234


def summary_of(db_path: str) -> dict:
    conn = connect(db_path)
    try:
        rows = conn.execute(
            "SELECT account, SUM(amount) FROM ledger GROUP BY account ORDER BY account"
        ).fetchall()
        return dict(rows)
    finally:
        conn.close()


class DuplicatesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "c.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_same_event_twice_applied_once(self):
        conn = connect(self.db)
        event = {"event_id": "G-0001", "account": "checking", "amount": 500}
        self.assertTrue(apply_event(conn, event))
        self.assertFalse(apply_event(conn, event))
        self.assertFalse(apply_event(conn, event))
        count = conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)
        self.assertEqual(summary_of(self.db), {"checking": 500})

    def test_replay_with_duplicates_and_out_of_order(self):
        batch = make_events(50, GIVE_SEED)
        delivery = list(reversed(batch)) + batch + batch[:10]
        run(self.db, delivery)
        self.assertEqual(summary_of(self.db), expected_summary(batch))

    def test_dedup_survives_restart(self):
        """Dedup survives a normal process restart (same database)."""
        batch = make_events(20, GIVE_SEED)
        run(self.db, batch)
        # New connection (new "process"), same data: replay the batch.
        run(self.db, batch)
        self.assertEqual(summary_of(self.db), expected_summary(batch))


class ConcurrencyTest(unittest.TestCase):
    def test_two_consumers_race_same_event(self):
        for _ in range(20):
            with tempfile.TemporaryDirectory() as d:
                db = os.path.join(d, "c.db")
                event = {"event_id": "G-0001", "account": "checking", "amount": 100}
                results: list[bool] = []
                errors: list[Exception] = []
                barrier = threading.Barrier(2)

                def worker():
                    try:
                        conn = connect(db)
                        try:
                            barrier.wait(timeout=10)
                            results.append(apply_event(conn, event))
                        finally:
                            conn.close()
                    except Exception as exc:
                        errors.append(exc)

                threads = [threading.Thread(target=worker) for _ in range(2)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(timeout=30)

                self.assertEqual(errors, [])
                self.assertEqual(sorted(results), [False, True])
                self.assertEqual(summary_of(db), {"checking": 100})

    def test_two_consumers_share_batch(self):
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "c.db")
            batch = make_events(40, GIVE_SEED)
            deliveries = (list(reversed(batch)), list(batch))

            def worker(events):
                run(db, events)

            threads = [threading.Thread(target=worker, args=(events,))
                       for events in deliveries]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(summary_of(db), expected_summary(batch))

class CrashTest(unittest.TestCase):
    def _run_g(self, db, events, crash_point=None, crash_at=None):
        with tempfile.TemporaryDirectory() as d:
            events_file = os.path.join(d, "events.json")
            with open(events_file, "w", encoding="utf-8") as f:
                json.dump(events, f)
            env = dict(os.environ)
            if crash_point is not None:
                env["CRASH_POINT"] = crash_point
                env["CRASH_AT_INDEX"] = str(crash_at)
            return subprocess.run(
                [sys.executable, os.path.join(ROOT, "consumer.py"),
                 "--db", db, "--events-file", events_file],
                env=env, capture_output=True, text=True,
            )

    def _assert_consistent(self, db):
        """No 'deduped but not written' and no 'written but not deduped'."""
        conn = connect(db)
        try:
            g = {r[0] for r in conn.execute("SELECT event_id FROM processed")}
            d = {r[0] for r in conn.execute("SELECT DISTINCT event_id FROM ledger")}
            self.assertEqual(g, d)
        finally:
            conn.close()

    def _check_crash_and_restart(self, crash_point):
        batch = make_events(20, GIVE_SEED)
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "c.db")
            # Crash mid-batch (index 7) at the chosen point.
            result = self._run_g(db, batch, crash_point=crash_point, crash_at=7)
            self.assertNotEqual(result.returncode, 0)
            # After the crash, the DB must be consistent: for every event,
            # either both tables have it or neither does.
            self._assert_consistent(db)
            # Restart and re-deliver the whole batch.
            result = self._run_g(db, batch)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(summary_of(db), expected_summary(batch))
            self._assert_consistent(db)

    def test_kill_after_dedup_before_business(self):
        self._check_crash_and_restart("after_dedup")

    def test_kill_after_business_before_commit(self):
        self._check_crash_and_restart("after_business")


if __name__ == "__main__":
    unittest.main()
