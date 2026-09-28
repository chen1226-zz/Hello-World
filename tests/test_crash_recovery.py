"""kill -9 mid-transaction, then restart: no partial/duplicate side effects."""

from __future__ import annotations
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from idempotent_consumer.consumer import consume_file
from idempotent_consumer.db import connect, count_entries, grand_total, init_db, totals_by_account
from idempotent_consumer.events import Event, gen_batch, write_jsonl
from idempotent_consumer.legacy import legacy_consume

ROOT = Path(__file__).resolve().parents[1]


class CrashRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.events = gen_batch()
        self.input = self.tmp / "events.jsonl"
        write_jsonl(str(self.input), self.events)

        self.truth_db = self.tmp / "truth.db"
        init_db(str(self.truth_db))
        consume_file(str(self.truth_db), str(self.input))
        conn = connect(str(self.truth_db))
        try:
            self.truth_accounts = totals_by_account(conn)
            self.truth_total = grand_total(conn)
        finally:
            conn.close()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_kill9_before_commit_leaves_no_partial_state(self) -> None:
        """SIGKILL after the ledger INSERT but before COMMIT must roll back.

        The consumer is killed while processing evt-0005 (events before it are
        already committed). After restart with the same stream the totals must
        equal a clean single pass -- the killed event is neither lost nor
        double-applied.
        """
        target = "evt-0005"
        db = self.tmp / "crash.db"
        init_db(str(db))

        cmd = [
            sys.executable,
            "-m",
            "idempotent_consumer.consumer",
            "--db",
            str(db),
            "--input",
            str(self.input),
            "--kill9-before-commit-on",
            target,
        ]
        proc = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertIn(proc.returncode, (-signal.SIGKILL, 137))

        # inspect post-crash state: events before target durable, target
        # itself must have neither a ledger row nor a processed marker
        conn = connect(str(db))
        try:
            self.assertEqual(count_entries(conn), 5)
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM ledger_entries WHERE event_id=?", (target,)
                ).fetchone()
            )
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM processed_events WHERE event_id=?", (target,)
                ).fetchone()
            )
        finally:
            conn.close()

        # restart: upstream re-delivers the full stream
        applied, duplicates = consume_file(str(db), str(self.input))
        self.assertEqual((applied, duplicates), (15, 5))

        conn = connect(str(db))
        try:
            self.assertEqual(count_entries(conn), 20)
            self.assertEqual(grand_total(conn), self.truth_total)
            self.assertEqual(totals_by_account(conn), self.truth_accounts)
        finally:
            conn.close()


class LegacyRegressionTests(unittest.TestCase):
    """Pin the original failures so the fix's motivation stays explicit."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.input = self.tmp / "replay.jsonl"
        from idempotent_consumer.events import replay_stream

        write_jsonl(str(self.input), replay_stream(gen_batch()))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_legacy_double_counts_on_redelivery(self) -> None:
        db = self.tmp / "legacy.db"
        legacy_consume(str(db), str(self.input))
        conn = connect(str(db))
        try:
            # legacy schema has no UNIQUE(event_id) on the ledger
            self.assertEqual(count_entries(conn), 42)
            self.assertEqual(grand_total(conn), 3900)
        finally:
            conn.close()

    def test_legacy_separate_txns_leave_inconsistent_crash_state(self) -> None:
        """Crash between the two commits: marked processed but not applied."""
        db = self.tmp / "legacy_crash.db"
        one = self.tmp / "one.jsonl"
        write_jsonl(
            str(one),
            [
                Event("evt-x", "acct-1", 500, "2026-09-28T10:00:00Z"),
            ],
        )
        with self.assertRaises(SystemExit):
            legacy_consume(str(db), str(one), crash_between_txns=True)
        conn = connect(str(db))
        try:
            self.assertEqual(count_entries(conn), 0)  # business effect missing
            marked = conn.execute(
                "SELECT COUNT(*) FROM processed_keys WHERE event_id='evt-x'"
            ).fetchone()[0]
            self.assertEqual(marked, 1)  # ...yet the dedup marker exists
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
