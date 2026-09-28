"""Two real consumer processes racing over the same event stream."""

from __future__ import annotations
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from idempotent_consumer.consumer import consume_file
from idempotent_consumer.db import connect, count_entries, grand_total, init_db, totals_by_account
from idempotent_consumer.events import gen_batch, write_jsonl

ROOT = Path(__file__).resolve().parents[1]


class ConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.events = gen_batch()
        self.input = self.tmp / "events.jsonl"
        write_jsonl(str(self.input), self.events)
        self.db = self.tmp / "shared.db"
        init_db(str(self.db))

        # ground truth
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

    def test_two_processes_consume_same_stream(self) -> None:
        procs = [
            subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "idempotent_consumer.consumer",
                    "--db",
                    str(self.db),
                    "--input",
                    str(self.input),
                ],
                cwd=ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for _ in range(2)
        ]
        outputs = [p.communicate(timeout=60) for p in procs]
        for idx, (proc, (stdout, stderr)) in enumerate(zip(procs, outputs)):
            self.assertEqual(
                proc.returncode,
                0,
                msg=f"consumer {idx} failed: {stderr}",
            )

        # across both processes: exactly 20 applied, exactly 20 duplicates,
        # zero errors
        applied_total = 0
        for stdout, _ in outputs:
            tokens = dict(part.split("=") for part in stdout.strip().split())
            applied_total += int(tokens["applied"])
            self.assertEqual(int(tokens["duplicates"]), 20 - int(tokens["applied"]))
        self.assertEqual(applied_total, 20)

        conn = connect(str(self.db))
        try:
            self.assertEqual(count_entries(conn), 20)
            self.assertEqual(grand_total(conn), self.truth_total)
            self.assertEqual(totals_by_account(conn), self.truth_accounts)
            dupes = conn.execute(
                "SELECT event_id, COUNT(*) FROM ledger_entries GROUP BY event_id "
                "HAVING COUNT(*) > 1"
            ).fetchall()
            self.assertEqual(dupes, [])
            claimed = conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()[0]
            self.assertEqual(claimed, 20)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
