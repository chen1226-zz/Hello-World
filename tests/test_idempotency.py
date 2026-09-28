"""Redelivery and restart idempotency tests for the fixed consumer."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from idempotent_consumer.consumer import consume_file
from idempotent_consumer.db import (
    connect,
    count_entries,
    grand_total,
    init_db,
    totals_by_account,
)
from idempotent_consumer.events import gen_batch, replay_stream, write_jsonl


class IdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.base = gen_batch()
        self.one_pass = self.tmp / "one.jsonl"
        self.replay = self.tmp / "replay.jsonl"
        write_jsonl(str(self.one_pass), self.base)
        write_jsonl(str(self.replay), replay_stream(self.base))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _ground_truth(self) -> tuple[dict[str, int], int]:
        db = self.tmp / "truth.db"
        init_db(str(db))
        consume_file(str(db), str(self.one_pass))
        conn = connect(str(db))
        try:
            return totals_by_account(conn), grand_total(conn)
        finally:
            conn.close()

    def test_single_pass(self) -> None:
        db = self.tmp / "single.db"
        init_db(str(db))
        applied, duplicates = consume_file(str(db), str(self.one_pass))
        self.assertEqual((applied, duplicates), (20, 0))
        conn = connect(str(db))
        try:
            self.assertEqual(count_entries(conn), 20)
        finally:
            conn.close()

    def test_redelivery_matches_single_pass(self) -> None:
        truth_accounts, truth_total = self._ground_truth()

        db = self.tmp / "replayed.db"
        init_db(str(db))
        applied, duplicates = consume_file(str(db), str(self.replay))
        self.assertEqual(applied, 20)
        self.assertEqual(duplicates, 22)

        conn = connect(str(db))
        try:
            self.assertEqual(count_entries(conn), 20)
            self.assertEqual(grand_total(conn), truth_total)
            self.assertEqual(totals_by_account(conn), truth_accounts)
            # each event_id appears in both tables exactly once
            dupes = conn.execute(
                "SELECT event_id, COUNT(*) FROM ledger_entries GROUP BY event_id "
                "HAVING COUNT(*) > 1"
            ).fetchall()
            self.assertEqual(dupes, [])
        finally:
            conn.close()

    def test_restart_replays_remaining_stream(self) -> None:
        """Process restart with re-delivery (at-least-once) stays idempotent."""
        truth_accounts, truth_total = self._ground_truth()
        db = self.tmp / "restart.db"
        init_db(str(db))

        # "run 1" dies/crashes halfway; on restart upstream resends the file.
        halfway = self.tmp / "half.jsonl"
        write_jsonl(str(halfway), self.base[:11])
        consume_file(str(db), str(halfway))
        consume_file(str(db), str(self.replay))

        conn = connect(str(db))
        try:
            self.assertEqual(count_entries(conn), 20)
            self.assertEqual(grand_total(conn), truth_total)
            self.assertEqual(totals_by_account(conn), truth_accounts)
        finally:
            conn.close()

    def test_out_of_order_does_not_change_summary(self) -> None:
        truth_accounts, truth_total = self._ground_truth()
        shuffled = self.tmp / "shuffled.jsonl"
        events = list(reversed(self.base))
        write_jsonl(str(shuffled), events)
        db = self.tmp / "shuffled.db"
        init_db(str(db))
        consume_file(str(db), str(shuffled))
        conn = connect(str(db))
        try:
            self.assertEqual(grand_total(conn), truth_total)
            self.assertEqual(totals_by_account(conn), truth_accounts)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
