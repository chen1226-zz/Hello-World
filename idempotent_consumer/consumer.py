"""Idempotent consumer (fixed implementation).

For every event we run ONE explicit transaction containing both mutations:

1. claim the upstream ``event_id`` in ``processed_events``
2. append the business row to ``ledger_entries``

``BEGIN IMMEDIATE`` acquires the write lock up front, so two consumers
processing the same event are serialised: the loser of the race gets a
UNIQUE violation, rolls the whole transaction back and treats the event as
already handled -- exactly one side effect is ever produced.

Because both writes share a transaction, a ``kill -9`` / power loss at any
point leaves either both rows present or neither; there is no state in
which the event is "deduplicated but not applied" or "applied but not
recorded".
"""

from __future__ import annotations

import os
import signal
import sqlite3
from collections.abc import Callable

from .db import connect, init_db
from .events import Event, read_jsonl


class DuplicateEvent(Exception):
    """Raised inside the transaction when the idempotency key already exists."""


def process_event(
    conn: sqlite3.Connection,
    event: Event,
    crash_hook: Callable[[Event], None] | None = None,
) -> bool:
    """Process one event atomically. Returns True if it was newly applied."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        claimed = conn.execute(
            "INSERT INTO processed_events(event_id) VALUES (?) ON CONFLICT(event_id) "
            "DO NOTHING",
            (event.event_id,),
        ).rowcount
        if claimed == 0:
            # Redelivery (or a concurrent consumer won the race): nothing may
            # be applied twice.
            conn.execute("ROLLBACK")
            return False

        # Crash-test hook: die after the business write but before COMMIT.
        # The open transaction is discarded by the OS, proving atomicity.
        if crash_hook is not None:
            crash_hook(event)

        conn.execute(
            "INSERT INTO ledger_entries(event_id, account, amount, emitted_at) "
            "VALUES (?, ?, ?, ?)",
            (event.event_id, event.account, event.amount, event.emitted_at),
        )
        conn.execute("COMMIT")
        return True
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def consume_file(
    db_path: str,
    input_path: str,
    crash_hook: Callable[[Event], None] | None = None,
) -> tuple[int, int]:
    """Consume a JSONL file. Returns (newly_applied, duplicates_seen)."""
    init_db(db_path)
    conn = connect(db_path)
    applied = duplicates = 0
    try:
        for event in read_jsonl(input_path):
            if process_event(conn, event, crash_hook):
                applied += 1
            else:
                duplicates += 1
    finally:
        conn.close()
    return applied, duplicates


def _kill9_hook(target_event_id: str) -> Callable[[Event], None]:
    """Hook used by the crash test/CLI: SIGKILL while the txn is uncommitted."""

    def hook(event: Event) -> None:
        if event.event_id == target_event_id:
            # flush WAL visibility isn't needed: an uncommitted txn is never
            # durable; SIGKILL simply discards the connection.
            os.kill(os.getpid(), signal.SIGKILL)

    return hook


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Idempotent event consumer")
    parser.add_argument("--db", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument(
        "--kill9-before-commit-on",
        metavar="EVENT_ID",
        help="crash-test hook: SIGKILL after processing this event but before COMMIT",
    )
    args = parser.parse_args(argv)

    hook = _kill9_hook(args.kill9_before_commit_on) if args.kill9_before_commit_on else None
    applied, duplicates = consume_file(args.db, args.input, hook)
    print(f"applied={applied} duplicates={duplicates}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
