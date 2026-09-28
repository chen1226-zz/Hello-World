"""Consumer: pull events from upstream and apply them to the ledger.

Exactly-once design (see README.md for the full rationale):

* Idempotency key = the *business* event id (``event_id``) assigned by the
  upstream. The key is stable across retries and must never contain a
  timestamp or any other per-delivery data.
* ``processed.event_id`` is a PRIMARY KEY, so the dedup record is guarded
  by a UNIQUE constraint at the database level.
* The dedup insert and the business write run in ONE transaction
  (``BEGIN IMMEDIATE`` ... ``COMMIT``). A crash either commits both or
  rolls both back -- never "deduped but not applied", never the reverse.
* ``BEGIN IMMEDIATE`` acquires the write lock up front, so concurrent
  consumers serialize on the same ``event_id``; the loser sees the unique
  constraint absorb its insert and skips the business write.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import time

SCHEMA = """\
CREATE TABLE IF NOT EXISTS processed (
    event_id     TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE TABLE IF NOT EXISTS ledger (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    account  TEXT NOT NULL,
    amount   INTEGER NOT NULL
);
"""

CRASH_POINTS = ("after_dedup", "after_business")


def connect(db_path: str) -> sqlite3.Connection:
    # isolation_level=None -> autocommit; transactions are managed
    # explicitly so the dedup insert and the business write share one
    # transaction boundary.
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.execute("PRAGMA busy_timeout = 30000")
    _ensure_wal(conn)
    conn.executescript(SCHEMA)
    return conn


def _ensure_wal(conn: sqlite3.Connection) -> None:
    """Switch to WAL, tolerating concurrent first-time connections.

    ``PRAGMA journal_mode`` does not honour ``busy_timeout``: when another
    connection holds the database lock it fails immediately with
    ``database is locked``. Retry briefly instead.
    """
    for _ in range(600):
        try:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            if mode.lower() == "wal":
                return
            conn.execute("PRAGMA journal_mode = WAL")
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc):
                raise
            time.sleep(0.05)
    raise sqlite3.OperationalError("could not set journal_mode=WAL: database is locked")


def _maybe_crash(point: str, index: int) -> None:
    """Fault injection for tests: SIGKILL ourselves at a chosen point."""
    if os.environ.get("CRASH_POINT") != point:
        return
    if int(os.environ.get("CRASH_AT_INDEX", "-1")) != index:
        return
    os.kill(os.getpid(), signal.SIGKILL)


def apply_event(conn: sqlite3.Connection, event: dict, index: int = -1) -> bool:
    """Apply one event exactly once.

    Returns True if the event was applied, False if it was a duplicate.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "INSERT OR IGNORE INTO processed(event_id) VALUES (?)",
            (event["event_id"],),
        )
        if cur.rowcount == 0:
            # Already processed: unique constraint absorbed the insert.
            conn.rollback()
            return False
        _maybe_crash("after_dedup", index)
        conn.execute(
            "INSERT INTO ledger(event_id, account, amount) VALUES (?, ?, ?)",
            (event["event_id"], event["account"], event["amount"]),
        )
        _maybe_crash("after_business", index)
        conn.commit()
        return True
    except BaseException:
        conn.rollback()
        raise


def run(db_path: str, events: list[dict]) -> int:
    """Apply a batch of events; returns how many were actually applied."""
    conn = connect(db_path)
    try:
        applied = 0
        for i, event in enumerate(events):
            if apply_event(conn, event, index=i):
                applied += 1
        return applied
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="path to the sqlite database")
    parser.add_argument("--events-file", required=True,
                        help="path to a JSON array of events to consume")
    args = parser.parse_args()

    with open(args.events_file, encoding="utf-8") as f:
        events = json.load(f)
    applied = run(args.db, events)
    print(f"applied={applied} delivered={len(events)}")


if __name__ == "__main__":
    main()
