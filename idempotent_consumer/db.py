"""SQLite schema and connection helpers.

Design
------
``processed_events(event_id PRIMARY KEY)`` is the idempotency ledger.  The
UNIQUE constraint is the real deduplication guarantee: even under process
restart or multi-process concurrency the database rejects a second
``event_id`` atomically.

``ledger_entries.event_id`` carries its own UNIQUE constraint as defence in
depth: the business write itself can never duplicate an event, independent
of the ledger.
"""

from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS ledger_entries (
    id          INTEGER PRIMARY KEY,
    event_id    TEXT NOT NULL UNIQUE,
    account     TEXT NOT NULL,
    amount      INTEGER NOT NULL,
    emitted_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id    TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    """Open a connection configured for multi-process consumers.

    ``isolation_level=None`` puts the driver in autocommit mode so that all
    transaction boundaries are explicit (``BEGIN IMMEDIATE`` / ``COMMIT``).
    WAL lets a reader and multiple writers coexist; the busy timeout turns
    lock contention into orderly waiting instead of ``database is locked``.
    """
    conn = sqlite3.connect(db_path, isolation_level=None, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def init_db(db_path: str) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


def totals_by_account(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT account, SUM(amount) FROM ledger_entries GROUP BY account"
    ).fetchall()
    return {account: total for account, total in rows}


def grand_total(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(SUM(amount), 0) FROM ledger_entries").fetchone()
    return int(row[0])


def count_entries(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM ledger_entries").fetchone()[0])
