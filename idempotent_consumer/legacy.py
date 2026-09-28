"""Original (broken) consumer, kept for reproduction and documentation.

Two deliberate defects make redelivery double-count:

1. Timestamp inside the idempotency key
   ``processed_keys.dedup_key = event_id || received_at``. Every redelivery
   happens at a different wall-clock instant, so the key never matches and
   the same event is inserted into ``processed_keys`` over and over.

2. Check / insert / business write in separate transactions
   The existence check, the dedup insert and the ledger insert each commit
   independently. Two consequences:
   * TOCTOU: two consumers can both see "not processed" before either
     insert lands, so both write the business row.
   * crash window: a kill between the dedup COMMIT and the ledger COMMIT
     leaves the event marked processed without its business effect (and the
     reverse order leaves an effect the dedup table does not know about).

The ledger also lacks any UNIQUE constraint on event_id, so nothing at the
database level stops duplicate business rows.
"""

from __future__ import annotations

import datetime as _dt
import sqlite3

from .events import Event, read_jsonl

LEGACY_SCHEMA = """
CREATE TABLE IF NOT EXISTS ledger_entries (
    id          INTEGER PRIMARY KEY,
    event_id    TEXT NOT NULL,
    account     TEXT NOT NULL,
    amount      INTEGER NOT NULL,
    emitted_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_keys (
    dedup_key    TEXT PRIMARY KEY,
    event_id     TEXT NOT NULL,
    received_at  TEXT NOT NULL
);
"""


def _legacy_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10.0)
    conn.execute("PRAGMA busy_timeout=10000")
    conn.executescript(LEGACY_SCHEMA)
    return conn


def _dedup_key(event: Event) -> str:
    # BUG: wall-clock receive timestamp makes every redelivery unique.
    received_at = _dt.datetime.now(_dt.timezone.utc).isoformat()
    return f"{event.event_id}|{received_at}"


def legacy_process_event(
    conn: sqlite3.Connection,
    event: Event,
    crash_between_txns: bool = False,
) -> bool:
    key = _dedup_key(event)
    exists = conn.execute(
        "SELECT 1 FROM processed_keys WHERE dedup_key=?", (key,)
    ).fetchone()
    if exists:
        return False

    # BUG: dedup record commits in its own transaction...
    conn.execute(
        "INSERT INTO processed_keys(dedup_key, event_id, received_at) "
        "VALUES (?, ?, ?)",
        (key, event.event_id, key.split("|", 1)[1]),
    )
    conn.commit()

    if crash_between_txns:
        # Simulates kill -9 landing in the gap between the two commits:
        # event is marked processed but the ledger write never happened.
        raise SystemExit(77)

    # ...and the business write commits separately -- no atomicity.
    conn.execute(
        "INSERT INTO ledger_entries(event_id, account, amount, emitted_at) "
        "VALUES (?, ?, ?, ?)",
        (event.event_id, event.account, event.amount, event.emitted_at),
    )
    conn.commit()
    return True


def legacy_consume(
    db_path: str, input_path: str, crash_between_txns: bool = False
) -> tuple[int, int]:
    conn = _legacy_connect(db_path)
    applied = duplicates = 0
    try:
        for event in read_jsonl(input_path):
            if legacy_process_event(conn, event, crash_between_txns):
                applied += 1
            else:
                duplicates += 1
    finally:
        conn.close()
    return applied, duplicates
