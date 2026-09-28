"""Concurrent-safe SQLite job queue.

Concurrency design (see README.md for the full before/after analysis):

* One ``JobStore`` (one ``sqlite3.Connection``) per process/thread.
  Connections are never shared across workers, so there is no
  contention on the Python object itself.
* WAL journal mode: readers never block the single writer and vice
  versa, which removes most lock conflicts up front.
* ``busy_timeout``: when a lock conflict does happen, SQLite waits
  inside the lock instead of raising ``database is locked`` at once.
* Short ``BEGIN IMMEDIATE`` transactions for claiming: the write lock
  is taken up front (no deferred-upgrade deadlock between two workers
  that both started as readers) and the select-then-update is atomic,
  so a job can never be claimed twice.
* Retries with backoff are only a last-resort safety net for transient
  ``SQLITE_BUSY``; correctness comes from the points above, not from
  retry counts.
"""

import sqlite3
import time

__all__ = ["JobStore"]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    owner TEXT,
    claimed_at REAL,
    finished_at REAL
);
CREATE TABLE IF NOT EXISTS claims (
    job_id INTEGER NOT NULL,
    worker TEXT NOT NULL,
    claimed_at REAL NOT NULL
);
"""

BUSY_TIMEOUT_MS = 30_000
MAX_RETRIES = 5


def _locked(exc):
    return isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc)


class JobStore:
    """SQLite-backed job queue. Create one instance per worker."""

    def __init__(self, path, busy_timeout_ms=BUSY_TIMEOUT_MS):
        self.path = str(path)
        self._conn = sqlite3.connect(
            self.path, timeout=busy_timeout_ms / 1000.0, isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _run(self, op):
        """Run ``op``; retry only transient lock errors as a safety net."""
        delay = 0.005
        for attempt in range(MAX_RETRIES):
            try:
                return op()
            except sqlite3.OperationalError as exc:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                if not _locked(exc) or attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.5)

    def add_job(self, payload):
        def op():
            cur = self._conn.execute(
                "INSERT INTO jobs (payload) VALUES (?)", (payload,)
            )
            return cur.lastrowid

        return self._run(op)

    def claim(self, worker):
        """Atomically claim the oldest pending job; None if queue empty."""
        def op():
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT id, payload FROM jobs"
                    " WHERE status='pending' ORDER BY id LIMIT 1"
                ).fetchone()
                if row is None:
                    self._conn.execute("COMMIT")
                    return None
                now = time.time()
                self._conn.execute(
                    "UPDATE jobs SET status='running', owner=?, claimed_at=?"
                    " WHERE id=?",
                    (worker, now, row[0]),
                )
                self._conn.execute(
                    "INSERT INTO claims (job_id, worker, claimed_at)"
                    " VALUES (?,?,?)",
                    (row[0], worker, now),
                )
                self._conn.execute("COMMIT")
                return {"id": row[0], "payload": row[1]}
            except BaseException:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise

        return self._run(op)

    def complete(self, job_id, worker):
        """Mark a running job owned by ``worker`` as done."""
        def op():
            cur = self._conn.execute(
                "UPDATE jobs SET status='done', finished_at=?"
                " WHERE id=? AND owner=? AND status='running'",
                (time.time(), job_id, worker),
            )
            return cur.rowcount == 1

        return self._run(op)

    def requeue_expired(self, lease_seconds):
        """Crash recovery: return stale running jobs to the pending pool."""
        cutoff = time.time() - lease_seconds

        def op():
            cur = self._conn.execute(
                "UPDATE jobs SET status='pending', owner=NULL, claimed_at=NULL"
                " WHERE status='running' AND claimed_at < ?",
                (cutoff,),
            )
            return cur.rowcount

        return self._run(op)

    def counts(self):
        def op():
            rows = self._conn.execute(
                "SELECT status, COUNT(*) FROM jobs GROUP BY status"
            ).fetchall()
            return dict(rows)

        return self._run(op)

    def claim_counts(self):
        """Return [(job_id, times_claimed)] for verification/auditing."""
        def op():
            return self._conn.execute(
                "SELECT job_id, COUNT(*) FROM claims GROUP BY job_id"
            ).fetchall()

        return self._run(op)
