"""Cross-process single-flight cache: lease + CAS write-back + SWR. db_path=None -> in-process."""
import random
import sqlite3
import threading
import time

_LEASE_SECONDS = 10.0


class HotCache:
    def __init__(self, origin, ttl, jitter=0.0, stale_ttl=0.0, db_path=None, clock=None):
        self.origin = origin
        self.ttl = ttl
        self.jitter = jitter
        self.stale_ttl = stale_ttl
        self.clock = clock or time.time
        self._mem = {}
        self._mem_lease = {}
        self._lock = threading.Lock()
        self._bg_lock = threading.Lock()
        self._bg = set()
        self._local_stats = {"refreshes": 0, "hits": 0, "stale_served": 0}
        self._db = None
        if db_path:
            self._db = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
            self._db.execute("PRAGMA busy_timeout=30000")
            for _ in range(3000):
                try:
                    self._db.execute("PRAGMA journal_mode=WAL")
                    break
                except sqlite3.OperationalError:
                    time.sleep(0.01)
            self._db.execute("CREATE TABLE IF NOT EXISTS hotcache("
                             "key TEXT PRIMARY KEY, value TEXT, version INTEGER,"
                             " expires_at REAL, refreshing_until REAL)")
            self._db.execute("CREATE TABLE IF NOT EXISTS hotcache_stats("
                             "name TEXT PRIMARY KEY, value INTEGER)")

    def get(self, key):
        now = self.clock()
        row = self._read(key)
        if row and row[2] > now:
            self._bump("hits")
            return row[0]
        if row and row[0] is not None and row[2] + self.stale_ttl > now:
            self._bump("hits")
            self._refresh_async(key)
            return row[0]
        return self._refresh_sync(key)

    def stats(self):
        if self._db is None:
            return dict(self._local_stats, keys=len(self._mem))
        counts = dict(self._db.execute("SELECT name, value FROM hotcache_stats"))
        keys = self._db.execute("SELECT COUNT(*) FROM hotcache").fetchone()[0]
        return {"refreshes": counts.get("refreshes", 0), "hits": counts.get("hits", 0),
                "stale_served": counts.get("stale_served", 0), "keys": keys}

    def _refresh_sync(self, key):
        status, value, version = self._try_lock(key)
        if status == "fresh":
            self._bump("hits")
            return value
        if status == "wait":
            return self._wait_for_holder(key)
        self._bump("refreshes")
        try:
            new_value = self.origin(key)
        except Exception:
            self._unlock(key, version)
            if value is not None:
                self._bump("stale_served")
                return value
            raise
        self._writeback(key, new_value, version)
        return new_value

    def _refresh_async(self, key):
        with self._bg_lock:
            if key in self._bg:
                return
            self._bg.add(key)
        threading.Thread(target=self._bg_refresh, args=(key,), daemon=True).start()

    def _bg_refresh(self, key):
        try:
            status, _value, version = self._try_lock(key)
            if status != "locked":
                return
            self._bump("refreshes")
            try:
                new_value = self.origin(key)
            except Exception:
                self._unlock(key, version)
                return
            self._writeback(key, new_value, version)
        finally:
            with self._bg_lock:
                self._bg.discard(key)

    def _wait_for_holder(self, key):
        deadline = time.monotonic() + _LEASE_SECONDS + 5.0
        while time.monotonic() < deadline:
            time.sleep(0.01)
            row = self._read(key)
            if row is None:
                continue
            value, _version, expires_at, lease = row
            if expires_at > self.clock():
                self._bump("hits")
                return value
            if lease is None or lease <= self.clock():
                if value is not None:
                    self._bump("stale_served")
                    return value
                raise RuntimeError("origin fetch failed for key %r" % (key,))
        raise TimeoutError("timed out waiting for refresh of key %r" % (key,))

    def _read(self, key):
        if self._db is None:
            with self._lock:
                e = self._mem.get(key)
                return None if e is None else (e[0], e[1], e[2], self._mem_lease.get(key))
        r = self._db.execute(
            "SELECT value, version, expires_at, refreshing_until FROM hotcache WHERE key=?",
            (key,)).fetchone()
        return tuple(r) if r else None

    def _try_lock(self, key):
        now = self.clock()
        lease_until = now + _LEASE_SECONDS
        if self._db is None:
            with self._lock:
                e = self._mem.get(key)
                if e and e[2] > now:
                    return ("fresh", e[0], e[1])
                lease = self._mem_lease.get(key)
                if lease is not None and lease > now:
                    return ("wait", None, None)
                self._mem_lease[key] = lease_until
                if e is None:
                    self._mem[key] = [None, 0, 0.0]
                    return ("locked", None, 0)
                return ("locked", e[0], e[1])
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                r = self._db.execute(
                    "SELECT value, version, expires_at, refreshing_until FROM hotcache WHERE key=?",
                    (key,)).fetchone()
                if r and r[2] > now:
                    result = ("fresh", r[0], r[1])
                elif r and r[3] is not None and r[3] > now:
                    result = ("wait", None, None)
                elif r is None:
                    self._db.execute(
                        "INSERT INTO hotcache VALUES(?, NULL, 0, 0.0, ?)", (key, lease_until))
                    result = ("locked", None, 0)
                else:
                    self._db.execute(
                        "UPDATE hotcache SET refreshing_until=? WHERE key=? AND version=?",
                        (lease_until, key, r[1]))
                    result = ("locked", r[0], r[1])
                self._db.execute("COMMIT")
                return result
            except Exception:
                self._db.execute("ROLLBACK")
                raise

    def _writeback(self, key, value, version):
        expires = self.clock() + self.ttl
        if self.jitter:
            expires += random.uniform(0.0, self.jitter)
        if self._db is None:
            with self._lock:
                e = self._mem.get(key)
                if e and e[1] == version:
                    self._mem[key] = [value, version + 1, expires]
                self._mem_lease.pop(key, None)
            return
        self._db.execute(
            "UPDATE hotcache SET value=?, version=?, expires_at=?, refreshing_until=NULL"
            " WHERE key=? AND version=?",
            (value, version + 1, expires, key, version))

    def _unlock(self, key, version):
        if self._db is None:
            with self._lock:
                self._mem_lease.pop(key, None)
        else:
            self._db.execute("UPDATE hotcache SET refreshing_until=NULL WHERE key=? AND version=?",
                             (key, version))

    def _bump(self, name):
        if self._db is None:
            self._local_stats[name] += 1
        else:
            self._db.execute("INSERT INTO hotcache_stats VALUES(?, 1)"
                             " ON CONFLICT(name) DO UPDATE SET value = value + 1", (name,))
