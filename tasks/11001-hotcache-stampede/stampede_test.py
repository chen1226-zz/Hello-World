"""Acceptance: 4 processes stampede the same expired keys against one sqlite db."""
import multiprocessing as mp
import os
import shutil
import sqlite3
import tempfile
import time

from hotcache import HotCache

TMP = tempfile.mkdtemp(prefix="hotcache-stampede-")
DB = os.path.join(TMP, "cache.db")
FLAG = os.path.join(TMP, "origin-down.flag")
KEYS = ["key-%d" % i for i in range(8)]


def origin(key):
    """Fake origin: counts calls in the db, slow, and can be toggled down."""
    if os.path.exists(FLAG):
        raise RuntimeError("origin is down")
    conn = sqlite3.connect(DB, timeout=30)
    conn.execute("CREATE TABLE IF NOT EXISTS origin_calls(key TEXT PRIMARY KEY, n INTEGER)")
    conn.execute("INSERT INTO origin_calls(key, n) VALUES(?, 1)"
                 " ON CONFLICT(key) DO UPDATE SET n = n + 1", (key,))
    conn.commit()
    conn.close()
    time.sleep(0.3)
    return "value-of-" + key


def worker(start_at, queue):
    while time.time() < start_at:
        time.sleep(0.005)
    cache = HotCache(origin, ttl=0.5, jitter=0.0, stale_ttl=30.0, db_path=DB)
    queue.put([cache.get(k) for k in KEYS])


def main():
    ctx = mp.get_context("fork")
    queue = ctx.Queue()
    start_at = time.time() + 1.0
    procs = [ctx.Process(target=worker, args=(start_at, queue)) for _ in range(4)]
    for p in procs:
        p.start()
    results = [queue.get() for _ in procs]
    for p in procs:
        p.join()
    expected = ["value-of-" + k for k in KEYS]
    assert all(r == expected for r in results), results

    cache = HotCache(origin, ttl=0.5, jitter=0.0, stale_ttl=30.0, db_path=DB)
    stats = cache.stats()
    conn = sqlite3.connect(DB)
    calls = dict(conn.execute("SELECT key, n FROM origin_calls"))
    versions = dict(conn.execute("SELECT key, version FROM hotcache"))
    conn.close()
    assert sum(calls.values()) == len(KEYS) == stats["refreshes"], (calls, stats)
    assert set(versions.values()) == {1}, versions

    # SWR: expired but inside stale window -> immediate stale return, one bg refresh.
    time.sleep(0.6)
    t0 = time.monotonic()
    value = cache.get(KEYS[0])
    elapsed = time.monotonic() - t0
    assert value == expected[0] and elapsed < 0.2, (value, elapsed)
    for _ in range(3):
        cache.get(KEYS[0])
    deadline = time.time() + 5.0
    n_calls = 1
    while time.time() < deadline:
        conn = sqlite3.connect(DB)
        row = conn.execute("SELECT n FROM origin_calls WHERE key=?", (KEYS[0],)).fetchone()
        version = conn.execute("SELECT version FROM hotcache WHERE key=?", (KEYS[0],)).fetchone()[0]
        conn.close()
        n_calls = row[0]
        if n_calls == 2 and version == 2:
            break
        time.sleep(0.05)
    assert n_calls == 2 and version == 2, (n_calls, version)

    # Failure fallback: expired beyond stale window, origin down -> last good value.
    cache_blocking = HotCache(origin, ttl=0.5, jitter=0.0, stale_ttl=0.0, db_path=DB)
    open(FLAG, "w").close()
    time.sleep(0.6)
    assert cache_blocking.get(KEYS[1]) == "value-of-" + KEYS[1]
    assert cache_blocking.stats()["stale_served"] >= 1

    # No known value -> the exception must propagate.
    try:
        cache_blocking.get("never-seen")
    except Exception:
        pass
    else:
        raise AssertionError("expected exception for key with no known value")
    os.unlink(FLAG)

    print("OK: cross-process refreshes == keys (%d), version == 1, "
          "swr served stale immediately, failure fell back to last good value" % len(KEYS))
    shutil.rmtree(TMP, ignore_errors=True)


if __name__ == "__main__":
    main()
