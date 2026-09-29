#!/usr/bin/env python3
"""Write-throughput benchmark: pre-fix (buggy) vs fixed vs brute-force fsync.

 buggy: original write path -- fsync BEFORE the write (wrong order), no CRC.
 fixed: walstore.WalStore   -- os.write + os.fdatasync(WAL) per put, nothing else.
 brute: naive fix           -- fsync WAL + segment + directory on EVERY put.
"""
import os
import shutil
import struct
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from walstore import WalStore

N = 2000
VALUE = b"v" * 100


class BuggyStore:
    """Pre-fix write path: fsync runs BEFORE the record is written."""

    def __init__(self, path):
        os.makedirs(path, exist_ok=True)
        self.fd = os.open(os.path.join(path, "wal.log"),
                          os.O_WRONLY | os.O_CREAT | os.O_APPEND)
        self.seq = 0

    def put(self, key, value):
        self.seq += 1
        os.fsync(self.fd)  # bug: syncs previous writes, not this record
        os.write(self.fd, struct.pack("<QII", self.seq, len(key), len(value))
                 + key + value)

    def close(self):
        os.close(self.fd)


class BruteStore(WalStore):
    """Naive fix: fsync every file on every write (for comparison only)."""

    def put(self, key, value):
        super().put(key, value)
        if os.path.exists(self.seg_path):
            fd = os.open(self.seg_path, os.O_RDONLY)
            os.fsync(fd)
            os.close(fd)
        dfd = os.open(self.dir, os.O_RDONLY)
        os.fsync(dfd)
        os.close(dfd)


def bench(cls, reps=5):
    samples = []
    for _ in range(reps):
        path = tempfile.mkdtemp(prefix="walbench-",
                                dir=os.path.dirname(os.path.abspath(__file__)))
        try:
            store = cls(path)
            start = time.perf_counter()
            for i in range(N):
                store.put(b"key%08d" % i, VALUE)
            store.close()
            samples.append(N / (time.perf_counter() - start))
        finally:
            shutil.rmtree(path, ignore_errors=True)
    return sorted(samples)[len(samples) // 2]  # median


def main():
    results = {name: bench(cls) for name, cls in
               (("buggy", BuggyStore), ("fixed", WalStore), ("brute", BruteStore))}
    for name, ops in results.items():
        print("%-6s %10.0f ops/s" % (name, ops))
    drop = (results["buggy"] - results["fixed"]) / results["buggy"] * 100
    print("fixed vs buggy: %+.1f%% throughput (limit: -20%%)" % -drop)
    print("brute vs buggy: %+.1f%% throughput" % ((results["brute"] - results["buggy"])
                                                  / results["buggy"] * 100))
    return 0 if drop <= 20 else 1


if __name__ == "__main__":
    sys.exit(main())
