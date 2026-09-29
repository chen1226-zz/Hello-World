import os
import random
import shutil
import signal
import struct
import sys
import tempfile
import time
import unittest

TASK_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, TASK_DIR)

from walstore import REC_HEADER, REC_MAGIC, WalStore, encode_record


class WalStoreTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="waltest-", dir=TASK_DIR)
        self.wal = os.path.join(self.dir, "wal.log")
        self.seg = os.path.join(self.dir, "segment.dat")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_put_get_and_clean_reopen(self):
        store = WalStore(self.dir)
        store.put("alpha", "1")
        store.put(b"beta", b"2")
        store.put("alpha", "3")
        store.close()
        store = WalStore(self.dir)
        self.assertEqual(store.get("alpha"), b"3")
        self.assertEqual(store.get(b"beta"), b"2")
        self.assertIsNone(store.get("missing"))
        store.close()

    def test_segment_flush_and_reopen(self):
        store = WalStore(self.dir, flush_every=5)
        for i in range(12):
            store.put(b"k%02d" % i, b"v%d" % i)
        self.assertTrue(os.path.exists(self.seg))
        store.close()
        store = WalStore(self.dir, flush_every=5)
        for i in range(12):
            self.assertEqual(store.get(b"k%02d" % i), b"v%d" % i)
        store.close()

    def test_crash_during_write(self):
        for _ in range(3):
            shutil.rmtree(self.dir, ignore_errors=True)
            rfd, wfd = os.pipe()
            pid = os.fork()
            if pid == 0:  # child: write and ack each fsync'd record
                os.close(rfd)
                store = WalStore(self.dir, flush_every=7)
                for i in range(60):
                    store.put(b"k%04d" % i, b"v%d-" % i + b"x" * (i % 13))
                    os.write(wfd, struct.pack("<i", i))
                os._exit(0)
            os.close(wfd)
            time.sleep(random.uniform(0.005, 0.05))
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            data = b""
            while True:
                chunk = os.read(rfd, 4096)
                if not chunk:
                    break
                data += chunk
            os.close(rfd)
            acked = [struct.unpack_from("<i", data, off)[0]
                     for off in range(0, len(data) - len(data) % 4, 4)]
            store = WalStore(self.dir, flush_every=7)
            for i in acked:  # every acked write must survive, untruncated
                self.assertEqual(store.get(b"k%04d" % i),
                                 b"v%d-" % i + b"x" * (i % 13))
            store.close()

    def test_torn_wal_tail(self):
        store = WalStore(self.dir)
        for i in range(5):
            store.put(b"k%d" % i, b"value-%d" % i)
        store.close()
        def reopen_and_check(good_size):
            store = WalStore(self.dir)
            for i in range(5):
                self.assertEqual(store.get(b"k%d" % i), b"value-%d" % i)
            self.assertIsNone(store.get(b"k9"))
            self.assertEqual(os.path.getsize(self.wal), good_size)
            store.put(b"after", b"torn")  # still writable after cleanup
            store.close()
            store = WalStore(self.dir)
            self.assertEqual(store.get(b"after"), b"torn")
            store.close()
            return os.path.getsize(self.wal)

        good_size = reopen_and_check(os.path.getsize(self.wal))
        with open(self.wal, "ab") as fh:  # partial header
            fh.write(REC_HEADER.pack(REC_MAGIC, 0, 99, 2, 100)[:10])
        good_size = reopen_and_check(good_size)
        rec = encode_record(99, b"k9", b"v" * 500)
        with open(self.wal, "ab") as fh:  # truncated payload
            fh.write(rec[:len(rec) - 100])
        good_size = reopen_and_check(good_size)
        bad = bytearray(encode_record(99, b"k9", b"corrupt"))
        bad[-1] ^= 0xFF
        with open(self.wal, "ab") as fh:  # crc mismatch
            fh.write(bytes(bad))
        reopen_and_check(good_size)

    def test_segment_wal_overlap(self):
        store = WalStore(self.dir, flush_every=10)
        for i in range(10):
            store.put(b"k%02d" % i, b"seg-%d" % i)  # 10th put flushes segment
        self.assertTrue(os.path.exists(self.seg))
        for i in range(10, 15):
            store.put(b"k%02d" % i, b"wal-%d" % i)
        # Simulate crash between segment rename and WAL reset: the WAL still
        # holds records whose seq is already covered by the segment.
        os.write(store.wal_fd, encode_record(7, b"k07", b"STALE"))
        os.fdatasync(store.wal_fd)
        store.close()
        store = WalStore(self.dir, flush_every=10)
        self.assertEqual(store.get(b"k07"), b"seg-7")  # segment wins, stale skipped
        for i in range(15):
            want = (b"seg-%d" % i) if i < 10 else (b"wal-%d" % i)
            self.assertEqual(store.get(b"k%02d" % i), want)
        store.put(b"k15", b"wal-15")  # seq continues, no reuse
        store.close()
        store = WalStore(self.dir, flush_every=10)
        self.assertEqual(store.get(b"k15"), b"wal-15")
        self.assertEqual(store.get(b"k07"), b"seg-7")
        store.close()

    def test_crash_during_recovery(self):
        store = WalStore(self.dir, flush_every=1000)
        for i in range(400):
            store.put(b"k%04d" % i, b"payload-%d" % i)
        store.close()
        with open(self.wal, "ab") as fh:  # torn tail to clean up during recovery
            fh.write(encode_record(9999, b"bad", b"x" * 300)[:40])
        for _ in range(4):  # kill children mid-recovery; recovery must be idempotent
            pid = os.fork()
            if pid == 0:
                WalStore(self.dir, flush_every=1000)
                os._exit(0)
            time.sleep(random.uniform(0.0, 0.01))
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
        store = WalStore(self.dir, flush_every=1000)
        for i in range(400):
            self.assertEqual(store.get(b"k%04d" % i), b"payload-%d" % i)
        self.assertIsNone(store.get(b"bad"))
        store.put(b"after", b"recovery")
        store.close()


if __name__ == "__main__":
    unittest.main()
