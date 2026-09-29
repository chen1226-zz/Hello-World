"""Append-only key-value store: WAL + periodic segment compaction + crash recovery.

Persistence order (diagrams in README.md):
  put:   encode record -> os.write(WAL) -> os.fdatasync(WAL) -> update index -> ack
  flush: write segment.tmp -> os.fsync -> os.replace -> os.fsync(dir)
         -> reset WAL -> os.fsync(dir)
Recovery is idempotent: it only reads the segment, replays the WAL, and
truncates a torn/corrupt WAL tail, so a crash *during* recovery is harmless.
"""
import os
import struct
import threading
import zlib

REC_MAGIC = b"WSR1"
SEG_MAGIC = b"WSS1"
REC_HEADER = struct.Struct("<4sIQII")  # magic, crc32, seq, key_len, value_len
SEG_HEADER = struct.Struct("<4sIQ")    # magic, crc32, max_seq
KV_HEADER = struct.Struct("<II")       # key_len, value_len


def _as_bytes(obj):
    return obj.encode() if isinstance(obj, str) else bytes(obj)


def encode_record(seq, key, value):
    body = struct.pack("<QII", seq, len(key), len(value)) + key + value
    return REC_HEADER.pack(REC_MAGIC, zlib.crc32(body), seq, len(key),
                           len(value)) + key + value


def _write_all(fd, data):
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class WalStore:
    def __init__(self, path, flush_every=256):
        self.dir = path
        self.flush_every = flush_every
        self.wal_path = os.path.join(path, "wal.log")
        self.seg_path = os.path.join(path, "segment.dat")
        self.index = {}
        self.seq = 0
        self._since_flush = 0
        self._lock = threading.Lock()
        self.wal_fd = None
        os.makedirs(path, exist_ok=True)
        self._recover()
        self.wal_fd = os.open(self.wal_path,
                              os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)

    # ---------------- write path ----------------
    def put(self, key, value):
        key, value = _as_bytes(key), _as_bytes(value)
        with self._lock:
            self.seq += 1
            _write_all(self.wal_fd, encode_record(self.seq, key, value))
            os.fdatasync(self.wal_fd)  # record is durable BEFORE put() returns
            self.index[key] = value
            self._since_flush += 1
            if self._since_flush >= self.flush_every:
                self._flush_segment()

    def get(self, key, default=None):
        return self.index.get(_as_bytes(key), default)

    def __len__(self):
        return len(self.index)

    def _flush_segment(self):
        payload = b"".join(KV_HEADER.pack(len(k), len(v)) + k + v
                              for k, v in self.index.items())
        blob = SEG_HEADER.pack(SEG_MAGIC, zlib.crc32(payload), self.seq) + payload
        tmp = self.seg_path + ".tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            _write_all(fd, blob)
            os.fsync(fd)                 # 1. segment content durable
        finally:
            os.close(fd)
        os.replace(tmp, self.seg_path)   # 2. atomic rename ...
        _fsync_dir(self.dir)             #    ... made durable
        os.close(self.wal_fd)            # 3. only now may the WAL be reset
        os.unlink(self.wal_path)
        _fsync_dir(self.dir)
        self.wal_fd = os.open(self.wal_path,
                              os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        self._since_flush = 0

    def close(self):
        with self._lock:
            if self.wal_fd is not None:
                os.fdatasync(self.wal_fd)
                os.close(self.wal_fd)
                self.wal_fd = None

    # ---------------- recovery path ----------------
    def _recover(self):
        tmp = self.seg_path + ".tmp"
        if os.path.exists(tmp):
            os.unlink(tmp)  # leftover of a flush interrupted before rename
        self._load_segment()
        if not os.path.exists(self.wal_path):
            return
        with open(self.wal_path, "rb") as fh:
            data = fh.read()
        off = 0
        while off + REC_HEADER.size <= len(data):
            magic, crc, seq, klen, vlen = REC_HEADER.unpack_from(data, off)
            end = off + REC_HEADER.size + klen + vlen
            if magic != REC_MAGIC or end > len(data):
                break  # torn header or truncated payload: stop here
            body = data[off + 8:off + REC_HEADER.size] + data[off + REC_HEADER.size:end]
            if zlib.crc32(body) != crc:
                break  # corrupt record: stop here
            key = data[off + REC_HEADER.size:off + REC_HEADER.size + klen]
            if seq > self.seq:  # skip records already captured by the segment
                self.index[key] = data[end - vlen:end]
                self.seq = seq
            off = end
        if off < len(data):
            fd = os.open(self.wal_path, os.O_WRONLY)
            try:
                os.ftruncate(fd, off)  # drop torn/corrupt tail
                os.fsync(fd)
            finally:
                os.close(fd)

    def _load_segment(self):
        if not os.path.exists(self.seg_path):
            return
        with open(self.seg_path, "rb") as fh:
            data = fh.read()
        if len(data) < SEG_HEADER.size:
            raise ValueError("segment file too short")
        magic, crc, max_seq = SEG_HEADER.unpack_from(data, 0)
        payload = data[SEG_HEADER.size:]
        if magic != SEG_MAGIC or zlib.crc32(payload) != crc:
            raise ValueError("segment file corrupt")
        off = 0
        while off < len(payload):
            klen, vlen = KV_HEADER.unpack_from(payload, off)
            off += KV_HEADER.size
            self.index[payload[off:off + klen]] = payload[off + klen:off + klen + vlen]
            off += klen + vlen
        self.seq = max_seq
