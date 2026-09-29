"""lockd: lease-based distributed lock with fencing tokens (stdlib only).

Safety model
------------
LockServer is the single source of truth and only uses a monotonic
clock, so client-side clock drift cannot forge or extend a lease.
Clients renew their lease in the background and must treat a failed
renewal as losing the lock immediately.  Because a paused or
partitioned client cannot *know* it lost the lease, every downstream
write must carry the fencing token and be re-validated (LockServer.
valid) before it is applied.  Renewal alone cannot save a frozen
process; fencing alone makes slow-but-healthy work lose the lock
needlessly -- both are required.
"""

import threading
import time

__all__ = ["LockServer", "LeaseLock", "LeaseLost"]


class LeaseLost(Exception):
    """The lease is gone (expired, fenced out, or server unreachable)."""


class _Record:
    __slots__ = ("owner", "token", "expiry")

    def __init__(self, owner, token, expiry):
        self.owner = owner
        self.token = token
        self.expiry = expiry


class LockServer:
    """Lease registry.  All safety decisions happen here, monotonic clock only."""

    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._mu = threading.Lock()
        self._recs = {}
        self._counter = 0

    def acquire(self, name, owner, ttl):
        """Return a fencing token (>0), or 0 if the lease is currently held."""
        with self._mu:
            now = self._clock()
            rec = self._recs.get(name)
            if rec is not None and now < rec.expiry:
                return 0
            self._counter += 1
            self._recs[name] = _Record(owner, self._counter, now + ttl)
            return self._counter

    def renew(self, name, token, ttl):
        """Extend the lease; False if token is not the current live holder."""
        with self._mu:
            now = self._clock()
            rec = self._recs.get(name)
            if rec is None or rec.token != token or now >= rec.expiry:
                return False
            rec.expiry = now + ttl
            return True

    def release(self, name, token):
        with self._mu:
            rec = self._recs.get(name)
            if rec is None or rec.token != token:
                return False
            del self._recs[name]
            return True

    def valid(self, name, token):
        """Fencing check: downstream resources must pass this before writing."""
        with self._mu:
            rec = self._recs.get(name)
            return rec is not None and rec.token == token and self._clock() < rec.expiry


class LeaseLock:
    """Client-side handle with a background renewer.

    Usage: acquire(), do the work, call check() before any externally
    visible side effect, pass `token` to fenced downstream writes, then
    release().  Poll `lost` to abort early when the lease is gone.
    """

    RENEW_DIVISOR = 3  # renew every ttl/3

    def __init__(self, server, name, owner, ttl, sleep=time.sleep):
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        self.server = server
        self.name = name
        self.owner = owner
        self.ttl = ttl
        self.token = 0
        self.before_renew = lambda: None  # fault-injection hook (chaos only)
        self._sleep = sleep
        self._lost = threading.Event()
        self._stop = threading.Event()
        self._thread = None

    def acquire(self, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            try:
                token = self.server.acquire(self.name, self.owner, self.ttl)
            except Exception:
                token = 0  # e.g. network partition: keep retrying
            if token:
                self.token = token
                self._lost.clear()
                self._stop.clear()
                self._thread = threading.Thread(target=self._renew_loop, daemon=True)
                self._thread.start()
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            self._sleep(min(self.ttl / 2, 0.05))

    def _renew_loop(self):
        interval = self.ttl / self.RENEW_DIVISOR
        while not self._stop.wait(interval):
            try:
                self.before_renew()
                ok = self.server.renew(self.name, self.token, self.ttl)
            except Exception:
                ok = False  # partition / server down: we may be fenced out
            if not ok:
                self._lost.set()
                return

    @property
    def lost(self):
        return self._lost.is_set()

    def check(self):
        """Raise LeaseLost unless we still hold the lease.  Call before side effects."""
        try:
            ok = self.server.valid(self.name, self.token)
        except Exception:
            ok = False
        if self._lost.is_set() or not ok:
            self._lost.set()
            raise LeaseLost("lease lost: %s (owner=%s token=%s)"
                            % (self.name, self.owner, self.token))

    def release(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None
        if self.token:
            try:
                self.server.release(self.name, self.token)
            except Exception:
                pass
            self.token = 0

    def __enter__(self):
        if not self.acquire():
            raise LeaseLost("could not acquire: %s" % self.name)
        return self

    def __exit__(self, *exc):
        self.release()
