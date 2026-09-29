"""Multi-key token-bucket rate limiter.

Thread-safe and based on a monotonic clock. See README.md for the
root-cause analysis of the concurrency and clock bugs this fixes.
"""

from __future__ import annotations

import threading
import time

__all__ = ["RateBucket", "BucketState"]


class BucketState:
    """Per-key mutable bucket state (all timestamps are monotonic)."""

    __slots__ = ("tokens", "updated", "last_seen")

    def __init__(self, tokens: float, now: float) -> None:
        self.tokens = tokens      # currently available tokens
        self.updated = now        # last refill timestamp
        self.last_seen = now      # last access timestamp (for idle TTL)


class RateBucket:
    """Multi-key token bucket limiter.

    rate:     tokens refilled per second, per key.
    burst:    maximum tokens a bucket can hold, per key.
    idle_ttl: seconds after which an untouched bucket is reset to full
              and becomes eligible for reclamation (None disables it).
    clock:    monotonic clock, injectable for tests. Must never move
              backwards in production; the default time.monotonic
              guarantees that. All internal math is nevertheless
              hardened against backwards steps.
    """

    def __init__(self, rate, burst, idle_ttl=None, clock=time.monotonic):
        if rate <= 0:
            raise ValueError("rate must be positive")
        if burst <= 0:
            raise ValueError("burst must be positive")
        self.rate = float(rate)
        self.burst = float(burst)
        self.idle_ttl = None if idle_ttl is None else float(idle_ttl)
        self._clock = clock
        # One lock serializes the whole read-refill-update critical
        # section, so concurrent threads can never double-spend tokens.
        self._lock = threading.Lock()
        self._buckets = {}

    @staticmethod
    def _elapsed(now: float, since: float) -> float:
        # Never let a backwards clock step produce negative elapsed time.
        return now - since if now > since else 0.0

    def _get(self, key, now: float) -> BucketState:
        st = self._buckets.get(key)
        if st is None:
            st = BucketState(self.burst, now)
            self._buckets[key] = st
            return st
        if self.idle_ttl is not None and self._elapsed(now, st.last_seen) >= self.idle_ttl:
            # Idle too long: reset to a fresh, full bucket.
            st.tokens = self.burst
            st.updated = now
        return st

    def _refill(self, st: BucketState, now: float) -> None:
        # Timestamps only ever move forwards: if the clock stepped back
        # we keep the old timestamp, so a later recovery cannot inflate
        # the elapsed window and over-refill the bucket.
        if now > st.updated:
            st.tokens = min(self.burst, st.tokens + (now - st.updated) * self.rate)
            st.updated = now

    @staticmethod
    def _touch(st: BucketState, now: float) -> None:
        if now > st.last_seen:
            st.last_seen = now

    def allow(self, key, cost: float = 1.0) -> bool:
        """Try to spend `cost` tokens for `key`. Atomic under the lock."""
        now = self._clock()
        with self._lock:
            st = self._get(key, now)
            self._refill(st, now)
            self._touch(st, now)
            if st.tokens >= cost:
                st.tokens -= cost
                return True
            return False

    def retry_after(self, key, cost: float = 1.0) -> float:
        """Seconds until `cost` tokens are available (0.0 if available now)."""
        now = self._clock()
        with self._lock:
            st = self._get(key, now)
            self._refill(st, now)
            self._touch(st, now)
            if st.tokens >= cost:
                return 0.0
            return (cost - st.tokens) / self.rate

    def reclaim_idle(self) -> int:
        """Drop buckets idle for at least idle_ttl. Returns removed count."""
        if self.idle_ttl is None:
            return 0
        now = self._clock()
        with self._lock:
            doomed = [
                key for key, st in self._buckets.items()
                if self._elapsed(now, st.last_seen) >= self.idle_ttl
            ]
            for key in doomed:
                del self._buckets[key]
            return len(doomed)

    def __len__(self) -> int:
        with self._lock:
            return len(self._buckets)
